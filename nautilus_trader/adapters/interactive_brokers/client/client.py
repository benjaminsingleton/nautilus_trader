# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

import asyncio
import enum
import os
import weakref
from collections.abc import Callable
from collections.abc import Coroutine
from contextlib import suppress
from inspect import iscoroutinefunction
from typing import Any

from ibapi import comm
from ibapi.client import EClient
from ibapi.commission_report import CommissionReport
from ibapi.common import MAX_MSG_LEN
from ibapi.common import NO_VALID_ID
from ibapi.common import BarData
from ibapi.errors import BAD_LENGTH
from ibapi.execution import Execution
from ibapi.utils import current_fn_name

# fmt: off
from nautilus_trader.adapters.interactive_brokers.client.account import InteractiveBrokersClientAccountMixin
from nautilus_trader.adapters.interactive_brokers.client.common import AccountOrderRef
from nautilus_trader.adapters.interactive_brokers.client.common import Request
from nautilus_trader.adapters.interactive_brokers.client.common import Requests
from nautilus_trader.adapters.interactive_brokers.client.common import Subscriptions
from nautilus_trader.adapters.interactive_brokers.client.connection import InteractiveBrokersClientConnectionMixin
from nautilus_trader.adapters.interactive_brokers.client.contract import InteractiveBrokersClientContractMixin
from nautilus_trader.adapters.interactive_brokers.client.error import InteractiveBrokersClientErrorMixin
from nautilus_trader.adapters.interactive_brokers.client.error import handle_ib_error
from nautilus_trader.adapters.interactive_brokers.client.market_data import InteractiveBrokersClientMarketDataMixin
from nautilus_trader.adapters.interactive_brokers.client.order import InteractiveBrokersClientOrderMixin
from nautilus_trader.adapters.interactive_brokers.client.wrapper import InteractiveBrokersEWrapper
from nautilus_trader.adapters.interactive_brokers.common import IB_VENUE
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import Component
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.model.identifiers import ClientId


# fmt: on


class ClientState(enum.Enum):
    """
    Defines the possible states of the Interactive Brokers client.

    These states are used to track and manage the client lifecycle.

    """

    CREATED = 0  # Initial state after creation
    CONNECTING = 1  # Attempting to connect to TWS/Gateway
    CONNECTED = 2  # Successfully connected to TWS/Gateway
    STARTING = 3  # Starting message processing and other services
    WAITING_API = 4  # Waiting for API initialization (account info, etc.)
    READY = 5  # Fully operational and ready for requests
    DEGRADED = 6  # Connected but with limited functionality
    RECONNECTING = 7  # Attempting to reconnect after failure
    STOPPING = 8  # In the process of shutting down
    STOPPED = 9  # Fully stopped
    DISPOSED = 10  # Resources released, object no longer usable


class StateMachine:
    """
    State machine for managing the client lifecycle.

    This implements a state machine pattern to enforce correct state transitions and
    validate operations in different states.

    """

    def __init__(self, initial_state: ClientState, logger):
        self._current_state = initial_state
        self._log = logger
        self._state_changed_callbacks: dict[ClientState, list] = {}

    @property
    def current_state(self) -> ClientState:
        """
        The current state of the client.
        """
        return self._current_state

    def register_state_changed_callback(self, state: ClientState, callback: Callable) -> None:
        """
        Register a callback to be called when the state changes to the specified state.

        Parameters
        ----------
        state : ClientState
            The state to watch for.
        callback : Callable
            The callback to call when the state changes to the specified state.

        """
        if state not in self._state_changed_callbacks:
            self._state_changed_callbacks[state] = []

        self._state_changed_callbacks[state].append(callback)

    def transition_to(self, new_state: ClientState) -> bool:
        """
        Transition to a new state if the transition is valid.

        Parameters
        ----------
        new_state : ClientState
            The new state to transition to.

        Returns
        -------
        bool
            True if the transition was successful, False otherwise.

        """
        if not self._is_valid_transition(self._current_state, new_state):
            self._log.warning(
                f"Invalid state transition attempted: {self._current_state} -> {new_state}",
            )
            return False

        old_state = self._current_state
        self._current_state = new_state
        self._log.debug(f"State transition: {old_state} -> {new_state}")

        # Call registered callbacks for this state
        if new_state in self._state_changed_callbacks:
            for callback in self._state_changed_callbacks[new_state]:
                try:
                    callback()
                except Exception as e:
                    self._log.exception(f"Error in state change callback: {e}")

        return True

    def _is_valid_transition(self, from_state: ClientState, to_state: ClientState) -> bool:
        """
        Check if a state transition is valid.

        Parameters
        ----------
        from_state : ClientState
            The current state.
        to_state : ClientState
            The target state.

        Returns
        -------
        bool
            True if the transition is valid, False otherwise.

        """
        # Define valid state transitions
        valid_transitions = {
            ClientState.CREATED: {ClientState.CONNECTING, ClientState.STOPPING},
            ClientState.CONNECTING: {
                ClientState.CONNECTED,
                ClientState.RECONNECTING,
                ClientState.STOPPING,
            },
            ClientState.CONNECTED: {
                ClientState.STARTING,
                ClientState.DEGRADED,
                ClientState.RECONNECTING,
                ClientState.STOPPING,
            },
            ClientState.STARTING: {
                ClientState.WAITING_API,
                ClientState.DEGRADED,
                ClientState.RECONNECTING,
                ClientState.STOPPING,
            },
            ClientState.WAITING_API: {
                ClientState.READY,
                ClientState.DEGRADED,
                ClientState.RECONNECTING,
                ClientState.STOPPING,
            },
            ClientState.READY: {
                ClientState.DEGRADED,
                ClientState.RECONNECTING,
                ClientState.STOPPING,
            },
            ClientState.DEGRADED: {
                ClientState.RECONNECTING,
                ClientState.READY,
                ClientState.STOPPING,
            },
            ClientState.RECONNECTING: {ClientState.CONNECTING, ClientState.STOPPING},
            ClientState.STOPPING: {ClientState.STOPPED},
            ClientState.STOPPED: {ClientState.DISPOSED, ClientState.CONNECTING},
            ClientState.DISPOSED: set(),  # No valid transitions from DISPOSED
        }

        # Check if the transition is valid
        return to_state in valid_transitions.get(from_state, set())


class TaskRegistry:
    """
    Registry for managing asynchronous tasks.

    This class provides centralized tracking and management of asyncio tasks, ensuring
    proper cleanup and preventing resource leaks.

    """

    def __init__(self, logger):
        self._tasks: dict[str, asyncio.Task] = {}
        self._log = logger

    def create_task(
        self,
        coro: Coroutine,
        name: str,
        on_done: Callable | None = None,
    ) -> asyncio.Task:
        """
        Create and register a new task.

        Parameters
        ----------
        coro : Coroutine
            The coroutine to run as a task.
        name : str
            A unique name for the task.
        on_done : Callable, optional
            A callback to run when the task completes.

        Returns
        -------
        asyncio.Task
            The created task.

        """
        # Cancel existing task with the same name if it exists
        self.cancel_task(name)

        # Create a new task
        task = asyncio.create_task(coro, name=name)

        # Store the task
        self._tasks[name] = task

        # Add completion callback
        task.add_done_callback(lambda t: self._on_task_done(t, name, on_done))

        self._log.debug(f"Task created: {name}")
        return task

    def cancel_task(self, name: str) -> bool:
        """
        Cancel a task by name.

        Parameters
        ----------
        name : str
            The name of the task to cancel.

        Returns
        -------
        bool
            True if a task was found and cancelled, False otherwise.

        """
        if name in self._tasks:
            task = self._tasks[name]
            if not task.done():
                task.cancel()
                self._log.debug(f"Task cancelled: {name}")
            return True
        return False

    def cancel_all_tasks(self) -> None:
        """
        Cancel all registered tasks.
        """
        for name in list(self._tasks.keys()):
            self.cancel_task(name)

    def _on_task_done(self, task: asyncio.Task, name: str, on_done: Callable | None) -> None:
        """
        Handle task completion.

        Parameters
        ----------
        task : asyncio.Task
            The completed task.
        name : str
            The name of the task.
        on_done : Callable, optional
            A callback to run when the task completes.

        """
        # Remove the task from the registry
        self._tasks.pop(name, None)

        # Check for exceptions
        if not task.cancelled():
            exception = task.exception()
            if exception:
                self._log.error(f"Task {name} failed with exception: {exception}")
            elif on_done:
                try:
                    on_done()
                except Exception as e:
                    self._log.error(f"Error in task completion callback: {e}")

        self._log.debug(f"Task completed: {name}")

    def get_task(self, name: str) -> asyncio.Task | None:
        """
        Get a task by name.

        Parameters
        ----------
        name : str
            The name of the task to get.

        Returns
        -------
        asyncio.Task or None
            The task if found, None otherwise.

        """
        return self._tasks.get(name)

    def is_running(self, name: str) -> bool:
        """
        Check if a task is currently running.

        Parameters
        ----------
        name : str
            The name of the task to check.

        Returns
        -------
        bool
            True if the task exists and is not done, False otherwise.

        """
        task = self.get_task(name)
        return task is not None and not task.done()


class InteractiveBrokersClient(
    Component,
    InteractiveBrokersClientConnectionMixin,
    InteractiveBrokersClientAccountMixin,
    InteractiveBrokersClientMarketDataMixin,
    InteractiveBrokersClientOrderMixin,
    InteractiveBrokersClientContractMixin,
    InteractiveBrokersClientErrorMixin,
):
    """
    A client component that interfaces with the Interactive Brokers TWS or Gateway.

    This class integrates various mixins to provide functionality for connection
    management, account management, market data, and order processing with
    Interactive Brokers. It inherits from both `Component` and various mixins to provide
    event-driven responses and custom component behavior.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    msgbus : MessageBus
        The message bus for the client.
    cache : Cache
        The cache for the client.
    clock : LiveClock
        The clock for the client.
    host : str, default "127.0.0.1"
        The hostname or IP address of the TWS or Gateway.
    port : int, default 7497
        The port number of the TWS or Gateway.
    client_id : int, default 1
        The client ID for the connection.

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        host: str = "127.0.0.1",
        port: int = 7497,
        client_id: int = 1,
    ) -> None:
        super().__init__(
            clock=clock,
            component_id=ClientId(f"{IB_VENUE.value}-{client_id:03d}"),
            component_name=f"{type(self).__name__}-{client_id:03d}",
            msgbus=msgbus,
        )

        # Config
        self._loop = loop
        self._cache = cache
        self._host = host
        self._port = port
        self._client_id = client_id

        # State management
        self._state_machine = StateMachine(ClientState.CREATED, self._log)

        # Register state change callbacks
        self._state_machine.register_state_changed_callback(
            ClientState.READY,
            lambda: self._is_client_ready.set(),
        )
        self._state_machine.register_state_changed_callback(
            ClientState.STOPPING,
            lambda: self._is_client_ready.clear(),
        )

        # TWS API
        self._eclient: EClient = EClient(
            wrapper=InteractiveBrokersEWrapper(
                nautilus_logger=self._log,
                client=self,
            ),
        )

        # EClient Overrides
        self._eclient.sendMsg = self.sendMsg
        self._eclient.logRequest = self.logRequest

        # Task management
        self._task_registry = TaskRegistry(self._log)
        self._internal_msg_queue: asyncio.Queue = asyncio.Queue()
        self._msg_handler_task_queue: asyncio.Queue = asyncio.Queue()

        # Event flags
        self._is_client_ready: asyncio.Event = asyncio.Event()
        self._is_ib_connected: asyncio.Event = asyncio.Event()

        # Hot caches
        self.registered_nautilus_clients: set = set()
        self._event_subscriptions: dict[str, Callable] = {}

        # Subscriptions
        self._requests = Requests()
        self._subscriptions = Subscriptions()

        # AccountMixin
        self._account_ids: set[str] = set()

        # ConnectionMixin
        self._connection_attempts: int = 0
        self._max_connection_attempts: int = int(os.getenv("IB_MAX_CONNECTION_ATTEMPTS", "0"))
        self._indefinite_reconnect: bool = False if self._max_connection_attempts else True
        self._reconnect_delay: int = int(os.getenv("IB_RECONNECT_DELAY", "5"))  # seconds
        self._last_heartbeat_time: float = 0
        self._heartbeat_interval: float = float(os.getenv("IB_HEARTBEAT_INTERVAL", "30"))  # seconds

        # MarketDataMixin
        self._bar_type_to_last_bar: dict[str, BarData | None] = {}

        # OrderMixin
        self._exec_id_details: dict[
            str,
            dict[str, Execution | CommissionReport | str],
        ] = {}
        self._order_id_to_order_ref: dict[int, AccountOrderRef] = {}
        self._next_valid_order_id: int = -1

        # Start client
        self._request_id_seq: int = 10000

        # Use weak references to allow proper garbage collection
        self._weakref = weakref.ref(self)

    @property
    def state(self) -> ClientState:
        """
        The current state of the client.
        """
        return self._state_machine.current_state

    def is_in_state(self, *states: ClientState) -> bool:
        """
        Check if the client is in one of the specified states.

        Parameters
        ----------
        *states : ClientState
            The states to check against.

        Returns
        -------
        bool
            True if the client is in one of the specified states, False otherwise.

        """
        return self.state in states

    def _start(self) -> None:
        """
        Start the client.

        This method is called when the client is first initialized and when the client
        is reset. It sets up the client and starts the connection sequence.

        """
        if not self._loop.is_running():
            self._log.warning("Started when loop is not running")
            self._loop.run_until_complete(self._start_async())
        else:
            self._task_registry.create_task(
                self._start_async(),
                "client_start",
            )

    @handle_ib_error
    async def _start_async(self) -> None:
        """
        Start the client asynchronously using a state machine approach.

        This method manages the client startup sequence, handling connection attempts,
        service initialization, and API startup. It uses a state machine to track and
        manage the client's progress through startup.

        """
        self._log.info(f"Starting InteractiveBrokersClient ({self._client_id})...")

        # Initialize state to CONNECTING
        if not self._state_machine.transition_to(ClientState.CONNECTING):
            self._log.error(f"Failed to transition to CONNECTING state from {self.state}")
            return

        while not self.is_in_state(ClientState.READY, ClientState.STOPPED, ClientState.DISPOSED):
            try:
                if self.is_in_state(ClientState.CONNECTING):
                    await self._handle_connecting_state()

                elif self.is_in_state(ClientState.CONNECTED):
                    self._handle_connected_state()

                elif self.is_in_state(ClientState.WAITING_API):
                    await self._handle_waiting_api_state()

            except TimeoutError:
                self._log.error("Client failed to initialize. Connection timeout.")
                # Ensure we retry connection
                self._state_machine.transition_to(ClientState.CONNECTING)
            except Exception as e:
                self._log.exception("Unhandled exception in client startup", e)
                self._state_machine.transition_to(ClientState.STOPPING)
                await self._stop_async()

        if self.is_in_state(ClientState.READY):
            self._log.debug("`_is_client_ready` set by `_start_async`.", LogColor.BLUE)
            self._connection_attempts = 0
        else:
            self._log.warning(f"Client startup ended in state {self.state}")

    @handle_ib_error
    async def _handle_connecting_state(self) -> None:
        """
        Handle the CONNECTING state of the client startup process.

        Manages connection attempt limits, reconnection delay, and establishing the
        socket connection.

        """
        # Check connection attempt limits
        self._connection_attempts += 1
        if (
            not self._indefinite_reconnect
            and self._connection_attempts > self._max_connection_attempts
        ):
            self._log.error(
                f"Max connection attempts ({self._max_connection_attempts}) reached, connection failed",
            )
            self._state_machine.transition_to(ClientState.STOPPING)
            await self._stop_async()
            return

        # Apply reconnection delay if needed
        if self._connection_attempts > 1:
            self._log.info(
                f"Attempt {self._connection_attempts}: attempting to reconnect "
                f"in {self._reconnect_delay} seconds...",
            )
            await asyncio.sleep(self._reconnect_delay)

        # Establish socket connection
        try:
            await self._connect()
            self._state_machine.transition_to(ClientState.CONNECTED)
        except Exception as e:
            self._log.error(f"Connection failed: {e}")
            # Don't change state, we'll retry in the next iteration

    def _handle_connected_state(self) -> None:
        """
        Handle the CONNECTED state of the client startup process.

        Starts all required services and initializes the API connection.

        """
        # Start all required services
        self._start_tws_incoming_msg_reader()
        self._start_internal_msg_queue_processor()
        self._eclient.startApi()
        self._state_machine.transition_to(ClientState.WAITING_API)

    @handle_ib_error
    async def _handle_waiting_api_state(self) -> None:
        """
        Handle the WAITING_API state of the client startup process.

        Waits for managed accounts message and completes the initialization.

        """
        # Wait for managed accounts message
        try:
            await asyncio.wait_for(self._is_ib_connected.wait(), 15)
            self._start_connection_watchdog()
            self._start_heartbeat_monitor()
            self._state_machine.transition_to(ClientState.READY)
        except TimeoutError:
            self._log.error("Timeout waiting for managed accounts message")
            # Return to connecting state for retry
            self._state_machine.transition_to(ClientState.CONNECTING)

    def _start_tws_incoming_msg_reader(self) -> None:
        """
        Start the incoming message reader task.

        This task reads messages from the TWS socket and places them in the internal
        message queue for processing.

        """
        self._task_registry.create_task(
            self._run_tws_incoming_msg_reader(),
            "tws_incoming_msg_reader",
        )

    def _start_internal_msg_queue_processor(self) -> None:
        """
        Start the internal message queue processing task.

        This task processes messages from the internal queue and dispatches them to the
        appropriate handlers.

        """
        self._task_registry.create_task(
            self._run_internal_msg_queue_processor(),
            "internal_msg_queue_processor",
        )

        self._task_registry.create_task(
            self._run_msg_handler_processor(),
            "msg_handler_processor",
        )

    def _start_connection_watchdog(self) -> None:
        """
        Start the connection watchdog task.

        This task monitors the connection to TWS/Gateway and initiates reconnection if
        the connection is lost.

        """
        self._task_registry.create_task(
            self._run_connection_watchdog(),
            "connection_watchdog",
        )

    def _start_heartbeat_monitor(self) -> None:
        """
        Start the heartbeat monitor task.

        This task sends periodic heartbeat messages to TWS/Gateway to detect connection
        issues even when there's no other activity.

        """
        self._task_registry.create_task(
            self._run_heartbeat_monitor(),
            "heartbeat_monitor",
        )

    def _stop(self) -> None:
        """
        Stop the client and cancel running tasks.

        This method initiates the client shutdown process.

        """
        self._state_machine.transition_to(ClientState.STOPPING)
        self._task_registry.create_task(self._stop_async(), "client_stop")

    @handle_ib_error
    async def _stop_async(self) -> None:
        """
        Stop the client asynchronously, cancelling all tasks and releasing resources.

        This method ensures a clean shutdown of the client, cancelling all tasks and
        cleaning up resources.

        """
        self._log.info(f"Stopping InteractiveBrokersClient ({self._client_id})...")

        if self._is_client_ready.is_set():
            self._is_client_ready.clear()
            self._log.debug("`_is_client_ready` unset by `_stop_async`.", LogColor.BLUE)

        # Cancel all tasks
        self._task_registry.cancel_all_tasks()

        # Disconnect from TWS/Gateway
        with suppress(Exception):
            self._eclient.disconnect()

        # Clear state
        self._account_ids = set()
        self.registered_nautilus_clients = set()
        self._state_machine.transition_to(ClientState.STOPPED)

    def _reset(self) -> None:
        """
        Restart the client.

        This method stops and then restarts the client.

        """
        self._task_registry.create_task(self._reset_async(), "client_reset")

    @handle_ib_error
    async def _reset_async(self) -> None:
        """
        Restart the client asynchronously.

        This method ensures a clean shutdown before restarting the client.

        """
        self._log.info(f"Resetting InteractiveBrokersClient ({self._client_id})...")
        await self._stop_async()
        await self._start_async()

    def _resume(self) -> None:
        """
        Resume the client and resubscribe to all subscriptions.

        This method is called after a reconnection to restore subscriptions.

        """
        self._task_registry.create_task(self._resume_async(), "client_resume")

    @handle_ib_error
    async def _resume_async(self) -> None:
        """
        Resume the client asynchronously, resubscribing to all subscriptions.

        This method waits for the client to be ready before attempting to resubscribe.

        """
        await self._is_client_ready.wait()
        self._log.info(f"Resuming InteractiveBrokersClient ({self._client_id})...")
        await self._resubscribe_all()

    def _degrade(self) -> None:
        """
        Degrade the client when connectivity is lost.

        This method puts the client into a degraded state, clearing certain state
        information but not fully stopping the client.

        """
        if not self.is_in_state(ClientState.DEGRADED):
            self._log.info(f"Degrading InteractiveBrokersClient ({self._client_id})...")
            self._is_client_ready.clear()
            self._account_ids = set()
            self._state_machine.transition_to(ClientState.DEGRADED)

    @handle_ib_error
    async def _resubscribe_all(self) -> None:
        """
        Cancel and restart all subscriptions.

        This method is called after a reconnection to restore all active subscriptions.

        """
        subscriptions = self._subscriptions.get_all()
        if not subscriptions:
            self._log.info("No subscriptions to resubscribe")
            return

        subscription_names = ", ".join([str(subscription.name) for subscription in subscriptions])
        self._log.info(f"Resubscribing to {len(subscriptions)} subscriptions: {subscription_names}")

        for subscription in subscriptions:
            self._log.info(f"Resubscribing to {subscription.name} subscription...")
            try:
                if iscoroutinefunction(subscription.handle):
                    await subscription.handle()
                else:
                    await asyncio.to_thread(subscription.handle)
            except Exception as e:
                self._log.exception(f"Failed to resubscribe to {subscription}", e)

    async def wait_until_ready(self, timeout: int = 300) -> None:
        """
        Wait until the client is running and ready within a given timeout.

        Parameters
        ----------
        timeout : int, default 300
            Time in seconds to wait for the client to be ready.

        Raises
        ------
        TimeoutError
            If the client does not become ready within the timeout period.

        """
        try:
            if not self._is_client_ready.is_set():
                await asyncio.wait_for(self._is_client_ready.wait(), timeout)
        except TimeoutError as e:
            self._log.error(f"Client is not ready after {timeout} seconds.")
            raise TimeoutError(f"Client is not ready after {timeout} seconds.") from e

    @handle_ib_error
    async def _run_heartbeat_monitor(self) -> None:
        """
        Run a heartbeat monitor to periodically check connection health.

        Sends a lightweight request to TWS/Gateway periodically to ensure the connection
        is still alive and responsive.

        """
        try:
            while not self.is_in_state(
                ClientState.STOPPING,
                ClientState.STOPPED,
                ClientState.DISPOSED,
            ):
                await asyncio.sleep(self._heartbeat_interval)

                if self.is_in_state(ClientState.READY):
                    try:
                        # Use reqCurrentTime as a lightweight heartbeat
                        self._eclient.reqCurrentTime()
                        self._last_heartbeat_time = self._clock.timestamp()
                    except Exception as e:
                        self._log.warning(f"Heartbeat check failed: {e}")
                        if self._is_ib_connected.is_set():
                            self._log.debug(
                                "`_is_ib_connected` unset by heartbeat failure",
                                LogColor.BLUE,
                            )
                            self._is_ib_connected.clear()
                            await self._handle_disconnection()
        except asyncio.CancelledError:
            self._log.debug("Heartbeat monitor task was canceled.")
        except Exception as e:
            self._log.exception(f"Error in heartbeat monitor: {e}")

    @handle_ib_error
    async def _run_connection_watchdog(self) -> None:
        """
        Run a watchdog to monitor and manage the health of the socket connection.

        Continuously checks the connection status, manages client state based on
        connection health, and handles reconnection in case of network failure.

        """
        try:
            while not self.is_in_state(
                ClientState.STOPPING,
                ClientState.STOPPED,
                ClientState.DISPOSED,
            ):
                await asyncio.sleep(1)

                if not self._is_ib_connected.is_set() or not self._eclient.isConnected():
                    self._log.error("Connection watchdog detects connection lost.")
                    await self._handle_disconnection()
                elif self.is_in_state(ClientState.READY):
                    # Check for heartbeat timeout if in READY state
                    current_time = self._clock.timestamp()
                    if (
                        self._last_heartbeat_time > 0
                        and current_time - self._last_heartbeat_time > self._heartbeat_interval * 2
                    ):
                        self._log.error(
                            f"Connection watchdog detected heartbeat timeout. Last heartbeat: "
                            f"{current_time - self._last_heartbeat_time:.1f}s ago",
                        )
                        await self._handle_disconnection()
        except asyncio.CancelledError:
            self._log.debug("Client connection watchdog task was canceled.")
        except Exception as e:
            self._log.exception(f"Error in connection watchdog: {e}")

    async def _handle_disconnection(self) -> None:
        """
        Handle the disconnection of the client from TWS/Gateway.

        This method degrades the client state and initiates reconnection after a delay.

        """
        if self.is_in_state(ClientState.READY, ClientState.CONNECTED, ClientState.WAITING_API):
            self._degrade()

        if self._is_ib_connected.is_set():
            self._log.debug("`_is_ib_connected` unset by `_handle_disconnection`.", LogColor.BLUE)
            self._is_ib_connected.clear()

        # Add a delay before reconnection to prevent rapid reconnection attempts
        await asyncio.sleep(5)

        # Only attempt reconnection if we're not in a stopping state
        if not self.is_in_state(ClientState.STOPPING, ClientState.STOPPED, ClientState.DISPOSED):
            self._state_machine.transition_to(ClientState.RECONNECTING)
            self._connection_attempts = 0  # Reset connection attempts for fresh reconnection
            await self._start_async()

    def _create_task(
        self,
        coro: Coroutine,
        log_msg: str | None = None,
        actions: Callable | None = None,
        success: str | None = None,
    ) -> asyncio.Task:
        """
        Create an asyncio task with error handling and optional callback actions.

        Parameters
        ----------
        coro : Coroutine
            The coroutine to run.
        log_msg : str, optional
            The log message to include with the task. Defaults to coroutine name.
        actions : Callable, optional
            The actions callback to run when the coroutine completes successfully.
        success : str, optional
            The log message to write on actions success.

        Returns
        -------
        asyncio.Task
            The created task.

        """
        log_msg = log_msg or coro.__name__
        self._log.debug(f"Creating task '{log_msg}'")

        # Create the task with proper name
        task = self._loop.create_task(coro, name=f"{log_msg}_{id(coro)}")

        # Create a weak reference to self to avoid creating a reference cycle
        weak_self = self._weakref

        # Define the callback function
        def _on_task_completed(task: asyncio.Task) -> None:
            """
            Handle task completion.
            """
            # Get the current instance from the weak reference
            self = weak_self()
            if self is None:
                return  # Instance has been garbage collected

            # Remove from active tasks tracking
            self._active_tasks.discard(task)

            # Handle task result
            try:
                if task.cancelled():
                    self._log.debug(f"Task '{log_msg}' was cancelled")
                elif exc := task.exception():
                    self._log.error(f"Error in task '{log_msg}': {exc!r}")
                else:
                    # Execute success actions
                    if actions:
                        try:
                            actions()
                            if success:
                                self._log.info(success, LogColor.GREEN)
                        except Exception as e:
                            self._log.error(
                                f"Failed triggering action {actions.__name__} on '{log_msg}': {e!r}",
                            )
            except asyncio.CancelledError:
                # This can happen if the task is cancelled while we're checking its status
                self._log.debug(f"Task '{log_msg}' was cancelled during completion handling")

        # Add the callback
        task.add_done_callback(_on_task_completed)

        # Track the task
        self._active_tasks.add(task)

        return task

    def subscribe_event(self, name: str, handler: Callable) -> None:
        """
        Subscribe a handler function to a named event.

        Parameters
        ----------
        name : str
            The name of the event to subscribe to.
        handler : Callable
            The handler function to be called when the event occurs.

        """
        self._event_subscriptions[name] = handler

    def unsubscribe_event(self, name: str) -> None:
        """
        Unsubscribe a handler from a named event.

        Parameters
        ----------
        name : str
            The name of the event to unsubscribe from.

        """
        self._event_subscriptions.pop(name, None)

    @handle_ib_error
    async def _await_request(
        self,
        request: Request,
        timeout: int,
        default_value: Any = None,
        suppress_timeout_warning: bool = False,
    ) -> Any:
        """
        Await the completion of a request within a specified timeout.

        Parameters
        ----------
        request : Request
            The request object to await.
        timeout : int
            The maximum time to wait for the request to complete, in seconds.
        default_value : Any, optional
            The default value to return if the request times out or fails. Defaults to None.
        suppress_timeout_warning: bool, optional
            Suppress the timeout warning. Defaults to False.

        Returns
        -------
        Any
            The result of the request, or default_value if the request times out or fails.

        """
        try:
            return await asyncio.wait_for(request.future, timeout)
        except TimeoutError as e:
            msg = f"Request timed out for {request}. Ending request."
            self._log.debug(msg) if suppress_timeout_warning else self._log.warning(msg)
            self._end_request(request.req_id, success=False, exception=e)
            return default_value
        except ConnectionError as e:
            self._log.error(f"Connection error during {request}. Ending request.")
            self._end_request(request.req_id, success=False, exception=e)
            return default_value

    def _end_request(
        self,
        req_id: int,
        success: bool = True,
        exception: type | BaseException | None = None,
    ) -> None:
        """
        End a request with a specified result or exception.

        Parameters
        ----------
        req_id : int
            The request ID to conclude.
        success : bool, optional
            Whether the request was successful. Defaults to True.
        exception : type | BaseException | None, optional
            An exception to set on request failure. Defaults to None.

        """
        if not (request := self._requests.get(req_id=req_id)):
            return

        if not request.future.done():
            if success:
                request.future.set_result(request.result)
            else:
                if exception:
                    request.future.set_exception(exception)
                else:
                    request.future.cancel()

        self._requests.remove(req_id=req_id)

    @handle_ib_error
    async def _run_tws_incoming_msg_reader(self) -> None:
        """
        Continuously read messages from TWS/Gateway and put them in the internal message
        queue for processing.
        """
        self._log.debug("Client TWS incoming message reader started.")
        buf = b""
        try:
            while self._eclient.conn and self._eclient.conn.isConnected():
                data = await asyncio.to_thread(self._eclient.conn.recvMsg)
                buf += data
                while buf:
                    _, msg, buf = comm.read_msg(buf)
                    self._log.debug(f"Msg buffer received: {buf!s}")
                    if msg:
                        # Place msg in the internal queue for processing
                        self._loop.call_soon_threadsafe(self._internal_msg_queue.put_nowait, msg)
                    else:
                        self._log.debug("More incoming packets are needed.")
                        break
        except asyncio.CancelledError:
            self._log.debug("Client TWS incoming message reader was cancelled.")
        except Exception as e:
            self._log.exception("Unhandled exception in Client TWS incoming message reader", e)
        finally:
            if self._is_ib_connected.is_set() and not self.is_disposed:
                self._log.debug(
                    "`_is_ib_connected` unset by `_run_tws_incoming_msg_reader`.",
                    LogColor.BLUE,
                )
                self._is_ib_connected.clear()
            self._log.debug("Client TWS incoming message reader stopped.")

    @handle_ib_error
    async def _run_internal_msg_queue_processor(self) -> None:
        """
        Continuously process messages from the internal incoming message queue.
        """
        self._log.debug(
            "Client internal message queue processor started.",
        )
        try:
            while (
                self._eclient.conn and self._eclient.conn.isConnected()
            ) or not self._internal_msg_queue.empty():
                # Use a timeout to allow for periodic checking of connection state
                try:
                    msg = await asyncio.wait_for(self._internal_msg_queue.get(), 1.0)
                except TimeoutError:
                    # Check if we should continue waiting for messages
                    if self._eclient.conn and self._eclient.conn.isConnected():
                        continue
                    elif self._internal_msg_queue.empty():
                        break
                    else:
                        continue

                if not await self._process_message(msg):
                    break

                self._internal_msg_queue.task_done()
        except asyncio.CancelledError:
            log_msg = f"Internal message queue processing was cancelled. (qsize={self._internal_msg_queue.qsize()})."
            (
                self._log.warning(log_msg)
                if not self._internal_msg_queue.empty()
                else self._log.debug(log_msg)
            )
        finally:
            self._log.debug("Internal message queue processor stopped.")

    @handle_ib_error
    async def _process_message(self, msg: str) -> bool:
        """
        Process a single message from TWS/Gateway.

        Parameters
        ----------
        msg : str
            The message to be processed.

        Returns
        -------
        bool
            True if processing should continue, False otherwise.

        """
        if len(msg) > MAX_MSG_LEN:
            await self.process_error(
                req_id=NO_VALID_ID,
                error_code=BAD_LENGTH.code(),
                error_string=f"{BAD_LENGTH.msg()}:{len(msg)}:{msg}",
            )
            return False

        fields: tuple[bytes] = comm.read_fields(msg)
        self._log.debug(f"Msg received: {msg}")
        self._log.debug(f"Msg received fields: {fields}")

        # The decoder identifies the message type based on its payload and calls the
        # corresponding method from the EWrapper.
        try:
            await asyncio.to_thread(self._eclient.decoder.interpret, fields)
        except Exception as e:
            self._log.error(f"Error processing message: {e}")
            return False

        return True

    @handle_ib_error
    async def _run_msg_handler_processor(self) -> None:
        """
        Process handler tasks from the message handler task queue.

        Continuously retrieves and executes tasks from the message handler task queue,
        which are typically partial functions representing message handling operations
        received from the ibapi wrapper.

        """
        try:
            while self._state not in (
                ClientState.STOPPING,
                ClientState.STOPPED,
                ClientState.DISPOSED,
            ):
                # Use a timeout to allow for periodic checking of client state
                try:
                    handler_task = await asyncio.wait_for(self._msg_handler_task_queue.get(), 1.0)
                except TimeoutError:
                    continue

                try:
                    await handler_task()
                except Exception as e:
                    self._log.error(f"Error executing handler task: {e}")
                finally:
                    self._msg_handler_task_queue.task_done()
        except asyncio.CancelledError:
            log_msg = f"Handler task processing was cancelled. (qsize={self._msg_handler_task_queue.qsize()})."
            (
                self._log.warning(log_msg)
                if not self._msg_handler_task_queue.empty()
                else self._log.debug(log_msg)
            )
        finally:
            self._log.debug("Handler task processor stopped.")

    def submit_to_msg_handler_queue(self, task: Callable[..., Any]) -> None:
        """
        Submit a task to the message handler's queue for processing.

        Parameters
        ----------
        task : Callable[..., Any]
            The task to be queued for processing.

        """
        self._log.debug(f"Submitting task to message handler queue: {task}")
        asyncio.run_coroutine_threadsafe(self._msg_handler_task_queue.put(task), self._loop)

    def _next_req_id(self) -> int:
        """
        Generate the next sequential request ID.

        Returns
        -------
        int
            The next request ID.

        """
        new_id = self._request_id_seq
        self._request_id_seq += 1
        return new_id

    # -- EClient overrides ------------------------------------------------------------------------

    def sendMsg(self, msg):
        """
        Override the logging for ibapi EClient.sendMsg.
        """
        full_msg = comm.make_msg(msg)
        self._log.debug(f"TWS API request sent: function={current_fn_name(1)} msg={full_msg}")
        self._eclient.conn.sendMsg(full_msg)

    def logRequest(self, fnName, fnParams):
        """
        Override the logging for ibapi EClient.logRequest.
        """
        if "self" in fnParams:
            prms = dict(fnParams)
            del prms["self"]
        else:
            prms = fnParams
        self._log.debug(f"TWS API prepared request: function={fnName} data={prms}")
