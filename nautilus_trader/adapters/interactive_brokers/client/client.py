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
import os
import random
import weakref
from collections.abc import Callable
from collections.abc import Coroutine
from contextlib import suppress
from decimal import Decimal
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
from nautilus_trader.adapters.interactive_brokers.client.account import AccountService
from nautilus_trader.adapters.interactive_brokers.client.common import AccountOrderRef
from nautilus_trader.adapters.interactive_brokers.client.common import ClientState
from nautilus_trader.adapters.interactive_brokers.client.common import IBPosition
from nautilus_trader.adapters.interactive_brokers.client.common import Request
from nautilus_trader.adapters.interactive_brokers.client.common import Requests
from nautilus_trader.adapters.interactive_brokers.client.common import Subscriptions
from nautilus_trader.adapters.interactive_brokers.client.connection import ConnectionService
from nautilus_trader.adapters.interactive_brokers.client.contract import InteractiveBrokersClientContractMixin
from nautilus_trader.adapters.interactive_brokers.client.error import InteractiveBrokersClientErrorMixin
from nautilus_trader.adapters.interactive_brokers.client.error import handle_ib_error
from nautilus_trader.adapters.interactive_brokers.client.market_data import InteractiveBrokersClientMarketDataMixin
from nautilus_trader.adapters.interactive_brokers.client.order import InteractiveBrokersClientOrderMixin
from nautilus_trader.adapters.interactive_brokers.client.wrapper import InteractiveBrokersEWrapper
from nautilus_trader.adapters.interactive_brokers.common import IB_VENUE
from nautilus_trader.adapters.interactive_brokers.common import IBContract
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import Component
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.model.identifiers import ClientId


# fmt: on
class StateMachine:
    def __init__(self, initial_state: ClientState, logger: Any) -> None:
        self._current_state = initial_state
        self._log = logger
        self._state_changed_callbacks: dict[ClientState, list[Callable[[], None]]] = {}
        self._transition_lock = asyncio.Lock()
        self._valid_transitions = {
            ClientState.CREATED: {ClientState.CONNECTING, ClientState.STOPPING},
            ClientState.CONNECTING: {
                ClientState.CONNECTED,
                ClientState.RECONNECTING,
                ClientState.STOPPING,
            },
            ClientState.CONNECTED: {
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
            ClientState.DISPOSED: set(),
        }

    @property
    def current_state(self) -> ClientState:
        return self._current_state

    def register_state_changed_callback(
        self,
        state: ClientState,
        callback: Callable[[], None],
    ) -> None:
        if state not in self._state_changed_callbacks:
            self._state_changed_callbacks[state] = []
        self._state_changed_callbacks[state].append(callback)

    async def transition_to(self, new_state: ClientState) -> bool:
        """
        Transition to a new state if valid, executing callbacks.

        Returns False if transition or callbacks fail.

        """
        async with self._transition_lock:
            if new_state not in self._valid_transitions.get(self._current_state, set()):
                self._log.warning(f"Invalid transition: {self._current_state} -> {new_state}")
                return False

            old_state = self._current_state
            self._current_state = new_state
            self._log.debug(f"State transition: {old_state} -> {new_state}")

            if new_state in self._state_changed_callbacks:
                for callback in self._state_changed_callbacks[new_state]:
                    try:
                        callback()
                    except Exception as e:
                        self._log.error(f"Callback failed for {new_state}: {e}")
                        self._current_state = old_state  # Rollback on failure
                        return False
            return True


class TaskRegistry:
    """
    Registry for managing asynchronous tasks.

    This class provides centralized tracking and management of asyncio tasks, ensuring
    proper cleanup and preventing resource leaks.

    """

    def __init__(self, logger: Any) -> None:
        self._tasks: dict[str, asyncio.Task] = {}
        self._log = logger
        self._lock = asyncio.Lock()  # Lock to ensure atomic task operations

    async def create_task(
        self,
        coro: Coroutine[Any, Any, Any],
        name: str,
        on_done: Callable[[], None] | Callable[[], Coroutine] | None = None,
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
            A callback to run when the task completes. Can be a regular function or coroutine.

        Returns
        -------
        asyncio.Task
            The created task.

        """
        async with self._lock:
            # Cancel existing task with the same name if it exists
            await self.cancel_task(name)

            # Create a new task
            task = asyncio.create_task(coro, name=name)

            # Store the task
            self._tasks[name] = task

            # Add completion callback
            task.add_done_callback(
                lambda t: asyncio.create_task(
                    self._on_task_done(t, name, on_done),
                ),
            )

            self._log.debug(f"Task created: {name}")
            return task

    async def cancel_task(self, name: str, timeout: float = 1.0) -> bool:
        """
        Cancel a task by name.

        Parameters
        ----------
        name : str
            The name of the task to cancel.
        timeout : float, default 1.0
            Maximum time to wait for the task to cancel, in seconds.

        Returns
        -------
        bool
            True if a task was found and cancelled, False otherwise.

        """
        async with self._lock:
            if name in self._tasks:
                task = self._tasks[name]
                if not task.done():
                    task.cancel()
                    # Wait for task to actually cancel with timeout
                    try:
                        with suppress(asyncio.CancelledError):
                            await asyncio.wait_for(task, timeout=timeout)
                    except TimeoutError:
                        self._log.warning(f"Task {name} did not cancel within {timeout}s timeout")
                    self._log.debug(f"Task cancelled: {name}")
                return True
            return False

    async def cancel_all_tasks(self) -> None:
        """
        Cancel all registered tasks.
        """
        async with self._lock:
            for name in list(self._tasks.keys()):
                await self.cancel_task(name)

    async def wait_for_all_tasks(self, timeout: float | None = None) -> bool:
        """
        Wait for all tasks to complete within the specified timeout.

        Parameters
        ----------
        timeout : float, optional
            Maximum time to wait for all tasks to complete, in seconds.
            If None, waits indefinitely.

        Returns
        -------
        bool
            True if all tasks completed, False if timeout occurred.

        """
        async with self._lock:
            if not self._tasks:
                return True

            all_tasks = list(self._tasks.values())

        try:
            done, pending = await asyncio.wait(
                all_tasks,
                timeout=timeout,
                return_when=asyncio.ALL_COMPLETED,
            )
            return len(pending) == 0
        except Exception as e:
            self._log.error(f"Error waiting for tasks to complete: {e}")
            return False

    async def _on_task_done(
        self,
        task: asyncio.Task,
        name: str,
        on_done: Callable[[], None] | Callable[[], Coroutine] | None,
    ) -> None:
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
        async with self._lock:
            self._tasks.pop(name, None)

        # Check for exceptions
        if not task.cancelled():
            try:
                exception = task.exception()
                if exception:
                    self._log.error(f"Task {name} failed with exception: {exception}")
                elif on_done:
                    try:
                        if iscoroutinefunction(on_done):
                            await on_done()
                        else:
                            result = on_done()
                            # Ensure any coroutine result is properly awaited
                            if result is not None and asyncio.iscoroutine(result):
                                await result
                    except Exception as e:
                        self._log.error(f"Error in on_done callback for task {name}: {e}")
            except asyncio.CancelledError:
                self._log.debug(f"Task {name} was cancelled during exception check")
            except Exception as e:
                self._log.error(f"Error in task completion handling: {e}")

        self._log.debug(f"Task completed: {name}")

    async def get_task(self, name: str) -> asyncio.Task | None:
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
        async with self._lock:
            return self._tasks.get(name)

    async def is_running(self, name: str) -> bool:
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
        task = await self.get_task(name)
        return task is not None and not task.done()

    @property
    def task_count(self) -> int:
        """
        Get the number of tasks currently registered.

        Returns
        -------
        int
            The number of tasks.

        """
        return len(self._tasks)

    @property
    def active_task_names(self) -> list[str]:
        """
        Get the names of all active tasks.

        Returns
        -------
        list[str]
            List of task names.

        """
        return list(self._tasks.keys())


class ConnectionManager:
    """
    Manages connection status and provides synchronized access to connection flags.

    This class centralizes connection state management to prevent race conditions.

    """

    def __init__(self, logger: Any) -> None:
        self._log = logger
        self._is_connected = asyncio.Event()
        self._is_ready = asyncio.Event()
        self._status_lock = asyncio.Lock()
        self._last_heartbeat_time = 0.0
        self._connection_state_change_callbacks: list[
            Callable[[bool, str], Coroutine[Any, Any, None]]
        ] = []
        self._ready_state_change_callbacks: list[
            Callable[[bool, str], Coroutine[Any, Any, None]]
        ] = []

    @property
    def is_connected(self) -> bool:
        """
        Whether the client is connected to TWS/Gateway.
        """
        return self._is_connected.is_set()

    @property
    def is_ready(self) -> bool:
        """
        Whether the client is fully initialized and ready to handle requests.
        """
        return self._is_ready.is_set()

    @property
    def last_heartbeat_time(self) -> float:
        """
        The timestamp of the last successful heartbeat.
        """
        return self._last_heartbeat_time

    def register_connection_callback(
        self,
        callback: Callable[[bool, str], Coroutine[Any, Any, None]],
    ) -> None:
        """
        Register a callback to be called when connection state changes.

        Parameters
        ----------
        callback : Callable
            The callback function. Should accept (connected: bool, reason: str) parameters.

        """
        self._connection_state_change_callbacks.append(callback)

    def register_ready_callback(
        self,
        callback: Callable[[bool, str], Coroutine[Any, Any, None]],
    ) -> None:
        """
        Register a callback to be called when ready state changes.

        Parameters
        ----------
        callback : Callable
            The callback function. Should accept (ready: bool, reason: str) parameters.

        """
        self._ready_state_change_callbacks.append(callback)

    async def set_connected(self, connected: bool, reason: str = "") -> None:
        """
        Set the connected state of the client.

        Parameters
        ----------
        connected : bool
            Whether the client is connected.
        reason : str, optional
            The reason for the connection state change.

        """
        async with self._status_lock:
            was_connected = self._is_connected.is_set()
            if connected != was_connected:
                if connected:
                    self._is_connected.set()
                    self._log.info(f"Connection established: {reason}")
                else:
                    self._is_connected.clear()
                    self._is_ready.clear()  # Also clear ready state when disconnected
                    self._log.info(f"Connection lost: {reason}")

                # Execute callbacks
                for callback in self._connection_state_change_callbacks:
                    try:
                        await callback(connected, reason)
                    except Exception as e:
                        self._log.exception(f"Error in connection state change callback: {e}")

    async def set_ready(self, ready: bool, reason: str = "") -> None:
        """
        Set the ready state of the client.

        Parameters
        ----------
        ready : bool
            Whether the client is ready.
        reason : str, optional
            The reason for the ready state change.

        """
        async with self._status_lock:
            was_ready = self._is_ready.is_set()
            if ready != was_ready:
                if ready:
                    # Can only be ready if connected
                    if not self._is_connected.is_set():
                        self._log.warning("Cannot set ready state when disconnected")
                        return
                    self._is_ready.set()
                    self._log.info(f"Client ready: {reason}")
                else:
                    self._is_ready.clear()
                    self._log.info(f"Client not ready: {reason}")

                # Execute callbacks
                for callback in self._ready_state_change_callbacks:
                    try:
                        await callback(ready, reason)
                    except Exception as e:
                        self._log.exception(f"Error in ready state change callback: {e}")

    async def update_heartbeat(self, timestamp: float) -> None:
        """
        Update the last heartbeat timestamp.

        Parameters
        ----------
        timestamp : float
            The timestamp of the heartbeat.

        """
        async with self._status_lock:
            self._last_heartbeat_time = timestamp

    async def wait_until_connected(self, timeout: float | None = None) -> bool:
        """
        Wait until the client is connected.

        Parameters
        ----------
        timeout : float, optional
            The maximum time to wait for the connection.

        Returns
        -------
        bool
            True if the client is connected, False if the timeout was reached.

        """
        try:
            await asyncio.wait_for(self._is_connected.wait(), timeout)
            return True
        except TimeoutError:
            return False

    async def wait_until_ready(self, timeout: float | None = None) -> bool:
        """
        Wait until the client is ready.

        Parameters
        ----------
        timeout : float, optional
            The maximum time to wait for the client to be ready.

        Returns
        -------
        bool
            True if the client is ready, False if the timeout was reached.

        """
        try:
            await asyncio.wait_for(self._is_ready.wait(), timeout)
            return True
        except TimeoutError:
            return False


class InteractiveBrokersClient(
    Component,
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

    SOCKET_CONNECT_TIMEOUT = float(os.getenv("IB_SOCKET_CONNECT_TIMEOUT", "10"))
    HANDSHAKE_TIMEOUT = float(os.getenv("IB_HANDSHAKE_TIMEOUT", "5"))
    HEARTBEAT_INTERVAL = float(os.getenv("IB_HEARTBEAT_INTERVAL", "30"))
    HEARTBEAT_TIMEOUT_MULTIPLIER = float(os.getenv("IB_HEARTBEAT_TIMEOUT_MULTIPLIER", "2"))
    MAX_CONNECTION_ATTEMPTS = int(os.getenv("IB_MAX_CONNECTION_ATTEMPTS", "0"))
    RECONNECT_DELAY = int(os.getenv("IB_RECONNECT_DELAY", "5"))
    MAX_RECONNECT_JITTER = float(os.getenv("IB_RECONNECT_MAX_JITTER", "2.0"))
    WAITING_API_TIMEOUT = float(os.getenv("IB_WAITING_API_TIMEOUT", "15"))
    DEFAULT_READY_TIMEOUT = float(os.getenv("IB_DEFAULT_READY_TIMEOUT", "300"))

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
        # Initialize Component first
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
        self._connection_manager = ConnectionManager(self._log)
        self._task_registry = TaskRegistry(self._log)

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
        self._internal_msg_queue: asyncio.Queue = asyncio.Queue()
        self._msg_handler_task_queue: asyncio.Queue = asyncio.Queue()

        # Hot caches
        self.registered_nautilus_clients: set = set()
        self._event_subscriptions: dict[str, Callable] = {}
        self._active_tasks: set[asyncio.Task] = set()  # For tracking tasks

        # Subscriptions
        self._requests = Requests()
        self._subscriptions = Subscriptions()

        # Initialize all mixins with appropriate parameters
        InteractiveBrokersClientMarketDataMixin.__init__(self)
        InteractiveBrokersClientOrderMixin.__init__(self)
        InteractiveBrokersClientContractMixin.__init__(self)
        InteractiveBrokersClientErrorMixin.__init__(self)

        # Create services
        self._connection_service = ConnectionService(
            log=self._log,
            host=self._host,
            port=self._port,
            client_id=self._client_id,
            eclient=self._eclient,
            state_machine=self._state_machine,
            connection_manager=self._connection_manager,
            requests=self._requests,
            create_task_func=self._create_task,
            is_in_state_func=self.is_in_state,
            socket_connect_timeout=self.SOCKET_CONNECT_TIMEOUT,
            handshake_timeout=self.HANDSHAKE_TIMEOUT,
        )

        self._is_ib_connected = asyncio.Event()  # This was in the account mixin

        # Create account service
        self._account_service = AccountService(
            log=self._log,
            eclient=self._eclient,
            requests=self._requests,
            subscriptions=self._subscriptions,
            event_subscriptions=self._event_subscriptions,
            is_ib_connected=self._is_ib_connected,
            next_req_id_func=self._next_req_id,
            await_request_func=self._await_request,
            end_request_func=self._end_request,
        )

        # Register state change callbacks
        self._state_machine.register_state_changed_callback(
            ClientState.READY,
            self._on_ready_state_entered,
        )
        self._state_machine.register_state_changed_callback(
            ClientState.STOPPING,
            self._on_stopping_state_entered,
        )

        # ConnectionMixin
        self._connection_attempts: int = 0
        self._max_connection_attempts: int = self.MAX_CONNECTION_ATTEMPTS
        self._indefinite_reconnect: bool = self._max_connection_attempts <= 0
        self._reconnect_delay: int = self.RECONNECT_DELAY
        self._reconnect_max_jitter: float = self.MAX_RECONNECT_JITTER
        self._heartbeat_interval: float = self.HEARTBEAT_INTERVAL

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

    def _on_ready_state_entered(self) -> None:
        """
        Handle transition to READY state.
        """
        # Create a properly managed task that won't be orphaned
        task = asyncio.create_task(self._handle_ready_state_entered())
        # Prevent the task from being garbage collected
        self._active_tasks.add(task)

    async def _handle_ready_state_entered(self) -> None:
        """
        Process the READY state transition asynchronously.
        """
        await self._task_registry.create_task(
            self._connection_manager.set_ready(True, "State machine entered READY state"),
            "set_ready_on_ready_state",
        )

    def _on_stopping_state_entered(self) -> None:
        """
        Handle transition to STOPPING state.
        """
        try:
            # Create a properly managed task that won't be orphaned
            task = asyncio.create_task(self._handle_stopping_state_entered())
            # Prevent the task from being garbage collected
            self._active_tasks.add(task)
        except Exception as e:
            self._log.warning(f"Error creating task for stopping state: {e}")

    async def _handle_stopping_state_entered(self) -> None:
        """
        Process the STOPPING state transition asynchronously.
        """
        await self._task_registry.create_task(
            self._connection_manager.set_ready(False, "State machine entered STOPPING state"),
            "set_not_ready_on_stopping_state",
        )

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
            asyncio.run_coroutine_threadsafe(
                self._create_start_task(),
                self._loop,
            )

    async def _create_start_task(self) -> None:
        """
        Create a start task in the event loop.
        """
        await self._task_registry.create_task(self._start_async(), "start_client")

    @handle_ib_error
    async def _start_async(self) -> None:
        """
        Start the client asynchronously using a state machine approach.

        This method manages the client startup sequence, handling connection attempts,
        service initialization, and API startup. It uses a state machine to track and
        manage the client's progress through startup.

        """
        self._log.info(f"Starting InteractiveBrokersClient ({self._client_id})...")
        await self._state_machine.transition_to(ClientState.CONNECTING)

        state_handlers = {
            ClientState.CONNECTING: self._handle_connecting_state,
            ClientState.CONNECTED: self._handle_connected_state,
            ClientState.WAITING_API: self._handle_waiting_api_state,
        }

        while not self.is_in_state(ClientState.READY, ClientState.STOPPED, ClientState.DISPOSED):
            handler = state_handlers.get(self.state)
            if handler:
                try:
                    await handler()
                except TimeoutError:
                    self._log.error("Connection timeout")
                    await asyncio.sleep(self._calculate_reconnect_delay())
                    await self._state_machine.transition_to(ClientState.CONNECTING)
                except ConnectionError as e:
                    self._log.error(f"Connection error: {e}")
                    await asyncio.sleep(self._calculate_reconnect_delay())
                    await self._state_machine.transition_to(ClientState.CONNECTING)
                except Exception as e:
                    self._log.exception(f"Startup failed: {e}")
                    await self._state_machine.transition_to(ClientState.STOPPING)
                    await self._stop_async()
                    break  # Exit loop after transitioning to STOPPING
            else:
                await asyncio.sleep(0.1)  # Short sleep, to be replaced with event in future

        if self.is_in_state(ClientState.READY):
            self._connection_attempts = 0
        else:
            self._log.warning(f"Startup ended in state {self.state}")

    def _calculate_reconnect_delay(self) -> float:
        """
        Calculate the delay before attempting to reconnect.

        Returns a base delay plus random jitter to prevent thundering herd problems.
        Uses exponential backoff with capped maximum delay.

        Returns
        -------
        float
            The delay in seconds before attempting to reconnect.

        """
        jitter = random.uniform(0, self._reconnect_max_jitter)  # noqa: S311
        backoff_factor = min(self._connection_attempts, 5)
        delay = self._reconnect_delay * (2 ** (backoff_factor - 1)) + jitter
        return min(delay, 60)

    @handle_ib_error
    async def _handle_connecting_state(self) -> None:
        """
        Handle the CONNECTING state of the client startup process.

        Manages connection attempt limits, reconnection delay, and establishing the
        socket connection.

        """
        self._connection_attempts += 1
        if (
            not self._indefinite_reconnect
            and self._connection_attempts > self._max_connection_attempts
        ):
            self._log.error(f"Max connection attempts ({self._max_connection_attempts}) reached")
            await self._state_machine.transition_to(ClientState.STOPPING)
            await self._stop_async()
            return

        if self._connection_attempts > 1:
            delay = self._calculate_reconnect_delay()
            self._log.info(f"Attempt {self._connection_attempts}: Reconnecting in {delay:.1f}s")
            await asyncio.sleep(delay)

        await self._connection_service.connect()
        # No need for set_connected and state transition as it's handled in the service

    @handle_ib_error
    async def _handle_connected_state(self) -> None:
        """
        Handle the CONNECTED state of the client startup process.

        Starts all required services and initializes the API connection.

        """
        await self._start_tws_incoming_msg_reader()
        await self._start_internal_msg_queue_processor()
        self._eclient.startApi()
        await self._state_machine.transition_to(ClientState.WAITING_API)

    @handle_ib_error
    async def _handle_waiting_api_state(self) -> None:
        """
        Handle the WAITING_API state of the client startup process.

        Waits for managed accounts message and completes the initialization.

        """
        await asyncio.wait_for(
            self._connection_manager.wait_until_connected(),
            self.WAITING_API_TIMEOUT,
        )
        await self._start_connection_watchdog()
        await self._start_heartbeat_monitor()
        await self._state_machine.transition_to(ClientState.READY)

    async def _start_tws_incoming_msg_reader(self) -> asyncio.Task:
        """
        Start the incoming message reader task.

        This task reads messages from the TWS socket and places them in the internal
        message queue for processing.

        Returns
        -------
        asyncio.Task
            The created task.

        """
        task = await self._task_registry.create_task(
            self._run_tws_incoming_msg_reader(),
            "tws_incoming_msg_reader",
        )
        return task

    async def _start_internal_msg_queue_processor(self) -> tuple[asyncio.Task, asyncio.Task]:
        """
        Start the internal message queue processing task.

        This task processes messages from the internal queue and dispatches them to the
        appropriate handlers.

        Returns
        -------
        tuple[asyncio.Task, asyncio.Task]
            The created tasks.

        """
        task1 = await self._task_registry.create_task(
            self._run_internal_msg_queue_processor(),
            "internal_msg_queue_processor",
        )

        task2 = await self._task_registry.create_task(
            self._run_msg_handler_processor(),
            "msg_handler_processor",
        )
        return task1, task2

    async def _start_connection_watchdog(self) -> asyncio.Task:
        """
        Start the connection watchdog task.

        This task monitors the connection to TWS/Gateway and initiates reconnection if
        the connection is lost.

        Returns
        -------
        asyncio.Task
            The created task.

        """
        task = await self._task_registry.create_task(
            self._run_connection_watchdog(),
            "connection_watchdog",
        )
        return task

    async def _start_heartbeat_monitor(self) -> asyncio.Task:
        """
        Start the heartbeat monitor task.

        This task sends periodic heartbeat messages to TWS/Gateway to detect connection
        issues even when there's no other activity.

        Returns
        -------
        asyncio.Task
            The created task.

        """
        task = await self._task_registry.create_task(
            self._run_heartbeat_monitor(),
            "heartbeat_monitor",
        )
        return task

    def _stop(self) -> None:
        """
        Stop the client and cancel running tasks.

        This method initiates the client shutdown process.

        """
        task = asyncio.create_task(self._create_stop_task())
        # Prevent the task from being garbage collected
        self._active_tasks.add(task)

    async def _create_stop_task(self) -> None:
        """
        Create a stop task in the event loop.
        """
        await self._task_registry.create_task(self._stop_async(), "stop_client")

    # Account service passthrough methods
    def accounts(self) -> set[str]:
        """
        Return a set of account identifiers managed by this instance.

        Returns
        -------
        set[str]
            The set of account identifiers.

        """
        return self._account_service.accounts()

    @property
    def _account_ids(self) -> set[str]:
        """
        Return a set of account identifiers managed by this instance.

        This property is needed for compatibility with tests that directly access the field.

        Returns
        -------
        set[str]
            The set of account identifiers.

        """
        return self._account_service.account_ids

    @_account_ids.setter
    def _account_ids(self, value: set[str]) -> None:
        """
        Set the account identifiers. For test compatibility.

        Parameters
        ----------
        value : set[str]
            The account identifiers to set.

        """
        self._account_service._account_ids = value

    def subscribe_account_summary(self) -> None:
        """
        Subscribe to the account summary for all accounts.
        """
        self._account_service.subscribe_account_summary()

    def unsubscribe_account_summary(self, account_id: str) -> None:
        """
        Unsubscribe from the account summary for the specified account.

        Parameters
        ----------
        account_id : str
            The identifier of the account to unsubscribe from.

        """
        self._account_service.unsubscribe_account_summary(account_id)

    async def get_positions(self, account_id: str) -> list[IBPosition]:
        """
        Fetch open positions for a specified account.

        Parameters
        ----------
        account_id: str
            The account identifier for which to fetch positions.

        Returns
        -------
        list[IBPosition]
            The list of positions for the account. May be empty if no positions found.

        """
        # Make sure we're getting the exact positions from the AccountService
        # and returning them to the caller without modifications
        positions = await self._account_service.get_positions(account_id)
        return positions

    async def process_account_summary(
        self,
        *,
        req_id: int,
        account_id: str,
        tag: str,
        value: str,
        currency: str,
    ) -> None:
        """
        Receive account information.

        Parameters
        ----------
        req_id : int
            The request ID.
        account_id : str
            The account ID.
        tag : str
            The account summary tag.
        value : str
            The tag value.
        currency : str
            The currency of the value.

        """
        await self._account_service.process_account_summary(
            req_id=req_id,
            account_id=account_id,
            tag=tag,
            value=value,
            currency=currency,
        )

    async def process_managed_accounts(self, *, accounts_list: str) -> None:
        """
        Receive a comma-separated string with the managed account ids.

        Occurs automatically on initial API client connection.

        Parameters
        ----------
        accounts_list : str
            Comma-separated string of account IDs.

        """
        await self._account_service.process_managed_accounts(accounts_list=accounts_list)

    async def process_position(
        self,
        *,
        account_id: str,
        contract: IBContract,
        position: Decimal,
        avg_cost: float,
    ) -> None:
        """
        Provide the portfolio's open positions.

        Parameters
        ----------
        account_id : str
            The account ID.
        contract : IBContract
            The contract information.
        position : Decimal
            The position quantity.
        avg_cost : float
            The average cost of the position.

        """
        await self._account_service.process_position(
            account_id=account_id,
            contract=contract,
            position=position,
            avg_cost=avg_cost,
        )

    async def process_position_end(self) -> None:
        """
        Indicate that all the positions have been transmitted.
        """
        await self._account_service.process_position_end()

    @handle_ib_error
    async def _stop_async(self) -> None:
        """
        Stop the client asynchronously, cancelling all tasks and releasing resources.

        This method ensures a clean shutdown of the client, cancelling all tasks and
        cleaning up resources.

        """
        self._log.info(f"Stopping InteractiveBrokersClient ({self._client_id})...")
        if not self.is_in_state(ClientState.STOPPING, ClientState.STOPPED, ClientState.DISPOSED):
            await self._state_machine.transition_to(ClientState.STOPPING)

        # First wait for tasks to complete naturally
        shutdown_timeout = float(os.getenv("IB_SHUTDOWN_TIMEOUT", "5.0"))
        tasks_completed = await self._task_registry.wait_for_all_tasks(timeout=shutdown_timeout)

        # Then cancel remaining tasks with timeout to prevent hangs
        if not tasks_completed:
            self._log.warning(
                f"Some tasks did not complete within {shutdown_timeout}s, cancelling remaining tasks",
            )
            try:
                # Add a timeout to cancel_all_tasks to prevent hanging
                await asyncio.wait_for(
                    self._task_registry.cancel_all_tasks(),
                    timeout=3.0,  # Shorter timeout for cancellation
                )
            except TimeoutError:
                self._log.warning("Task cancellation timed out, continuing with shutdown")

        try:
            # Use connection service for proper disconnection
            await self._connection_service.disconnect()
        except Exception as e:
            self._log.error(f"Error during disconnection: {e}")

        # Clear account IDs (for compatibility with tests)
        self._account_service._account_ids.clear()
        self.registered_nautilus_clients.clear()
        await self._connection_manager.set_connected(False, "Client stopped")
        await self._state_machine.transition_to(ClientState.STOPPED)
        self._log.info(f"InteractiveBrokersClient ({self._client_id}) stopped")

    @handle_ib_error
    async def _reset_async(self) -> None:
        """
        Restart the client asynchronously.

        This method ensures a clean shutdown before restarting the client.

        """
        self._log.info(f"Resetting InteractiveBrokersClient ({self._client_id})...")

        # Stop the client
        await self._stop_async()

        # Wait a brief moment to ensure clean state
        await asyncio.sleep(1.0)

        # Restart the client
        await self._start_async()

        self._log.info(f"InteractiveBrokersClient ({self._client_id}) reset completed")

    def _resume(self) -> None:
        """
        Resume the client and resubscribe to all subscriptions.

        This method is called after a reconnection to restore subscriptions.

        """
        task = asyncio.create_task(self._resume_async())
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)

    @handle_ib_error
    async def _resume_async(self) -> None:
        """
        Resume the client asynchronously, resubscribing to all subscriptions.

        This method waits for the client to be ready before attempting to resubscribe.

        """
        await self._connection_manager.wait_until_ready()
        self._log.info(f"Resuming InteractiveBrokersClient ({self._client_id})...")
        await self._resubscribe_all()

    async def _degrade(self) -> None:
        """
        Degrade the client when connectivity is lost.

        This method puts the client into a degraded state, clearing certain state
        information but not fully stopping the client.

        """
        if not self.is_in_state(ClientState.DEGRADED):
            self._log.info(f"Degrading InteractiveBrokersClient ({self._client_id})...")
            await self._connection_manager.set_ready(False, "Client degraded")
            self._account_ids = set()
            await self._state_machine.transition_to(ClientState.DEGRADED)

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

    async def wait_until_ready(self, timeout: float | None = 300) -> None:
        """
        Wait until the client is running and ready within a given timeout.

        Parameters
        ----------
        timeout : float, default 300
            Time in seconds to wait for the client to be ready.

        Raises
        ------
        TimeoutError
            If the client does not become ready within the timeout period.

        """
        success = await self._connection_manager.wait_until_ready(timeout)
        if not success:
            self._log.error(f"Client is not ready after {timeout} seconds.")
            raise TimeoutError(f"Client is not ready after {timeout} seconds.")

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
                        self._eclient.reqCurrentTime()
                        await asyncio.sleep(0.1)  # Small delay to ensure response
                        await self._connection_manager.update_heartbeat(self._clock.timestamp())
                    except Exception as e:
                        self._log.warning(f"Heartbeat failed: {e}")
                        if self._connection_manager.is_connected:
                            await self._handle_disconnection()
        except asyncio.CancelledError:
            self._log.debug("Heartbeat monitor cancelled")
        except Exception as e:
            self._log.exception(f"Heartbeat monitor error: {e}")

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
                if not self._connection_manager.is_connected:
                    self._log.error("Watchdog detected connection loss")
                    await self._handle_disconnection()
                elif self.is_in_state(ClientState.READY):
                    heartbeat_age = (
                        self._clock.timestamp() - self._connection_manager.last_heartbeat_time
                    )
                    if heartbeat_age > self._heartbeat_interval * self.HEARTBEAT_TIMEOUT_MULTIPLIER:
                        self._log.error(f"Heartbeat timeout: {heartbeat_age:.1f}s")
                        await self._handle_disconnection()
        except asyncio.CancelledError:
            self._log.debug("Watchdog cancelled")
        except Exception as e:
            self._log.exception(f"Watchdog error: {e}")

    @handle_ib_error
    async def _handle_disconnection(self) -> None:
        """
        Handle the disconnection of the client from TWS/Gateway.

        This method degrades the client state and initiates reconnection after a delay.
        It ensures proper synchronization between various state objects.

        """
        if self.is_in_state(ClientState.STOPPING, ClientState.STOPPED, ClientState.DISPOSED):
            self._log.info("Client is stopping or stopped, not attempting to reconnect")
            return

        # Lock to ensure state transitions happen atomically
        async with self._state_machine._transition_lock:
            # Use the connection service to set the disconnected state
            await self._connection_service.set_disconnected("Disconnected by watchdog")

            # Degrade state if in an operational state
            if self.is_in_state(ClientState.READY, ClientState.CONNECTED, ClientState.WAITING_API):
                await self._degrade()

            # Wait before reconnecting
            await asyncio.sleep(self._calculate_reconnect_delay())

            # Ensure we're in RECONNECTING state before starting reconnection
            if not self.is_in_state(ClientState.RECONNECTING):
                await self._state_machine.transition_to(ClientState.RECONNECTING)

            # Reset connection attempts counter
            self._connection_attempts = 0

            # Start reconnection process
            await self._start_async()

    @handle_ib_error
    async def _initiate_reconnection(self) -> None:
        """
        Initiate the reconnection process with backoff strategy.
        """
        # Calculate and apply reconnection delay with backoff
        delay = self._calculate_reconnect_delay()
        self._log.info(
            f"Initiating reconnection after {delay:.1f}s delay (attempt {self._connection_attempts + 1})",
        )
        await asyncio.sleep(delay)

        # Transition to reconnecting state and start connection
        await self._state_machine.transition_to(ClientState.RECONNECTING)
        self._connection_attempts = 0  # Reset connection attempts for fresh reconnection
        await self._start_async()

    def _create_task(
        self,
        coro: Coroutine[Any, Any, Any],
        log_msg: str | None = None,
        actions: Callable[[], None] | None = None,
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

        # Break the function into smaller parts to reduce complexity
        try:
            while self._eclient.conn and self._eclient.conn.isConnected():
                try:
                    data = await self._read_socket_data()
                    if not data:
                        continue

                    buf += data
                    buf = await self._process_message_buffer(buf)

                except Exception as e:
                    self._log.error(f"Error receiving/processing data: {e}")
                    await self._handle_message_processing_error()
        except asyncio.CancelledError:
            self._log.debug("Client TWS incoming message reader was cancelled.")
        except Exception as e:
            self._log.exception("Unhandled exception in Client TWS incoming message reader", e)
        finally:
            await self._handle_message_reader_cleanup()

    async def _read_socket_data(self) -> bytes:
        """
        Read data from the socket.
        """
        data = await asyncio.to_thread(self._eclient.conn.recvMsg)
        if not data and self._connection_manager.is_connected:
            # Empty data could indicate a closed connection
            self._log.warning("Received empty data from TWS, possible connection issue")
            # Wait briefly and retry before declaring connection lost
            await asyncio.sleep(0.5)
        return data

    async def _process_message_buffer(self, buf: bytes) -> bytes:
        """
        Process message buffer and extract messages.
        """
        remaining_buf = buf
        while remaining_buf:
            try:
                _, msg, remaining_buf = comm.read_msg(remaining_buf)
                if msg:
                    # Place msg in the internal queue for processing
                    self._loop.call_soon_threadsafe(self._internal_msg_queue.put_nowait, msg)
                else:
                    # Need more data to complete a message
                    self._log.debug("More incoming packets are needed.")
                    break
            except Exception as e:
                # Error parsing message, log and discard corrupted buffer
                self._log.error(f"Error parsing message: {e}. Discarding corrupted buffer.")
                return b""
        return remaining_buf

    async def _handle_message_processing_error(self) -> None:
        """
        Handle errors in message processing.
        """
        # Check if still connected after error
        if not (self._eclient.conn and self._eclient.conn.isConnected()):
            return
        # Brief pause to prevent tight loop if recurring errors
        await asyncio.sleep(0.1)

    async def _handle_message_reader_cleanup(self) -> None:
        """
        Clean up after message reader exits.
        """
        if self._connection_manager.is_connected:
            await self._connection_manager.set_connected(
                False,
                "Message reader task ended",
            )
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
            return True  # Continue processing

        fields: tuple[bytes] = comm.read_fields(msg)
        self._log.debug(f"Msg received: {msg}")
        self._log.debug(f"Msg received fields: {fields}")

        try:
            await asyncio.to_thread(self._eclient.decoder.interpret, fields)
        except Exception as e:
            self._log.error(f"Error processing message: {e}")
            return True  # Continue unless critical
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
            while not self.is_in_state(
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
