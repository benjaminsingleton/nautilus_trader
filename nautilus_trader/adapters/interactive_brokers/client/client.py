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
        self._state = ClientState.CREATED

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

        # Tasks
        self._active_tasks: set[asyncio.Task] = set()
        self._connection_watchdog_task: asyncio.Task | None = None
        self._tws_incoming_msg_reader_task: asyncio.Task | None = None
        self._internal_msg_queue_processor_task: asyncio.Task | None = None
        self._internal_msg_queue: asyncio.Queue = asyncio.Queue()
        self._msg_handler_processor_task: asyncio.Task | None = None
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
        self._reconnect_delay: int = 5  # seconds

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
            self._create_task(self._start_async())

    @handle_ib_error
    async def _start_async(self) -> None:
        """
        Start the client asynchronously using a state machine approach.

        This method manages the client startup sequence, handling connection attempts,
        service initialization, and API startup. It uses a state machine to track and
        manage the client's progress through startup.

        """
        self._log.info(f"Starting InteractiveBrokersClient ({self._client_id})...")
        self._state = ClientState.CONNECTING

        while self._state not in (ClientState.READY, ClientState.STOPPED, ClientState.DISPOSED):
            try:
                if self._state == ClientState.CONNECTING:
                    await self._handle_connecting_state()

                elif self._state == ClientState.CONNECTED:
                    self._handle_connected_state()

                elif self._state == ClientState.WAITING_API:
                    await self._handle_waiting_api_state()

            except TimeoutError:
                self._log.error("Client failed to initialize. Connection timeout.")
                self._state = ClientState.CONNECTING
            except Exception as e:
                self._log.exception("Unhandled exception in client startup", e)
                self._state = ClientState.STOPPED
                await self._stop_async()

        if self._state == ClientState.READY:
            self._is_client_ready.set()
            self._log.debug("`_is_client_ready` set by `_start_async`.", LogColor.BLUE)
            self._connection_attempts = 0
        else:
            self._log.warning(f"Client startup ended in state {self._state}")

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
            self._log.error("Max connection attempts reached, connection failed")
            self._state = ClientState.STOPPED
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
        await self._connect()
        self._state = ClientState.CONNECTED

    def _handle_connected_state(self) -> None:
        """
        Handle the CONNECTED state of the client startup process.

        Starts all required services and initializes the API connection.

        """
        # Start all required services
        self._start_tws_incoming_msg_reader()
        self._start_internal_msg_queue_processor()
        self._eclient.startApi()
        self._state = ClientState.WAITING_API

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
            self._state = ClientState.READY
        except TimeoutError:
            self._log.error("Timeout waiting for managed accounts message")
            # Return to connecting state for retry
            self._state = ClientState.CONNECTING

    def _start_tws_incoming_msg_reader(self) -> None:
        """
        Start the incoming message reader task.

        This task reads messages from the TWS socket and places them in the internal
        message queue for processing.

        """
        if self._tws_incoming_msg_reader_task:
            self._tws_incoming_msg_reader_task.cancel()
            self._active_tasks.discard(self._tws_incoming_msg_reader_task)

        self._tws_incoming_msg_reader_task = self._create_task(
            self._run_tws_incoming_msg_reader(),
            "TWS incoming message reader",
        )
        self._active_tasks.add(self._tws_incoming_msg_reader_task)

    def _start_internal_msg_queue_processor(self) -> None:
        """
        Start the internal message queue processing task.

        This task processes messages from the internal queue and dispatches them to the
        appropriate handlers.

        """
        if self._internal_msg_queue_processor_task:
            self._internal_msg_queue_processor_task.cancel()
            self._active_tasks.discard(self._internal_msg_queue_processor_task)

        self._internal_msg_queue_processor_task = self._create_task(
            self._run_internal_msg_queue_processor(),
            "Internal message queue processor",
        )
        self._active_tasks.add(self._internal_msg_queue_processor_task)

        if self._msg_handler_processor_task:
            self._msg_handler_processor_task.cancel()
            self._active_tasks.discard(self._msg_handler_processor_task)

        self._msg_handler_processor_task = self._create_task(
            self._run_msg_handler_processor(),
            "Message handler processor",
        )
        self._active_tasks.add(self._msg_handler_processor_task)

    def _start_connection_watchdog(self) -> None:
        """
        Start the connection watchdog task.

        This task monitors the connection to TWS/Gateway and initiates reconnection if
        the connection is lost.

        """
        if self._connection_watchdog_task:
            self._connection_watchdog_task.cancel()
            self._active_tasks.discard(self._connection_watchdog_task)

        self._connection_watchdog_task = self._create_task(
            self._run_connection_watchdog(),
            "Connection watchdog",
        )
        self._active_tasks.add(self._connection_watchdog_task)

    def _stop(self) -> None:
        """
        Stop the client and cancel running tasks.

        This method initiates the client shutdown process.

        """
        self._state = ClientState.STOPPING
        self._create_task(self._stop_async())

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

        # Cancel specific tasks
        specific_tasks = [
            self._connection_watchdog_task,
            self._tws_incoming_msg_reader_task,
            self._internal_msg_queue_processor_task,
            self._msg_handler_processor_task,
        ]

        for task in specific_tasks:
            if task and not task.cancelled():
                task.cancel()
                self._active_tasks.discard(task)

        # Cancel any remaining tasks
        remaining_tasks = set(
            self._active_tasks,
        )  # Create a copy to avoid modification during iteration
        for task in remaining_tasks:
            if not task.cancelled():
                task.cancel()
                self._active_tasks.discard(task)

        # Wait for all tasks to complete
        if self._active_tasks:
            try:
                await asyncio.gather(*self._active_tasks, return_exceptions=True)
                self._log.info("All tasks canceled successfully.")
            except Exception as e:
                self._log.exception(f"Error occurred while canceling tasks: {e}", e)
            finally:
                self._active_tasks.clear()

        # Disconnect from TWS/Gateway
        self._eclient.disconnect()

        # Clear state
        self._account_ids = set()
        self.registered_nautilus_clients = set()
        self._state = ClientState.STOPPED

    def _reset(self) -> None:
        """
        Restart the client.

        This method stops and then restarts the client.

        """
        self._create_task(self._reset_async())

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
        self._create_task(self._resume_async())

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
        if self._state != ClientState.DEGRADED:
            self._log.info(f"Degrading InteractiveBrokersClient ({self._client_id})...")
            self._is_client_ready.clear()
            self._account_ids = set()
            self._state = ClientState.DEGRADED

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
    async def _run_connection_watchdog(self) -> None:
        """
        Run a watchdog to monitor and manage the health of the socket connection.

        Continuously checks the connection status, manages client state based on
        connection health, and handles reconnection in case of network failure.

        """
        try:
            while self._state not in (
                ClientState.STOPPING,
                ClientState.STOPPED,
                ClientState.DISPOSED,
            ):
                await asyncio.sleep(1)
                if not self._is_ib_connected.is_set() or not self._eclient.isConnected():
                    self._log.error(
                        "Connection watchdog detects connection lost.",
                    )
                    await self._handle_disconnection()
        except asyncio.CancelledError:
            self._log.debug("Client connection watchdog task was canceled.")

    async def _handle_disconnection(self) -> None:
        """
        Handle the disconnection of the client from TWS/Gateway.

        This method degrades the client state and initiates reconnection after a delay.

        """
        if self._state in (ClientState.READY, ClientState.CONNECTED, ClientState.WAITING_API):
            self._degrade()

        if self._is_ib_connected.is_set():
            self._log.debug("`_is_ib_connected` unset by `_handle_disconnection`.", LogColor.BLUE)
            self._is_ib_connected.clear()

        # Add a delay before reconnection to prevent rapid reconnection attempts
        await asyncio.sleep(5)

        # Only attempt reconnection if we're not in a stopping state
        if self._state not in (ClientState.STOPPING, ClientState.STOPPED, ClientState.DISPOSED):
            self._state = ClientState.RECONNECTING
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
