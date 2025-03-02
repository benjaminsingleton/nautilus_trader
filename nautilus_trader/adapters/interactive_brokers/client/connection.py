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
import functools
from typing import Any, cast

from ibapi import comm
from ibapi import decoder
from ibapi.client import EClient
from ibapi.common import NO_VALID_ID
from ibapi.connection import Connection
from ibapi.errors import CONNECT_FAIL
from ibapi.server_versions import MAX_CLIENT_VER
from ibapi.server_versions import MIN_CLIENT_VER

from nautilus_trader.adapters.interactive_brokers.client.common import ClientState
from nautilus_trader.adapters.interactive_brokers.client.common import Requests
from nautilus_trader.adapters.interactive_brokers.client.error import handle_ib_error
from nautilus_trader.common.component import Logger


class ConnectionState(enum.Enum):
    """
    Defines the connection states for the Interactive Brokers client.

    These states are used for more granular tracking of the socket connection status and
    align with the ClientState transitions.

    """

    DISCONNECTED = (
        0  # Not connected to TWS/Gateway (aligns with ClientState.CREATED|STOPPED|DISPOSED)
    )
    CONNECTING = 1  # In the process of establishing connection (aligns with ClientState.CONNECTING)
    HANDSHAKING = (
        2  # Connection established, performing API handshake (aligns with ClientState.CONNECTED)
    )
    CONNECTED = 3  # Fully connected with handshake complete (aligns with ClientState.READY)
    DISCONNECTING = 4  # In the process of disconnecting (aligns with ClientState.STOPPING)


class ConnectionResult:
    """
    Represents the result of a connection attempt.

    This class encapsulates the success/failure status of a connection attempt along
    with any relevant error information.

    """

    def __init__(self, success: bool, error: Exception | None = None, message: str = ""):
        self.success = success
        self.error = error
        self.message = message

    def __bool__(self) -> bool:
        return self.success

    def __str__(self) -> str:
        return f"ConnectionResult(success={self.success}, error='{self.error}', message='{self.message}')"


class ConnectionService:
    """
    Service that manages the connection to TWS/Gateway for the InteractiveBrokersClient.

    This class is responsible for establishing and maintaining the socket connection,
    handling server communication, monitoring the connection's health, and managing
    reconnections.

    Parameters
    ----------
    log : Logger
        The logger for the service.
    host : str
        The hostname or IP address of the TWS/Gateway.
    port : int
        The port of the TWS/Gateway.
    client_id : int
        The client ID for the connection.
    eclient : EClient
        The EClient instance.
    state_machine : Any
        The state machine for managing client state.
    connection_manager : Any
        The connection manager for the client.
    requests : Requests
        The requests manager.
    socket_connect_timeout : float
        The timeout for socket connection in seconds.
    handshake_timeout : float
        The timeout for handshake in seconds.

    """

    def __init__(
        self,
        log: Logger,
        host: str,
        port: int,
        client_id: int,
        eclient: EClient,
        state_machine: Any,
        connection_manager: Any,
        requests: Requests,
        create_task_func: Any,
        is_in_state_func: Any,
        socket_connect_timeout: float = 10.0,
        handshake_timeout: float = 5.0,
        max_connection_attempts: int = 0,
        reconnect_delay: int = 5,
        reconnect_max_jitter: float = 2.0,
        heartbeat_interval: float = 30.0,
    ) -> None:
        self._log = log
        self._host = host
        self._port = port
        self._client_id = client_id
        self._eclient = eclient
        self._state_machine = state_machine
        self._connection_manager = connection_manager
        self._requests = requests
        self._create_task = create_task_func
        self._is_in_state = is_in_state_func
        self._socket_connect_timeout = socket_connect_timeout
        self._handshake_timeout = handshake_timeout
        self._connection_lock = asyncio.Lock()

        # Connection attempt tracking
        self._connection_attempts: int = 0
        self._max_connection_attempts: int = max_connection_attempts
        self._indefinite_reconnect: bool = self._max_connection_attempts <= 0
        self._reconnect_delay: int = reconnect_delay
        self._reconnect_max_jitter: float = reconnect_max_jitter
        self._heartbeat_interval: float = heartbeat_interval

    def initialize_connection_params(self) -> None:
        """
        Initialize the connection parameters in the EClient.
        """
        self._eclient.reset()
        self._eclient._host = self._host
        self._eclient._port = self._port
        self._eclient.clientId = self._client_id

    def get_connection_attempts(self) -> int:
        """
        Get the current number of connection attempts.

        Returns
        -------
        int
            The current number of connection attempts.

        """
        return self._connection_attempts

    def reset_connection_attempts(self) -> None:
        """
        Reset the connection attempts counter to zero.
        """
        self._connection_attempts = 0

    def get_heartbeat_interval(self) -> float:
        """
        Get the heartbeat interval.

        Returns
        -------
        float
            The heartbeat interval in seconds.

        """
        return self._heartbeat_interval

    def calculate_reconnect_delay(self) -> float:
        """
        Calculate the delay before attempting to reconnect.

        Returns a base delay plus random jitter to prevent thundering herd problems.
        Uses exponential backoff with capped maximum delay.

        Returns
        -------
        float
            The delay in seconds before attempting to reconnect.

        """
        import random

        jitter = random.uniform(0, self._reconnect_max_jitter)  # noqa: S311
        backoff_factor = min(self._connection_attempts, 5)
        delay = self._reconnect_delay * (2 ** (backoff_factor - 1)) + jitter
        return min(delay, 60)

    async def send_version_info(self) -> ConnectionResult:
        """
        Send version information to the TWS/Gateway.

        Returns
        -------
        ConnectionResult
            The result of sending version information.

        """
        try:
            v100prefix = "API\0"
            v100version = f"v{MIN_CLIENT_VER}..{MAX_CLIENT_VER}"
            if self._eclient.connectionOptions:
                v100version += f" {self._eclient.connectionOptions}"
            msg = comm.make_msg(v100version)
            msg2 = str.encode(v100prefix, "ascii") + msg
            await asyncio.to_thread(functools.partial(self._eclient.conn.sendMsg, msg2))
            return ConnectionResult(success=True, message="Version info sent")
        except Exception as e:
            return ConnectionResult(
                success=False,
                error=e,
                message=f"Failed to send version info: {e}",
            )

    @handle_ib_error
    async def connect(self) -> None:
        """
        Establish the socket connection with TWS/Gateway.

        This initializes the connection, connects the socket, sends and receives version
        information, and then sets a flag that the connection has been successfully
        established.

        Raises
        ------
        ConnectionError
            If connection fails at any step or max connection attempts are reached.
        TimeoutError
            If connection steps timeout.

        """
        async with self._connection_lock:
            # Track connection attempt and check limits
            await self._track_connection_attempt()

            try:
                self.initialize_connection_params()
                conn_result = await self.connect_socket()
                if not conn_result.success:
                    raise ConnectionError(conn_result.message)

                self._eclient.setConnState(EClient.CONNECTING)
                handshake_result = await self.perform_handshake()
                if not handshake_result.success:
                    raise ConnectionError(handshake_result.message)

                # Ensure transition to CONNECTED state happens before setting connState
                # to avoid race conditions between state machine and EClient state
                await self._state_machine.transition_to(ClientState.CONNECTED)
                await self._connection_manager.set_connected(True, "Socket connected successfully")
                self._eclient.setConnState(EClient.CONNECTED)
                self._log.info(
                    f"Connected to IB (v{self._eclient.serverVersion_}) at "
                    f"{self._eclient.connTime.decode()} from {self._host}:{self._port} "
                    f"with client id: {self._client_id}",
                )
            except Exception as e:
                self._log.error(f"Connection failed: {e}")
                if self._eclient.wrapper:
                    self._eclient.wrapper.error(
                        NO_VALID_ID,
                        CONNECT_FAIL.code(),
                        f"{CONNECT_FAIL.msg()}: {e}",
                    )
                # Use correct ClientState since DISCONNECTED is not in the valid_transitions
                await self._state_machine.transition_to(ClientState.STOPPING)
                raise ConnectionError(f"Failed to connect: {e}") from e

    async def perform_handshake(self) -> ConnectionResult:
        """
        Handle the handshake process with the IB server.

        Returns
        -------
        ConnectionResult
            The result of the handshake.

        """
        try:
            version_result = await self.send_version_info()
            if not version_result.success:
                return version_result

            self._eclient.decoder = decoder.Decoder(
                wrapper=self._eclient.wrapper,
                serverVersion=self._eclient.serverVersion(),
            )
            server_result = await self.receive_server_info()
            return server_result
        except Exception as e:
            return ConnectionResult(
                success=False,
                error=e,
                message=f"Handshake failed: {e}",
            )

    async def connect_socket(self) -> ConnectionResult:
        """
        Connect the socket to TWS / Gateway and change the connection state to
        CONNECTING.

        It is an asynchronous method that runs within the event loop executor with
        a timeout to prevent hanging if the server is unreachable.

        Returns
        -------
        ConnectionResult
            The result of the connection attempt.

        """
        self._log.info(f"Connecting to {self._host}:{self._port} with client id: {self._client_id}")
        try:
            self._eclient.conn = Connection(self._host, self._port)
            await asyncio.wait_for(
                asyncio.to_thread(self._eclient.conn.connect),
                timeout=self._socket_connect_timeout,
            )
            if (
                self._eclient.conn
                and self._eclient.conn.socket
                and self._eclient.conn.socket.fileno() != -1
            ):
                return ConnectionResult(
                    success=True,
                    message=f"Socket connected to {self._host}:{self._port}",
                )
            return ConnectionResult(success=False, message="Socket connection status check failed")
        except TimeoutError as e:
            return ConnectionResult(
                success=False,
                error=e,
                message=f"Connection timed out after {self._socket_connect_timeout}s",
            )
        except ConnectionRefusedError as e:
            return ConnectionResult(
                success=False,
                error=e,
                message=f"Connection refused by {self._host}:{self._port}",
            )
        except OSError as e:
            return ConnectionResult(
                success=False,
                error=e,
                message=f"Socket error: {e}",
            )
        except Exception as e:
            return ConnectionResult(
                success=False,
                error=e,
                message=f"Unexpected error during socket connection: {e}",
            )

    async def receive_server_info(self) -> ConnectionResult:
        """
        Receive and process the server version information.

        Waits for the server to send its version information and connection time.
        Retries receiving this information up to a specified number of attempts.

        Returns
        -------
        ConnectionResult
            The result of receiving server info.

        """
        retries_remaining = 5
        fields: list[str] = []  # Type annotation added here

        try:
            # Use a timeout for the entire operation
            async with asyncio.timeout(self._handshake_timeout):
                while retries_remaining > 0 and len(fields) < 2:
                    buf = await asyncio.to_thread(self._eclient.conn.recvMsg)

                    # Check for empty buffer which might indicate connection issues
                    if not buf:
                        retries_remaining -= 1
                        self._log.warning(
                            f"Received empty buffer. Retries remaining: {retries_remaining}.",
                        )
                        await asyncio.sleep(0.5)
                        continue

                    # Process the received buffer
                    try:
                        _, msg, _ = comm.read_msg(buf)
                        if msg:
                            new_fields = comm.read_fields(msg)
                            fields.extend(
                                cast(list[str], new_fields),
                            )  # Explicit cast for type checker
                        else:
                            self._log.debug("Incomplete message received.")
                    except Exception as e:
                        self._log.warning(f"Error processing server message: {e}")

                    # Check if we need more data
                    if len(fields) < 2:
                        retries_remaining -= 1
                        self._log.warning(
                            "Incomplete server version information. "
                            f"Retries remaining: {retries_remaining}.",
                        )
                        await asyncio.sleep(0.5)  # Short wait between retries

                if len(fields) < 2:
                    return ConnectionResult(
                        success=False,
                        message="Failed to receive complete server version information after multiple attempts.",
                    )

                # Process the received fields
                self.process_server_version(fields)
                return ConnectionResult(
                    success=True,
                    message=f"Successfully received server version {self._eclient.serverVersion_}",
                )
        except TimeoutError as e:
            return ConnectionResult(
                success=False,
                error=e,
                message=f"Timeout receiving server version information after {self._handshake_timeout}s",
            )
        except Exception as e:
            return ConnectionResult(
                success=False,
                error=e,
                message=f"Error receiving server information: {e}",
            )

    async def _track_connection_attempt(self) -> None:
        """
        Track a connection attempt and check if maximum attempts have been reached.

        Raises
        ------
        ConnectionError
            If maximum connection attempts have been reached.

        """
        self._connection_attempts += 1
        if (
            not self._indefinite_reconnect
            and self._connection_attempts > self._max_connection_attempts
        ):
            self._log.error(f"Max connection attempts ({self._max_connection_attempts}) reached")
            await self._state_machine.transition_to(ClientState.STOPPING)
            raise ConnectionError(
                f"Max connection attempts ({self._max_connection_attempts}) reached",
            )

    def process_server_version(self, fields: list[str]) -> None:
        """
        Process and log the server version information.

        Extracts and sets the server version and connection time from the received fields.
        Logs the server version and connection time.

        Parameters
        ----------
        fields : list[str]
            The fields containing server version and connection time.

        """
        server_version, conn_time = int(fields[0]), fields[1]
        self._eclient.connTime = conn_time
        self._eclient.serverVersion_ = server_version
        self._eclient.decoder.serverVersion = server_version

    async def disconnect(self) -> None:
        """
        Disconnect from TWS/Gateway and clear the connection flags.
        """
        async with self._connection_lock:
            if self._is_in_state(ClientState.STOPPING, ClientState.STOPPED, ClientState.DISPOSED):
                self._log.info("Client is stopping or stopped, skipping disconnection")
                return
            try:
                # First transition state to STOPPING before disconnecting
                # to ensure other components know we're disconnecting intentionally
                await self._state_machine.transition_to(ClientState.STOPPING)
                if hasattr(self._eclient, "conn") and self._eclient.conn:
                    self._eclient.disconnect()
                await self._state_machine.transition_to(ClientState.STOPPED)
                self._log.info("Disconnected from Interactive Brokers API")
            except Exception as e:
                self._log.error(f"Disconnection failed: {e}")
                # Ensure we still transition to STOPPED state even on failure
                if not self._is_in_state(ClientState.STOPPED):
                    await self._state_machine.transition_to(ClientState.STOPPED)

    def handle_connection_closed(self) -> None:
        """
        Handle when the connection is closed by TWS/Gateway.

        Sets the pending requests to fail with ConnectionError.

        """
        for future in self._requests.get_futures():
            if not future.done():
                future.set_exception(ConnectionError("Socket disconnected"))

        # Use _create_task to ensure proper task management
        self._create_task(
            self.notify_connection_closed(),
            log_msg="Connection closed notification",
        )

    async def notify_connection_closed(self) -> None:
        """
        Notify the main client that the connection has been closed.

        This method is called by handle_connection_closed to update the connection
        status asynchronously.

        """
        await self.set_disconnected("Connection closed by TWS/Gateway")

    async def set_disconnected(self, reason: str) -> None:
        """
        Update connection state when connection is lost.

        This method ensures proper synchronization between the connection manager and
        the state machine.

        Parameters
        ----------
        reason : str
            The reason for the disconnection.

        """
        # First update connection manager to ensure callbacks are triggered
        await self._connection_manager.set_connected(False, reason)

        # Then update state machine if in a state that allows transition to RECONNECTING
        if self._is_in_state(
            ClientState.CONNECTED,
            ClientState.WAITING_API,
            ClientState.READY,
            ClientState.DEGRADED,
        ):
            await self._state_machine.transition_to(ClientState.RECONNECTING)
        elif not self._is_in_state(ClientState.STOPPING, ClientState.STOPPED, ClientState.DISPOSED):
            # If unexpected state, go to STOPPING to allow proper shutdown
            await self._state_machine.transition_to(ClientState.STOPPING)
