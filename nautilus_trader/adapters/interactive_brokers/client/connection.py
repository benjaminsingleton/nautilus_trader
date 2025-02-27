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
from typing import cast

from ibapi import comm
from ibapi import decoder
from ibapi.client import EClient
from ibapi.common import NO_VALID_ID
from ibapi.connection import Connection
from ibapi.errors import CONNECT_FAIL
from ibapi.server_versions import MAX_CLIENT_VER
from ibapi.server_versions import MIN_CLIENT_VER

from nautilus_trader.adapters.interactive_brokers.client.common import BaseMixin
from nautilus_trader.adapters.interactive_brokers.client.error import handle_ib_error


class ConnectionState(enum.Enum):
    """
    Defines the connection states for the Interactive Brokers client.

    These states are used for more granular tracking of the socket connection status.

    """

    DISCONNECTED = 0  # Not connected to TWS/Gateway
    CONNECTING = 1  # In the process of establishing connection
    HANDSHAKING = 2  # Connection established, performing API handshake
    CONNECTED = 3  # Fully connected with handshake complete
    DISCONNECTING = 4  # In the process of disconnecting


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
        if self.success:
            return f"ConnectionResult(success=True, message='{self.message}')"
        return f"ConnectionResult(success=False, error='{self.error}', message='{self.message}')"


class InteractiveBrokersClientConnectionMixin(BaseMixin):
    """
    Manages the connection to TWS/Gateway for the InteractiveBrokersClient.

    This class is responsible for establishing and maintaining the socket connection,
    handling server communication, monitoring the connection's health, and managing
    reconnections.

    """

    def __init__(self) -> None:
        """
        Initialize connection state.

        Note: This is not actually called directly as a constructor, but is used
        for IDE type hinting and documentation. The attributes defined here
        will be initialized in the main class that inherits from this mixin.

        """
        self._conn_state = ConnectionState.DISCONNECTED
        self._socket_connect_timeout = 10  # seconds
        self._handshake_timeout = 5  # seconds
        self._connection_lock = asyncio.Lock()  # Lock for connection operations

    @handle_ib_error
    async def _connect(self) -> None:
        """
        Establish the socket connection with TWS/Gateway.

        This initializes the connection, connects the socket, sends and receives version
        information, and then sets a flag that the connection has been successfully
        established.

        Raises
        ------
        ConnectionError
            If connection fails at any step.
        TimeoutError
            If connection steps timeout.

        """
        async with self._connection_lock:  # Ensure only one connection attempt at a time
            try:
                self._conn_state = ConnectionState.CONNECTING
                self._initialize_connection_params()

                # Connect socket
                conn_result = await self._connect_socket()
                if not conn_result.success:
                    raise ConnectionError(conn_result.message)

                self._conn_state = ConnectionState.HANDSHAKING
                self._eclient.setConnState(EClient.CONNECTING)

                # Send version info
                version_result = await self._send_version_info()
                if not version_result.success:
                    raise ConnectionError(version_result.message)

                # Initialize decoder
                self._eclient.decoder = decoder.Decoder(
                    wrapper=self._eclient.wrapper,
                    serverVersion=self._eclient.serverVersion(),
                )

                # Receive server info
                server_result = await self._receive_server_info()
                if not server_result.success:
                    raise ConnectionError(server_result.message)

                self._conn_state = ConnectionState.CONNECTED
                self._eclient.setConnState(EClient.CONNECTED)

                self._log.info(
                    f"Connected to Interactive Brokers (v{self._eclient.serverVersion_}) "
                    f"at {self._eclient.connTime.decode()} from {self._host}:{self._port} "
                    f"with client id: {self._client_id}.",
                )
            except asyncio.CancelledError:
                self._log.info("Connection cancelled.")
                await self._disconnect()
                raise
            except Exception as e:
                self._log.error(f"Connection failed: {e}")
                self._conn_state = ConnectionState.DISCONNECTED

                if self._eclient.wrapper:
                    self._eclient.wrapper.error(
                        NO_VALID_ID,
                        CONNECT_FAIL.code(),
                        f"{CONNECT_FAIL.msg()}: {e!s}",
                    )

                # Re-raise for proper handling in calling code
                raise ConnectionError(f"Failed to connect to IB: {e}") from e

    async def _disconnect(self) -> None:
        """
        Disconnect from TWS/Gateway and clear the connection flags.
        """
        async with self._connection_lock:
            try:
                self._conn_state = ConnectionState.DISCONNECTING

                if hasattr(self._eclient, "conn") and self._eclient.conn:
                    self._eclient.disconnect()

                self._conn_state = ConnectionState.DISCONNECTED
                self._log.info("Disconnected from Interactive Brokers API.")
            except Exception as e:
                self._log.error(f"Disconnection failed: {e}")
                self._conn_state = ConnectionState.DISCONNECTED

    def _initialize_connection_params(self) -> None:
        """
        Initialize the connection parameters before attempting to connect.

        Sets up the host, port, and client ID for the EClient instance and increments
        the connection attempt counter. Logs the attempt information.

        """
        self._eclient.reset()
        self._eclient._host = self._host
        self._eclient._port = self._port
        self._eclient.clientId = self._client_id

    async def _connect_socket(self) -> ConnectionResult:
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
        self._log.info(
            f"Connecting to {self._host}:{self._port} with client id: {self._client_id}",
        )

        try:
            self._eclient.conn = Connection(self._host, self._port)

            # This call will raise a socket error if connection fails
            await asyncio.wait_for(
                asyncio.to_thread(self._eclient.conn.connect),
                timeout=self._socket_connect_timeout,
            )

            # If we reach here, connection was successful
            # Check if socket is actually connected
            if (
                self._eclient.conn
                and self._eclient.conn.socket
                and self._eclient.conn.socket.fileno() != -1
            ):
                return ConnectionResult(
                    success=True,
                    message=f"Socket connected to {self._host}:{self._port}",
                )
            else:
                return ConnectionResult(
                    success=False,
                    message=f"Socket connection status check failed for {self._host}:{self._port}",
                )

        except TimeoutError as e:
            return ConnectionResult(
                success=False,
                error=e,
                message=f"Connection to {self._host}:{self._port} timed out after {self._socket_connect_timeout}s",
            )
        except Exception as e:
            return ConnectionResult(
                success=False,
                error=e,
                message=f"Socket connection failed: {e}",
            )

    async def _send_version_info(self) -> ConnectionResult:
        """
        Send the API version information to TWS / Gateway.

        Constructs and sends a message containing the API version prefix and the version
        range supported by the client. This is part of the initial handshake process
        with the server.

        Returns
        -------
        ConnectionResult
            The result of sending the version info.

        """
        try:
            v100prefix = "API\0"
            v100version = f"v{MIN_CLIENT_VER}..{MAX_CLIENT_VER}"

            if self._eclient.connectionOptions:
                v100version += f" {self._eclient.connectionOptions}"

            msg = comm.make_msg(v100version)
            msg2 = str.encode(v100prefix, "ascii") + msg

            await asyncio.to_thread(functools.partial(self._eclient.conn.sendMsg, msg2))
            return ConnectionResult(success=True, message="Version info sent successfully")
        except Exception as e:
            return ConnectionResult(
                success=False,
                error=e,
                message=f"Failed to send version info: {e}",
            )

    async def _receive_server_info(self) -> ConnectionResult:
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
                self._process_server_version(fields)
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

    def _process_server_version(self, fields: list[str]) -> None:
        """
        Process and log the server version information. Extracts and sets the server
        version and connection time from the received fields. Logs the server version
        and connection time.

        Parameters
        ----------
        fields : list[str]
            The fields containing server version and connection time.

        """
        server_version, conn_time = int(fields[0]), fields[1]
        self._eclient.connTime = conn_time
        self._eclient.serverVersion_ = server_version
        self._eclient.decoder.serverVersion = server_version

    def process_connection_closed(self) -> None:
        """
        Indicate the API connection has closed.

        Following a API <-> TWS broken socket connection, this function is not called
        automatically but must be triggered by API client code.

        """
        # Set futures to exception state to unblock any pending requests
        for future in self._requests.get_futures():
            if not future.done():
                future.set_exception(ConnectionError("Socket disconnected."))

        # Update connection status through main client class
        # Store the task to prevent garbage collection
        task = asyncio.create_task(
            self._notify_connection_closed(),
        )
        # Add handlers to ensure proper cleanup
        task.add_done_callback(
            lambda t: self._log.debug("Connection closed notification completed"),
        )

    async def _notify_connection_closed(self) -> None:
        """
        Notify the main client that the connection has been closed.

        This method is called by process_connection_closed to update the connection
        status asynchronously.

        """
        try:
            # This relies on the main client having a connection manager
            await self._set_disconnected("Connection closed by TWS/Gateway")
        except Exception as e:
            self._log.error(f"Error notifying connection closed: {e}")

    async def _set_disconnected(self, reason: str) -> None:
        """
        Set the disconnected state.

        This is a placeholder method that should be overridden by the main client
        class to provide the actual implementation.

        Parameters
        ----------
        reason : str
            The reason for disconnection.

        """
        # This will be overridden by the main client class
        self._log.debug(f"Connection closed: {reason}")
