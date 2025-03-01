import asyncio
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from ibapi.common import NO_VALID_ID
from ibapi.errors import CONNECT_FAIL

from nautilus_trader.adapters.interactive_brokers.client.common import ClientState


@pytest.mark.asyncio
async def test_connect_success(ib_client):
    ib_client._initialize_connection_params = MagicMock()
    ib_client._connect_socket = AsyncMock()
    ib_client._send_version_info = AsyncMock()
    ib_client._receive_server_info = AsyncMock()
    ib_client._eclient.connTime = MagicMock()
    ib_client._eclient.setConnState = MagicMock()
    ib_client._connection_manager = MagicMock()
    ib_client._connection_manager.set_connected = MagicMock()
    ib_client._state_machine = MagicMock()
    ib_client._state_machine.transition_to = AsyncMock()

    await ib_client._connect()

    ib_client._initialize_connection_params.assert_called_once()
    ib_client._connect_socket.assert_awaited_once()
    ib_client._send_version_info.assert_awaited_once()
    ib_client._receive_server_info.assert_awaited_once()
    ib_client._eclient.setConnState.assert_called_with(ib_client._eclient.CONNECTED)
    ib_client._connection_manager.set_connected.assert_called_once()
    ib_client._state_machine.transition_to.assert_called_with(ClientState.CONNECTED)


@pytest.mark.asyncio
async def test_connect_cancelled(ib_client):
    ib_client._initialize_connection_params = MagicMock()
    ib_client._connect_socket = AsyncMock(side_effect=asyncio.CancelledError())
    ib_client._disconnect = AsyncMock()

    await ib_client._connect()

    ib_client._disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_connect_fail(ib_client):
    ib_client._initialize_connection_params = MagicMock()
    ib_client._connect_socket = AsyncMock(side_effect=Exception("Connection failed"))
    ib_client._disconnect = AsyncMock()
    ib_client._handle_reconnect = AsyncMock()
    ib_client._eclient.wrapper.error = MagicMock()

    await ib_client._connect()

    ib_client._eclient.wrapper.error.assert_called_with(
        NO_VALID_ID,
        CONNECT_FAIL.code(),
        CONNECT_FAIL.msg(),
    )
    ib_client._handle_reconnect.assert_awaited_once()


# Test for successful reconnection
@pytest.mark.asyncio
async def test_reconnect_success(ib_client):
    """
    Test case for a successful reconnection.
    """
    # Mocking the connection manager and state machine
    ib_client._connection_manager = MagicMock()
    ib_client._state_machine = MagicMock()
    ib_client._state_machine.transition_to = AsyncMock()
    ib_client._state_machine.current_state = ClientState.READY

    # Mocking the disconnect and connect methods
    ib_client.disconnect = AsyncMock()
    ib_client.connect = AsyncMock()

    # Simulating a successful reconnection
    ib_client._connection_manager.is_connected = False
    ib_client.isConnected = MagicMock(return_value=True)

    # Attempting to reconnect
    await ib_client._handle_disconnection()
    await ib_client._handle_connection()

    # Assertions to ensure disconnect and connect methods were called appropriately
    ib_client._state_machine.transition_to.assert_any_call(ClientState.RECONNECTING)
    ib_client._state_machine.transition_to.assert_any_call(ClientState.CONNECTING)


# Test for failed reconnection
@pytest.mark.asyncio
async def test_reconnect_fail(ib_client):
    """
    Test case for a failed reconnection.
    """
    # Mocking the connection manager and state machine
    ib_client._connection_manager = MagicMock()
    ib_client._state_machine = MagicMock()
    ib_client._state_machine.transition_to = AsyncMock()
    ib_client._state_machine.current_state = ClientState.READY

    # Mocking the disconnect and connect methods with connect failing
    ib_client.disconnect = AsyncMock()
    ib_client._connect = AsyncMock(side_effect=Exception("Failed to reconnect"))
    ib_client._handle_reconnect = AsyncMock()

    # Simulating a failed reconnection
    ib_client._connection_manager.is_connected = False
    ib_client.isConnected = MagicMock(return_value=False)

    # Attempting to reconnect and expecting the proper error handling path to be called
    await ib_client._handle_disconnection()

    # We need to mock the wait_until_connected timeout behavior
    ib_client._wait_until_connected = AsyncMock(side_effect=TimeoutError())

    # Mock that we attempt to connect but it fails
    await ib_client._handle_reconnect()

    # Assertions to ensure proper state transitions
    ib_client._state_machine.transition_to.assert_any_call(ClientState.RECONNECTING)
    ib_client._connect.assert_called_once()
    # The state should eventually go to RECONNECTING again when connect fails
    ib_client._state_machine.transition_to.assert_any_call(ClientState.RECONNECTING)
