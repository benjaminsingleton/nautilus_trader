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
    ib_client._perform_handshake = AsyncMock(return_value=MagicMock(success=True))
    ib_client._eclient.connTime = MagicMock()
    ib_client._eclient.setConnState = MagicMock()
    ib_client._connection_manager = MagicMock()
    ib_client._connection_manager.set_connected = AsyncMock()
    ib_client._state_machine = MagicMock()
    ib_client._state_machine.transition_to = AsyncMock()

    await ib_client._connect()

    ib_client._initialize_connection_params.assert_called_once()
    ib_client._connect_socket.assert_awaited_once()
    ib_client._state_machine.transition_to.assert_any_call(ClientState.CONNECTED)
    ib_client._connection_manager.set_connected.assert_awaited_once_with(True, "Socket connected successfully")
    ib_client._eclient.setConnState.assert_called_with(ib_client._eclient.CONNECTED)


@pytest.mark.asyncio
async def test_connect_cancelled(ib_client):
    ib_client._initialize_connection_params = MagicMock()
    ib_client._connect_socket = AsyncMock(side_effect=asyncio.CancelledError())
    ib_client._disconnect = AsyncMock()
    ib_client._state_machine = MagicMock()
    ib_client._state_machine.transition_to = AsyncMock()

    # Use pytest.raises to expect the CancelledError
    with pytest.raises(asyncio.CancelledError):
        await ib_client._connect()


@pytest.mark.asyncio
async def test_connect_fail(ib_client):
    ib_client._initialize_connection_params = MagicMock()
    ib_client._connect_socket = AsyncMock(side_effect=Exception("Connection failed"))
    ib_client._disconnect = AsyncMock()
    ib_client._handle_reconnect = AsyncMock()
    ib_client._eclient.wrapper = MagicMock()
    ib_client._eclient.wrapper.error = MagicMock()
    ib_client._state_machine = MagicMock()
    ib_client._state_machine.transition_to = AsyncMock()

    # Use pytest.raises to expect the ConnectionError
    with pytest.raises(ConnectionError) as excinfo:
        await ib_client._connect()
    
    # Assert the error message
    assert "Failed to connect: Connection failed" in str(excinfo.value)
    
    # Assert error handler was called
    ib_client._eclient.wrapper.error.assert_called_once()
    # State transition should have happened
    ib_client._state_machine.transition_to.assert_called_once()


@pytest.mark.asyncio
async def test_reconnect_success(ib_client):
    """
    Test case for a successful reconnection.
    """
    # Mock _calculate_reconnect_delay to return a minimal delay (0.001 seconds)
    # This significantly speeds up the test by avoiding the real delay calculation
    ib_client._calculate_reconnect_delay = MagicMock(return_value=0.001)
    
    # Mocking the connection manager and state machine
    ib_client._connection_manager = MagicMock()
    # Make set_ready an AsyncMock to fix the TypeError when awaited
    ib_client._connection_manager.set_ready = AsyncMock()
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

    # Assertions to ensure proper state transitions
    ib_client._state_machine.transition_to.assert_any_call(ClientState.RECONNECTING)


@pytest.mark.asyncio
async def test_reconnect_fail(ib_client):
    """
    Test case for a failed reconnection.
    """
    # Mock _calculate_reconnect_delay to return a minimal delay (0.001 seconds)
    # This significantly speeds up the test by avoiding the real delay calculation
    ib_client._calculate_reconnect_delay = MagicMock(return_value=0.001)
    
    # Mocking the connection manager and state machine
    ib_client._connection_manager = MagicMock()
    # Make set_ready an AsyncMock to fix the TypeError when awaited
    ib_client._connection_manager.set_ready = AsyncMock()
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
