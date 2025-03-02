import asyncio
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.interactive_brokers.client.common import ClientState


@pytest.mark.asyncio
async def test_connect_success(ib_client):
    # Mock methods on ConnectionService instead of client
    ib_client._connection_service.initialize_connection_params = MagicMock()
    ib_client._connection_service.connect_socket = AsyncMock(
        return_value=MagicMock(success=True),
    )
    ib_client._connection_service.perform_handshake = AsyncMock(
        return_value=MagicMock(success=True),
    )
    ib_client._eclient.connTime = MagicMock()
    ib_client._eclient.setConnState = MagicMock()

    # Make sure we're mocking objects that are referenced inside ConnectionService
    ib_client._connection_service._eclient = ib_client._eclient
    ib_client._connection_service._state_machine = MagicMock()
    ib_client._connection_service._state_machine.transition_to = AsyncMock()
    ib_client._connection_service._connection_manager = MagicMock()
    ib_client._connection_service._connection_manager.set_connected = AsyncMock()

    await ib_client._connection_service.connect()

    ib_client._connection_service.initialize_connection_params.assert_called_once()
    ib_client._connection_service.connect_socket.assert_awaited_once()
    ib_client._connection_service._state_machine.transition_to.assert_any_call(
        ClientState.CONNECTED,
    )
    ib_client._connection_service._connection_manager.set_connected.assert_awaited_once_with(
        True,
        "Socket connected successfully",
    )
    ib_client._eclient.setConnState.assert_called_with(ib_client._eclient.CONNECTED)


@pytest.mark.asyncio
async def test_connect_cancelled(ib_client):
    # Mock methods on ConnectionService instead of client
    ib_client._connection_service.initialize_connection_params = MagicMock()
    ib_client._connection_service.connect_socket = AsyncMock(side_effect=asyncio.CancelledError())

    # Mock the state machine inside ConnectionService
    ib_client._connection_service._state_machine = MagicMock()
    ib_client._connection_service._state_machine.transition_to = AsyncMock()

    # Use pytest.raises to expect the CancelledError
    with pytest.raises(asyncio.CancelledError):
        await ib_client._connection_service.connect()


@pytest.mark.asyncio
async def test_connect_fail(ib_client):
    # Mock methods on ConnectionService instead of client
    ib_client._connection_service.initialize_connection_params = MagicMock()
    ib_client._connection_service.connect_socket = AsyncMock(
        side_effect=Exception("Connection failed"),
    )

    # Mock needed objects inside the ConnectionService
    ib_client._connection_service._eclient = ib_client._eclient
    ib_client._connection_service._eclient.wrapper = MagicMock()
    ib_client._connection_service._eclient.wrapper.error = MagicMock()
    ib_client._connection_service._state_machine = MagicMock()
    ib_client._connection_service._state_machine.transition_to = AsyncMock()

    # Use pytest.raises to expect the ConnectionError
    with pytest.raises(ConnectionError) as excinfo:
        await ib_client._connection_service.connect()

    # Assert the error message
    assert "Failed to connect: Connection failed" in str(excinfo.value)

    # Assert error handler was called
    ib_client._connection_service._eclient.wrapper.error.assert_called_once()
    # State transition should have happened
    ib_client._connection_service._state_machine.transition_to.assert_called_once()


@pytest.mark.asyncio
async def test_reconnect_success(ib_client):
    """
    Test case for a successful reconnection.
    """
    # Mock _calculate_reconnect_delay to return a minimal delay (0.001 seconds)
    # This significantly speeds up the test by avoiding the real delay calculation
    ib_client._calculate_reconnect_delay = MagicMock(return_value=0.001)

    # Mock set_disconnected to avoid actual logic
    ib_client._connection_service.set_disconnected = AsyncMock()

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

    # Check that set_disconnected was called
    ib_client._connection_service.set_disconnected.assert_awaited_once_with(
        "Disconnected by watchdog",
    )


@pytest.mark.asyncio
async def test_reconnect_fail(ib_client):
    """
    Test case for a failed reconnection.
    """
    # Mock _calculate_reconnect_delay to return a minimal delay (0.001 seconds)
    # This significantly speeds up the test by avoiding the real delay calculation
    ib_client._calculate_reconnect_delay = MagicMock(return_value=0.001)

    # Mock set_disconnected to avoid actual logic
    ib_client._connection_service.set_disconnected = AsyncMock()

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

    # Check that set_disconnected was called
    ib_client._connection_service.set_disconnected.assert_awaited_once_with(
        "Disconnected by watchdog",
    )
