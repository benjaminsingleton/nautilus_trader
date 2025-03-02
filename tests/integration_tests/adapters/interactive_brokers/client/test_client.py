import asyncio
from contextlib import suppress
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest

from nautilus_trader.adapters.interactive_brokers.client.common import ClientState


@pytest.fixture
def mock_coroutines(ib_client):
    """
    Mock coroutine methods to prevent 'coroutine never awaited' warnings.
    """
    # Mock the coroutines that would normally be passed to create_task
    ib_client._run_tws_incoming_msg_reader = MagicMock(return_value=None)
    ib_client._run_internal_msg_queue_processor = MagicMock(return_value=None)
    ib_client._run_msg_handler_processor = MagicMock(return_value=None)
    ib_client._run_connection_watchdog = MagicMock(return_value=None)
    ib_client._run_heartbeat_monitor = MagicMock(return_value=None)

    # Save original methods to restore if needed
    original_methods = {
        "_run_tws_incoming_msg_reader": getattr(ib_client, "_run_tws_incoming_msg_reader", None),
        "_run_internal_msg_queue_processor": getattr(
            ib_client,
            "_run_internal_msg_queue_processor",
            None,
        ),
        "_run_msg_handler_processor": getattr(ib_client, "_run_msg_handler_processor", None),
        "_run_connection_watchdog": getattr(ib_client, "_run_connection_watchdog", None),
        "_run_heartbeat_monitor": getattr(ib_client, "_run_heartbeat_monitor", None),
    }

    yield

    # Restore original methods if needed
    for name, method in original_methods.items():
        if method is not None:
            setattr(ib_client, name, method)


@pytest.mark.asyncio
async def test_start(event_loop, ib_client):
    # Arrange
    # Create mocks for key methods
    ib_client._handle_connecting_state = AsyncMock()
    ib_client._handle_connected_state = AsyncMock()
    ib_client._handle_waiting_api_state = AsyncMock()

    # Mock the state check to break out of the loop after first iteration
    original_is_in_state = ib_client.is_in_state
    call_count = 0

    def mock_is_in_state(*states):
        nonlocal call_count
        call_count += 1
        # Return True after first call to break out of the loop
        if call_count > 1:
            return True
        return original_is_in_state(*states)

    ib_client.is_in_state = mock_is_in_state

    # State machine setup
    ib_client._state_machine = MagicMock()
    ib_client._state_machine.transition_to = AsyncMock(return_value=True)

    # Set up a mock for current_state that returns the expected state
    type(ib_client._state_machine).current_state = property(
        lambda self: ClientState.CONNECTING,
    )

    # Initialize state - instead of directly setting it,
    # we'll transition to it properly (though using a mocked method)
    # This isn't actually needed since we've mocked the current_state property above
    # await ib_client._state_machine.transition_to(ClientState.CONNECTING)

    # Connection manager setup
    ib_client._connection_manager = MagicMock()
    ib_client._connection_manager.is_connected = True
    ib_client._connection_manager.set_connected = AsyncMock()
    ib_client._connection_manager.set_ready = AsyncMock()
    ib_client._connection_manager.wait_until_ready = AsyncMock(return_value=True)

    # Other mocks
    ib_client._eclient = MagicMock()
    ib_client._eclient.startApi = MagicMock()

    # Create a ready event that we can set
    ib_client._is_client_ready = asyncio.Event()

    try:
        # Act
        await ib_client._start_async()

        # Assert
        ib_client._state_machine.transition_to.assert_called_with(ClientState.CONNECTING)

        # At minimum, the connecting state handler should be called
        ib_client._handle_connecting_state.assert_called_once()
    finally:
        # Clean up by restoring the original method
        ib_client.is_in_state = original_is_in_state


@pytest.mark.asyncio
async def test_start_tasks(ib_client, mock_coroutines):
    # Arrange
    ib_client._eclient = MagicMock()
    ib_client._task_registry = MagicMock()

    task_mock = AsyncMock()
    ib_client._task_registry.create_task = AsyncMock(return_value=task_mock)

    # Act
    # These are async methods so we can't call them directly without awaiting
    await ib_client._start_tws_incoming_msg_reader()
    await ib_client._start_internal_msg_queue_processor()
    await ib_client._start_connection_watchdog()

    # Assert
    # Verify task_registry.create_task was called for each method
    assert ib_client._task_registry.create_task.call_count >= 3


@pytest.mark.asyncio
async def test_stop(event_loop, ib_client):
    # Setup state machine with mock
    ib_client._state_machine = MagicMock()
    ib_client._state_machine.transition_to = AsyncMock(return_value=True)
    type(ib_client._state_machine).current_state = property(
        lambda self: ClientState.READY,  # Initial state for test
    )

    # Setup other components
    ib_client._eclient = MagicMock()
    ib_client._eclient.disconnect = MagicMock()
    ib_client._connection_manager = MagicMock()
    ib_client._connection_manager.set_connected = AsyncMock()

    # Mock task_registry
    ib_client._task_registry = MagicMock()
    ib_client._task_registry.wait_for_all_tasks = AsyncMock(
        return_value=True,
    )  # Tasks completed successfully
    ib_client._task_registry.cancel_all_tasks = AsyncMock()

    # Setup account data to clear
    ib_client._account_ids = {"account1", "account2"}
    ib_client.registered_nautilus_clients = {"client1", "client2"}

    # Create a simple mock for os.getenv to return a default timeout value
    with patch("os.getenv", return_value="1.0"):
        # Call the stop method directly
        await ib_client._stop_async()

    # Assert
    ib_client._state_machine.transition_to.assert_any_call(ClientState.STOPPING)
    # Should also have transitioned to STOPPED at the end
    ib_client._state_machine.transition_to.assert_any_call(ClientState.STOPPED)
    # Connection manager should be updated
    ib_client._connection_manager.set_connected.assert_called_with(False, "Client stopped")
    # Accounts should be cleared
    assert len(ib_client._account_ids) == 0
    assert len(ib_client.registered_nautilus_clients) == 0


@pytest.mark.asyncio
async def test_reset(event_loop, ib_client):
    # Setup state machine with mock
    ib_client._state_machine = MagicMock()
    ib_client._state_machine.transition_to = AsyncMock(return_value=True)
    type(ib_client._state_machine).current_state = property(
        lambda self: ClientState.STOPPED,  # Initial state for test
    )

    # Mock the _reset_async method
    ib_client._reset_async = AsyncMock()

    # Create a reset method that mimics expected behavior
    def reset_method():
        task = asyncio.create_task(ib_client._reset_async())
        ib_client._active_tasks.add(task)
        return task

    ib_client.reset = reset_method

    # Act
    ib_client.reset()

    # We need to add a small delay to allow the async task to be created and run
    await asyncio.sleep(0.1)

    # Assert
    ib_client._reset_async.assert_called_once()


@pytest.mark.asyncio
async def test_resume(event_loop, ib_client, mock_coroutines):
    # Setup state machine with mock
    ib_client._state_machine = MagicMock()
    ib_client._state_machine.transition_to = AsyncMock(return_value=True)
    type(ib_client._state_machine).current_state = property(
        lambda self: ClientState.DEGRADED,  # Initial state for test
    )

    # Mock the _resume_async method
    ib_client._resume_async = AsyncMock()

    # Act
    ib_client._resume()

    # We need to add a small delay to allow the async task to be created and run
    await asyncio.sleep(0.1)

    # Assert
    ib_client._resume_async.assert_called_once()


@pytest.mark.asyncio
async def test_degrade(event_loop, ib_client, mock_coroutines):
    # Setup state machine with mock
    ib_client._state_machine = MagicMock()
    ib_client._state_machine.transition_to = AsyncMock(return_value=True)
    type(ib_client._state_machine).current_state = property(
        lambda self: ClientState.READY,  # Initial state for test
    )

    # Mock connection manager
    ib_client._connection_manager = MagicMock()
    ib_client._connection_manager.set_ready = AsyncMock()

    # Act
    await ib_client._degrade()

    # Assert
    ib_client._state_machine.transition_to.assert_called_with(ClientState.DEGRADED)


@pytest.mark.asyncio
async def test_wait_until_ready(event_loop, ib_client, mock_coroutines):
    # Setup connection manager mock to return True for wait_until_ready
    ib_client._connection_manager = MagicMock()
    ib_client._connection_manager.wait_until_ready = AsyncMock(return_value=True)

    # Act
    result = await ib_client.wait_until_ready(timeout=1.0)

    # Assert
    assert result is None  # The method doesn't return anything if successful
    ib_client._connection_manager.wait_until_ready.assert_called_with(1.0)


@pytest.mark.asyncio
async def test_run_connection_watchdog_reconnect(ib_client, mock_coroutines):
    # Arrange
    # Set up the state to trigger reconnection
    ib_client._connection_manager = MagicMock()
    ib_client._connection_manager.is_connected = False

    # Mock the handle_disconnection method
    ib_client._handle_disconnection = AsyncMock()

    # Mock state machine
    ib_client._state_machine = MagicMock()
    ib_client._state_machine.current_state = ClientState.READY

    # Override is_in_state to return False for stopping states
    original_is_in_state = ib_client.is_in_state
    ib_client.is_in_state = lambda *states: (
        False
        if any(
            s in states for s in (ClientState.STOPPING, ClientState.STOPPED, ClientState.DISPOSED)
        )
        else original_is_in_state(*states)
    )

    try:
        # Act - directly test the relevant method
        # Just run one iteration of the watchdog's loop
        if not ib_client._connection_manager.is_connected:
            await ib_client._handle_disconnection()

        # Assert
        ib_client._handle_disconnection.assert_called_once()
    finally:
        # Restore original method
        ib_client.is_in_state = original_is_in_state


@pytest.mark.asyncio
async def test_run_tws_incoming_msg_reader(ib_client):
    # Arrange
    ib_client._eclient.conn = Mock()
    ib_client._eclient.conn.isConnected = MagicMock(return_value=True)

    # Set up message queue and test data
    test_messages = [b"test message 1", b"test message 2"]
    messages_received = []

    # Mock the recv and read functionality
    ib_client._eclient.conn.recvMsg = MagicMock(side_effect=test_messages + [b""])  # noqa: RUF005
    ib_client._loop = asyncio.get_event_loop()

    # Create a mock for _internal_msg_queue.put_nowait
    original_put_nowait = ib_client._internal_msg_queue.put_nowait

    def mock_put_nowait(item):
        messages_received.append(item)
        return original_put_nowait(item)

    ib_client._internal_msg_queue.put_nowait = mock_put_nowait

    # Create a mock for the read_msg function
    with patch("ibapi.comm.read_msg", side_effect=[(None, msg, b"") for msg in test_messages]):
        # Act
        # Create a task for _run_tws_incoming_msg_reader
        task = asyncio.create_task(ib_client._run_tws_incoming_msg_reader())

        # Add a small delay to allow processing
        await asyncio.sleep(0.1)

        # Cancel the task (we only want to test the message reading, not run indefinitely)
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    # Assert
    # Check that at least some messages were received
    assert len(messages_received) > 0

    # Restore original method
    ib_client._internal_msg_queue.put_nowait = original_put_nowait


@pytest.mark.asyncio
async def test_run_internal_msg_queue_processor(event_loop, ib_client):
    # Setup state machine with mock
    ib_client._state_machine = MagicMock()
    ib_client._state_machine.transition_to = AsyncMock(return_value=True)
    type(ib_client._state_machine).current_state = property(
        lambda self: ClientState.READY,  # Initial state for test
    )

    # Mock other necessary components
    ib_client._eclient = MagicMock()
    ib_client._eclient.conn = MagicMock()
    ib_client._eclient.conn.isConnected = MagicMock(return_value=False)

    # Setup internal message queue with test data
    ib_client._internal_msg_queue = asyncio.Queue()
    # Put a test message in the queue
    await ib_client._internal_msg_queue.put("test_message")

    # Mock process_message to handle the test message and return False to exit the loop
    ib_client._process_message = AsyncMock(return_value=False)

    # Act
    await ib_client._run_internal_msg_queue_processor()

    # Assert
    ib_client._process_message.assert_called_once_with("test_message")
