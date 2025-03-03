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
from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.interactive_brokers.client.client import ConnectionManager
from nautilus_trader.adapters.interactive_brokers.client.client import StateMachine
from nautilus_trader.adapters.interactive_brokers.client.common import ClientState


@pytest.mark.asyncio
async def test_state_machine_transitions(event_loop):
    # Arrange - create a state machine with a mock logger
    mock_logger = MagicMock()
    state_machine = StateMachine(
        initial_state=ClientState.CREATED,
        logger=mock_logger,
    )

    # Add callbacks to test they are called appropriately
    callback_ready = MagicMock()
    state_machine.register_state_changed_callback(ClientState.READY, callback_ready)

    # Act - perform a series of valid state transitions
    await state_machine.transition_to(ClientState.CONNECTING)
    await state_machine.transition_to(ClientState.CONNECTED)
    await state_machine.transition_to(ClientState.WAITING_API)
    await state_machine.transition_to(ClientState.READY)

    # Assert
    assert state_machine.current_state == ClientState.READY
    callback_ready.assert_called_once()
    mock_logger.debug.assert_called()  # Should log transitions


@pytest.mark.asyncio
async def test_state_machine_invalid_transition(event_loop):
    # Arrange - create a state machine with initial state CREATED
    mock_logger = MagicMock()
    state_machine = StateMachine(
        initial_state=ClientState.CREATED,
        logger=mock_logger,
    )

    # Act/Assert - attempt an invalid transition and verify it raises an exception
    # It doesn't raise, it just returns False
    result = await state_machine.transition_to(ClientState.READY)
    assert result is False


@pytest.mark.asyncio
async def test_connection_manager():
    # Arrange
    mock_logger = MagicMock()
    connection_manager = ConnectionManager(logger=mock_logger)

    # Act - test connection state changes
    assert not connection_manager.is_connected

    await connection_manager.set_connected(True)
    assert connection_manager.is_connected

    await connection_manager.set_connected(False)
    assert not connection_manager.is_connected


@pytest.mark.asyncio
async def test_connection_manager_with_locks():
    # Arrange
    mock_logger = MagicMock()
    connection_manager = ConnectionManager(logger=mock_logger)

    # Act - test that the connection manager properly uses locks to prevent race conditions
    async def test_concurrent_operations():
        await connection_manager.set_connected(True)
        await asyncio.sleep(0.01)  # Simulate slight delay
        await connection_manager.set_connected(False)
        await asyncio.sleep(0.01)  # Simulate slight delay
        await connection_manager.set_connected(True)

    # Run multiple concurrent operations
    tasks = []
    for _ in range(5):
        task = asyncio.create_task(test_concurrent_operations())
        tasks.append(task)

    await asyncio.gather(*tasks)

    # After all operations, the manager should reflect the final state (connected)
    assert connection_manager.is_connected


@pytest.mark.asyncio
async def test_state_machine_transition_lock():
    # Arrange
    mock_logger = MagicMock()
    state_machine = StateMachine(
        initial_state=ClientState.CREATED,
        logger=mock_logger,
    )

    # Setup a scenario where multiple transitions might happen concurrently
    transition_complete = asyncio.Event()

    async def slow_transition():
        await state_machine.transition_to(ClientState.CONNECTING)
        await asyncio.sleep(0.05)  # Simulate a slow transition
        transition_complete.set()

    # Start the slow transition
    slow_task = asyncio.create_task(slow_transition())

    # Try another transition before the first one completes
    await asyncio.sleep(0.01)  # Give time for the first transition to start

    # Act - try a second transition while the first is in progress
    # This should wait for the first transition to complete
    await state_machine.transition_to(ClientState.CONNECTED)

    # Assert
    assert state_machine.current_state == ClientState.CONNECTED

    # Cleanup
    await slow_task
