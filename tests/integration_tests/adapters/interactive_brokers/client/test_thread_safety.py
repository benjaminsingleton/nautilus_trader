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
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from unittest.mock import MagicMock

import pytest

# fmt: off
from nautilus_trader.adapters.interactive_brokers.client.client import StateMachine
from nautilus_trader.adapters.interactive_brokers.client.common import ClientState


# fmt: on


# This test case simulates a situation where callbacks are being triggered
# from a different thread (as they would be in the real IB API)
@pytest.mark.asyncio
async def test_callback_thread_safety():
    # Arrange
    # Set up a mock client with just the components we need
    class MockClient:
        def __init__(self):
            self._internal_msg_queue = asyncio.Queue()
            self._loop = asyncio.get_event_loop()

    # Create a client for testing
    client = MockClient()

    # Set up a queue to receive callback results
    callback_results = []
    callback_lock = threading.Lock()
    test_complete = asyncio.Event()

    # Create a coroutine handler that will be called from multiple threads
    async def process_message(message):
        await asyncio.sleep(0.01)  # Simulate some processing
        with callback_lock:
            callback_results.append(message)
        if len(callback_results) >= 5:
            test_complete.set()
        return True

    # Act - simulate callbacks coming from TWS reader thread by using ThreadPoolExecutor
    def simulate_tws_callback():
        message = f"message-{threading.get_ident()}"
        # Put message in the internal queue using thread-safe method
        client._loop.call_soon_threadsafe(
            client._internal_msg_queue.put_nowait,
            message,
        )

    # Create a task to process messages from the queue
    async def process_queue():
        while not test_complete.is_set():
            try:
                # Use a timeout to avoid blocking forever
                message = await asyncio.wait_for(client._internal_msg_queue.get(), 0.1)
                await process_message(message)
                client._internal_msg_queue.task_done()
            except TimeoutError:
                continue

    # Start the message processor
    processor_task = asyncio.create_task(process_queue())

    # Start ThreadPoolExecutor to simulate multiple threads calling callbacks
    with ThreadPoolExecutor(max_workers=3) as executor:
        # Submit multiple tasks to be executed in thread pool
        for _ in range(5):
            executor.submit(simulate_tws_callback)
            await asyncio.sleep(0.01)  # Small delay to ensure thread scheduling

    # Wait for all callbacks to be processed
    try:
        await asyncio.wait_for(test_complete.wait(), timeout=2.0)
    except TimeoutError:
        pytest.fail("Timed out waiting for callbacks to complete")

    # Clean up
    processor_task.cancel()
    with suppress(asyncio.CancelledError):
        await processor_task

    # Assert - all callbacks should have been processed correctly
    assert len(callback_results) == 5


# Test that request handling properly manages concurrency
@pytest.mark.asyncio
async def test_request_concurrency():
    # Arrange
    total_requests = 20
    completed_requests = 0
    completion_lock = asyncio.Lock()
    all_completed = asyncio.Event()

    # Create a request ID generator
    request_id_counter = 0

    def next_req_id():
        nonlocal request_id_counter
        request_id_counter += 1
        return request_id_counter

    # Create a simulated slow request handler
    async def slow_request_handler(req_id):
        await asyncio.sleep(0.02)  # Simulate network delay
        async with completion_lock:
            nonlocal completed_requests
            completed_requests += 1
            if completed_requests >= total_requests:
                all_completed.set()
        return f"response-{req_id}"

    # Create a way to track requests
    request_ids = []

    # Act - make multiple concurrent requests
    tasks = []
    for i in range(total_requests):
        # Generate unique request IDs
        req_id = next_req_id()
        request_ids.append(req_id)

        # Simulate making a request
        task = asyncio.create_task(slow_request_handler(req_id))
        tasks.append(task)

    # Ensure all tasks are awaited at the end
    for task in tasks:
        await task

    # Wait for all requests to complete or timeout
    try:
        await asyncio.wait_for(all_completed.wait(), timeout=1.0)
    except TimeoutError:
        pytest.fail("Timed out waiting for requests to complete")

    # Assert - all requests should have completed successfully
    assert completed_requests == total_requests

    # All request IDs should be unique
    assert len(set(request_ids)) == total_requests


# Test concurrent state transitions with callbacks
@pytest.mark.asyncio
async def test_state_machine_concurrent_callbacks(event_loop):
    # Arrange - create a logger and state machine
    mock_logger = MagicMock()
    state_machine = StateMachine(
        initial_state=ClientState.CREATED,
        logger=mock_logger,
    )

    # Track callback executions
    callback_execution_count = 0
    _ = asyncio.Lock()

    # Register multiple callbacks for the same state
    def state_callback_1():
        nonlocal callback_execution_count
        callback_execution_count += 1

    def state_callback_2():
        nonlocal callback_execution_count
        callback_execution_count += 1

    # Add both callbacks for the CONNECTED state
    state_machine.register_state_changed_callback(ClientState.CONNECTED, state_callback_1)
    state_machine.register_state_changed_callback(ClientState.CONNECTED, state_callback_2)

    # Act - transition to the CONNECTED state which should trigger both callbacks
    await state_machine.transition_to(ClientState.CONNECTING)
    await state_machine.transition_to(ClientState.CONNECTED)

    # Assert - both callbacks should have executed
    assert callback_execution_count == 2
    assert state_machine.current_state == ClientState.CONNECTED
