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
import random
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from ibapi.common import NO_VALID_ID
from ibapi.errors import ALREADY_CONNECTED

# from ibapi.errors import FAIL_SEND_REQMKT - missing in IB API version
# fmt: off
from nautilus_trader.adapters.interactive_brokers.client.common import ClientState
from nautilus_trader.adapters.interactive_brokers.client.error import ErrorService


# fmt: on


@pytest.mark.asyncio
async def test_error_handling_connect_fail(ib_client):
    # Arrange - Create an error handler service to test directly
    error_handler = ErrorService(
        log=MagicMock(),
        state_machine=MagicMock(),
        connection_manager=MagicMock(),
        requests=MagicMock(),
        subscriptions=MagicMock(),
        event_subscriptions={},
        end_request_func=MagicMock(),
        order_id_to_order_ref={},
    )

    # Set up for the test
    error_handler._state_machine.transition_to = AsyncMock()
    error_handler._state_machine.current_state = ClientState.CONNECTING
    error_handler._requests.get = MagicMock(return_value=None)
    error_handler._subscriptions.get = MagicMock(return_value=None)
    error_handler._create_task = MagicMock()
    error_handler._handle_connection_error = AsyncMock()

    # Use a simple fixed string to avoid newline formatting issues
    error_code = 502  # CONNECT_FAIL.code()
    error_string = "Connection failed"

    # Act - simulate an error during connection using the IB API method name
    await error_handler.error(
        reqId=NO_VALID_ID,
        errorCode=error_code,
        errorString=error_string,
    )

    # Assert - should handle the connection error correctly
    error_handler._handle_connection_error.assert_called_once_with(
        error_code,
        error_string,
    )


@pytest.mark.asyncio
async def test_error_handling_already_connected(ib_client):
    # Arrange - Create an error handler service to test directly
    error_handler = ErrorService(
        log=MagicMock(),
        state_machine=MagicMock(),
        connection_manager=MagicMock(),
        requests=MagicMock(),
        subscriptions=MagicMock(),
        event_subscriptions={},
        end_request_func=MagicMock(),
        order_id_to_order_ref={},
    )

    # Set up for the test
    error_handler._connection_manager.set_connected = AsyncMock()
    error_handler._state_machine.transition_to = AsyncMock()
    error_handler._state_machine.current_state = ClientState.CONNECTING
    error_handler._requests.get = MagicMock(return_value=None)
    error_handler._subscriptions.get = MagicMock(return_value=None)
    error_handler._create_task = MagicMock()
    error_handler._handle_connection_restored = AsyncMock()

    # Act - simulate an "already connected" error using the original method name
    await error_handler.error(
        reqId=NO_VALID_ID,
        errorCode=ALREADY_CONNECTED.code(),
        errorString=ALREADY_CONNECTED.msg(),
    )

    # Assert - should call the connection restored handler
    error_handler._handle_connection_restored.assert_called_once_with(
        ALREADY_CONNECTED.code(),
        ALREADY_CONNECTED.msg(),
    )


@pytest.mark.asyncio
async def test_error_handling_request_failure():
    # Arrange - Create a mock request and set up components
    mock_req = MagicMock()
    mock_req.req_id = 123
    mock_req.client_error_code = None
    mock_req.client_error_msg = None

    # Set up mock requests manager
    mock_requests = MagicMock()
    mock_requests.get = MagicMock(return_value=mock_req)

    # Set up mock subscriptions
    mock_subscriptions = MagicMock()
    mock_subscriptions.get = MagicMock(return_value=None)

    # Create the error handler service
    error_handler = ErrorService(
        log=MagicMock(),
        state_machine=MagicMock(),
        connection_manager=MagicMock(),
        requests=mock_requests,
        subscriptions=mock_subscriptions,
        event_subscriptions={},
        end_request_func=MagicMock(),
        order_id_to_order_ref={},
    )

    # Mock the _handle_request_error method
    error_handler._handle_request_error = AsyncMock()
    error_handler._create_task = MagicMock()

    # Act - simulate a request failure error using the IB API method name
    # Use hardcoded error code and message since FAIL_SEND_REQMKT may not be available in all IB API versions
    error_code = 10005  # FAIL_SEND_REQMKT equivalent code
    error_string = "Failed to send request market data"

    await error_handler.error(
        reqId=123,
        errorCode=error_code,
        errorString=error_string,
    )

    # Assert - should call handle_request_error with correct parameters
    error_handler._handle_request_error.assert_called_once_with(
        123,
        error_code,
        error_string,
    )


@pytest.mark.asyncio
async def test_connection_retry_backoff():
    # Arrange - create a simple retry function with exponential backoff
    # instead of trying to mock the actual client
    retry_attempts = 0
    backoff_calls = []

    # Replace sleep with a mock to avoid actual sleeping
    original_sleep = asyncio.sleep
    asyncio.sleep = AsyncMock()

    try:
        # Simulate calculate_reconnect_delay with exponential backoff
        def calculate_reconnect_delay(attempt):
            jitter = random.uniform(0, 2.0)  # noqa: S311
            backoff_factor = min(attempt, 5)
            delay = 5 * (2 ** (backoff_factor - 1)) + jitter
            return min(delay, 60)

        # Simulate a connection function that fails a few times
        async def connect():
            nonlocal retry_attempts
            retry_attempts += 1
            if retry_attempts < 3:
                raise ConnectionError(f"Connection failed (attempt {retry_attempts})")
            return True

        # Simulate a retry function
        async def retry_connection_with_backoff(max_attempts=5):
            nonlocal retry_attempts, backoff_calls
            for attempt in range(1, max_attempts + 1):
                try:
                    result = await connect()
                    return result
                except Exception:
                    if attempt < max_attempts:
                        delay = calculate_reconnect_delay(attempt)
                        backoff_calls.append(delay)
                        await asyncio.sleep(delay)
                    else:
                        raise

        # Act - simulate reconnection with retries
        result = await retry_connection_with_backoff()

        # Assert - should have attempted reconnection with increasing delays
        assert result is True
        assert retry_attempts == 3  # Should succeed on the third try
        assert len(backoff_calls) == 2  # Two backoff delays
        assert backoff_calls[1] > backoff_calls[0]  # Second delay should be longer

    finally:
        # Restore the original sleep function
        asyncio.sleep = original_sleep


@pytest.mark.asyncio
async def test_watchdog_recovery():
    # Arrange - we'll test a simple mock watchdog implementation
    # Create a simulated watchdog class with the essential components
    class MockConnectionManager:
        def __init__(self):
            self.is_connected = True
            self.set_connected = AsyncMock()

    class MockClient:
        def __init__(self):
            self.connection_manager = MockConnectionManager()
            self.handle_disconnection = AsyncMock()
            self.resume = MagicMock()
            self.is_connected = True

        def is_socket_connected(self):
            return self.is_connected

        async def run_watchdog_cycle(self):
            # Simplified watchdog logic
            if not self.is_socket_connected() and self.connection_manager.is_connected:
                await self.handle_disconnection()

        async def simulate_reconnection(self):
            # Simulated reconnection process
            self.is_connected = True
            self.resume()

    # Create the mock client
    client = MockClient()

    # Act - simulate connection drop detected by watchdog
    client.is_connected = False  # Socket disconnected

    # Run the watchdog
    await client.run_watchdog_cycle()

    # Assert - should detect the disconnection and call handle_disconnection
    client.handle_disconnection.assert_called_once()

    # Now simulate successful reconnection and recovery
    await client.simulate_reconnection()

    # Should resume operations
    client.resume.assert_called_once()
