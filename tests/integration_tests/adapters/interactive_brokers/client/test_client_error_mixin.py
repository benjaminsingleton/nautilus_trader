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

from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from ibapi.common import NO_VALID_ID

# fmt: off
from nautilus_trader.adapters.interactive_brokers.client.common import ClientState
from nautilus_trader.adapters.interactive_brokers.client.common import Request
from nautilus_trader.adapters.interactive_brokers.client.error import ErrorCategory
from nautilus_trader.adapters.interactive_brokers.client.error import ErrorSeverity
from nautilus_trader.adapters.interactive_brokers.client.error import InteractiveBrokersClientErrorMixin


# fmt: on


@pytest.mark.asyncio
async def test_error_mixin_initialize():
    # Test that we can create an error mixin and set up required attributes
    error_mixin = InteractiveBrokersClientErrorMixin()
    error_mixin._log = MagicMock()
    error_mixin._state_machine = MagicMock()
    error_mixin._connection_manager = MagicMock()
    error_mixin._is_client_ready = MagicMock()
    error_mixin._requests = MagicMock()
    error_mixin._subscriptions = MagicMock()

    # Verify it has the expected attributes and methods
    assert hasattr(error_mixin, "classify_error")
    assert hasattr(error_mixin, "process_error")
    assert hasattr(error_mixin, "_handle_subscription_error")
    assert hasattr(error_mixin, "_handle_request_error")
    assert hasattr(error_mixin, "_handle_order_error")


@pytest.mark.asyncio
async def test_error_classification():
    # Test error severity and category classification
    error_mixin = InteractiveBrokersClientErrorMixin()

    # Test connectivity lost error code
    severity, category = error_mixin.classify_error(1100)  # Connectivity between IB and TWS has been lost
    assert severity == ErrorSeverity.CRITICAL
    assert category == ErrorCategory.CONNECTION

    # Test warning code
    severity, category = error_mixin.classify_error(1101)  # Connectivity restored
    assert severity == ErrorSeverity.WARNING
    assert category == ErrorCategory.CONNECTION

    # Test order error code
    severity, category = error_mixin.classify_error(201)  # Order rejected
    assert severity == ErrorSeverity.ERROR
    assert category == ErrorCategory.ORDER


@pytest.mark.asyncio
async def test_process_connection_error():
    # Arrange
    error_mixin = InteractiveBrokersClientErrorMixin()
    error_mixin._log = MagicMock()
    error_mixin._state_machine = MagicMock()
    error_mixin._state_machine.transition_to = AsyncMock()
    error_mixin._state_machine.current_state = ClientState.CONNECTING
    error_mixin._connection_manager = MagicMock()
    error_mixin._connection_manager.set_connected = AsyncMock()
    error_mixin._connection_manager.is_connected = True
    error_mixin._requests = MagicMock()
    error_mixin._requests.get = MagicMock(return_value=None)
    error_mixin._subscriptions = MagicMock()
    error_mixin._subscriptions.get = MagicMock(return_value=None)
    error_mixin._order_id_to_order_ref = {}

    # Act - simulate an error during connection with a connectivity lost code
    # 1100 is in CONNECTIVITY_LOST_CODES
    await error_mixin.process_error(
        req_id=NO_VALID_ID,
        error_code=1100,  # Connectivity between IB and TWS has been lost
        error_string="Connectivity lost",
    )

    # Assert - should handle the connectivity lost error correctly
    error_mixin._connection_manager.set_connected.assert_awaited_once_with(
        False, "Connection error 1100: Connectivity lost"
    )


@pytest.mark.asyncio
async def test_handle_request_error():
    # Arrange
    error_mixin = InteractiveBrokersClientErrorMixin()
    error_mixin._log = MagicMock()
    error_mixin._requests = MagicMock()
    error_mixin._requests.get_request = MagicMock()

    # Create a mock request
    mock_request = MagicMock(spec=Request)
    mock_request.req_id = 123
    error_mixin._requests.get_request.return_value = mock_request
    error_mixin._end_request = MagicMock()

    # Act - simulate a request error
    await error_mixin._handle_request_error(123, 200, "Security not found")

    # Assert
    error_mixin._end_request.assert_called_with(123, success=False)


@pytest.mark.asyncio
async def test_handle_subscription_error():
    # Arrange
    error_mixin = InteractiveBrokersClientErrorMixin()
    error_mixin._log = MagicMock()
    error_mixin._subscriptions = MagicMock()
    error_mixin._subscriptions.get = MagicMock()
    error_mixin._create_task = MagicMock()

    # Create a mock subscription
    mock_subscription = MagicMock()
    mock_subscription.name = "test_subscription"
    mock_subscription.req_id = 456
    mock_subscription.handle = MagicMock()
    error_mixin._subscriptions.get.return_value = mock_subscription

    # Act - simulate a subscription error that should trigger re-subscription
    await error_mixin._handle_subscription_error(456, 366, "No market data")

    # Assert - should cancel and handle the subscription
    mock_subscription.cancel.assert_called_once()
    mock_subscription.handle.assert_called_once()
