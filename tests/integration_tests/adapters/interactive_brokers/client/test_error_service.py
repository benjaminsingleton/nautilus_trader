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
from nautilus_trader.adapters.interactive_brokers.client.error import ErrorService
from nautilus_trader.adapters.interactive_brokers.client.error import ErrorSeverity


# fmt: on


@pytest.mark.asyncio
async def test_error_service_initialize():
    # Test that we can create an error service and set up required attributes
    error_service = ErrorService(
        log=MagicMock(),
        state_machine=MagicMock(),
        connection_manager=MagicMock(),
        requests=MagicMock(),
        subscriptions=MagicMock(),
        event_subscriptions={},
        end_request_func=MagicMock(),
        order_id_to_order_ref={},
    )

    # Verify it has the expected attributes and methods
    assert hasattr(error_service, "classify_error")
    assert hasattr(error_service, "process_error")
    assert hasattr(error_service, "_handle_subscription_error")
    assert hasattr(error_service, "_handle_request_error")
    assert hasattr(error_service, "_handle_order_error")


@pytest.mark.asyncio
async def test_error_classification():
    # Test error severity and category classification
    error_service = ErrorService(
        log=MagicMock(),
        state_machine=MagicMock(),
        connection_manager=MagicMock(),
        requests=MagicMock(),
        subscriptions=MagicMock(),
        event_subscriptions={},
        end_request_func=MagicMock(),
        order_id_to_order_ref={},
    )

    # Test connectivity lost error code
    severity, category = error_service.classify_error(
        1100,
    )  # Connectivity between IB and TWS has been lost
    assert severity == ErrorSeverity.CRITICAL
    assert category == ErrorCategory.CONNECTION

    # Test warning code
    severity, category = error_service.classify_error(1101)  # Connectivity restored
    assert severity == ErrorSeverity.WARNING
    assert category == ErrorCategory.CONNECTION

    # Test order error code
    severity, category = error_service.classify_error(201)  # Order rejected
    assert severity == ErrorSeverity.ERROR
    assert category == ErrorCategory.ORDER


@pytest.mark.asyncio
async def test_process_connection_error():
    # Arrange
    mock_state_machine = MagicMock()
    mock_state_machine.transition_to = AsyncMock()
    mock_state_machine.current_state = ClientState.CONNECTING
    
    mock_connection_manager = MagicMock()
    mock_connection_manager.set_connected = AsyncMock()
    mock_connection_manager.is_connected = True
    
    mock_requests = MagicMock()
    mock_requests.get = MagicMock(return_value=None)
    
    mock_subscriptions = MagicMock()
    mock_subscriptions.get = MagicMock(return_value=None)
    
    error_service = ErrorService(
        log=MagicMock(),
        state_machine=mock_state_machine,
        connection_manager=mock_connection_manager,
        requests=mock_requests,
        subscriptions=mock_subscriptions,
        event_subscriptions={},
        end_request_func=MagicMock(),
        order_id_to_order_ref={},
    )

    # Act - simulate an error during connection with a connectivity lost code
    # 1100 is in CONNECTIVITY_LOST_CODES
    await error_service.process_error(
        req_id=NO_VALID_ID,
        error_code=1100,  # Connectivity between IB and TWS has been lost
        error_string="Connectivity lost",
    )

    # Assert - should handle the connectivity lost error correctly
    mock_connection_manager.set_connected.assert_awaited_once_with(
        False,
        "Connection error 1100: Connectivity lost",
    )


@pytest.mark.asyncio
async def test_handle_request_error():
    # Arrange
    mock_requests = MagicMock()
    mock_requests.get = MagicMock()
    
    # Create a mock request
    mock_request = MagicMock(spec=Request)
    mock_request.req_id = 123
    mock_requests.get.return_value = mock_request
    
    mock_end_request_func = MagicMock()
    
    error_service = ErrorService(
        log=MagicMock(),
        state_machine=MagicMock(),
        connection_manager=MagicMock(),
        requests=mock_requests,
        subscriptions=MagicMock(),
        event_subscriptions={},
        end_request_func=mock_end_request_func,
        order_id_to_order_ref={},
    )

    # Act - simulate a request error
    await error_service._handle_request_error(123, 200, "Security not found")

    # Assert
    mock_end_request_func.assert_called_with(123, success=False)


@pytest.mark.asyncio
async def test_handle_subscription_error():
    # Arrange
    mock_subscriptions = MagicMock()
    mock_subscriptions.get = MagicMock()
    
    # Create a mock subscription
    mock_subscription = MagicMock()
    mock_subscription.name = "test_subscription"
    mock_subscription.req_id = 456
    mock_subscription.handle = MagicMock()
    mock_subscriptions.get.return_value = mock_subscription
    
    error_service = ErrorService(
        log=MagicMock(),
        state_machine=MagicMock(),
        connection_manager=MagicMock(),
        requests=MagicMock(),
        subscriptions=mock_subscriptions,
        event_subscriptions={},
        end_request_func=MagicMock(),
        order_id_to_order_ref={},
    )
    error_service._create_task = MagicMock()

    # Act - simulate a subscription error that should trigger re-subscription
    await error_service._handle_subscription_error(456, 366, "No market data")

    # Assert - should cancel and handle the subscription
    mock_subscription.cancel.assert_called_once()
    mock_subscription.handle.assert_called_once()
