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

from nautilus_trader.adapters.interactive_brokers.client.common import ClientState


@pytest.mark.asyncio
async def test_ib_is_ready_by_notification_1101(ib_client):
    # Arrange
    # Create a new error service with our mocks
    connection_manager = MagicMock()
    connection_manager.set_connected = AsyncMock()
    state_machine = MagicMock()
    state_machine.current_state = ClientState.CONNECTED

    # Set up the client with our mocked services
    ib_client._connection_manager = connection_manager
    ib_client._state_machine = state_machine

    # Also need to mock the error service to use our mocked connection manager
    from nautilus_trader.adapters.interactive_brokers.client.error import ErrorService

    ib_client._error_service = ErrorService(
        log=ib_client._log,
        state_machine=state_machine,
        connection_manager=connection_manager,
        requests=ib_client._requests,
        subscriptions=ib_client._subscriptions,
        event_subscriptions=ib_client._event_subscriptions,
        end_request_func=ib_client._end_request,
        order_id_to_order_ref=ib_client._order_id_to_order_ref,
    )

    # Act
    await ib_client.process_error(
        req_id=-1,
        error_code=1101,
        error_string="Connectivity between IB and Trader Workstation has been restored",
    )

    # Assert
    connection_manager.set_connected.assert_called_with(
        True,
        "Connection restored 1101: Connectivity between IB and Trader Workstation has been restored",
    )


@pytest.mark.asyncio
async def test_ib_is_ready_by_notification_1102(ib_client):
    # Arrange
    # Create a new error service with our mocks
    connection_manager = MagicMock()
    connection_manager.set_connected = AsyncMock()
    state_machine = MagicMock()
    state_machine.current_state = ClientState.CONNECTED

    # Set up the client with our mocked services
    ib_client._connection_manager = connection_manager
    ib_client._state_machine = state_machine

    # Also need to mock the error service to use our mocked connection manager
    from nautilus_trader.adapters.interactive_brokers.client.error import ErrorService

    ib_client._error_service = ErrorService(
        log=ib_client._log,
        state_machine=state_machine,
        connection_manager=connection_manager,
        requests=ib_client._requests,
        subscriptions=ib_client._subscriptions,
        event_subscriptions=ib_client._event_subscriptions,
        end_request_func=ib_client._end_request,
        order_id_to_order_ref=ib_client._order_id_to_order_ref,
    )

    # Act
    await ib_client.process_error(
        req_id=-1,
        error_code=1102,
        error_string="Connectivity between IB and Trader Workstation has been restored",
    )

    # Assert
    connection_manager.set_connected.assert_called_with(
        True,
        "Connection restored 1102: Connectivity between IB and Trader Workstation has been restored",
    )


@pytest.mark.asyncio
async def test_ib_is_not_ready_by_error_10182(ib_client):
    # Arrange
    req_id = 6
    # Create a new error service with our mocks
    connection_manager = MagicMock()
    connection_manager.set_connected = AsyncMock()
    connection_manager.is_connected = True
    state_machine = MagicMock()
    state_machine.current_state = ClientState.CONNECTED

    # Set up the client with our mocked services
    ib_client._connection_manager = connection_manager
    ib_client._state_machine = state_machine

    # Set up subscription
    ib_client._subscriptions.add(req_id, "EUR.USD", ib_client._eclient.reqHistoricalData, {})

    # Also need to mock the error service to use our mocked connection manager
    from nautilus_trader.adapters.interactive_brokers.client.error import ErrorService

    ib_client._error_service = ErrorService(
        log=ib_client._log,
        state_machine=state_machine,
        connection_manager=connection_manager,
        requests=ib_client._requests,
        subscriptions=ib_client._subscriptions,
        event_subscriptions=ib_client._event_subscriptions,
        end_request_func=ib_client._end_request,
        order_id_to_order_ref=ib_client._order_id_to_order_ref,
    )

    # Act
    await ib_client.process_error(
        req_id=req_id,
        error_code=10182,
        error_string="Failed to request live updates (disconnected).",
    )

    # Assert
    connection_manager.set_connected.assert_called_with(
        False,
        "Market data halted: Failed to request live updates (disconnected).",
    )
