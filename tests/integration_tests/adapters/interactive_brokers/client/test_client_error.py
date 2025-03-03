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
    ib_client._connection_manager = MagicMock()
    ib_client._connection_manager.set_connected = AsyncMock()
    ib_client._state_machine = MagicMock()
    ib_client._state_machine.current_state = ClientState.CONNECTED

    # Act
    await ib_client.process_error(
        req_id=-1,
        error_code=1101,
        error_string="Connectivity between IB and Trader Workstation has been restored",
    )

    # Assert
    ib_client._connection_manager.set_connected.assert_called_with(
        True,
        "Connection restored 1101: Connectivity between IB and Trader Workstation has been restored",
    )


@pytest.mark.asyncio
async def test_ib_is_ready_by_notification_1102(ib_client):
    # Arrange
    ib_client._connection_manager = MagicMock()
    ib_client._connection_manager.set_connected = AsyncMock()
    ib_client._state_machine = MagicMock()
    ib_client._state_machine.current_state = ClientState.CONNECTED

    # Act
    await ib_client.process_error(
        req_id=-1,
        error_code=1102,
        error_string="Connectivity between IB and Trader Workstation has been restored",
    )

    # Assert
    ib_client._connection_manager.set_connected.assert_called_with(
        True,
        "Connection restored 1102: Connectivity between IB and Trader Workstation has been restored",
    )


@pytest.mark.asyncio
async def test_ib_is_not_ready_by_error_10182(ib_client):
    # Arrange
    req_id = 6
    ib_client._connection_manager = MagicMock()
    ib_client._connection_manager.set_connected = AsyncMock()
    ib_client._connection_manager.is_connected = True
    ib_client._subscriptions.add(req_id, "EUR.USD", ib_client._eclient.reqHistoricalData, {})

    # Act
    await ib_client.process_error(
        req_id=req_id,
        error_code=10182,
        error_string="Failed to request live updates (disconnected).",
    )

    # Assert
    ib_client._connection_manager.set_connected.assert_called_with(
        False,
        "Market data halted: Failed to request live updates (disconnected).",
    )
