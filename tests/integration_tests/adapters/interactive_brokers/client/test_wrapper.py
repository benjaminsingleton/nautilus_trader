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
from ibapi.errors import CONNECT_FAIL

# fmt: off
from nautilus_trader.adapters.interactive_brokers.client.wrapper import InteractiveBrokersEWrapper


# fmt: on


@pytest.mark.asyncio
async def test_wrapper_error_handling(ib_client):
    # Arrange
    # Create a wrapper instance with a mock client
    mock_client = MagicMock()
    mock_client.error = AsyncMock()
    wrapper = InteractiveBrokersEWrapper(
        nautilus_logger=MagicMock(),
        client=mock_client,
    )

    # Act - simulate error handling through the wrapper
    wrapper.error(
        reqId=NO_VALID_ID,
        errorCode=CONNECT_FAIL.code(),
        errorString=CONNECT_FAIL.msg(),
    )

    # Assert - error should be submitted to the message handler queue
    mock_client.submit_to_msg_handler_queue.assert_called_once()


@pytest.mark.asyncio
async def test_wrapper_submit_to_handler(ib_client):
    # Arrange - use a real client but with mocked message queue
    ib_client._msg_handler_task_queue = MagicMock()
    ib_client._msg_handler_task_queue.put = AsyncMock()

    # Create a dummy task to submit
    def dummy_task():
        pass

    # Act - call the submit method
    ib_client.submit_to_msg_handler_queue(dummy_task)

    # Assert - the task should be scheduled via run_coroutine_threadsafe
    # This is difficult to assert directly, since the call happens in another thread
    # So we're primarily testing that no exceptions are raised


@pytest.mark.asyncio
async def test_wrapper_managed_accounts(ib_client):
    # Arrange
    # Create a wrapper with a mock client
    mock_client = MagicMock()
    mock_client.process_managed_accounts = AsyncMock()
    wrapper = InteractiveBrokersEWrapper(
        nautilus_logger=MagicMock(),
        client=mock_client,
    )

    # Act - call managed accounts
    accounts_str = "DU1234567,DU89012345"
    wrapper.managedAccounts(accounts_str)

    # Assert - the method should submit the task to the message handler queue
    mock_client.submit_to_msg_handler_queue.assert_called_once()
