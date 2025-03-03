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
import functools
from collections.abc import Callable
from decimal import Decimal
from typing import Any

from ibapi.account_summary_tags import AccountSummaryTags
from ibapi.client import EClient

from nautilus_trader.adapters.interactive_brokers.client.common import IBPosition
from nautilus_trader.adapters.interactive_brokers.client.common import Requests
from nautilus_trader.adapters.interactive_brokers.client.common import Subscriptions
from nautilus_trader.adapters.interactive_brokers.common import IBContract
from nautilus_trader.common.component import Logger
from nautilus_trader.common.enums import LogColor


class AccountService:
    """
    Service that manages account-related functionality for the InteractiveBrokersClient.

    This class is responsible for account operations, position querying, and processing
    account-related events from TWS/Gateway.

    Parameters
    ----------
    log : Logger
        The logger for the service.
    eclient : EClient
        The EClient instance.
    requests : Requests
        The requests manager.
    subscriptions : Subscriptions
        The subscriptions manager.
    event_subscriptions : dict[str, Any]
        The event subscriptions mapping.
    is_ib_connected : asyncio.Event
        Event indicating if IB is connected.
    next_req_id_func : callable
        Function to get the next request ID.
    await_request_func : callable
        Function to await a request's completion.
    end_request_func : callable
        Function to end a request.

    """

    def __init__(
        self,
        log: Logger,
        eclient: EClient,
        requests: Requests,
        subscriptions: Subscriptions,
        event_subscriptions: dict[str, Any],
        is_ib_connected: asyncio.Event,
        next_req_id_func: Callable[[], int],
        await_request_func: Callable[..., Any],
        end_request_func: Callable[..., Any],
    ) -> None:
        self._log = log
        self._eclient = eclient
        self._requests = requests
        self._subscriptions = subscriptions
        self._event_subscriptions = event_subscriptions
        self._is_ib_connected = is_ib_connected
        self._next_req_id = next_req_id_func
        self._await_request = await_request_func
        self._end_request = end_request_func

        # Initialize account state
        self._account_ids: set[str] = set()

    def accounts(self) -> set[str]:
        """
        Return a set of account identifiers managed by this instance.

        Returns
        -------
        set[str]
            The set of account identifiers.

        """
        return self._account_ids.copy()

    @property
    def account_ids(self) -> set[str]:
        """
        Return a set of account identifiers managed by this instance.

        This property is needed for compatibility with tests that directly access the field.

        Returns
        -------
        set[str]
            The set of account identifiers.

        """
        return self._account_ids

    def subscribe_account_summary(self) -> None:
        """
        Subscribe to the account summary for all accounts.

        It sends a request to Interactive Brokers to retrieve account summary
        information.

        """
        name = "accountSummary"
        if not (subscription := self._subscriptions.get(name=name)):
            req_id = self._next_req_id()
            subscription = self._subscriptions.add(
                req_id=req_id,
                name=name,
                handle=functools.partial(
                    self._eclient.reqAccountSummary,
                    reqId=req_id,
                    groupName="All",
                    tags=AccountSummaryTags.AllTags,
                ),
                cancel=functools.partial(
                    self._eclient.cancelAccountSummary,
                    reqId=req_id,
                ),
            )
        # Allow fetching all tags upon request even if already subscribed
        if not subscription:
            return
        subscription.handle()

    def unsubscribe_account_summary(self, account_id: str) -> None:
        """
        Unsubscribe from the account summary for the specified account.

        Parameters
        ----------
        account_id : str
            The identifier of the account to unsubscribe from.

        """
        name = "accountSummary"
        if subscription := self._subscriptions.get(name=name):
            self._subscriptions.remove(subscription.req_id)
            self._eclient.cancelAccountSummary(reqId=subscription.req_id)
            self._log.debug(f"Unsubscribed from {subscription}")
        else:
            self._log.debug(f"Subscription doesn't exist for {name}")

    async def get_positions(self, account_id: str) -> list[IBPosition] | None:
        """
        Fetch open positions for a specified account.

        Parameters
        ----------
        account_id: str
            The account identifier for which to fetch positions.

        Returns
        -------
        list[IBPosition] | ``None``
            The list of positions for the account, or None if no positions found.

        """
        self._log.debug(f"Requesting open positions for {account_id}")
        name = "OpenPositions"
        if not (request := self._requests.get(name=name)):
            request = self._requests.add(
                req_id=self._next_req_id(),
                name=name,
                handle=self._eclient.reqPositions,
            )
            if not request:
                return []  # Return empty list instead of None
            request.handle()
            all_positions = await self._await_request(request, 30)
        else:
            all_positions = await self._await_request(request, 30)

        if not all_positions:
            return []  # Return empty list instead of None

        positions = []
        for position in all_positions:
            if position.account_id == account_id:
                positions.append(position)

        return positions

    async def process_account_summary(
        self,
        *,
        req_id: int,
        account_id: str,
        tag: str,
        value: str,
        currency: str,
    ) -> None:
        """
        Receive account information.

        Parameters
        ----------
        req_id : int
            The request ID.
        account_id : str
            The account ID.
        tag : str
            The account summary tag.
        value : str
            The tag value.
        currency : str
            The currency of the value.

        """
        name = f"accountSummary-{account_id}"
        if handler := self._event_subscriptions.get(name, None):
            handler(tag, value, currency)

    async def process_managed_accounts(self, *, accounts_list: str) -> None:
        """
        Receive a comma-separated string with the managed account ids.

        Occurs automatically on initial API client connection.

        Parameters
        ----------
        accounts_list : str
            Comma-separated string of account IDs.

        """
        self._account_ids = {a for a in accounts_list.split(",") if a}
        self._log.debug(f"Managed accounts set: {self._account_ids}")
        if not self._is_ib_connected.is_set():
            self._log.debug("`_is_ib_connected` set by `managedAccounts`", LogColor.BLUE)
            self._is_ib_connected.set()

    async def process_position(
        self,
        *,
        account_id: str,
        contract: IBContract,
        position: Decimal,
        avg_cost: float,
    ) -> None:
        """
        Provide the portfolio's open positions.

        Parameters
        ----------
        account_id : str
            The account ID.
        contract : IBContract
            The contract information.
        position : Decimal
            The position quantity.
        avg_cost : float
            The average cost of the position.

        """
        if request := self._requests.get(name="OpenPositions"):
            request.result.append(IBPosition(account_id, contract, position, avg_cost))

    async def process_position_end(self) -> None:
        """
        Indicate that all the positions have been transmitted.
        """
        if request := self._requests.get(name="OpenPositions"):
            self._end_request(request.req_id)
