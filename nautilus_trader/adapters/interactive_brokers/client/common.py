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
from abc import ABC
from abc import abstractmethod
from collections.abc import Callable
from decimal import Decimal
from typing import Annotated, Any, Generic, NamedTuple, TypeVar, cast

import msgspec
from ibapi.client import EClient
from ibapi.commission_report import CommissionReport
from ibapi.common import BarData
from ibapi.execution import Execution

from nautilus_trader.adapters.interactive_brokers.common import IBContract
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import Logger
from nautilus_trader.common.component import MessageBus
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId


class AccountOrderRef(NamedTuple):
    """
    A reference to an order in an account.

    Parameters
    ----------
    account_id : str
        The account identifier.
    order_id : str
        The order identifier within the account.

    """

    account_id: str
    order_id: str


class IBPosition(NamedTuple):
    """
    A position in an Interactive Brokers account.

    Parameters
    ----------
    account_id : str
        The account identifier.
    contract : IBContract
        The contract for the position.
    quantity : Decimal
        The quantity of the position.
    avg_cost : float
        The average cost of the position.

    """

    account_id: str
    contract: IBContract
    quantity: Decimal
    avg_cost: float


T = TypeVar("T")


class Request(msgspec.Struct, frozen=True):
    """
    Container for data request details.

    Parameters
    ----------
    req_id : int
        The request identifier.
    name : str | tuple
        The name or identifier for the request.
    handle : Callable
        The function that handles the request.
    cancel : Callable
        The function that cancels the request.
    future : asyncio.Future
        The future that will be resolved when the request completes.
    result : list[Any]
        The results of the request.

    """

    req_id: Annotated[int, msgspec.Meta(gt=0)]
    name: str | tuple
    handle: Callable
    cancel: Callable
    future: asyncio.Future
    result: list[Any]

    def __hash__(self) -> int:
        return hash((self.req_id, self.name))


class Subscription(msgspec.Struct, frozen=True):
    """
    Container for subscription details.

    Parameters
    ----------
    req_id : int
        The request identifier.
    name : str | tuple
        The name or identifier for the subscription.
    handle : functools.partial | Callable
        The function that handles subscription updates.
    cancel : Callable
        The function that cancels the subscription.
    last : Any
        The last data received for this subscription.

    """

    req_id: Annotated[int, msgspec.Meta(gt=0)]
    name: str | tuple
    handle: functools.partial | Callable
    cancel: Callable
    last: Any

    def __hash__(self) -> int:
        return hash((self.req_id, self.name))


class ResourceManager(Generic[T], ABC):
    """
    Abstract base class for managing request and subscription resources.

    This class provides common functionality for tracking resources by request ID and
    name, and for adding, removing, and retrieving resources.

    """

    def __init__(self) -> None:
        """
        Initialize the resource manager.
        """
        self._req_id_to_name: dict[int, str | tuple] = {}
        self._req_id_to_handle: dict[int, Callable] = {}
        self._req_id_to_cancel: dict[int, Callable] = {}

    def __repr__(self) -> str:
        """
        Return a string representation of the resource manager.
        """
        resources = [self.get(req_id=k) for k in self._req_id_to_name]
        return f"{self.__class__.__name__}(resources={resources!r})"

    def _name_to_req_id(self, name: Any) -> int | None:
        """
        Map a given name to its corresponding request ID.

        Parameters
        ----------
        name : Any
            The name to find the corresponding request ID for.

        Returns
        -------
        int or None
            The request ID if found, None otherwise.

        """
        for req_id, req_name in self._req_id_to_name.items():
            if req_name == name:
                return req_id
        return None

    def _validation_check(self, req_id: int, name: Any) -> None:
        """
        Validate that the provided request ID and name are not already in use.

        Parameters
        ----------
        req_id : int
            The request ID to validate.
        name : Any
            The name to validate.

        Raises
        ------
        KeyError
            If the request ID or name is already in use.

        """
        if req_id in self._req_id_to_name:
            existing = self.get(req_id=req_id)
            raise KeyError(f"Duplicate entry for {req_id=} not allowed, existing entry: {existing}")
        if name in self._req_id_to_name.values():
            existing = self.get(name=name)
            raise KeyError(f"Duplicate entry for {name=} not allowed, existing entry: {existing}")

    def add_req_id(
        self,
        req_id: int,
        name: str | tuple,
        handle: Callable,
        cancel: Callable,
    ) -> None:
        """
        Add a new request ID along with associated name, handle, and cancel callback to
        the mappings.

        Parameters
        ----------
        req_id : int
            The request ID to add.
        name : str | tuple
            The name associated with the request ID.
        handle : Callable
            The handler function for the request.
        cancel : Callable
            The cancel callback function for the request.

        Raises
        ------
        KeyError
            If the request ID or name is already in use.

        """
        self._validation_check(req_id, name)
        self._req_id_to_name[req_id] = name
        self._req_id_to_handle[req_id] = handle
        self._req_id_to_cancel[req_id] = cancel

    def remove_req_id(self, req_id: int) -> None:
        """
        Remove a request ID and its associated mappings from the manager.

        Parameters
        ----------
        req_id : int
            The request ID to remove.

        """
        self._req_id_to_name.pop(req_id, None)
        self._req_id_to_handle.pop(req_id, None)
        self._req_id_to_cancel.pop(req_id, None)

    def remove(
        self,
        req_id: int | None = None,
        name: InstrumentId | BarType | str | None = None,
    ) -> None:
        """
        Remove a resource identified by either its request ID or name.

        Parameters
        ----------
        req_id : int, optional
            The request ID to remove. If None, name is used to determine the request ID.
        name : InstrumentId | BarType | str, optional
            The name associated with the request ID.

        """
        if req_id is None:
            req_id = self._name_to_req_id(name)
            if req_id is None:
                return  # If no matching req_id is found, exit the method

        self._req_id_to_name.pop(req_id, None)
        self._req_id_to_handle.pop(req_id, None)
        self._req_id_to_cancel.pop(req_id, None)

    def get_all(self) -> list[T]:
        """
        Retrieve all stored mappings as a list of their respective resource objects.

        Returns
        -------
        list[T]
            A list of resource objects.

        """
        result: list[T] = []
        for req_id in self._req_id_to_name:
            resource = self.get(req_id=req_id)
            if resource is not None:
                result.append(cast(T, resource))
        return result

    @abstractmethod
    def get(
        self,
        req_id: int | None = None,
        name: str | tuple | None = None,
    ) -> T | None:
        """
        Retrieve a resource based on the request ID or name.

        Parameters
        ----------
        req_id : int, optional
            The request ID of the resource to retrieve. If None, name is used.
        name : str | tuple, optional
            The name associated with the request ID.

        Returns
        -------
        T or None
            The resource if found, None otherwise.

        """


class Subscriptions(ResourceManager[Subscription]):
    """
    Manages and stores Subscriptions which are identified and accessed using request
    IDs.

    This class extends ResourceManager to provide functionality specific to subscription
    management.

    """

    def __init__(self) -> None:
        """
        Initialize the subscription manager.
        """
        super().__init__()
        self._req_id_to_last: dict[int, Any] = {}

    def add(
        self,
        req_id: int,
        name: str | tuple,
        handle: Callable,
        cancel: Callable = lambda: None,
    ) -> Subscription | None:
        """
        Add a new subscription with the given request ID, name, handle, and optional
        cancel callback.

        Parameters
        ----------
        req_id : int
            The request ID for the new subscription.
        name : str | tuple
            The name associated with the subscription.
        handle : Callable
            The handler function for the subscription.
        cancel : Callable, optional
            The cancel callback function for the subscription. Defaults to a no-op lambda.

        Returns
        -------
        Subscription or None
            The created subscription, or None if creation failed.

        Raises
        ------
        KeyError
            If the request ID or name is already in use.

        """
        super().add_req_id(req_id, name, handle, cancel)
        self._req_id_to_last[req_id] = None
        return self.get(req_id=req_id)

    def remove(self, req_id: int | None = None, name: str | tuple | None = None) -> None:
        """
        Remove a subscription identified by either its request ID or name.

        Parameters
        ----------
        req_id : int, optional
            The request ID of the subscription to remove. If None, name is used.
        name : str | tuple, optional
            The name of the subscription to remove.

        """
        if req_id is None:
            req_id = self._name_to_req_id(name)
        if req_id is not None:
            super().remove_req_id(req_id)
            self._req_id_to_last.pop(req_id, None)

    def get(
        self,
        req_id: int | None = None,
        name: str | tuple | None = None,
    ) -> Subscription | None:
        """
        Retrieve a Subscription based on the request ID or name.

        Parameters
        ----------
        req_id : int, optional
            The request ID of the subscription to retrieve. If None, name is used.
        name : str | tuple, optional
            The name associated with the request ID.

        Returns
        -------
        Subscription or None
            The subscription if found, None otherwise.

        """
        if req_id is None:
            req_id = self._name_to_req_id(name)
        if req_id is None or req_id not in self._req_id_to_name:
            return None

        subscription_name = self._req_id_to_name.get(req_id)
        if subscription_name is None:
            return None

        return Subscription(
            req_id=req_id,
            name=subscription_name,
            last=self._req_id_to_last[req_id],
            handle=self._req_id_to_handle[req_id],
            cancel=self._req_id_to_cancel[req_id],
        )

    def update_last(self, req_id: int, value: Any) -> None:
        """
        Update the 'last' value for a given subscription.

        Parameters
        ----------
        req_id : int
            The request ID of the subscription to update.
        value : Any
            The new value to set as the 'last' value for the subscription.

        """
        self._req_id_to_last[req_id] = value


class Requests(ResourceManager[Request]):
    """
    Manages and stores data requests, inheriting common functionalities from the
    ResourceManager class.

    Requests are identified and accessed using request IDs.

    """

    def __init__(self) -> None:
        """
        Initialize the request manager.
        """
        super().__init__()
        self._req_id_to_future: dict[int, asyncio.Future] = {}
        self._req_id_to_result: dict[int, list] = {}

    def get_futures(self) -> list[asyncio.Future]:
        """
        Retrieve all asyncio Futures associated with the stored requests.

        Returns
        -------
        list[asyncio.Future]
            List of futures for all active requests.

        """
        return list(self._req_id_to_future.values())

    def add(
        self,
        req_id: int,
        name: str | tuple,
        handle: Callable,
        cancel: Callable = lambda: None,
    ) -> Request | None:
        """
        Add a new data request with the specified request ID, name, handle, and an
        optional cancel callback.

        Parameters
        ----------
        req_id : int
            The request ID for the new data request.
        name : str | tuple
            The name associated with the data request.
        handle : Callable
            The handler function for the data request.
        cancel : Callable, optional
            The cancel callback function for the data request. Defaults to a no-op lambda.

        Returns
        -------
        Request or None
            The created request, or None if creation failed.

        Raises
        ------
        KeyError
            If the request ID or name is already in use.

        """
        super().add_req_id(req_id, name, handle, cancel)
        self._req_id_to_future[req_id] = asyncio.Future()
        self._req_id_to_result[req_id] = []
        return self.get(req_id=req_id)

    def remove(self, req_id: int | None = None, name: str | tuple | None = None) -> None:
        """
        Remove a data request identified by either its request ID or name.

        Parameters
        ----------
        req_id : int, optional
            The request ID of the data request to remove. If None, name is used.
        name : str | tuple, optional
            The name of the data request to remove.

        """
        if req_id is None:
            req_id = self._name_to_req_id(name)
        if req_id is not None:
            super().remove_req_id(req_id)
            self._req_id_to_future.pop(req_id, None)
            self._req_id_to_result.pop(req_id, None)

    def get(
        self,
        req_id: int | None = None,
        name: str | tuple | None = None,
    ) -> Request | None:
        """
        Retrieve a Request based on the request ID or name.

        Parameters
        ----------
        req_id : int, optional
            The request ID of the request to retrieve. If None, name is used.
        name : str | tuple, optional
            The name associated with the request ID.

        Returns
        -------
        Request or None
            The request if found, None otherwise.

        """
        if req_id is None:
            req_id = self._name_to_req_id(name)
        if req_id is None or req_id not in self._req_id_to_name:
            return None

        request_name = self._req_id_to_name.get(req_id)
        if request_name is None:
            return None

        return Request(
            req_id=req_id,
            name=request_name,
            handle=self._req_id_to_handle[req_id],
            cancel=self._req_id_to_cancel[req_id],
            future=self._req_id_to_future[req_id],
            result=self._req_id_to_result[req_id],
        )


class BaseMixin:
    """
    Provide type hints for InteractiveBrokerClient Mixins.

    This class defines the shared attributes and methods that are used by the various
    mixin classes in the InteractiveBrokersClient.

    """

    # Client
    is_running: bool
    _loop: asyncio.AbstractEventLoop
    _log: Logger
    _cache: Cache
    _clock: LiveClock
    _msgbus: MessageBus
    _host: str
    _port: int
    _client_id: int
    _requests: Requests
    _subscriptions: Subscriptions
    _event_subscriptions: dict[str, Callable]
    _eclient: EClient
    _is_ib_connected: asyncio.Event
    _start: Callable
    _startup: Callable
    _reset: Callable
    _stop: Callable
    _resume: Callable
    _degrade: Callable
    _end_request: Callable
    _await_request: Callable
    _next_req_id: Callable
    _resubscribe_all: Callable
    _create_task: Callable
    logAnswer: Callable

    # Account
    accounts: Callable

    # Connection
    _reconnect_attempts: int
    _reconnect_delay: int
    _max_reconnect_attempts: int
    _indefinite_reconnect: bool

    # MarketData
    _bar_type_to_last_bar: dict[str, BarData | None]
    _order_id_to_order_ref: dict[int, AccountOrderRef]

    # Order
    _next_valid_order_id: int
    _exec_id_details: dict[
        str,
        dict[str, Execution | (CommissionReport | str)],
    ]
