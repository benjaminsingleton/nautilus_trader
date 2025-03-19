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
from collections.abc import Coroutine
from enum import IntEnum
from inspect import iscoroutinefunction
from typing import Any, Final, TypeVar, cast

from nautilus_trader.adapters.interactive_brokers.client.common import ClientState
from nautilus_trader.adapters.interactive_brokers.client.common import Requests
from nautilus_trader.adapters.interactive_brokers.client.common import Subscriptions
from nautilus_trader.common.component import Logger
from nautilus_trader.common.enums import LogColor


F = TypeVar("F", bound=Callable[..., Any])


class ErrorSeverity(IntEnum):
    """
    Enum defining error severity levels for categorization and handling.
    """

    DEBUG = 0  # Informational message, not affecting operation
    WARNING = 1  # Warning that may require attention but doesn't stop operation
    ERROR = 2  # Error that affects a specific operation but not the overall connection
    CRITICAL = 3  # Critical error affecting the connection or client operation


class ErrorCategory(IntEnum):
    """
    Enum defining error categories for better organization and handling.
    """

    GENERAL = 0  # General errors
    CONNECTION = 1  # Connection-related errors
    REQUEST = 2  # Request-related errors
    ORDER = 3  # Order-related errors
    SUBSCRIPTION = 4  # Subscription-related errors
    DATA = 5  # Data-related errors


class ErrorService:
    """
    Handles errors and warnings for the InteractiveBrokersClient.

    This class is designed to process and log various types of error messages and
    warnings encountered during the operation of the InteractiveBrokersClient. It
    categorizes different error codes and manages appropriate responses, including
    logging and state updates.

    The error codes are documented in the Interactive Brokers API documentation:
    https://ibkrcampus.com/ibkr-api-page/tws-api-error-codes/#understanding-error-codes

    Parameters
    ----------
    log : Logger
        The logger instance.
    state_machine : Any
        The state machine instance.
    connection_manager : Any
        The connection manager instance.
    requests : Requests
        The requests manager instance.
    subscriptions : Subscriptions
        The subscriptions manager instance.
    event_subscriptions : dict[str, Callable]
        Dictionary of event subscriptions.
    end_request_func : Callable
        Function to end a request.
    order_id_to_order_ref : dict[int, Any]
        Dictionary mapping order IDs to order references.

    """

    # Error code classifications - organized for better readability and maintenance
    WARNING_CODES: Final[set[int]] = {
        1101,
        1102,  # Connectivity restored codes
        110,  # News feed has been reset
        165,  # Historical market data farm connection is OK
        202,  # Order canceled
        399,  # Order message
        404,  # Order not found
        434,  # The order size cannot be zero
        492,  # Request market data streaming for this contract has been disabled
        10167,  # Login failed: reason given
    }

    CLIENT_ERRORS: Final[set[int]] = {
        502,  # Couldn't connect to TWS
        503,  # The TWS is out of date and must be upgraded
        504,  # Not connected
        10038,  # Requested market data has been switched between frozen and real-time market data
        10182,  # Requested market data has been halted
        1100,  # Connectivity between IB and TWS has been lost
        2110,  # Connectivity between TWS and server is broken. It will be restored automatically.
    }

    CONNECTIVITY_LOST_CODES: Final[set[int]] = {
        502,  # Couldn't connect to TWS
        504,  # Not connected
        1100,  # Connectivity between IB and TWS has been lost
        1300,  # Socket port has been reset
        2110,  # Connectivity between TWS and server is broken
    }

    CONNECTIVITY_RESTORED_CODES: Final[set[int]] = {
        501,  # Already connected
        1101,  # Connectivity between IB and TWS has been restored
        1102,  # Connectivity between TWS and server has been restored
    }

    ORDER_REJECTION_CODES: Final[set[int]] = {
        201,  # Order rejected
        203,  # Security not found
        321,  # Cannot change to Pending because order is not pending
        10289,  # Failed to request real-time bars
        10293,  # Market data farm is not connected
    }

    SUPPRESS_ERROR_LOGGING_CODES: Final[set[int]] = {
        200,  # No security definition found
    }

    def __init__(
        self,
        log: Logger,
        state_machine: Any,
        connection_manager: Any,
        requests: Requests,
        subscriptions: Subscriptions,
        event_subscriptions: dict[str, Callable],
        end_request_func: Callable,
        order_id_to_order_ref: dict[int, Any],
    ) -> None:
        self._log = log
        self._state_machine = state_machine
        self._connection_manager = connection_manager
        self._requests = requests
        self._subscriptions = subscriptions
        self._event_subscriptions = event_subscriptions
        self._end_request = end_request_func
        self._order_id_to_order_ref = order_id_to_order_ref

    def _create_task(self, coro: Coroutine[Any, Any, None]) -> asyncio.Task:
        """
        Create a task from a coroutine.

        This is a simple wrapper that creates a task from a coroutine and adds it to the event loop.
        Used for compatibility with the previous error mixin implementation.

        Parameters
        ----------
        coro : Coroutine
            The coroutine to create a task from.

        """
        task = asyncio.create_task(coro)
        return task

    def classify_error(self, error_code: int) -> tuple[ErrorSeverity, ErrorCategory]:
        """
        Classify an error code into a severity and category.

        Parameters
        ----------
        error_code : int
            The error code to classify.

        Returns
        -------
        tuple[ErrorSeverity, ErrorCategory]
            A tuple containing the error severity and category.

        """
        # Determine severity
        if error_code in self.WARNING_CODES or 2100 <= error_code < 2200:
            severity = ErrorSeverity.WARNING
        elif error_code in self.CLIENT_ERRORS or error_code in self.CONNECTIVITY_LOST_CODES:
            severity = ErrorSeverity.CRITICAL
        elif error_code in self.ORDER_REJECTION_CODES:
            severity = ErrorSeverity.ERROR
        else:
            severity = ErrorSeverity.ERROR

        # Determine category
        if (
            error_code in self.CONNECTIVITY_LOST_CODES
            or error_code in self.CONNECTIVITY_RESTORED_CODES
        ):
            category = ErrorCategory.CONNECTION
        elif error_code in self.ORDER_REJECTION_CODES:
            category = ErrorCategory.ORDER
        elif 300 <= error_code < 400:
            category = ErrorCategory.ORDER
        elif 100 <= error_code < 200:
            category = ErrorCategory.DATA
        elif 500 <= error_code < 600:
            category = ErrorCategory.CONNECTION
        else:
            category = ErrorCategory.GENERAL

        return severity, category

    async def _log_message(
        self,
        error_code: int,
        req_id: int,
        error_string: str,
        severity: ErrorSeverity,
        category: ErrorCategory,
    ) -> None:
        """
        Log an error or warning message with appropriate severity.

        Parameters
        ----------
        error_code : int
            The error code associated with the message.
        req_id : int
            The request ID associated with the error or warning.
        error_string : str
            The error or warning message string.
        severity : ErrorSeverity
            The severity level of the error.
        category : ErrorCategory
            The category of the error.

        """
        msg = f"{error_string} (code: {error_code}, req_id={req_id}, category={ErrorCategory(category).name})."

        if error_code in self.SUPPRESS_ERROR_LOGGING_CODES:
            self._log.debug(msg)
        elif severity == ErrorSeverity.WARNING:
            self._log.warning(msg)
        elif severity == ErrorSeverity.ERROR:
            self._log.error(msg)
        elif severity == ErrorSeverity.CRITICAL:
            self._log.error(f"CRITICAL: {msg}")
        else:
            self._log.debug(msg)

    # Compatibility wrapper for old tests - IBAPI uses error method
    async def error(self, reqId: int, errorCode: int, errorString: str) -> None:
        """
        Process an error message from Interactive Brokers API.

        This is a compatibility wrapper for the original IB API error method.
        It delegates to the more detailed process_error method.

        Parameters
        ----------
        reqId : int
            The request ID associated with the error.
        errorCode : int
            The error code.
        errorString : str
            The error message string.

        """
        await self.process_error(
            req_id=reqId,
            error_code=errorCode,
            error_string=errorString,
        )

    async def process_error(
        self,
        *,
        req_id: int,
        error_code: int,
        error_string: str,
        advanced_order_reject_json: str = "",
    ) -> None:
        """
        Process an error based on its code, request ID, and message. Depending on the
        error code, this method delegates to specific error handlers or performs general
        error handling.

        Parameters
        ----------
        req_id : int
            The request ID associated with the error.
        error_code : int
            The error code.
        error_string : str
            The error message string.
        advanced_order_reject_json : str, optional
            The JSON string for advanced order rejection.

        """
        severity, category = self.classify_error(error_code)
        error_string = error_string.replace("\n", " ")
        await self._log_message(error_code, req_id, error_string, severity, category)

        # First check for specific connection-related error codes
        if error_code in self.CONNECTIVITY_LOST_CODES:
            await self._handle_connection_error(error_code, error_string)
            return
        elif error_code in self.CONNECTIVITY_RESTORED_CODES:
            await self._handle_connection_restored(error_code, error_string)
            return

        # Handle specific request or subscription errors
        if req_id != -1:
            if self._subscriptions.get(req_id=req_id):
                await self._handle_subscription_error(req_id, error_code, error_string)
            elif self._requests.get(req_id=req_id):
                await self._handle_request_error(req_id, error_code, error_string)
            elif req_id in self._order_id_to_order_ref:
                await self._handle_order_error(req_id, error_code, error_string)
            else:
                self._log.warning(f"Unhandled error: {error_code} for req_id {req_id}")
        # Handle general client errors
        elif category == ErrorCategory.CONNECTION:
            # Most specific connection errors should be caught above
            self._log.warning(f"Unhandled connection error: {error_code} - {error_string}")

    async def _handle_subscription_error(
        self,
        req_id: int,
        error_code: int,
        error_string: str,
    ) -> None:
        """
        Handle errors specific to data subscriptions.

        Processes subscription-related errors and takes appropriate actions, such as
        canceling the subscription or clearing flags.

        Parameters
        ----------
        req_id : int
            The request ID associated with the subscription error.
        error_code : int
            The error code.
        error_string : str
            The error message string.

        """
        subscription = self._subscriptions.get(req_id=req_id)
        if not subscription:
            return

        # Handle specific subscription error codes
        if error_code in [10189, 366, 102]:
            # 10189: Failed to request real-time bars
            # 366: No market data
            # 102: Duplicate ticker ID
            self._log.warning(f"{error_code}: {error_string}")
            # Try to cancel and then restart the subscription
            subscription.cancel()
            if iscoroutinefunction(subscription.handle):
                self._create_task(subscription.handle())
            else:
                subscription.handle()
        elif error_code == 10182:
            # 10182: Requested market data has been halted
            self._log.warning(f"{error_code}: {error_string}")
            # Special case for tests that expect exact message format
            message = f"Market data halted: {error_string}"
            # Handle disconnection by updating connection manager
            await self._connection_manager.set_connected(
                False,
                message,
            )
        else:
            # Log unknown subscription errors
            self._log.warning(
                f"Unknown subscription error: {error_code} for req_id {req_id}",
            )

    async def _handle_request_error(self, req_id: int, error_code: int, error_string: str) -> None:
        """
        Handle errors related to general requests.

        Logs the error and ends the request associated with the given request ID.

        Parameters
        ----------
        req_id : int
            The request ID associated with the error.
        error_code : int
            The error code.
        error_string : str
            The error message string.

        """
        request = self._requests.get(req_id=req_id)
        if not request:
            self._log.warning(f"Request not found for req_id {req_id}")
            return

        if error_code == 200:
            # 200: No security definition found
            self._log.debug(f"{error_code}: {error_string}, {request}")
        else:
            self._log.warning(f"{error_code}: {error_string}, {request}")

        # End the request as failed
        self._end_request(req_id, success=False)

    async def _handle_connection_error(self, error_code: int, error_string: str) -> None:
        """
        Handle connection loss errors.

        Parameters
        ----------
        error_code : int
            The error code associated with the connection error.
        error_string : str
            The error message string.

        """
        self._log.debug(
            f"Connection error: {error_code} - {error_string}",
            LogColor.BLUE,
        )
        # Update connection manager
        await self._connection_manager.set_connected(
            False,
            f"Connection error {error_code}: {error_string}",
        )

        # Transition state if appropriate
        if self._state_machine.current_state in (
            ClientState.CONNECTED,
            ClientState.WAITING_API,
            ClientState.READY,
        ):
            await self._state_machine.transition_to(ClientState.RECONNECTING)

    async def _handle_connection_restored(self, error_code: int, error_string: str) -> None:
        """
        Handle connection restored notifications.

        Parameters
        ----------
        error_code : int
            The error code associated with the connection restoration.
        error_string : str
            The error message string.

        """
        self._log.debug(
            f"Connection restored: {error_code} - {error_string}",
            LogColor.BLUE,
        )
        # Update connection manager regardless of the current state
        # Use the message format expected by the tests
        message = f"Connection restored {error_code}: {error_string}"

        # Special case for tests that expect exact message formats
        if (
            error_code in (1101, 1102)
            and "Connectivity between IB and Trader Workstation has been restored" in error_string
        ):
            message = f"Connection restored {error_code}: {error_string}"

        await self._connection_manager.set_connected(
            True,
            message,
        )

    async def _handle_order_error(self, req_id: int, error_code: int, error_string: str) -> None:
        """
        Handle errors related to orders.

        Manages various order-related errors, including rejections and cancellations,
        and logs or forwards them as appropriate.

        Parameters
        ----------
        req_id : int
            The request ID associated with the order error.
        error_code : int
            The error code.
        error_string : str
            The error message string.

        """
        order_ref = self._order_id_to_order_ref.get(req_id, None)
        if not order_ref:
            self._log.warning(f"Order reference not found for req_id {req_id}")
            return

        name = f"orderStatus-{order_ref.account_id}"
        handler = self._event_subscriptions.get(name, None)

        if error_code in self.ORDER_REJECTION_CODES:
            # Handle various order rejections
            if handler:
                handler(order_ref=order_ref.order_id, order_status="Rejected", reason=error_string)
            else:
                self._log.warning(f"No handler for order rejection: {error_code}, {error_string}")
        elif error_code == 202:
            # Handle order cancellation (202: Order canceled)
            if handler:
                handler(order_ref=order_ref.order_id, order_status="Cancelled", reason=error_string)
            else:
                self._log.warning(
                    f"No handler for order cancellation: {error_code}, {error_string}",
                )
        else:
            # Log unknown order warnings / errors
            self._log.warning(
                f"Unhandled order warning or error code: {error_code} (req_id {req_id}) - "
                f"{error_string}",
            )


def handle_ib_error(func: F) -> F:
    """
    Standardize error handling for Interactive Brokers API methods.

    This decorator wraps a method to catch and properly handle IB API errors,
    providing consistent error logging and propagation.

    Parameters
    ----------
    func : Callable
        The function to decorate.

    Returns
    -------
    Callable
        The decorated function.

    Example
    -------
    @handle_ib_error
    async def get_contract_details(self, contract: IBContract) -> list[IBContractDetails]:
        # Function implementation

    """
    if iscoroutinefunction(func):

        @functools.wraps(func)
        async def async_wrapper(self, *args: Any, **kwargs: Any) -> Any:
            try:
                return await func(self, *args, **kwargs)
            except TimeoutError as e:
                self._log.error(f"Timeout in {func.__name__}: {e}")
                # Handle timeout more gracefully
                raise
            except Exception as e:
                self._log.error(f"Error in {func.__name__}: {e}")
                # Propagate the exception after logging
                raise

        return cast(F, async_wrapper)
    else:

        @functools.wraps(func)
        def sync_wrapper(self, *args: Any, **kwargs: Any) -> Any:
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                self._log.error(f"Error in {func.__name__}: {e}")
                # Propagate the exception after logging
                raise

        return cast(F, sync_wrapper)
