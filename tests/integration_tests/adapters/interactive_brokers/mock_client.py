import asyncio
from collections.abc import Callable
from unittest.mock import MagicMock
from unittest.mock import AsyncMock

from ibapi.client import EClient
from ibapi.order import Order as IBOrder
from ibapi.contract import Contract as IBContract

# fmt: off
from nautilus_trader.adapters.interactive_brokers.client.client import InteractiveBrokersClient
from nautilus_trader.adapters.interactive_brokers.client.common import ClientState
from nautilus_trader.adapters.interactive_brokers.client.wrapper import InteractiveBrokersEWrapper
from nautilus_trader.adapters.interactive_brokers.common import IBContract as NautilusIBContract
from nautilus_trader.adapters.interactive_brokers.parsing.instruments import ib_contract_to_instrument_id
from nautilus_trader.common.enums import LogColor
from tests.integration_tests.adapters.interactive_brokers.test_kit import IBTestContractStubs
from tests.integration_tests.adapters.interactive_brokers.test_kit import IBTestDataStubs
from tests.integration_tests.adapters.interactive_brokers.test_kit import IBTestExecStubs


class MockEClient(EClient):
    """
    MockEClient is a subclass of EClient which is used for simulating Interactive
    Brokers' client operations.

    This class overloads a few methods of the parent class to better accommodate testing
    needs. More methods can be added as and when needed, depending on the testing
    requirements.

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._next_valid_order_id = 1
        self._next_valid_req_id = 1
        self._next_valid_ticker_id = 1
        self._order_map = {}  # client_order_id -> IB Order ID
        
    def _handle_task(self, handler: Callable, **kwargs):
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(handler(**kwargs))  # noqa: RUF006
        else:
            loop.run_until_complete(handler(**kwargs))
    
    def nextValidId(self, order_id: int):
        self._next_valid_order_id = order_id
        self._handle_task(
            self.wrapper._client.process_next_valid_id,
            order_id=order_id,
        )
    
    def _get_next_valid_order_id(self):
        current_id = self._next_valid_order_id
        self._next_valid_order_id += 1
        return current_id
    
    def _get_next_valid_req_id(self):
        current_id = self._next_valid_req_id
        self._next_valid_req_id += 1
        return current_id

    def _get_next_valid_ticker_id(self):
        current_id = self._next_valid_ticker_id
        self._next_valid_ticker_id += 1
        return current_id

    #########################################################################
    ################## Market Data
    #########################################################################
    
    def reqMktData(self, req_id, contract, generic_tick_list, snapshot, regulatory_snapshot, mkt_data_options):
        # Simulate returning market data
        self._handle_task(
            self.wrapper._client.process_tick_price,
            req_id=req_id,
            tick_type=1,  # bid price
            price=100.0,
            attrib={},
        )
        self._handle_task(
            self.wrapper._client.process_tick_size,
            req_id=req_id,
            tick_type=0,  # bid size
            size=100,
        )
        self._handle_task(
            self.wrapper._client.process_tick_price,
            req_id=req_id,
            tick_type=2,  # ask price
            price=100.1,
            attrib={},
        )
        self._handle_task(
            self.wrapper._client.process_tick_size,
            req_id=req_id,
            tick_type=3,  # ask size
            size=200,
        )
        self._handle_task(
            self.wrapper._client.process_tick_price,
            req_id=req_id,
            tick_type=4,  # last price
            price=100.05,
            attrib={},
        )
        self._handle_task(
            self.wrapper._client.process_tick_size,
            req_id=req_id,
            tick_type=5,  # last size
            size=10,
        )

    def cancelMktData(self, req_id):
        pass  # No need to do anything as this is just a mock

    #########################################################################
    ################## Options
    #########################################################################

    #########################################################################
    ################## Orders
    #########################################################################
    
    def placeOrder(self, order_id, contract, order):
        # Store the order information for later reference
        self._order_map[order.clientId] = order_id
        
        # Notify about order placement
        self._handle_task(
            self.wrapper._client.process_open_order,
            order_id=order_id,
            contract=contract,
            order=order,
            order_state=IBTestExecStubs.order_state("Submitted"),
        )
        
        # Update order status
        self._handle_task(
            self.wrapper._client.process_order_status,
            order_id=order_id,
            status="Submitted",
            filled=0,
            remaining=order.totalQuantity,
            avg_fill_price=0.0,
            perm_id=1,
            parent_id=0,
            last_fill_price=0.0,
            client_id=order.clientId,
            why_held="",
            mkt_cap_price=0.0,
        )
        
        # If this is a whatIf order, simulate a pre-submitted status
        if hasattr(order, "whatIf") and order.whatIf:
            self._handle_task(
                self.wrapper._client.process_order_status,
                order_id=order_id,
                status="PreSubmitted",
                filled=0,
                remaining=order.totalQuantity,
                avg_fill_price=0.0,
                perm_id=1,
                parent_id=0,
                last_fill_price=0.0,
                client_id=order.clientId,
                why_held="",
                mkt_cap_price=0.0,
            )
        else:
            # For normal orders, simulate accepted status
            self._handle_task(
                self.wrapper._client.process_order_status,
                order_id=order_id,
                status="Submitted",
                filled=0,
                remaining=order.totalQuantity,
                avg_fill_price=0.0,
                perm_id=1,
                parent_id=0,
                last_fill_price=0.0,
                client_id=order.clientId,
                why_held="",
                mkt_cap_price=0.0,
            )
    
    def cancelOrder(self, order_id, manual_cancel_order_time=""):
        # Simulate order cancellation
        self._handle_task(
            self.wrapper._client.process_order_status,
            order_id=order_id,
            status="Cancelled",
            filled=0,
            remaining=100,  # Assuming original quantity was 100
            avg_fill_price=0.0,
            perm_id=1,
            parent_id=0,
            last_fill_price=0.0,
            client_id=1,
            why_held="",
            mkt_cap_price=0.0,
        )

    #########################################################################
    ################## Account and Portfolio
    #########################################################################
    
    def reqAccountSummary(self, req_id, group_name, tags):
        account_values = IBTestDataStubs.account_values()
        for summary in account_values:
            self._handle_task(
                self.wrapper._client.process_account_summary,
                req_id=req_id,
                account=summary["account"],
                tag=summary["tag"],
                value=summary["value"],
                currency=summary["currency"],
            )
        
        self._handle_task(
            self.wrapper._client.process_account_summary_end,
            req_id=req_id,
        )

    def cancelAccountSummary(self, req_id):
        pass  # No need to do anything as this is just a mock

    #########################################################################
    ################## Daily PnL
    #########################################################################

    #########################################################################
    ################## Executions
    #########################################################################

    #########################################################################
    ################## Contract Details
    #########################################################################

    def reqContractDetails(self, reqId: int, contract: NautilusIBContract):
        instrument_id = ib_contract_to_instrument_id(contract)
        
        # Handle known contract types
        match instrument_id.value:
            case "AAPL.NASDAQ":
                self._handle_task(
                    self.wrapper._client.process_contract_details,
                    req_id=reqId,
                    contract_details=IBTestContractStubs.aapl_equity_contract_details(),
                )
            case "CLZ23.NYMEX":
                self._handle_task(
                    self.wrapper._client.process_contract_details,
                    req_id=reqId,
                    contract_details=IBTestContractStubs.cl_future_contract_details(),
                )
            case "TSLA230120C00100000.MIAX":
                self._handle_task(
                    self.wrapper._client.process_contract_details,
                    req_id=reqId,
                    contract_details=IBTestContractStubs.tsla_option_contract_details(),
                )
            case "EUR/USD.IDEALPRO":
                self._handle_task(
                    self.wrapper._client.process_contract_details,
                    req_id=reqId,
                    contract_details=IBTestContractStubs.eurusd_forex_contract_details(),
                )
            case _:
                # Default to AAPL for unknown contracts
                self._handle_task(
                    self.wrapper._client.process_contract_details,
                    req_id=reqId,
                    contract_details=IBTestContractStubs.aapl_equity_contract_details(),
                )

        # Always send end signal
        self._handle_task(
            self.wrapper._client.process_contract_details_end,
            req_id=reqId,
        )

    # Support for finding by contract ID
    def reqContractDetailsById(self, reqId: int, contract_id: int):
        # Map common contract IDs to contract details
        contract_map = {
            12087792: IBTestContractStubs.eurusd_forex_contract_details(),
            174230596: IBTestContractStubs.cl_future_contract_details(),
            495512572: IBTestContractStubs.aapl_equity_contract_details(),
            553946872: IBTestContractStubs.tsla_option_contract_details(),
        }
        
        if contract_id in contract_map:
            self._handle_task(
                self.wrapper._client.process_contract_details,
                req_id=reqId,
                contract_details=contract_map[contract_id],
            )
        
        self._handle_task(
            self.wrapper._client.process_contract_details_end,
            req_id=reqId,
        )

    #########################################################################
    ################## Market Depth
    #########################################################################

    #########################################################################
    ################## News Bulletins
    #########################################################################

    #########################################################################
    ################## Financial Advisors
    #########################################################################
    def reqManagedAccts(self):
        self._handle_task(
            self.wrapper._client.process_managed_accounts,
            accounts_list="DU1234567,",
        )

    #########################################################################
    ################## Historical Data
    #########################################################################
    
    def reqHistoricalData(
        self, 
        req_id, 
        contract, 
        end_date_time, 
        duration_str, 
        bar_size_setting, 
        what_to_show, 
        use_rth, 
        format_date, 
        keep_up_to_date, 
        chart_options
    ):
        # Simulate sending some historical data
        bars = IBTestDataStubs.historical_bars()
        for bar in bars:
            self._handle_task(
                self.wrapper._client.process_historical_data,
                req_id=req_id,
                bar=bar,
            )
        
        self._handle_task(
            self.wrapper._client.process_historical_data_end,
            req_id=req_id,
            start_date="",
            end_date="",
        )

    #########################################################################
    ################## Market Scanners
    #########################################################################

    #########################################################################
    ################## Real Time Bars
    #########################################################################

    #########################################################################
    ################## Fundamental Data
    #########################################################################

    ########################################################################
    ################## News
    #########################################################################

    #########################################################################
    ################## Display Groups
    #########################################################################


class MockInteractiveBrokersClient(InteractiveBrokersClient):
    """
    MockInteractiveBrokersClient is a subclass of InteractiveBrokersClient used for
    simulating client operations.

    This class initializes the EClient with a mocked version for testing purposes.
    It overrides several async methods to make them more reliable in a testing context.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Initialize with mock client
        self._eclient = MockEClient(
            wrapper=InteractiveBrokersEWrapper(
                nautilus_logger=self._log,
                client=self,
            ),
        )
        
        # Configure connection manager for mock
        self._connection_manager = MagicMock()
        self._connection_manager.is_connected = True
        self._connection_manager.set_connected = AsyncMock()
        self._connection_manager.set_ready = AsyncMock()
        
        # Mock task registry to avoid actual task creation/cancellation issues
        self._task_registry = MagicMock()
        self._task_registry.create_task = AsyncMock()
        self._task_registry.cancel_task = AsyncMock()
        self._task_registry.cancel_all_tasks = AsyncMock()
        self._task_registry.wait_for_all_tasks = AsyncMock(return_value=True)
        
        # Pre-set some account IDs for testing
        self._account_ids = {"DU1234567"}

    async def _start_async(self):
        """Override to avoid actual async operations and simulate immediate startup."""
        self._eclient.startApi()
        self._eclient.nextValidId(1)  # Initialize with order ID 1
        
        # Transition to READY state immediately
        await self._state_machine.transition_to(ClientState.READY)
        
        # Update the connection manager
        await self._connection_manager.set_ready(True, "Client ready")
        
        self._log.debug("Client ready", LogColor.BLUE)
        self._connection_attempts = 0
    
    async def _connect(self):
        """Override to avoid actual connection attempts."""
        # Just set the state to ready without actual connection logic
        await self._state_machine.transition_to(ClientState.READY)
        return True
    
    async def _disconnect(self):
        """Override to avoid actual disconnection attempts."""
        await self._state_machine.transition_to(ClientState.DISCONNECTED)
        return True
    
    async def _await_request(self, req_id, timeout=0.1):
        """
        Override to avoid long timeouts in tests.
        Returns immediately to speed up tests.
        """
        # Just return True immediately instead of waiting
        return True
    
    async def _calculate_reconnect_delay(self):
        """Override to avoid long reconnection delays in tests."""
        return 0.001  # Very short delay for testing purposes
    
    async def get_contract_details(self, contract):
        """Override to avoid actual API calls and return test data synchronously."""
        instrument_id = ib_contract_to_instrument_id(contract)
        
        # Map known instrument IDs to contract details
        if "AAPL.NASDAQ" in instrument_id.value:
            return [IBTestContractStubs.aapl_equity_contract_details()]
        elif "CLZ23.NYMEX" in instrument_id.value:
            return [IBTestContractStubs.cl_future_contract_details()]
        elif "TSLA230120C00100000.MIAX" in instrument_id.value:
            return [IBTestContractStubs.tsla_option_contract_details()]
        elif "EUR/USD.IDEALPRO" in instrument_id.value:
            return [IBTestContractStubs.eurusd_forex_contract_details()]
        else:
            # Default to AAPL for unknown contracts
            return [IBTestContractStubs.aapl_equity_contract_details()]
    
    async def get_contract_details_by_id(self, contract_id):
        """Override to avoid actual API calls and return test data synchronously."""
        # Map common contract IDs to contract details
        contract_map = {
            12087792: [IBTestContractStubs.eurusd_forex_contract_details()],
            174230596: [IBTestContractStubs.cl_future_contract_details()],
            495512572: [IBTestContractStubs.aapl_equity_contract_details()],
            553946872: [IBTestContractStubs.tsla_option_contract_details()],
        }
        
        if contract_id in contract_map:
            return contract_map[contract_id]
        else:
            # Default to AAPL for unknown contract IDs
            return [IBTestContractStubs.aapl_equity_contract_details()]
