# Fixed OrderTransformer with proper mappings

from abc import ABC, abstractmethod
from typing import Dict, Any, Type

from ibapi.order import Order as IBOrder

# Import the necessary mappings directly from execution.py
from nautilus_trader.adapters.interactive_brokers.parsing.execution import MAP_ORDER_ACTION
from nautilus_trader.adapters.interactive_brokers.parsing.execution import MAP_TIME_IN_FORCE
from nautilus_trader.adapters.interactive_brokers.parsing.execution import MAP_ORDER_TYPE
from nautilus_trader.adapters.interactive_brokers.parsing.execution import MAP_TRIGGER_METHOD

from nautilus_trader.model.orders.base import Order
from nautilus_trader.model.orders.limit_if_touched import LimitIfTouchedOrder
from nautilus_trader.model.orders.market_if_touched import MarketIfTouchedOrder
from nautilus_trader.model.orders.stop_limit import StopLimitOrder
from nautilus_trader.model.orders.stop_market import StopMarketOrder
from nautilus_trader.model.orders.trailing_stop_limit import TrailingStopLimitOrder
from nautilus_trader.model.orders.trailing_stop_market import TrailingStopMarketOrder
from nautilus_trader.model.enums import OrderSide, OrderType, TimeInForce, TrailingOffsetType, TriggerType
from nautilus_trader.model.enums import trailing_offset_type_to_str


class OrderTransformer(ABC):
    """
    Abstract base class for order transformers.
    
    Order transformers are responsible for converting Nautilus orders
    to Interactive Brokers orders with proper field mapping and validation.
    """
    
    @abstractmethod
    def transform(self, order: Order, ib_order: IBOrder) -> IBOrder:
        """
        Transform a Nautilus order to an Interactive Brokers order.
        
        Parameters
        ----------
        order : Order
            The Nautilus order to transform.
        ib_order : IBOrder
            The IB order to populate (may already have some fields set).
            
        Returns
        -------
        IBOrder
            The populated Interactive Brokers order.
        """
        pass


class BaseOrderTransformer(OrderTransformer):
    """
    Base order transformer for common order fields.
    
    This transformer handles fields that are common to all order types.
    """
    
    def __init__(self, exec_client):
        """
        Initialize the transformer with reference to the execution client.
        
        Parameters
        ----------
        exec_client : InteractiveBrokersExecutionClient
            The execution client containing needed references.
        """
        self._exec_client = exec_client
    
    def transform(self, order: Order, ib_order: IBOrder) -> IBOrder:
        """
        Transform common order fields from Nautilus to IB format.
        
        Parameters
        ----------
        order : Order
            The Nautilus order to transform.
        ib_order : IBOrder
            The IB order to populate.
            
        Returns
        -------
        IBOrder
            The populated Interactive Brokers order with common fields.
        """
        # Client order ID
        ib_order.orderRef = order.client_order_id.value
        
        # Order side - using imported MAP_ORDER_ACTION
        ib_order.action = MAP_ORDER_ACTION[order.side]
        
        # Time in force - using imported MAP_TIME_IN_FORCE
        time_in_force = order.time_in_force
        ib_order.tif = MAP_TIME_IN_FORCE[time_in_force]
        
        # Quantity
        ib_order.totalQuantity = order.quantity.as_decimal()
        
        # Display quantity if specified
        if hasattr(order, 'display_qty') and order.display_qty:
            ib_order.displaySize = order.display_qty.as_double()
        
        # Expire time for GTD orders
        if order.time_in_force == TimeInForce.GTD and order.expire_time:
            ib_order.goodTillDate = order.expire_time.strftime("%Y%m%d %H:%M:%S %Z")
        
        # Parent order ID if specified
        if hasattr(order, 'parent_order_id') and order.parent_order_id:
            ib_order.parentId = order.parent_order_id.value
        
        # Handle inverse instruments
        instrument = self._exec_client.instrument_provider.find(order.instrument_id)
        if hasattr(instrument, 'is_inverse') and instrument.is_inverse:
            ib_order.cashQty = int(ib_order.totalQuantity)
            ib_order.totalQuantity = 0
            
        return ib_order


class LimitOrderTransformer(BaseOrderTransformer):
    """Transformer for limit orders."""
    
    def transform(self, order: Order, ib_order: IBOrder) -> IBOrder:
        """Transform limit order fields."""
        # Apply base transformation first
        ib_order = super().transform(order, ib_order)
        
        # Set orderType based on time_in_force - using MAP_ORDER_TYPE when needed
        if order.time_in_force == TimeInForce.AT_THE_CLOSE:
            ib_order.orderType = "LOC"
        else:
            ib_order.orderType = "LMT"
        
        # Set limit price
        if order.price is not None:
            ib_order.lmtPrice = order.price.as_double()
            
        return ib_order


class MarketOrderTransformer(BaseOrderTransformer):
    """Transformer for market orders."""
    
    def transform(self, order: Order, ib_order: IBOrder) -> IBOrder:
        """Transform market order fields."""
        # Apply base transformation first
        ib_order = super().transform(order, ib_order)
        
        # Set orderType based on time_in_force - using MAP_ORDER_TYPE when needed
        if order.time_in_force == TimeInForce.AT_THE_CLOSE:
            ib_order.orderType = "MOC"
        else:
            ib_order.orderType = "MKT"
            
        return ib_order


class StopMarketOrderTransformer(BaseOrderTransformer):
    """Transformer for stop market orders."""
    
    def transform(self, order: StopMarketOrder, ib_order: IBOrder) -> IBOrder:
        """Transform stop market order fields."""
        # Apply base transformation first
        ib_order = super().transform(order, ib_order)
        
        ib_order.orderType = "STP"
        
        # Set stop price
        if order.trigger_price is not None:
            ib_order.auxPrice = order.trigger_price.as_double()
            
        # Set trigger method if specified - using imported MAP_TRIGGER_METHOD
        if hasattr(order, 'trigger_type') and order.trigger_type:
            ib_order.triggerMethod = MAP_TRIGGER_METHOD[order.trigger_type]
            
        return ib_order


class StopLimitOrderTransformer(BaseOrderTransformer):
    """Transformer for stop limit orders."""
    
    def transform(self, order: StopLimitOrder, ib_order: IBOrder) -> IBOrder:
        """Transform stop limit order fields."""
        # Apply base transformation first
        ib_order = super().transform(order, ib_order)
        
        ib_order.orderType = "STP LMT"
        
        # Set limit price
        if order.price is not None:
            ib_order.lmtPrice = order.price.as_double()
            
        # Set stop price
        if order.trigger_price is not None:
            ib_order.auxPrice = order.trigger_price.as_double()
            
        # Set trigger method if specified - using imported MAP_TRIGGER_METHOD
        if hasattr(order, 'trigger_type') and order.trigger_type:
            ib_order.triggerMethod = MAP_TRIGGER_METHOD[order.trigger_type]
            
        return ib_order


class MarketIfTouchedOrderTransformer(BaseOrderTransformer):
    """Transformer for MIT orders."""
    
    def transform(self, order: MarketIfTouchedOrder, ib_order: IBOrder) -> IBOrder:
        """Transform MIT order fields."""
        # Apply base transformation first
        ib_order = super().transform(order, ib_order)
        
        ib_order.orderType = "MIT"
        
        # Set trigger price
        if order.trigger_price is not None:
            ib_order.auxPrice = order.trigger_price.as_double()
            
        return ib_order


class LimitIfTouchedOrderTransformer(BaseOrderTransformer):
    """Transformer for LIT orders."""
    
    def transform(self, order: LimitIfTouchedOrder, ib_order: IBOrder) -> IBOrder:
        """Transform LIT order fields."""
        # Apply base transformation first
        ib_order = super().transform(order, ib_order)
        
        ib_order.orderType = "LIT"
        
        # Set limit price
        if order.price is not None:
            ib_order.lmtPrice = order.price.as_double()
            
        # Set trigger price
        if order.trigger_price is not None:
            ib_order.auxPrice = order.trigger_price.as_double()
            
        return ib_order


class TrailingStopMarketOrderTransformer(BaseOrderTransformer):
    """Transformer for trailing stop market orders."""
    
    def transform(self, order: TrailingStopMarketOrder, ib_order: IBOrder) -> IBOrder:
        """Transform trailing stop market order fields."""
        # Apply base transformation first
        ib_order = super().transform(order, ib_order)
        
        ib_order.orderType = "TRAIL"
        
        # Validate trailing offset type
        if order.trailing_offset_type != TrailingOffsetType.PRICE:
            raise ValueError(
                f"`TrailingOffsetType` {trailing_offset_type_to_str(order.trailing_offset_type)} is not supported",
            )
            
        # Set trailing amount
        ib_order.auxPrice = float(order.trailing_offset)
        
        # Set initial stop price if provided
        if order.trigger_price:
            ib_order.trailStopPrice = order.trigger_price.as_double()
            
        # Set trigger method if specified - using imported MAP_TRIGGER_METHOD
        if hasattr(order, 'trigger_type') and order.trigger_type:
            ib_order.triggerMethod = MAP_TRIGGER_METHOD[order.trigger_type]
            
        return ib_order


class TrailingStopLimitOrderTransformer(BaseOrderTransformer):
    """Transformer for trailing stop limit orders."""
    
    def transform(self, order: TrailingStopLimitOrder, ib_order: IBOrder) -> IBOrder:
        """Transform trailing stop limit order fields."""
        # Apply base transformation first
        ib_order = super().transform(order, ib_order)
        
        ib_order.orderType = "TRAIL LIMIT"
        
        # Validate trailing offset type
        if order.trailing_offset_type != TrailingOffsetType.PRICE:
            raise ValueError(
                f"`TrailingOffsetType` {trailing_offset_type_to_str(order.trailing_offset_type)} is not supported",
            )
            
        # Set trailing amount
        ib_order.auxPrice = float(order.trailing_offset)
        
        # Set limit price offset
        if order.limit_offset is not None:
            ib_order.lmtPriceOffset = order.limit_offset
            
        # Set initial stop price if provided
        if order.trigger_price:
            ib_order.trailStopPrice = order.trigger_price.as_double()
            
        # Set trigger method if specified - using imported MAP_TRIGGER_METHOD
        if hasattr(order, 'trigger_type') and order.trigger_type:
            ib_order.triggerMethod = MAP_TRIGGER_METHOD[order.trigger_type]
            
        return ib_order


class OrderTransformerFactory:
    """
    Factory for creating order transformers based on order type.
    
    This factory creates and caches appropriate transformers for different
    order types, providing a clean way to handle different order transformations.
    """
    
    def __init__(self, exec_client):
        """
        Initialize the factory with reference to the execution client.
        
        Parameters
        ----------
        exec_client : InteractiveBrokersExecutionClient
            The execution client reference.
        """
        self._exec_client = exec_client
        self._transformers: Dict[Type[Order], OrderTransformer] = {
            # Market orders
            OrderType.MARKET: MarketOrderTransformer(exec_client),
            # Limit orders
            OrderType.LIMIT: LimitOrderTransformer(exec_client),
            # Stop orders
            StopMarketOrder: StopMarketOrderTransformer(exec_client),
            StopLimitOrder: StopLimitOrderTransformer(exec_client),
            # If-touched orders
            MarketIfTouchedOrder: MarketIfTouchedOrderTransformer(exec_client),
            LimitIfTouchedOrder: LimitIfTouchedOrderTransformer(exec_client),
            # Trailing stop orders
            TrailingStopMarketOrder: TrailingStopMarketOrderTransformer(exec_client),
            TrailingStopLimitOrder: TrailingStopLimitOrderTransformer(exec_client),
        }
    
    def transform_order(self, order: Order) -> IBOrder:
        """
        Transform a Nautilus order to an Interactive Brokers order.
        
        Parameters
        ----------
        order : Order
            The Nautilus order to transform.
            
        Returns
        -------
        IBOrder
            The transformed Interactive Brokers order.
            
        Raises
        ------
        ValueError
            If the order type is not supported.
        """
        # Create a new IB order
        ib_order = IBOrder()
        
        # Find the appropriate transformer
        transformer = None
        
        # Try to get transformer by order instance type
        for order_type, order_transformer in self._transformers.items():
            if isinstance(order_type, type) and isinstance(order, order_type):
                transformer = order_transformer
                break
        
        # If not found, try to get by order type enum
        if transformer is None and hasattr(order, 'order_type'):
            transformer = self._transformers.get(order.order_type)
            
        if transformer is None:
            raise ValueError(f"Unsupported order type: {type(order)}")
            
        # Apply transformation
        ib_order = transformer.transform(order, ib_order)
        
        # Set contract details
        details = self._exec_client.instrument_provider.contract_details[order.instrument_id.value]
        ib_order.contract = details.contract
        
        # Set account details
        ib_order.account = self._exec_client.account_id.get_id()
        ib_order.clearingAccount = self._exec_client.account_id.get_id()
        
        # Handle order tags
        if order.tags:
            ib_order = self._exec_client._attach_order_tags(ib_order, order)
            
        return ib_order


# Replace the existing _transform_order_to_ib_order method in InteractiveBrokersExecutionClient
def _transform_order_to_ib_order(self, order: Order) -> IBOrder:
    """
    Transform a Nautilus order to an Interactive Brokers order.
    
    Parameters
    ----------
    order : Order
        The Nautilus order to transform.
        
    Returns
    -------
    IBOrder
        The transformed Interactive Brokers order.
        
    Raises
    ------
    ValueError
        If the order type is not supported or if required order
        properties are missing or invalid.
    """
    # Ensure a transformer factory exists
    if not hasattr(self, '_order_transformer_factory'):
        self._order_transformer_factory = OrderTransformerFactory(self)
        
    # Transform the order using the appropriate transformer
    return self._order_transformer_factory.transform_order(order)
