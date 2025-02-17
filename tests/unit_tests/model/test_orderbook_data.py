# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2023 Nautech Systems Pty Ltd. All rights reserved.
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

import pickle

from nautilus_trader.model.data import NULL_ORDER
from nautilus_trader.model.data import BookOrder
from nautilus_trader.model.data import OrderBookDelta
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.enums import BookAction
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs


AUDUSD = TestIdStubs.audusd_id()


def test_book_order_from_raw() -> None:
    # Arrange, Act
    order = BookOrder.from_raw(
        side=OrderSide.BUY,
        price_raw=10000000000,
        price_prec=1,
        size_raw=5000000000,
        size_prec=0,
        order_id=1,
    )

    # Assert
    assert str(order) == "BookOrder { side: Buy, price: 10.0, size: 5, order_id: 1 }"


def test_delta_fully_qualified_name() -> None:
    # Arrange, Act, Assert
    assert OrderBookDelta.fully_qualified_name() == "nautilus_trader.model.data:OrderBookDelta"


def test_delta_from_raw() -> None:
    # Arrange, Act
    delta = OrderBookDelta.from_raw(
        instrument_id=AUDUSD,
        action=BookAction.ADD,
        side=OrderSide.BUY,
        price_raw=10000000000,
        price_prec=1,
        size_raw=5000000000,
        size_prec=0,
        order_id=1,
        flags=0,
        sequence=123456789,
        ts_event=5_000_000,
        ts_init=1_000_000_000,
    )

    # Assert
    assert (
        str(delta)
        == "OrderBookDelta(instrument_id=AUD/USD.SIM, action=ADD, order=BookOrder { side: Buy, price: 10.0, size: 5, order_id: 1 }, flags=0, sequence=123456789, ts_event=5000000, ts_init=1000000000)"  # noqa
    )


def test_delta_pickle_round_trip() -> None:
    order = BookOrder(
        side=OrderSide.BUY,
        price=Price.from_str("10.0"),
        size=Quantity.from_str("5"),
        order_id=1,
    )

    delta = OrderBookDelta(
        instrument_id=AUDUSD,
        action=BookAction.ADD,
        order=order,
        flags=0,
        sequence=123456789,
        ts_event=0,
        ts_init=1_000_000_000,
    )

    # Act
    pickled = pickle.dumps(delta)
    unpickled = pickle.loads(pickled)  # noqa

    # Assert
    assert delta == unpickled


def test_delta_hash_str_and_repr() -> None:
    # Arrange
    order = BookOrder(
        side=OrderSide.BUY,
        price=Price.from_str("10.0"),
        size=Quantity.from_str("5"),
        order_id=1,
    )

    delta = OrderBookDelta(
        instrument_id=AUDUSD,
        action=BookAction.ADD,
        order=order,
        flags=0,
        sequence=123456789,
        ts_event=0,
        ts_init=1_000_000_000,
    )

    # Act, Assert
    assert isinstance(hash(delta), int)
    assert (
        str(delta)
        == "OrderBookDelta(instrument_id=AUD/USD.SIM, action=ADD, order=BookOrder { side: Buy, price: 10.0, size: 5, order_id: 1 }, flags=0, sequence=123456789, ts_event=0, ts_init=1000000000)"  # noqa
    )
    assert (
        repr(delta)
        == "OrderBookDelta(instrument_id=AUD/USD.SIM, action=ADD, order=BookOrder { side: Buy, price: 10.0, size: 5, order_id: 1 }, flags=0, sequence=123456789, ts_event=0, ts_init=1000000000)"  # noqa
    )


def test_delta_with_null_book_order() -> None:
    # Arrange
    delta = OrderBookDelta(
        instrument_id=AUDUSD,
        action=BookAction.CLEAR,
        order=NULL_ORDER,
        flags=32,
        sequence=123456789,
        ts_event=0,
        ts_init=1_000_000_000,
    )

    # Act, Assert
    assert isinstance(hash(delta), int)
    assert (
        str(delta)
        == "OrderBookDelta(instrument_id=AUD/USD.SIM, action=CLEAR, order=BookOrder { side: NoOrderSide, price: 0, size: 0, order_id: 0 }, flags=32, sequence=123456789, ts_event=0, ts_init=1000000000)"  # noqa
    )
    assert (
        repr(delta)
        == "OrderBookDelta(instrument_id=AUD/USD.SIM, action=CLEAR, order=BookOrder { side: NoOrderSide, price: 0, size: 0, order_id: 0 }, flags=32, sequence=123456789, ts_event=0, ts_init=1000000000)"  # noqa
    )


def test_delta_clear() -> None:
    # Arrange, Act
    delta = OrderBookDelta.clear(
        instrument_id=AUDUSD,
        ts_event=0,
        ts_init=1_000_000_000,
        sequence=42,
    )

    # Assert
    assert delta.action == BookAction.CLEAR
    assert delta.sequence == 42
    assert delta.ts_event == 0
    assert delta.ts_init == 1_000_000_000


def test_delta_to_dict_with_order_returns_expected_dict() -> None:
    # Arrange
    order = BookOrder(
        side=OrderSide.BUY,
        price=Price.from_str("10.0"),
        size=Quantity.from_str("5"),
        order_id=1,
    )

    delta = OrderBookDelta(
        instrument_id=AUDUSD,
        action=BookAction.ADD,
        order=order,
        flags=0,
        sequence=3,
        ts_event=1,
        ts_init=2,
    )

    # Act
    result = OrderBookDelta.to_dict(delta)

    # Assert
    assert result == {
        "type": "OrderBookDelta",
        "instrument_id": "AUD/USD.SIM",
        "action": "ADD",
        "order": {
            "side": "BUY",
            "price": "10.0",
            "size": "5",
            "order_id": 1,
        },
        "flags": 0,
        "sequence": 3,
        "ts_event": 1,
        "ts_init": 2,
    }


def test_delta_from_dict_returns_expected_delta() -> None:
    # Arrange
    order = BookOrder(
        side=OrderSide.BUY,
        price=Price.from_str("10.0"),
        size=Quantity.from_str("5"),
        order_id=1,
    )

    delta = OrderBookDelta(
        instrument_id=AUDUSD,
        action=BookAction.ADD,
        order=order,
        flags=0,
        sequence=3,
        ts_event=1,
        ts_init=2,
    )

    # Act
    result = OrderBookDelta.from_dict(OrderBookDelta.to_dict(delta))

    # Assert
    assert result == delta


def test_delta_from_dict_returns_expected_clear() -> None:
    # Arrange
    delta = OrderBookDelta(
        instrument_id=AUDUSD,
        action=BookAction.CLEAR,
        order=None,
        flags=0,
        sequence=3,
        ts_event=0,
        ts_init=0,
    )

    # Act
    result = OrderBookDelta.from_dict(OrderBookDelta.to_dict(delta))

    # Assert
    assert result == delta


def test_deltas_fully_qualified_name() -> None:
    # Arrange, Act, Assert
    assert OrderBookDeltas.fully_qualified_name() == "nautilus_trader.model.data:OrderBookDeltas"


def test_deltas_hash_str_and_repr() -> None:
    # Arrange
    order1 = BookOrder(
        side=OrderSide.BUY,
        price=Price.from_str("10.0"),
        size=Quantity.from_str("5"),
        order_id=1,
    )

    delta1 = OrderBookDelta(
        instrument_id=AUDUSD,
        action=BookAction.ADD,
        order=order1,
        flags=0,
        sequence=0,
        ts_event=0,
        ts_init=0,
    )

    order2 = BookOrder(
        side=OrderSide.BUY,
        price=Price.from_str("10.0"),
        size=Quantity.from_str("15"),
        order_id=2,
    )

    delta2 = OrderBookDelta(
        instrument_id=AUDUSD,
        action=BookAction.ADD,
        order=order2,
        flags=0,
        sequence=1,
        ts_event=0,
        ts_init=0,
    )

    deltas = OrderBookDeltas(
        instrument_id=AUDUSD,
        deltas=[delta1, delta2],
    )

    # Act, Assert
    assert isinstance(hash(deltas), int)
    assert (
        str(deltas)
        == "OrderBookDeltas(instrument_id=AUD/USD.SIM, [OrderBookDelta(instrument_id=AUD/USD.SIM, action=ADD, order=BookOrder { side: Buy, price: 10.0, size: 5, order_id: 1 }, flags=0, sequence=0, ts_event=0, ts_init=0), OrderBookDelta(instrument_id=AUD/USD.SIM, action=ADD, order=BookOrder { side: Buy, price: 10.0, size: 15, order_id: 2 }, flags=0, sequence=1, ts_event=0, ts_init=0)], is_snapshot=False, sequence=1, ts_event=0, ts_init=0)"  # noqa
    )
    assert (
        repr(deltas)
        == "OrderBookDeltas(instrument_id=AUD/USD.SIM, [OrderBookDelta(instrument_id=AUD/USD.SIM, action=ADD, order=BookOrder { side: Buy, price: 10.0, size: 5, order_id: 1 }, flags=0, sequence=0, ts_event=0, ts_init=0), OrderBookDelta(instrument_id=AUD/USD.SIM, action=ADD, order=BookOrder { side: Buy, price: 10.0, size: 15, order_id: 2 }, flags=0, sequence=1, ts_event=0, ts_init=0)], is_snapshot=False, sequence=1, ts_event=0, ts_init=0)"  # noqa
    )


def test_deltas_to_dict() -> None:
    # Arrange
    order1 = BookOrder(
        side=OrderSide.BUY,
        price=Price.from_str("10.0"),
        size=Quantity.from_str("5"),
        order_id=1,
    )

    delta1 = OrderBookDelta(
        instrument_id=AUDUSD,
        action=BookAction.ADD,
        order=order1,
        flags=0,
        sequence=0,
        ts_event=0,
        ts_init=0,
    )

    order2 = BookOrder(
        side=OrderSide.BUY,
        price=Price.from_str("10.0"),
        size=Quantity.from_str("15"),
        order_id=2,
    )

    delta2 = OrderBookDelta(
        instrument_id=AUDUSD,
        action=BookAction.ADD,
        order=order2,
        flags=0,
        sequence=1,
        ts_event=0,
        ts_init=0,
    )

    deltas = OrderBookDeltas(
        instrument_id=AUDUSD,
        deltas=[delta1, delta2],
    )

    # Act
    result = OrderBookDeltas.to_dict(deltas)

    # Assert
    assert result
    assert result == {
        "type": "OrderBookDeltas",
        "instrument_id": "AUD/USD.SIM",
        "deltas": b'[{"type":"OrderBookDelta","instrument_id":"AUD/USD.SIM","action":"ADD","order":{"side":"BUY","price":"10.0","size":"5","order_id":1},"flags":0,"sequence":0,"ts_event":0,"ts_init":0},{"type":"OrderBookDelta","instrument_id":"AUD/USD.SIM","action":"ADD","order":{"side":"BUY","price":"10.0","size":"15","order_id":2},"flags":0,"sequence":1,"ts_event":0,"ts_init":0}]',  # noqa
    }


def test_deltas_from_dict_returns_expected_dict() -> None:
    # Arrange
    order1 = BookOrder(
        side=OrderSide.BUY,
        price=Price.from_str("10.0"),
        size=Quantity.from_str("5"),
        order_id=1,
    )

    delta1 = OrderBookDelta(
        instrument_id=AUDUSD,
        action=BookAction.ADD,
        order=order1,
        flags=0,
        sequence=0,
        ts_event=0,
        ts_init=0,
    )

    order2 = BookOrder(
        side=OrderSide.BUY,
        price=Price.from_str("10.0"),
        size=Quantity.from_str("15"),
        order_id=2,
    )

    delta2 = OrderBookDelta(
        instrument_id=AUDUSD,
        action=BookAction.ADD,
        order=order2,
        flags=0,
        sequence=1,
        ts_event=0,
        ts_init=0,
    )

    deltas = OrderBookDeltas(
        instrument_id=AUDUSD,
        deltas=[delta1, delta2],
    )

    # Act
    result = OrderBookDeltas.from_dict(OrderBookDeltas.to_dict(deltas))

    # Assert
    assert result == deltas
