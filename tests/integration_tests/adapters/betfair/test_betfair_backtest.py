from nautilus_trader.adapters.betfair.constants import BETFAIR_VENUE
from nautilus_trader.adapters.betfair.parsing.core import BetfairParser
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.engine import BacktestEngineConfig
from nautilus_trader.backtest.engine import Decimal
from nautilus_trader.config import LoggingConfig
from nautilus_trader.examples.strategies.orderbook_imbalance import OrderBookImbalance
from nautilus_trader.examples.strategies.orderbook_imbalance import OrderBookImbalanceConfig
from nautilus_trader.model.currencies import GBP
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.objects import Money
from tests.integration_tests.adapters.betfair.test_kit import BetfairDataProvider
from tests.integration_tests.adapters.betfair.test_kit import betting_instrument


def test_betfair_backtest():
    # Arrange
    config = BacktestEngineConfig(
        trader_id="BACKTESTER-001",
        logging=LoggingConfig(bypass_logging=True),
    )

    # Build the backtest engine
    engine = BacktestEngine(config=config)

    # Add a trading venue (multiple venues possible)
    engine.add_venue(
        venue=BETFAIR_VENUE,
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,  # Spot CASH account (not for perpetuals or futures)
        base_currency=GBP,  # Multi-currency account
        starting_balances=[Money(100_000, GBP)],
        book_type=BookType.L2_MBP,
    )

    # Add instruments
    instruments = [
        betting_instrument(
            market_id="1.166811431",
            selection_id=19248890,
            selection_handicap=None,
        ),
        betting_instrument(
            market_id="1.166811431",
            selection_id=38848248,
            selection_handicap=None,
        ),
    ]
    engine.add_instrument(instruments[0])
    engine.add_instrument(instruments[1])

    # Add data
    raw = list(BetfairDataProvider.market_updates())
    parser = BetfairParser(currency="GBP")
    updates = [upd for update in raw for upd in parser.parse(update)]
    engine.add_data(updates, client_id=ClientId("BETFAIR"))

    # Configure your strategy
    strategies = [
        OrderBookImbalance(
            config=OrderBookImbalanceConfig(
                instrument_id=instrument.id.value,
                max_trade_size=Decimal(10),
                order_id_tag=instrument.selection_id,
            ),
        )
        for instrument in instruments
    ]
    engine.add_strategies(strategies)

    # Act
    engine.run()

    # Assert
    account = engine.trader.generate_account_report(BETFAIR_VENUE)
    fills = engine.trader.generate_order_fills_report()
    positions = engine.trader.generate_positions_report()
    assert account.iloc[-1]["total"] == "49095.42"
    assert len(fills) == 2708
    assert len(positions) == 2
