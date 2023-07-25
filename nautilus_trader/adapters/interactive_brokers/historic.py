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

import datetime
import logging
from typing import Literal, Optional, Union, Iterable

import pandas as pd
import pytz
from ib_insync import IB
from ib_insync import BarData
from ib_insync import BarDataList
from ib_insync import Contract
from ib_insync import HistoricalTickBidAsk
from ib_insync import HistoricalTickLast

from nautilus_trader.adapters.interactive_brokers.parsing.data import generate_trade_id
from nautilus_trader.adapters.interactive_brokers.parsing.data import timedelta_to_duration_str
from nautilus_trader.adapters.interactive_brokers.parsing.instruments import parse_instrument
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarSpecification
from nautilus_trader.model.data import BarAggregation
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.enums import bar_aggregation_to_str
from nautilus_trader.model.enums import price_type_to_str
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
from nautilus_trader.persistence.external.core import write_objects


logger = logging.getLogger(__name__)


def generate_filename(
    catalog: ParquetDataCatalog,
    instrument_id: InstrumentId,
    kind: Literal["BID_ASK", "TRADES"],
    date: datetime.date,
) -> str:
    fn_kind = {"BID_ASK": "quote_tick", "TRADES": "trade_tick", "BARS": "bars"}[kind.split("-")[0]]
    return f"{catalog.path}/data/{fn_kind}.parquet/instrument_id={instrument_id.value}/{date:%Y%m%d}-0.parquet"

def back_fill_catalog_with_bars(
    ib: IB,
    catalog: ParquetDataCatalog,
    contracts: list[Contract],
    tz_name: str,
    end_date: datetime.date,
    start_date: datetime.date = None,
    duration: datetime.timedelta = None,
):
    pass

def back_fill_catalog(
    ib: IB,
    catalog: ParquetDataCatalog,
    contracts: list[Contract],
    start_date: datetime.date,
    end_date: datetime.date,
    tz_name: str,
    kinds: Iterable = ("BID_ASK", "TRADES"),
):
    """
    Backfill the data catalog with market data from Interactive Brokers.

    Parameters
    ----------
    ib : IB
        The ib_insync client.
    catalog : ParquetDataCatalog
        The data catalog to write the data to.
    contracts : list[Contract]
        The list of IB Contracts to collect data for.
    start_date : datetime.date
        The start_date for the backfill.
    end_date : datetime.date
        The end_date for the backfill.
    tz_name : str
        The timezone of the contracts
    kinds : tuple[str] (default: ('BID_ASK', 'TRADES')
        The kinds to query data for, can be any of:
        - BID_ASK
        - TRADES
        - A bar specification, i.e. BARS-1-MINUTE-LAST or BARS-5-SECOND-MID

    """
    for date in pd.date_range(start_date, end_date, tz=tz_name):
        for contract in contracts:
            instrument = instrument_from_contract(contract)

            # Check if this instrument exists in the catalog, if not, write it.
            if not catalog.instruments(instrument_ids=[instrument.id.value], as_nautilus=True):
                write_objects(catalog=catalog, chunk=[instrument])

            for kind in kinds:
                fn = generate_filename(catalog, instrument_id=instrument.id, kind=kind, date=date)
                if catalog.fs.exists(fn):
                    logger.info(
                        f"file for {instrument.id.value} {kind} {date:%Y-%m-%d} exists, skipping",
                    )
                    continue
                logger.info(f"Fetching {instrument.id.value} {kind} for {date:%Y-%m-%d}")

                data = request_data(
                    contract=contract,
                    instrument=instrument,
                    date=date.date(),
                    duration=datetime.timedelta(days=1),
                    kind=kind,
                    tz_name=tz_name,
                    ib=ib,
                )
                if data is None:
                    continue

                template = f"{date:%Y%m%d}" + "-{i}.parquet"
                write_objects(catalog=catalog, chunk=data, basename_template=template)


def request_data(
    contract: Contract,
    instrument: Instrument,
    date: datetime.date,
    duration: datetime.timedelta,
    kind: str,
    tz_name: str,
    ib: IB,
):
    if kind in ("TRADES", "BID_ASK"):
        raw = request_tick_data(
            contract=contract,
            date=date,
            duration=duration,
            kind=kind,
            tz_name=tz_name,
            ib=ib
        )
    elif kind.split("-")[0] == "BARS":
        bar_spec = BarSpecification.from_str(kind.split("-", maxsplit=1)[1])
        raw = request_bar_data(
            contract=contract,
            date=date,
            duration=duration,
            bar_spec=bar_spec,
            tz_name=tz_name,
            ib=ib,
        )
    else:
        raise RuntimeError(f"Unknown {kind=}")

    if not raw:
        logging.info(f"No ticks for {date=} {kind=} {contract=}, skipping")
        return
    logger.info(f"Fetched {len(raw)} raw {kind}")
    if kind == "TRADES":
        return parse_historic_trade_ticks(historic_ticks=raw, instrument=instrument)
    elif kind == "BID_ASK":
        return parse_historic_quote_ticks(historic_ticks=raw, instrument=instrument)
    elif kind.split("-")[0] == "BARS":
        return parse_historic_bars(historic_bars=raw, instrument=instrument, kind=kind)
    else:
        raise RuntimeError(f"Unknown {kind=}")


def request_tick_data(
    contract: Contract,
    date: datetime.date,
    kind: str,
    tz_name: str,
    ib: IB,
) -> list:
    assert kind in ("TRADES", "BID_ASK")
    data: list = []

    while True:
        start_time = _determine_next_timestamp(
            date=date,
            timestamps=[d.time for d in data],
            tz_name=tz_name,
        )
        logger.debug(f"Using start_time: {start_time}")

        ticks = _request_historical_ticks(
            ib=ib,
            contract=contract,
            start_time=start_time.strftime("%Y%m%d %H:%M:%S %Z"),
            what=kind,
        )

        ticks = [t for t in ticks if t not in data]

        if not ticks or ticks[-1].time < start_time:
            break

        logger.debug(f"Received {len(ticks)} ticks between {ticks[0].time} and {ticks[-1].time}")

        last_timestamp = pd.Timestamp(ticks[-1].time)
        last_date = last_timestamp.astimezone(tz_name).date()

        if last_date != date:
            # May contain data from next date, filter this out
            data.extend(
                [
                    tick
                    for tick in ticks
                    if pd.Timestamp(tick.time).astimezone(tz_name).date() == date
                ],
            )
            break
        else:
            data.extend(ticks)
    return data


def request_bar_data(
    ib: IB,
    contract: Contract,
    end_date: datetime.date = datetime.datetime.utcnow().date(),
    duration: datetime.timedelta = datetime.timedelta(days=365 * 5),
    tz_name: str = "UTC",
    bar_spec: BarSpecification = BarSpecification(
        step=1,
        aggregation=BarAggregation.DAY,
        price_type=PriceType.LAST
    )
) -> list:

    if tz_name not in pytz.all_timezones:
        raise ValueError(f"{tz_name} is not a valid timezone.")

    end_date = pd.Timestamp(end_date).tz_localize(tz_name)

    bar_data_list: BarDataList = _request_historical_bars(
        ib=ib,
        contract=contract,
        end_time=end_date.strftime("%Y%m%d %H:%M:%S %Z"),
        duration_str=timedelta_to_duration_str(duration),
        bar_spec=bar_spec,
    )

    bars = [bar for bar in bar_data_list if bar.volume != 0]

    logger.info(f"Received {len(bars)} bars")

    return bars


def _request_contract_details(ib: IB, contract: Contract):
    [details] = ib.reqContractDetails(contract=contract)
    return details


def instrument_from_contract(ib: IB, contract: Contract) -> Instrument:
    details = _request_contract_details(ib, contract)
    return parse_instrument(contract_details=details)


def _request_historical_ticks(ib: IB, contract: Contract, start_time: str, what: str = "BID_ASK"):
    return ib.reqHistoricalTicks(
        contract=contract,
        startDateTime=start_time,
        endDateTime="",
        numberOfTicks=1000,
        whatToShow=what,
        useRth=False,
    )


def _bar_spec_to_hist_data_request(bar_spec: BarSpecification) -> dict[str, str]:
    aggregation = bar_aggregation_to_str(bar_spec.aggregation)

    accepted_aggregations = ("SECOND", "MINUTE", "HOUR", "DAY")
    if aggregation not in accepted_aggregations:
        raise RuntimeError(f"Historic bars only supports the following bar aggregations: {accepted_aggregations}")

    size_mapping = {
        "SECOND": "sec",
        "MINUTE": "min",
        "HOUR": "hour",
        "DAY": "day",
        "WEEK": "week",
        "MONTH": "month"
    }
    bar_size = size_mapping[aggregation]
    suffix = "" if bar_spec.step == 1 and aggregation != "SECOND" else "s"
    bar_size_setting = f"{bar_spec.step} {bar_size + suffix}"

    price_type = price_type_to_str(bar_spec.price_type)
    historical_data_type_mapping = {"MID": "MIDPOINT", "LAST": "TRADES"}
    what_to_show = historical_data_type_mapping[price_type]

    return {"barSizeSetting": bar_size_setting, "whatToShow": what_to_show}


def _request_historical_bars(
    ib: IB,
    contract: Contract,
    end_date: datetime.date,
    duration: datetime.timedelta,
    bar_spec: BarSpecification
) -> BarDataList:
    end_datetime = datetime.datetime.combine(end_date, datetime.time())
    spec = _bar_spec_to_hist_data_request(bar_spec=bar_spec)
    return ib.reqHistoricalData(
        contract=contract,
        endDateTime=end_datetime.strftime(""),
        durationStr=timedelta_to_duration_str(duration),
        barSizeSetting=spec["barSizeSetting"],
        whatToShow=spec["whatToShow"],
        useRTH=False,
        formatDate=2,
    )


def _determine_next_timestamp(timestamps: list[pd.Timestamp], date: datetime.date, tz_name: str):
    """
    While looping over available data, it is possible for very liquid products that a 1s
    period may contain 1000 ticks, at which point we need to step the time forward to
    avoid getting stuck when iterating.
    """
    if not timestamps:
        return pd.Timestamp(date, tz=tz_name).tz_convert("UTC")
    unique_values = set(timestamps)
    if len(unique_values) == 1:
        timestamp = timestamps[-1]
        return timestamp + pd.Timedelta(seconds=1)
    else:
        return timestamps[-1]


def parse_response_datetime(
    dt: Union[datetime.datetime, pd.Timestamp],
    tz_name: str,
) -> datetime.datetime:
    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()
    if dt.tzinfo is None:
        tz = pytz.timezone(tz_name)
        dt = tz.localize(dt)
    return dt


def parse_historic_quote_ticks(
    historic_ticks: list[HistoricalTickBidAsk],
    instrument: Instrument,
) -> list[QuoteTick]:
    trades = []
    for tick in historic_ticks:
        ts_init = dt_to_unix_nanos(tick.time)
        quote_tick = QuoteTick(
            instrument_id=instrument.id,
            bid=Price(value=tick.priceBid, precision=instrument.price_precision),
            bid_size=Quantity(value=tick.sizeBid, precision=instrument.size_precision),
            ask=Price(value=tick.priceAsk, precision=instrument.price_precision),
            ask_size=Quantity(value=tick.sizeAsk, precision=instrument.size_precision),
            ts_init=ts_init,
            ts_event=ts_init,
        )
        trades.append(quote_tick)

    return trades


def parse_historic_trade_ticks(
    historic_ticks: list[HistoricalTickLast],
    instrument: Instrument,
) -> list[TradeTick]:
    trades = []
    for tick in historic_ticks:
        ts_init = dt_to_unix_nanos(tick.time)
        trade_tick = TradeTick(
            instrument_id=instrument.id,
            price=Price(value=tick.price, precision=instrument.price_precision),
            size=Quantity(value=tick.size, precision=instrument.size_precision),
            aggressor_side=AggressorSide.NO_AGGRESSOR,
            trade_id=generate_trade_id(
                ts_event=ts_init,
                price=tick.price,
                size=tick.size,
            ),
            ts_init=ts_init,
            ts_event=ts_init,
        )
        trades.append(trade_tick)

    return trades


def parse_historic_bars(
    historic_bars: list[BarData],
    instrument: Instrument,
    kind: str,
) -> list[Bar]:
    bars = []
    bar_type = BarType(
        bar_spec=BarSpecification.from_str(kind.split("-", maxsplit=1)[1]),
        instrument_id=instrument.id,
        aggregation_source=AggregationSource.EXTERNAL,
    )
    precision = instrument.price_precision
    for bar in historic_bars:
        ts_init = dt_to_unix_nanos(bar.date)
        trade_tick = Bar(
            bar_type=bar_type,
            open=Price(bar.open, precision),
            high=Price(bar.high, precision),
            low=Price(bar.low, precision),
            close=Price(bar.close, precision),
            volume=Quantity(bar.volume, instrument.size_precision),
            ts_init=ts_init,
            ts_event=ts_init,
        )
        bars.append(trade_tick)

    return bars
