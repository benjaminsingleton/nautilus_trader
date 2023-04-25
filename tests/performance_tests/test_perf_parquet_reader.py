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

import itertools
import os
import time

import pytest

from nautilus_trader.core.nautilus_pyo3.persistence import ParquetReader
from nautilus_trader.core.nautilus_pyo3.persistence import ParquetReaderType
from nautilus_trader.core.nautilus_pyo3.persistence import ParquetType
from nautilus_trader.core.nautilus_pyo3.persistence import PythonCatalog
from nautilus_trader.model.data.tick import QuoteTick
from nautilus_trader.persistence.wranglers import list_from_capsule
from tests import TEST_DATA_DIR
from nautilus_trader import PACKAGE_ROOT


@pytest.mark.benchmark(
    group="parquet-reader",
    min_rounds=5,
    timer=time.time,
    disable_gc=True,
    warmup=True,
)
def test_pyo3_benchmark_parquet_buffer_reader(benchmark):
    parquet_data_path = os.path.join(TEST_DATA_DIR, "quote_tick_data.parquet")
    file_data = None
    with open(parquet_data_path, "rb") as f:
        file_data = f.read()

    @benchmark
    def run():
        reader = ParquetReader(
            "",
            1000,
            ParquetType.QuoteTick,
            ParquetReaderType.Buffer,
            file_data,
        )
        data = map(QuoteTick.list_from_capsule, reader)
        ticks = list(itertools.chain(*data))
        print(len(ticks))


@pytest.mark.benchmark(
    group="parquet-reader",
    min_rounds=5,
    max_time = 20,
    timer=time.time,
    disable_gc=True,
)
def test_pyo3_catalog_v2(benchmark):
    file_path = os.path.join(PACKAGE_ROOT, "bench_data/quotes_0005.parquet")
    session = PythonCatalog()
    session.add_file("quote_ticks", file_path, ParquetType.QuoteTick)
    result = session.to_query_result()
    
    @benchmark
    def run():
        count = 0
        for chunk in result:
            count += len(list_from_capsule(chunk))

        assert count == 9689614


@pytest.mark.benchmark(
    group="parquet-reader",
    min_rounds=5,
    max_time = 60,
    timer=time.time,
    disable_gc=True,
)
def test_python_catalog_v2_multi_stream(benchmark):
    dir_path = os.path.join(PACKAGE_ROOT, "bench_data/multi_stream_data/")

    session = PythonCatalog()

    for dirpath, _, filenames in os.walk(dir_path):
        for filename in filenames:
            if filename.endswith("parquet"):
                filestem = os.path.splitext(filename)[0]
                if "quotes" in filename:
                    full_path = os.path.join(dirpath, filename)
                    session.add_file(filestem, full_path, ParquetType.QuoteTick)
                elif "trades" in filename:
                    full_path = os.path.join(dirpath, filename)
                    session.add_file(filestem, full_path, ParquetType.TradeTick)

    result = session.to_query_result()

    @benchmark
    def run():
        count = 0
        for chunk in result:
            ticks = list_from_capsule(chunk)
            count += len(ticks)

        # check total count is correct
        assert count == 72536038
