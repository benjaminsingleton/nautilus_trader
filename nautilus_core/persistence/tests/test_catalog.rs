// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2023 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use arrow2::io::parquet::read::{self, FileReader};
use std::fs::{self, File};

use nautilus_model::data::tick::{Data, QuoteTick, TradeTick};
use nautilus_persistence::{
    parquet::{GroupFilterArg, ParquetReader},
    session::{PersistenceCatalog, QueryResult},
};
use rstest::rstest;

#[test]
fn arrow2_test() {
    let mut reader = File::open("../test_data.parquet").expect("Unable to open given file");
    let metadata = read::read_metadata(&mut reader).expect("Unable to read metadata");
    let schema = read::infer_schema(&metadata).expect("Unable to infer schema");
    let mut fr = FileReader::new(reader, metadata.row_groups, schema, Some(1000), None, None);
    assert!(fr.next().is_some())
}

#[rstest]
#[case("../../tests/test_data/quote_tick_data.parquet", 9500)]
#[case("../../bench_data/quotes_0005.parquet", 9689614)]
fn test_v1_bench_data(#[case] file_path: &str, #[case] length: usize) {
    let file = File::open(file_path).expect("Unable to open given file");
    let reader: ParquetReader<QuoteTick, File> =
        ParquetReader::new(file, 5000, GroupFilterArg::None);
    let data: Vec<QuoteTick> = reader.flatten().collect();
    assert_eq!(data.len(), length);
}

// Note: "current_thread" configuration hangs up for some reason
#[rstest]
#[case("../../tests/test_data/quote_tick_data.parquet", 9500)]
#[case("../../bench_data/quotes_0005.parquet", 9689614)]
#[tokio::test(flavor = "multi_thread")]
async fn test_v2_bench_data(#[case] file_path: &str, #[case] length: usize) {
    let mut catalog = PersistenceCatalog::new(10000);
    catalog
        .add_file::<QuoteTick>("quotes_0005", file_path)
        .await
        .unwrap();
    let query_result: QueryResult = catalog.to_query_result();
    let ticks: Vec<Data> = query_result.flatten().collect();

    // NOTE: is_sorted_by_key is unstable otherwise use
    // ticks.is_sorted_by_key(|tick| tick.ts_init)
    // https://github.com/rust-lang/rust/issues/53485
    let is_ascending_by_init = |ticks: &Vec<Data>| {
        for i in 1..ticks.len() {
            // previous tick is more recent than current tick
            // this is not ascending order
            if ticks[i - 1].get_ts_init() > ticks[i].get_ts_init() {
                return false;
            }
        }
        true
    };

    assert_eq!(ticks.len(), length);
    assert!(is_ascending_by_init(&ticks));
}

// Note: "current_thread" hangs up for some reason
#[tokio::test(flavor = "multi_thread")]
async fn test_data_ticks() {
    let mut catalog = PersistenceCatalog::new(5000);
    catalog
        .add_file::<QuoteTick>(
            "quote_tick",
            "../../tests/test_data/quote_tick_data.parquet",
        )
        .await
        .unwrap();
    catalog
        .add_file::<TradeTick>(
            "quote_tick_2",
            "../../tests/test_data/trade_tick_data.parquet",
        )
        .await
        .unwrap();
    let query_result: QueryResult = catalog.to_query_result();
    let ticks: Vec<Data> = query_result.flatten().collect();

    // NOTE: is_sorted_by_key is unstable otherwise use
    // ticks.is_sorted_by_key(|tick| tick.ts_init)
    // https://github.com/rust-lang/rust/issues/53485
    let is_ascending_by_init = |ticks: &Vec<Data>| {
        for i in 1..ticks.len() {
            // previous tick is more recent than current tick
            // this is not ascending order
            if ticks[i - 1].get_ts_init() > ticks[i].get_ts_init() {
                return false;
            }
        }
        true
    };

    assert_eq!(ticks.len(), 9600);
    assert!(is_ascending_by_init(&ticks));
}
