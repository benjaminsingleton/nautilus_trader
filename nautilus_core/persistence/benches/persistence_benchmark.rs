use std::fs::File;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use nautilus_model::data::tick::QuoteTick;
use nautilus_persistence::{
    parquet::{GroupFilterArg, ParquetReader},
    session::{PersistenceCatalog, QueryResult},
};
use pyo3_asyncio::tokio::get_runtime;

fn single_stream_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_stream");
    group.sample_size(10);
    let chunk_size = 5000;
    // about 10 M records
    let file_path = "/home/twitu/Downloads/0005-quotes.parquet";
    // let file_path = "/home/twitu/Code/nautilus_trader/tests/test_data/quote_tick_data.parquet";

    // group.bench_function("persistence v1", |b| {
    //     b.iter_batched(
    //         || {
    //             ()
    //         },
    //         // |reader: ParquetReader<QuoteTick, File>| {
    //         |temp: ()| {
    //             let f = File::open(file_path).unwrap();
    //             let mut reader: ParquetReader<QuoteTick, File> = ParquetReader::new(f, chunk_size, GroupFilterArg::None);
    //             // let count: usize = reader.map(|vec: Vec<QuoteTick>| vec.len()).sum();
    //             // let count = reader.flatten().collect::<Vec<QuoteTick>>().len();
    //             let count = reader.next().unwrap().len();
    //             assert_eq!(count, 9689614);
    //         },
    //         BatchSize::SmallInput,
    //     )
    // });

    group.bench_function("persistence v2", |b| {
        b.iter_batched(
            || {
                let rt = get_runtime();
                let mut catalog = PersistenceCatalog::new(chunk_size);
                rt.block_on(catalog.add_file::<QuoteTick>("quote_tick", file_path))
                    .unwrap();
                let _guard = rt.enter();
                catalog.to_query_result()
            },
            |query_result: QueryResult| {
                let rt = get_runtime();
                let _guard = rt.enter();
                let count: usize = query_result.map(|vec| vec.len()).sum();
                assert_eq!(count, 9689614);
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, single_stream_bench);
criterion_main!(benches);
