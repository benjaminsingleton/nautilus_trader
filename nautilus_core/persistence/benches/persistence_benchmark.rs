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
    let chunk_size = 5000;
    // about 10 M records
    let file_path =
        "/Users/twitu/Downloads/nautilus_test_data/single_stream_data/0005-quotes.parquet";

    group.bench_function("persistence v1", |b| {
        b.iter_batched(
            || {
                let f = File::open(file_path).unwrap();
                ParquetReader::new(f, chunk_size, GroupFilterArg::None)
            },
            |reader: ParquetReader<QuoteTick, File>| {
                let count: usize = reader.map(|vec| vec.len()).sum();
                assert_eq!(count, 9689614);
            },
            BatchSize::SmallInput,
        )
    });

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
