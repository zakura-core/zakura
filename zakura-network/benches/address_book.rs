//! Address-book indexing benchmarks.
#![allow(missing_docs)]

use std::{net::SocketAddr, time::Duration};

use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use tracing::Span;

use zakura_chain::parameters::Network::Mainnet;
use zakura_network::{
    constants::{
        DEFAULT_MAX_CONNS_PER_IP, MAX_ADDRS_IN_ADDRESS_BOOK, MAX_ADDRS_IN_MESSAGE,
        MAX_PEER_MISBEHAVIOR_SCORE,
    },
    types::MetaAddr,
    AddressBook, PeerSocketAddr,
};

const SMALL_ADDRESS_BOOK_SIZE: usize = 16;
const SAME_IP_ADDRESS_COUNT: usize = 16;
const SAME_IP_PORT_START: u16 = 10_000;
const UNIQUE_IP_START: u32 = u32::from_be_bytes([11, 0, 0, 1]);
const ZCASH_MAINNET_PORT: u16 = 8_233;

fn same_ip_addr(index: usize) -> PeerSocketAddr {
    let port_offset = u16::try_from(index).expect("benchmark index fits in u16");
    let port = SAME_IP_PORT_START
        .checked_add(port_offset)
        .expect("benchmark port stays below u16::MAX");

    SocketAddr::from(([12, 0, 0, 1], port)).into()
}

fn unique_ip_addr(index: usize) -> PeerSocketAddr {
    let index = u32::try_from(index).expect("address-book limit fits in u32");
    let ip = UNIQUE_IP_START
        .checked_add(index)
        .expect("benchmark IP stays below u32::MAX")
        .to_be_bytes();

    SocketAddr::from((ip, ZCASH_MAINNET_PORT)).into()
}

fn peer_addr(index: usize) -> PeerSocketAddr {
    if index < SAME_IP_ADDRESS_COUNT {
        same_ip_addr(index)
    } else {
        unique_ip_addr(index)
    }
}

fn populated_address_book(size: usize) -> AddressBook {
    let mut address_book = AddressBook::new(
        "0.0.0.0:0".parse().expect("valid benchmark listener"),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        Span::none(),
    );

    for index in 0..size {
        let updated = address_book.update(MetaAddr::new_initial_peer(peer_addr(index)));
        assert!(updated.is_some(), "benchmark peer should be inserted");
    }

    assert_eq!(address_book.len(), size);
    address_book
}

fn address_book(c: &mut Criterion) {
    let mut group = c.benchmark_group("address_book");
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(50);
    group.throughput(Throughput::Elements(1));

    for size in [SMALL_ADDRESS_BOOK_SIZE, MAX_ADDRS_IN_ADDRESS_BOOK] {
        group.throughput(Throughput::Elements(1));

        let address_book = populated_address_book(size);
        let existing_addr = peer_addr(SAME_IP_ADDRESS_COUNT - 1);
        let new_addr = unique_ip_addr(size);
        let batch_size = size.min(MAX_ADDRS_IN_MESSAGE);
        let batch_updates: Vec<_> = (0..batch_size)
            .map(|index| MetaAddr::new_misbehavior(peer_addr(index), 1))
            .collect();

        let mut update_check = address_book.clone();
        let update_change = MetaAddr::new_misbehavior(existing_addr, 1);
        assert!(update_check.update(update_change).is_some());

        let mut insert_check = address_book.clone();
        let insert_change = MetaAddr::new_initial_peer(new_addr);
        assert!(insert_check.update(insert_change).is_some());
        assert_eq!(
            insert_check.len(),
            (size + 1).min(MAX_ADDRS_IN_ADDRESS_BOOK)
        );

        let mut ban_check = address_book.clone_with_fresh_bans_for_benchmark();
        let ban_change = MetaAddr::new_misbehavior(existing_addr, MAX_PEER_MISBEHAVIOR_SCORE);
        assert!(ban_check.update(ban_change).is_none());
        assert_eq!(ban_check.len(), size - SAME_IP_ADDRESS_COUNT);
        assert!(ban_check.bans().contains(existing_addr.ip()));

        let mut batch_check = address_book.clone();
        batch_check.extend(batch_updates.iter().copied());
        assert_eq!(batch_check.len(), size);

        group.bench_with_input(
            BenchmarkId::new("clone_and_drop", size),
            &size,
            |b, _size| {
                b.iter(|| black_box(address_book.clone()));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("iterate_in_priority_order", size),
            &size,
            |b, _size| {
                b.iter(|| black_box(address_book.peers().count()));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("lookup_existing", size),
            &size,
            |b, _size| {
                let mut address_book = address_book.clone();
                b.iter(|| black_box(address_book.get(black_box(existing_addr))));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("update_existing", size),
            &size,
            |b, _size| {
                b.iter_batched_ref(
                    || address_book.clone(),
                    |address_book| {
                        let change = MetaAddr::new_misbehavior(black_box(existing_addr), 1);
                        black_box(address_book.update(change))
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        group.bench_with_input(BenchmarkId::new("insert_peer", size), &size, |b, _size| {
            b.iter_batched_ref(
                || address_book.clone(),
                |address_book| {
                    let change = MetaAddr::new_initial_peer(black_box(new_addr));
                    black_box(address_book.update(change))
                },
                BatchSize::PerIteration,
            );
        });

        group.bench_with_input(
            BenchmarkId::new("ban_ip_with_16_addresses", size),
            &size,
            |b, _size| {
                b.iter_batched_ref(
                    || address_book.clone_with_fresh_bans_for_benchmark(),
                    |address_book| {
                        let change = MetaAddr::new_misbehavior(
                            black_box(existing_addr),
                            MAX_PEER_MISBEHAVIOR_SCORE,
                        );
                        black_box(address_book.update(change))
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        group.throughput(Throughput::Elements(
            u64::try_from(batch_size).expect("address batch size fits in u64"),
        ));
        group.bench_with_input(BenchmarkId::new("update_batch", size), &size, |b, _size| {
            b.iter_batched_ref(
                || address_book.clone(),
                |address_book| {
                    address_book.extend(batch_updates.iter().copied());
                    black_box(address_book);
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, address_book);
criterion_main!(benches);
