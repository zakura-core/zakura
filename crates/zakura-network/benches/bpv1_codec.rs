//! W2 Reed–Solomon + Merkle part-tree codec benchmarks.
//!
//! Measures encode and decode for:
//! - The 2 MB consensus activation target (`1_990_000` bytes under `Profile::W2`), and
//! - One full W2 stripe (`k = 64, n = 86, S = 65,536`, 4 MiB of source bytes).
//!   Larger bodies cost one full stripe per 4 MiB.
#![allow(missing_docs)]

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use zakura_network::zakura::bpv1::{decode_and_check_stripe, encode_body, Profile, VerifiedPart};

fn bench_body(c: &mut Criterion, group: &str, len: usize) {
    // Masking to 0xff makes the narrowing cast to u8 lossless.
    let body: Vec<u8> = (0..len)
        .map(|i| ((i.wrapping_mul(31) ^ (i >> 8)) & 0xff) as u8)
        .collect();
    let mut group = c.benchmark_group(group);
    group.sample_size(10);
    // Widening usize to u64 is lossless on supported targets.
    group.throughput(Throughput::Bytes(len as u64));

    group.bench_function("encode_and_tree", |b| {
        b.iter(|| encode_body(&Profile::W2, &body, 512).expect("encode"));
    });

    let encoded = encode_body(&Profile::W2, &body, 512).expect("encode");
    let coding = encoded.coding;
    // Source slot 0 is missing, so decode needs parity and the re-encode check.
    let mut held: Vec<Option<VerifiedPart>> = vec![None; coding.n as usize];
    for (slot, entry) in held
        .iter_mut()
        .enumerate()
        .take(coding.k as usize + 1)
        .skip(1)
    {
        *entry = Some(encoded.parts[slot].clone());
    }
    group.bench_function("decode_and_reencode_check", |b| {
        b.iter(|| {
            decode_and_check_stripe(&coding, &encoded.encoding_id, 0, &held)
                .expect("decode and re-encode check")
        });
    });
    group.finish();
}

fn bench_w2_activation_2mb(c: &mut Criterion) {
    bench_body(c, "bpv1_w2_activation_2mb", 1_990_000);
}

fn bench_w2_full_stripe(c: &mut Criterion) {
    bench_body(c, "bpv1_w2_full_stripe_4mib", 64 * 65_536);
}

criterion_group!(benches, bench_w2_activation_2mb, bench_w2_full_stripe);
criterion_main!(benches);
