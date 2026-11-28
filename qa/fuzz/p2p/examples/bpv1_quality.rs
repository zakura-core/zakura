//! Prints what each BPv1 fuzz target reached: its seeds, then a short run of
//! generated inputs. The optional argument sets how many generated inputs
//! each target replays (default 200). It runs no campaign and writes no
//! corpus.

use std::env;

use zakura_network::zakura::bpv1::fuzz::measure_bpv1_targets;

fn main() {
    let generated = env::args()
        .nth(1)
        .map_or(200, |n| n.parse().expect("the argument is an input count"));
    for target in measure_bpv1_targets(generated, 0x5EED) {
        let reached = target
            .reached
            .iter()
            .filter(|n| target.expected.contains(n));
        let rows: Vec<&str> = target
            .reached
            .iter()
            .filter(|n| n.starts_with("bpv1-close-"))
            .collect();
        println!(
            "{}: {} inputs, {}/{} expected names, close rows {:?}",
            target.target,
            target.inputs,
            reached.count(),
            target.expected.len(),
            rows
        );
        for name in target.missing() {
            println!("  missing {name}");
        }
    }
}
