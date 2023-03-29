/// Load-balancing policy among sessions within `CompositeSession`.
#[derive(Debug)]
pub(crate) enum LbPolicy {
    PickFirst,
    RoundRobin,
}
