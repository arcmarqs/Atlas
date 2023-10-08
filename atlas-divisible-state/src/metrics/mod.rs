use atlas_metrics::{MetricLevel, MetricRegistry};
use atlas_metrics::metrics::MetricKind;

/// State transfer will take the
/// 6XX metric ID range
pub const CREATE_CHECKPOINT_TIME : &str = "CREATE_CHECKPOINT_TIME";
pub const CREATE_CHECKPOINT_TIME_ID : usize = 800;
pub const CHECKPOINT_SIZE: &str = "DIV_CHECKPOINT_SIZE";
pub const CHECKPOINT_SIZE_ID: usize = 803;


pub fn metrics() -> Vec<MetricRegistry> {
    vec![
        (CREATE_CHECKPOINT_TIME_ID, CREATE_CHECKPOINT_TIME.to_string(), MetricKind::Duration, MetricLevel::Info).into(),
        (CHECKPOINT_SIZE_ID, CHECKPOINT_SIZE.to_string(), MetricKind::Counter, MetricLevel::Info).into(),
    ]
}