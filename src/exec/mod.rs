pub mod child;
pub mod sinks;
pub mod tag;
pub mod tee;

pub use tag::TaggedWriter;
pub use tee::{SinkOutcome, Tee, TeeOutcome};
