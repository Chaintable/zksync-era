//! Dependency injection for observability.

pub use self::{prometheus_exporter::PrometheusExporterLayer, sigint::SigintHandlerLayer, sigterm::SigtermHandlerLayer};

mod prometheus_exporter;
mod sigint;

mod sigterm;