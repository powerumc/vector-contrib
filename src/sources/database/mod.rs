#[cfg(feature = "sources-database")]
pub mod client;

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "database-integration-tests"))]
mod integration_tests;
mod mysql;