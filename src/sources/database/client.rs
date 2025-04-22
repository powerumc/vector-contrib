//! Provides various database client sources for Vector.
//!
//! These sources allow Vector to connect to different databases and periodically
//! query them for observability data based on a defined schedule (e.g., using cron syntax).
//! The results of the query are then ingested into the Vector pipeline as events.
//!
//! This functionality is similar to database input plugins found in other tools,
//! such as the Logstash JDBC input plugin.
//!
//! Currently supported database types include:
//! - MySQL
//!
//! For more context on similar concepts, see:
//! [logstash/plugins/plugins-inputs-jdbc](https://www.elastic.co/docs/reference/logstash/plugins/plugins-inputs-jdbc)

use crate::config::{log_schema, LogNamespace, SourceConfig, SourceContext, SourceOutput};
use crate::serde::default_decoding;
use crate::sources::database::mysql::MySqlConfig;
use crate::sources::Source;
use vector_config_macros::configurable_component;
use vector_lib::codecs::decoding::DeserializerConfig;
use vrl::prelude::Kind;

pub(crate) const DEFAULT_HOST: &str = "localhost";

/// Database source type.
#[configurable_component]
#[configurable(metadata(docs::advanced))]
#[derive(Clone, Debug)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "database_type")]
pub enum DatabaseType {
    /// MySQL database source.
    #[configurable(description = "MySQL database source")]
    MySQL(MySqlConfig),
}

/// Configuration for the `database` source.
#[configurable_component(source(
    "database",
    "Pull observability data from a database by scheduling a query to run at a specific time."
))]
#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    #[serde(flatten)]
    inner: DatabaseType,

    /// The SQL statement to execute against the database.
    #[configurable(metadata(docs::examples = "SELECT * FROM users"))]
    pub statement: String,

    /// The cron expression for the database query.
    /// TODO: There is no schedule by default. If no schedule is given, then the statement is run exactly once
    #[configurable(metadata(docs::examples = "*/10 * * * *"))]
    pub schedule: Option<String>,

    /// The timezone to use for the cron expression.
    /// This is typically the timezone that you want to use for the cron expression.
    #[configurable(metadata(docs::examples = "UTC"))]
    pub schedule_timezone: Option<String>,

    #[configurable(derived)]
    #[serde(default = "default_decoding")]
    decoding: DeserializerConfig,

    /// The namespace to use for logs. This overrides the global setting.
    #[configurable(metadata(docs::hidden))]
    #[serde(default)]
    log_namespace: Option<bool>,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            inner: DatabaseType::MySQL(MySqlConfig::default()),
            statement: "".to_string(),
            schedule: None,
            schedule_timezone: None,
            decoding: default_decoding(),
            log_namespace: None,
        }
    }
}

impl_generate_config_from_default!(DatabaseConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "database")]
impl SourceConfig for DatabaseConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<Source> {
        match &self.inner {
            DatabaseType::MySQL(config) => config.build(self.clone(), cx).await,
        }
    }

    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        let log_namespace = global_log_namespace.merge(self.log_namespace);

        let mut schema_definition = self
            .decoding
            .schema_definition(log_namespace)
            .with_standard_vector_source_metadata();

        if let Some(timestamp_key) = log_schema().timestamp_key() {
            schema_definition = schema_definition.optional_field(
                timestamp_key,
                Kind::timestamp(),
                Some("timestamp"),
            )
        }

        vec![SourceOutput::new_maybe_logs(
            self.decoding.output_type(),
            schema_definition,
        )]
    }

    fn can_acknowledge(&self) -> bool {
        match &self.inner {
            DatabaseType::MySQL(config) => config.can_acknowledge(),
        }
    }
}
