use crate::config::{log_schema, LogNamespace, SourceConfig, SourceContext, SourceOutput};
use crate::serde::default_decoding;
use crate::sinks::prelude::*;
use crate::sources::odbc::{
    load_params, map_value, save_params, ClosedSnafu, Column, Columns, OdbcError, OdbcSchedule,
    OdbcSnafu, Rows,
};
use crate::sources::Source;
use chrono::Utc;
use chrono_tz::Tz;
use itertools::Itertools;
use odbc_api::buffers::TextRowSet;
use odbc_api::parameter::VarCharBox;
use odbc_api::{ConnectionOptions, Cursor, Environment, IntoParameter, ResultSetMetadata};
use serde_with::serde_as;
use serde_with::DurationSeconds;
use snafu::ResultExt;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use typetag::serde;
use vector_lib::codecs::decoding::DeserializerConfig;
use vrl::prelude::*;

/// Configuration for the `odbc` source.
#[serde_as]
#[configurable_component(
    source("odbc", "Pull observability data from a ODBC interface by scheduling a query to run at a specific time."
    )
)]
#[derive(Clone, Debug)]
pub struct OdbcConfig {
    /// The connection string to use for odbc.
    #[configurable(metadata(
        docs::examples = "driver={MySQL ODBC 8.0 ANSI Driver};server=<ip or host>;port=<port number>;database=<database name>;uid=<user>;pwd=<password>"
    ))]
    pub connection_string: String,

    /// The SQL statement to execute.
    /// This SQL statement is executed periodically according to the `schedule`.
    /// The default is none. If no SQL statement is given, then an error will occur.
    #[configurable(metadata(docs::examples = "SELECT * FROM users"))]
    pub statement: Option<String>,

    /// This is the maximum time to wait for the SQL statement to execute.
    /// If the SQL statement does not complete within this time, it will be canceled and wait for the next schedule.
    /// The default is 3 seconds.
    #[configurable(metadata(docs::examples = 3))]
    #[configurable(metadata(
        docs::additional_props_description = "Maximum time to wait for the SQL statement to execute"
    ))]
    #[serde(default = "default_query_timeout_sec")]
    #[serde_as(as = "DurationSeconds<u64>")]
    pub statement_timeout: Duration,

    /// This is the parameter for the first execution of the statement.
    /// If the `last_run_metadata_path` file path does not exist, this parameter is used for the first execution.
    /// The initial value is set in the order of the parameter name.
    /// The value is always a string.
    ///
    /// # Examples
    ///
    /// When the data source is first executed, the file at `last_run_metadata_path` does not exist.
    /// In this case, you need to declare the initial value in `statement_init_params`.
    ///
    /// ```yaml
    /// [sources.odbc]
    /// statement = "SELECT * FROM users WHERE id = ?"
    /// statement_init_params = ["0"]
    /// tracking_columns = ["id"]
    /// last_run_metadata_path = "/path/to/tracking.json"
    /// # The rest of the fields are omitted
    ///
    /// [sources.odbc.statement_init_params]
    /// id = 0
    /// ```
    #[configurable(metadata(docs::examples = "id = 0"))]
    #[configurable(metadata(
        docs::additional_props_description = "Initial value for the SQL statement parameters. The value is always a string."
    ))]
    #[configurable(derived)]
    pub statement_init_params: Option<BTreeMap<String, String>>,

    /// The cron expression for the database query.
    /// There is no schedule by default.
    /// If no schedule is given, then the statement is run exactly once
    #[configurable(derived)]
    pub schedule: Option<OdbcSchedule>,

    /// The timezone to use for the `schedule`.
    /// This is typically the timezone that you want to use for the cron expression.
    /// The default is UTC.
    ///
    /// [Wikipedia]: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
    #[configurable(metadata(docs::examples = "UTC"))]
    #[configurable(metadata(
        docs::additional_props_description = "Timezone to use for the schedule"
    ))]
    #[serde(default = "default_schedule_timezone")]
    pub schedule_timezone: Tz,

    /// The batch size for the ODBC driver.
    /// This is the number of rows to fetch at a time.
    /// The default is 100.
    #[configurable(metadata(docs::examples = 100))]
    #[serde(default = "default_odbc_batch_size")]
    pub odbc_batch_size: usize,

    /// The maximum string length for the ODBC driver.
    /// The default is 4096.
    #[configurable(metadata(docs::examples = 4096))]
    #[serde(default = "default_odbc_batch_size")]
    pub odbc_max_str_limit: usize,

    /// The timezone to use for the database date/time type without a timezone.
    /// The default is UTC.
    #[configurable(metadata(docs::examples = "UTC"))]
    #[configurable(metadata(
        docs::additional_props_description = "Timezone to use for the database date/time type without a timezone"
    ))]
    #[serde(default = "default_odbc_default_timezone")]
    pub odbc_default_timezone: Tz,

    /// Specifies the columns to track from the last row of the statement result set.
    /// This column is passed as a parameter to the SQL statement of the next schedule in order.
    ///
    /// # Examples
    ///
    /// ```yaml
    /// [sources.odbc]
    /// statement = "SELECT * FROM users WHERE id = ?"
    /// tracking_columns = ["id"]
    /// # The rest of the fields are omitted
    /// ```
    #[configurable(metadata(docs::examples = "[\"id\"]"))]
    pub tracking_columns: Option<Vec<String>>,

    /// The path to the file where the last row of the result set will be saved.
    /// The last row of the result set is saved in JSON format.
    /// This file is used as a parameter for the SQL query of the next schedule.
    ///
    /// # Examples
    ///
    /// If `tracking_columns = ["id", "name"]`, it is saved as the following JSON data.
    ///
    /// ```json
    /// {"id":1, "name": "vector"}
    /// ```
    #[configurable(metadata(docs::examples = "~/metadata_file"))]
    pub last_run_metadata_path: Option<String>,

    /// Decoder to use on the HTTP responses.
    #[configurable(derived)]
    #[serde(default = "default_decoding")]
    decoding: DeserializerConfig,

    /// The namespace to use for logs. This overrides the global setting.
    #[configurable(metadata(docs::hidden))]
    #[serde(default)]
    log_namespace: Option<bool>,
}

const fn default_query_timeout_sec() -> Duration {
    Duration::from_secs(3)
}

const fn default_schedule_timezone() -> Tz {
    Tz::UTC
}

const fn default_odbc_batch_size() -> usize {
    100
}

const fn default_odbc_max_str_limit() -> usize {
    4096
}

fn default_odbc_default_timezone() -> Tz {
    default_schedule_timezone()
}

impl Default for OdbcConfig {
    fn default() -> Self {
        Self {
            connection_string: "".to_string(),
            statement: None,
            schedule: None,
            schedule_timezone: Tz::UTC,
            statement_init_params: None,
            odbc_batch_size: default_odbc_batch_size(),
            odbc_max_str_limit: default_odbc_max_str_limit(),
            odbc_default_timezone: Tz::UTC,
            tracking_columns: None,
            last_run_metadata_path: None,
            statement_timeout: Duration::from_secs(3),
            decoding: default_decoding(),
            log_namespace: None,
        }
    }
}

impl_generate_config_from_default!(OdbcConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "odbc")]
impl SourceConfig for OdbcConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<Source> {
        let guard = Context::new(self.clone(), cx)?;
        let context = Box::new(guard);
        Ok(context.run_schedule().boxed())
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
        false
    }
}

struct Context {
    cfg: OdbcConfig,
    env: Arc<Environment>,
    cx: SourceContext,
}

impl Context {
    fn new(cfg: OdbcConfig, cx: SourceContext) -> Result<Self, OdbcError> {
        let env = Environment::new().context(OdbcSnafu)?;

        Ok(Self {
            cfg,
            env: Arc::new(env),
            cx,
        })
    }

    async fn run_schedule(self: Box<Self>) -> Result<(), ()> {
        let mut shutdown = self.cx.shutdown.clone();
        let Some(ref sql) = self.cfg.statement else {
            return Ok(());
        };

        loop {
            if let Some(ref schedule) = self.cfg.schedule {
                let next = schedule
                    .upcoming(self.cfg.schedule_timezone)
                    .next()
                    .unwrap();
                let now = Utc::now().with_timezone(&self.cfg.schedule_timezone);
                let delay = next - now;
                let duration = delay.to_std().unwrap_or_default();

                tokio::select! {
                    _ = &mut shutdown => {
                     debug!("Shutting down database client source");
                     break;
                    }
                    _ = sleep(duration) => {
                     debug!("Sleeping for {} seconds", duration.as_secs())
                    }
                }
            }

            if let Err(e) = self.process(sql.clone()).await {
                error!(
                    message = "Error processing ODBC SQL statement",
                    statement = %sql,
                    error = %e);
            }

            // If there is no schedule, we only run once
            if self.cfg.schedule.is_none() {
                break;
            }
        }

        Ok(())
    }

    async fn process(&self, sql: String) -> Result<(), OdbcError> {
        let out = self.cx.out.clone();
        let log_schema = log_schema();
        let env = self.env.clone();

        // Load the last run metadata from the file.
        // If the file does not exist, use the initial parameters.
        let sql_params = self
            .cfg
            .last_run_metadata_path
            .clone()
            .and_then(|path| load_params(&path, self.cfg.tracking_columns.as_ref()))
            .unwrap_or_else(|| {
                self.cfg
                    .statement_init_params
                    .clone()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(_, value)| value.to_string().into_parameter())
                    .collect::<Vec<_>>()
            });
        let cfg = self.cfg.clone();

        let rows = execute_query(
            &env,
            &cfg.connection_string,
            &sql,
            sql_params,
            cfg.statement_timeout,
            cfg.odbc_default_timezone,
            cfg.odbc_batch_size,
            cfg.odbc_max_str_limit,
        )
        .unwrap_or_default();

        let mut event = LogEvent::default();
        event.maybe_insert(Some("timestamp"), Value::Timestamp(Utc::now()));
        event.maybe_insert(
            log_schema.message_key_target_path(),
            Value::Array(rows.clone()),
        );
        let mut out = out.clone();
        out.send_batch(vec![Event::from(event)])
            .await
            .context(ClosedSnafu)?;

        if let Some(last) = rows.last() {
            let Some(path) = cfg.last_run_metadata_path else {
                return Ok(());
            };
            let Some(tracking_columns) = cfg.tracking_columns else {
                return Ok(());
            };
            extract_and_save_tracking(&path, last.clone(), tracking_columns).await?;
        }

        Ok(())
    }
}

async fn extract_and_save_tracking(
    path: &str,
    obj: Value,
    tracking_columns: Vec<String>,
) -> Result<(), OdbcError> {
    let tracking_columns = tracking_columns
        .iter()
        .map(|col| col.as_str())
        .collect_vec();

    if let Value::Object(obj) = obj {
        let save_obj = obj
            .iter()
            .filter(|item| tracking_columns.contains(&item.0.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<ObjectMap>();

        save_params(path, &save_obj)?;
    }

    Ok(())
}

fn execute_query<'a>(
    env: &'a Environment,
    conn_str: &str,
    sql: &str,
    sql_params: Vec<VarCharBox>,
    timeout: Duration,
    tz: Tz,
    batch_size: usize,
    max_str_limit: usize,
) -> Result<Rows, OdbcError> {
    let conn = env
        .connect_with_connection_string(conn_str, ConnectionOptions::default())
        .context(OdbcSnafu)?;
    let mut stmt = conn.preallocate().context(OdbcSnafu)?;
    stmt.set_query_timeout_sec(timeout.as_secs() as usize)
        .context(OdbcSnafu)?;

    let result = if sql_params.is_empty() {
        stmt.execute(sql, ()).unwrap()
    } else {
        stmt.execute(sql, &sql_params[..]).unwrap()
    };

    let Some(mut cursor) = result else {
        return Ok(Rows::default());
    };

    let names = cursor
        .column_names()
        .context(OdbcSnafu)?
        .collect::<Result<Vec<String>, _>>()
        .context(OdbcSnafu)?;
    let types = (1..=names.len())
        .map(|col_index| cursor.col_data_type(col_index as u16).unwrap())
        .collect_vec();
    let columns = names
        .into_iter()
        .zip(types.into_iter())
        .map(|(name, data_type)| Column {
            column_name: name,
            column_type: data_type,
        })
        .collect::<Columns>();

    let buffer =
        TextRowSet::for_cursor(batch_size, &mut cursor, Some(max_str_limit)).context(OdbcSnafu)?;
    let mut row_set_cursor = cursor.bind_buffer(buffer).context(OdbcSnafu)?;
    let mut rows = Rows::with_capacity(batch_size);

    while let Some(batch) = row_set_cursor.fetch().unwrap() {
        let num_rows = batch.num_rows();

        for row_index in 0..num_rows {
            let mut cols = ObjectMap::new();

            for col_index in 0..batch.num_cols() {
                let column = &columns[col_index];
                let data_name = &column.column_name;
                let data_type = &column.column_type;
                let data_value = batch.at(col_index, row_index);
                let key = KeyString::from(data_name.as_str());
                let value = map_value(data_type, data_value, &tz);
                cols.insert(key, value);
            }

            rows.push(Value::Object(cols))
        }
    }

    Ok(rows)
}
