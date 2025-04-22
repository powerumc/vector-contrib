use crate::config::{log_schema, SourceContext};
use crate::event::{Event, LogEvent};
use crate::sources;
use crate::sources::database::client::{DatabaseConfig, DEFAULT_HOST};
use chrono::{NaiveDate, Utc};
use chrono_tz::Tz;
use cron::Schedule;
use futures_util::FutureExt;
use itertools::Itertools;
use mysql::prelude::Queryable;
use mysql::{OptsBuilder, Params, Pool, PoolConstraints, PoolOpts, Row};
use serde_with::serde_as;
use std::borrow::Cow;
use std::str::FromStr;
use tokio::time::sleep;
use vector_common::Error;
use vector_config_macros::configurable_component;
use vrl::prelude::*;

pub(crate) const DEFAULT_PORT: u16 = 3306;

/// Configuration for the `database` source.
#[serde_as]
#[configurable_component(source(
    "mysql",
    "Pull observability data from a MySQL database by scheduling a query to run at a specific time."
))]
#[derive(Clone, Debug)]
pub struct MySqlConfig {
    /// The connection string to the database.
    #[configurable(metadata(docs::examples = "localhost"))]
    #[serde(default = "default_host")]
    pub host: String,

    /// The port to connect to the database.
    #[configurable(metadata(docs::examples = 3306))]
    #[serde(default = "default_port")]
    pub port: u16,

    /// The name of the database to connect to.
    #[configurable(metadata(docs::examples = "my_database"))]
    pub database: Option<String>,

    /// The user to connect to the database.
    #[configurable(metadata(docs::examples = "root"))]
    pub user: Option<String>,

    /// The password to connect to the database.
    /// This field is optional and can be omitted if the database does not require a password.
    #[configurable(metadata(docs::examples = "your_password"))]
    pub password: Option<String>,
}

impl_generate_config_from_default!(MySqlConfig);

fn default_host() -> String {
    DEFAULT_HOST.to_owned()
}

const fn default_port() -> u16 {
    DEFAULT_PORT
}

impl Default for MySqlConfig {
    fn default() -> Self {
        Self {
            host: DEFAULT_HOST.to_owned(),
            port: DEFAULT_PORT,
            database: None,
            user: None,
            password: None,
        }
    }
}

impl MySqlConfig {
    pub(crate) async fn build(
        &self,
        config: DatabaseConfig,
        cx: SourceContext,
    ) -> crate::Result<sources::Source> {
        Ok(run(config, self.clone(), cx).boxed())
    }

    pub(crate) const fn can_acknowledge(&self) -> bool {
        false
    }
}

pub(crate) async fn run(
    database_config: DatabaseConfig,
    config: MySqlConfig,
    cx: SourceContext,
) -> Result<(), ()> {
    let opts = OptsBuilder::new()
        .ip_or_hostname(Some(config.host))
        .tcp_port(config.port)
        .db_name(config.database)
        .user(config.user)
        .pass(config.password)
        .pool_opts(PoolOpts::new().with_constraints(PoolConstraints::new(1, 1).unwrap()));
    let pool = Pool::new(opts).unwrap();

    let mut out = cx.out.clone();
    let mut shutdown = cx.shutdown.clone();

    let schedule = Schedule::from_str(database_config.schedule.as_ref().unwrap().as_str()).unwrap();
    let timezone = Tz::from_str(
        database_config
            .schedule_timezone
            .as_ref()
            .map_or("UTC", |v| v),
    ).unwrap_or(chrono_tz::UTC);

    loop {
        let next = schedule.upcoming(timezone).next().unwrap();
        let now = Utc::now().with_timezone(&timezone);
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

        let timeout = std::time::Duration::from_secs(3);
        let mut conn = pool.try_get_conn(timeout).unwrap();
        let statement = conn.prep(database_config.statement.as_str()).unwrap();
        let rows: Vec<Row> = conn.exec(statement, Params::Empty).unwrap();

        let results = rows
            .iter()
            .map(|row|
                Value::Object(row
                    .columns_ref()
                    .iter()
                    .enumerate()
                    .map(|(index, col)| {
                        let key = KeyString::from(col.name_str());
                        let column_value: mysql::Value = row.get(index).unwrap_or(mysql::Value::NULL);
                        let value = map_value(col.name_str(), column_value).unwrap_or(Value::Null);
                        (key, value)
                    }).collect()
                )
            )
            .collect_vec();

        let log_schema = log_schema();
        let mut event = LogEvent::default();
        event.maybe_insert(Some("timestamp"), Value::Timestamp(Utc::now()));
        event.maybe_insert(
            log_schema.message_key_target_path(),
            Value::Array(results),
        );
        out.send_batch(vec![Event::from(event)]).await.unwrap();
    }

    Ok(())
}

/// Convert `mysql::Value` to `vrl::value:Value`
///
/// If MySQL does not 'Binary Protocol', all columns returned as `Value::Bytes`. [issues/288](https://github.com/blackbeam/rust-mysql-simple/issues/288)
fn map_value(column_name: Cow<str>, value: mysql::Value) -> crate::Result<Value> {
    match value {
        mysql::Value::NULL => Ok(Value::Null),
        mysql::Value::Bytes(bytes) => Ok(Value::Bytes(Bytes::from(bytes))),
        mysql::Value::Int(int) => Ok(Value::Integer(int)),
        mysql::Value::UInt(uint) => i64::try_from(uint)
            .map(Value::Integer)
            .map_err(|e| Error::from(format!("{e}: {column_name}"))),
        mysql::Value::Float(float) => NotNan::new(float as f64)
            .map(Value::Float)
            .map_err(|e| Error::from(format!("{e}: {column_name}"))),
        mysql::Value::Double(double) => NotNan::new(double)
            .map(Value::Float)
            .map_err(|e| Error::from(format!("{e}: {column_name}"))),
        mysql::Value::Date(years, month, days, hours, minutes, seconds, micro) =>
            NaiveDate::from_ymd_opt(years as i32, month as u32, days as u32)
                .and_then(|native_date| {
                    native_date.and_hms_micro_opt(
                        hours as u32,
                        minutes as u32,
                        seconds as u32,
                        micro,
                    )
                })
                .map(|datetime| Value::Timestamp(datetime.and_utc()))
                .ok_or(Error::from(format!("Invalid date: {column_name}"))),
        mysql::Value::Time(negative, days, hours, minutes, seconds, micro) =>
            Ok(Value::Bytes(
                Bytes::copy_from_slice(&[
                negative as u8,
                days as u8,
                hours,
                minutes,
                seconds,
                micro as u8,
                ])
            ))
    }
}
