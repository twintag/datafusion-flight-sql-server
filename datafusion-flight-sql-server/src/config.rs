#[derive(Default)]
pub struct FlightSqlServiceConfig {
    /// When true, includes table names in field metadata under the "table_name" key.
    /// This allows clients to identify the source table or alias for each column in query results.
    pub schema_with_metadata: bool,
}

impl FlightSqlServiceConfig {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }
}
