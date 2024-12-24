# Pulse Analytics

Welcome to the Pulse Analytics repository. This project contains SQL views that connect battery telemetry data with metadata on the parts and devices involved in testing. In addition, there are several Superset assets included that implement dashboards over these data sources for various use cases. The flexible architecture used in this project allows for easy integration—developers can connect any of their data sources through a central query engine and explore them through a robust visualization tool.

## Schema Overview

### Telemetry Sources

Tables generated by the pulse-telemetry project:
- **Telemetry**: Individual records from the battery.
- **Statistics Step**: Aggregation of telemetry data at the charge/discharge step level.
- **Statistics Cycle**: Aggregation of telemetry data at the cycle level.

### Metadata Sources

Tables that describe how parts, tests, and devices intersect:
- **Device Metadata**: Data describing each test device.
- **Recipe Metadata**: Data describing versioned test recipes.
- **Device-Test-Recipe**: Links each test id to the test recipe.
- **Part Metadata**: Data describing each part under test.
- **Device-Test-Part**: Links each test id to the part under test.

### Test Marts

Tables that enable dynamic queries against groups of devices and test recipes:
- **Test Telemetry**: Telemetry records with device and recipe metadata.
- **Test Statistics Step**: Step records with device and recipe metadata.
- **Test Statistics Cycle**: Cycle records with device and recipe metadata.

### Part Marts

Tables that concatenate multiple tests run on a part:
- **Part Telemetry**: Telemetry records with part metadata.
- **Part Statistics Step**: Step records with part metadata.
- **Part Statistics Cycle**: Cycle records with part metadata.

*Also reindexes record, step, and cycle number to the part-level.*

## Superset Dashboards

Coming soon...

## Developer Notes

### Test Coverage

- **Data Quality Checks**: There are basic quality checks in DBT for uniqueness and nullability constraints. Additional quality validations are implemented in `pytest`, which are run in both integration and e2e tests.

- **Integration Tests**: Integration tests run on DuckDB to validate data transformations, DBT model execution, and data quality checks. Test setup and teardown is lightweight and fast.

- **End-to-End Tests**: End-to-end tests perform the same checks as integration tests but use Trino as the backend. This setup provides a production-like environment, where telemetry, metadata, and the DBT targets may reside in separate data catalogs.

### DBT Project Configuration

- **Views as Targets**: All DBT targets are implemented as views, allowing dynamic swapping of metadata and telemetry sources in real-time. This approach is more error-tolerant when concatenating test files for parts.

- **Source Abstraction Layer**: The six source views create an abstraction layer for downstream marts. These can be pulled in from many types of databases including postgres, iceberg, or google sheets. See the Trino documentation for supported catalogs.

- **Configurable Sources and Targets**: Sources and targets are fully configurable via environment variables. See the dbt-job manifest in the `examples` directory for a complete list. Ensure all sources and targets are set up as catalogs in Trino so they are accessible by DBT.

### Trino Catalog Configuration

- **Broad Catalog Support**: Trino supports a wide range of catalogs, allowing you to bring in metadata from any database source you are using. This flexibility means you can integrate data from sources like PostgreSQL, BigQuery, and others.

- **Recommended Catalog Setup**: It’s recommended to set up three separate catalogs to help you enforce robust access controls across data domains:
  - **Telemetry Catalog**: Contains real tables for telemetry data.
  - **Metadata Catalog**: Stores real tables with metadata.
  - **Analytics Catalog**: Holds views and other artifacts created by DBT.

- **Access Requirements**: This project requires read access to both the telemetry and metadata catalogs, and write access to the analytics catalog for creating and managing views.
