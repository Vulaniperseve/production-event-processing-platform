# Serverless SQL over the gold layer: Glue Data Catalog tables + an Athena
# workgroup. There is no cluster and no crawler (crawlers are billed per
# run); the schemas are declared here and match the Parquet that Spark writes.

resource "aws_glue_catalog_database" "lake" {
  name        = replace("${var.project}_gold", "-", "_")
  description = "Gold layer of the market data lake (Parquet in S3)"
}

locals {
  gold_location = "s3://${aws_s3_bucket.lake.bucket}/${var.lake_prefix}/gold"

  parquet_serde = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
  parquet_in    = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
  parquet_out   = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
}

resource "aws_glue_catalog_table" "daily_metrics" {
  name          = "daily_metrics"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"

  # Partition projection: Athena works out the symbol=... partitions from
  # these settings, so new data needs no MSCK REPAIR or crawler.
  parameters = {
    "classification"            = "parquet"
    "projection.enabled"        = "true"
    "projection.symbol.type"    = "enum"
    "projection.symbol.values"  = join(",", var.symbols)
    "storage.location.template" = "${local.gold_location}/daily_metrics/symbol=$${symbol}"
    "parquet.compression"       = "SNAPPY"
  }

  partition_keys {
    name = "symbol"
    type = "string"
  }

  storage_descriptor {
    location      = "${local.gold_location}/daily_metrics/"
    input_format  = local.parquet_in
    output_format = local.parquet_out

    ser_de_info {
      serialization_library = local.parquet_serde
    }

    columns {
      name = "trade_date"
      type = "date"
    }
    columns {
      name = "close_price"
      type = "double"
    }
    columns {
      name = "prev_close"
      type = "double"
    }
    columns {
      name = "daily_return"
      type = "double"
    }
    columns {
      name = "log_return"
      type = "double"
    }
    columns {
      name = "sma_5"
      type = "double"
    }
    columns {
      name = "sma_20"
      type = "double"
    }
    columns {
      name = "volatility_20d"
      type = "double"
    }
    columns {
      name = "volatility_20d_annualized"
      type = "double"
    }
    columns {
      name = "running_peak"
      type = "double"
    }
    columns {
      name = "drawdown"
      type = "double"
    }
    columns {
      name = "trading_day_number"
      type = "int"
    }
  }
}

resource "aws_glue_catalog_table" "monthly_summary" {
  name          = "monthly_summary"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"      = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "${local.gold_location}/monthly_summary/"
    input_format  = local.parquet_in
    output_format = local.parquet_out

    ser_de_info {
      serialization_library = local.parquet_serde
    }

    columns {
      name = "symbol"
      type = "string"
    }
    columns {
      name = "month"
      type = "string"
    }
    columns {
      name = "trading_days"
      type = "bigint"
    }
    columns {
      name = "first_close"
      type = "double"
    }
    columns {
      name = "last_close"
      type = "double"
    }
    columns {
      name = "avg_daily_return"
      type = "double"
    }
    columns {
      name = "daily_volatility"
      type = "double"
    }
    columns {
      name = "best_day_return"
      type = "double"
    }
    columns {
      name = "worst_day_return"
      type = "double"
    }
    columns {
      name = "deepest_drawdown"
      type = "double"
    }
    columns {
      name = "monthly_return"
      type = "double"
    }
  }
}

resource "aws_glue_catalog_table" "return_correlation" {
  name          = "return_correlation"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"      = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "${local.gold_location}/return_correlation/"
    input_format  = local.parquet_in
    output_format = local.parquet_out

    ser_de_info {
      serialization_library = local.parquet_serde
    }

    columns {
      name = "symbol_a"
      type = "string"
    }
    columns {
      name = "symbol_b"
      type = "string"
    }
    columns {
      name = "return_correlation"
      type = "double"
    }
    columns {
      name = "overlapping_days"
      type = "bigint"
    }
  }
}

# Athena bills per byte scanned. The workgroup enforces its own settings
# (users can't override them), encrypts results and cancels runaway queries.
resource "aws_athena_workgroup" "lake" {
  name          = "${var.project}-lake"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_limit

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/${local.athena_results}/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
