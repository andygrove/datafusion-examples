use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("date", DataType::Date32, false),
        Field::new("cost", DataType::Decimal128(15, 2), false),
    ]);
    let csv_options = CsvReadOptions::default().has_header(true).schema(&schema);
    ctx.read_csv("data.csv", csv_options)
        .await?
        .repartition(Partitioning::Hash(vec![col("id")], 24))?
        .write_parquet("data.parquet", None)
        .await?;
    Ok(())
}
