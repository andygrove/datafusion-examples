extern crate core;

use async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::common::IPCWriter;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream, Statistics,
};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use std::any::Any;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let filename = "test.parquet";

    // convert a parquet file to an IPC file
    let ctx = SessionContext::new();
    ctx.register_parquet("test", filename, ParquetReadOptions::default())
        .await?;
    let df = ctx.sql("SELECT * FROM test").await?;
    let batches = df.collect().await?;
    let schema = Arc::new(batches[0].schema().as_ref().clone());
    let ipc_filename = "/tmp/foo.ipc";
    write_to_ipc(schema.clone(), batches, ipc_filename)?;

    // now run a SQL query directly against the IPC data
    ctx.register_table("ipc", Arc::new(IpcTableProvider::new(schema, ipc_filename)))?;
    let df = ctx.sql("SELECT * FROM ipc").await?;
    let batches = df.collect().await?;
    println!("{}", pretty_format_batches(&batches)?);

    Ok(())
}

fn write_to_ipc(schema: SchemaRef, batches: Vec<RecordBatch>, filename: &str) -> Result<()> {
    let mut w = IPCWriter::new(Path::new(filename), &schema)?;
    for batch in &batches {
        w.write(batch)?;
    }
    w.finish()
}

struct IpcTableProvider {
    schema: SchemaRef,
    filename: String,
}

impl IpcTableProvider {
    pub fn new(schema: SchemaRef, filename: &str) -> Self {
        Self {
            schema,
            filename: filename.to_string(),
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for IpcTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IpcPlan {
            schema: self.schema.clone(),
            filename: self.filename.clone(),
        }))
    }
}

#[derive(Debug)]
struct IpcPlan {
    schema: SchemaRef,
    filename: String,
}

impl ExecutionPlan for IpcPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let f = File::open(&self.filename)?;
        let reader = BufReader::new(f);
        let ipc = FileReader::try_new(reader, None)?;
        let mut batches = vec![];
        for maybe_batch in ipc {
            let batch = maybe_batch?;
            batches.push(batch);
        }
        let stream = MemoryStream::try_new(batches, self.schema.clone(), None)?;
        Ok(Box::pin(stream))
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

impl DisplayAs for IpcPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "")
    }
}
