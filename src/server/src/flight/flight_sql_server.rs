use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::server::{
    FlightSqlService, PeekableFlightDataStream,
};
use arrow_flight::sql::{
    CommandStatementQuery, CommandStatementUpdate,
};
use arrow_flight::{
    flight_descriptor::DescriptorType, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    Ticket,
};
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::writer::{DictionaryTracker, IpcWriteOptions};
use datafusion::arrow::ipc::{self};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use dobbydb_sql::parser::ExtendedParser;
use dobbydb_sql::planner::ExtendedQueryPlanner;
use futures::{Stream, StreamExt};
use prost::Message;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, Streaming};

use crate::DobbyDBServer;

/// Flight SQL 服务器实现
pub struct DobbyDBFlightSqlService {
    server: Arc<DobbyDBServer>,
    sessions: Arc<RwLock<std::collections::HashMap<String, Arc<SessionContext>>>>,
}

impl DobbyDBFlightSqlService {
    pub fn new(server: Arc<DobbyDBServer>) -> Self {
        Self {
            server,
            sessions: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    async fn get_or_create_session(&self, handle: &str) -> Result<Arc<SessionContext>, Status> {
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.get(handle) {
            return Ok(session.clone());
        }

        let session = self
            .server
            .create_session_context()
            .await
            .map_err(|e| Status::internal(format!("Failed to create session: {}", e)))?;
        sessions.insert(handle.to_string(), session.clone());
        Ok(session)
    }

    fn record_batch_to_flight_data(
        batch: &RecordBatch,
    ) -> Result<FlightData, datafusion::arrow::error::ArrowError> {
        let options = IpcWriteOptions::default();
        let mut dictionary_tracker = DictionaryTracker::new(false);
        
        let mut schema_buffer = Vec::new();
        let mut writer = ipc::writer::StreamWriter::new(&mut schema_buffer, &options);
        writer.write(batch, &mut dictionary_tracker)?;
        writer.finish()?;

        let mut flight_data = FlightData::default();
        flight_data.data_header = schema_buffer;
        flight_data.data_body = batch_to_bytes(batch)?;
        Ok(flight_data)
    }

    fn schema_to_bytes(schema: &Schema) -> Result<Vec<u8>, datafusion::arrow::error::ArrowError> {
        let options = IpcWriteOptions::default();
        let mut buffer = Vec::new();
        let mut writer = ipc::writer::StreamWriter::new(&mut buffer, &options);
        writer.finish()?;
        Ok(buffer)
    }

    async fn execute_query(
        &self,
        session: Arc<SessionContext>,
        query: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>, Status> {
        let parser = ExtendedParser::parse_sql(query)
            .map_err(|e| Status::invalid_argument(format!("SQL parse error: {}", e)))?;

        if parser.is_empty() {
            return Err(Status::invalid_argument("Empty query"));
        }

        if parser.len() > 1 {
            return Err(Status::invalid_argument("Only single statement queries are supported"));
        }

        let stmt = &parser[0];
        let planner = ExtendedQueryPlanner::new(session.clone())
            .map_err(|e| Status::internal(format!("Failed to create planner: {}", e)))?;

        let logic_plan = planner
            .create_logical_plan(stmt)
            .await
            .map_err(|e| Status::internal(format!("Failed to create logical plan: {}", e)))?;

        let physical_plan = planner
            .create_physical_plan(&logic_plan)
            .await
            .map_err(|e| Status::internal(format!("Failed to create physical plan: {}", e)))?;

        let batch_stream = planner
            .execute_physical_plan(physical_plan)
            .await
            .map_err(|e| Status::internal(format!("Failed to execute plan: {}", e)))?;

        let flight_data_stream = batch_stream
            .map(|batch_result| {
                batch_result
                    .map_err(|e| Status::internal(format!("Execution error: {}", e)))
                    .and_then(|batch| {
                        Self::record_batch_to_flight_data(&batch)
                            .map_err(|e| Status::internal(format!("Failed to convert batch: {}", e)))
                    })
            });

        Ok(Box::pin(flight_data_stream))
    }
}

fn batch_to_bytes(batch: &RecordBatch) -> Result<Vec<u8>, datafusion::arrow::error::ArrowError> {
    let mut buffer = Vec::new();
    let options = IpcWriteOptions::default();
    let mut writer = ipc::writer::FileWriter::new(&mut buffer, batch.schema(), Some(&options))?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(buffer)
}

#[tonic::async_trait]
impl FlightSqlService for DobbyDBFlightSqlService {
    type FlightService = DobbyDBFlightSqlService;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        // 简单的握手实现，返回空响应
        let output = futures::stream::iter(vec![Ok(HandshakeResponse {
            protocol_version: 0,
            payload: vec![],
        })]);
        Ok(Response::new(Box::pin(output)))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // 生成一个简单的 ticket
        let ticket = Ticket {
            ticket: query.encode_to_vec().into(),
        };

        // 创建 FlightInfo
        let flight_info = FlightInfo {
            schema: vec![], // 将在 DoGet 中提供
            flight_descriptor: Some(FlightDescriptor {
                r#type: DescriptorType::Cmd as i32,
                cmd: query.encode_to_vec().into(),
                path: vec![],
            }),
            endpoint: vec![arrow_flight::FlightEndpoint {
                ticket: Some(ticket),
                location: vec![],
            }],
            total_records: -1,
            total_bytes: -1,
            ordered: false,
            app_metadata: vec![],
        };

        Ok(Response::new(flight_info))
    }

    async fn do_get_statement(
        &self,
        cmd: arrow_flight::sql::TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as arrow_flight::flight_service_server::FlightService>::DoGetStream>, Status> {
        // 从 ticket 中解码查询
        let query_cmd = CommandStatementQuery::decode(cmd.statement_handle.as_ref())
            .map_err(|e| Status::invalid_argument(format!("Invalid ticket: {}", e)))?;
        // 使用默认会话
        let session = self.get_or_create_session("default").await?;
        let stream = self.execute_query(session, &query_cmd.query).await?;

        Ok(Response::new(Box::pin(PeekableFlightDataStream::new(stream))))
    }

    async fn do_put_statement_update(
        &self,
        _cmd: CommandStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        // 暂时不支持更新操作
        Err(Status::unimplemented("Statement updates not yet implemented"))
    }

    async fn get_flight_info_prepared_statement(
        &self,
        _query: arrow_flight::sql::CommandPreparedStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Prepared statements not yet implemented"))
    }

    async fn do_get_prepared_statement(
        &self,
        _cmd: arrow_flight::sql::CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as arrow_flight::flight_service_server::FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Prepared statements not yet implemented"))
    }

    async fn do_put_prepared_statement_update(
        &self,
        _cmd: arrow_flight::sql::CommandPreparedStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented("Prepared statements not yet implemented"))
    }

    async fn get_flight_info_catalogs(
        &self,
        _query: arrow_flight::sql::CommandGetCatalogs,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // 返回空结果
        let schema = Schema::new(vec![Field::new("catalog_name", DataType::Utf8, false)]);
        let flight_info = FlightInfo {
            schema: Self::schema_to_bytes(&schema)
                .map_err(|e| Status::internal(format!("Failed to encode schema: {}", e)))?,
            flight_descriptor: None,
            endpoint: vec![arrow_flight::FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: b"catalogs".to_vec().into(),
                }),
                location: vec![],
            }],
            total_records: -1,
            total_bytes: -1,
            ordered: false,
            app_metadata: vec![],
        };
        Ok(Response::new(flight_info))
    }

    async fn do_get_catalogs(
        &self,
        _query: arrow_flight::sql::CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as arrow_flight::flight_service_server::FlightService>::DoGetStream>, Status> {
        // 返回空结果
        let schema = Schema::new(vec![Field::new("catalog_name", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(vec![] as Vec<&str>))],
        )
        .map_err(|e| Status::internal(format!("Failed to create batch: {}", e)))?;

        let flight_data = Self::record_batch_to_flight_data(&batch)
            .map_err(|e| Status::internal(format!("Failed to convert batch: {}", e)))?;

        let stream = futures::stream::iter(vec![Ok(flight_data)]);
        Ok(Response::new(Box::pin(PeekableFlightDataStream::new(Box::pin(stream)))))
    }

    async fn get_flight_info_schemas(
        &self,
        _query: arrow_flight::sql::CommandGetDbSchemas,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, false),
        ]);
        let flight_info = FlightInfo {
            schema: Self::schema_to_bytes(&schema)
                .map_err(|e| Status::internal(format!("Failed to encode schema: {}", e)))?,
            flight_descriptor: None,
            endpoint: vec![arrow_flight::FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: b"schemas".to_vec().into(),
                }),
                location: vec![],
            }],
            total_records: -1,
            total_bytes: -1,
            ordered: false,
            app_metadata: vec![],
        };
        Ok(Response::new(flight_info))
    }

    async fn do_get_schemas(
        &self,
        _query: arrow_flight::sql::CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as arrow_flight::flight_service_server::FlightService>::DoGetStream>, Status> {
        let schema = Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec![] as Vec<Option<&str>>)),
                Arc::new(StringArray::from(vec![] as Vec<&str>)),
            ],
        )
        .map_err(|e| Status::internal(format!("Failed to create batch: {}", e)))?;

        let flight_data = Self::record_batch_to_flight_data(&batch)
            .map_err(|e| Status::internal(format!("Failed to convert batch: {}", e)))?;

        let stream = futures::stream::iter(vec![Ok(flight_data)]);
        Ok(Response::new(Box::pin(PeekableFlightDataStream::new(Box::pin(stream)))))
    }

    async fn get_flight_info_tables(
        &self,
        _query: arrow_flight::sql::CommandGetTables,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]);
        let flight_info = FlightInfo {
            schema: Self::schema_to_bytes(&schema)
                .map_err(|e| Status::internal(format!("Failed to encode schema: {}", e)))?,
            flight_descriptor: None,
            endpoint: vec![arrow_flight::FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: b"tables".to_vec().into(),
                }),
                location: vec![],
            }],
            total_records: -1,
            total_bytes: -1,
            ordered: false,
            app_metadata: vec![],
        };
        Ok(Response::new(flight_info))
    }

    async fn do_get_tables(
        &self,
        _query: arrow_flight::sql::CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as arrow_flight::flight_service_server::FlightService>::DoGetStream>, Status> {
        let schema = Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec![] as Vec<Option<&str>>)),
                Arc::new(StringArray::from(vec![] as Vec<Option<&str>>)),
                Arc::new(StringArray::from(vec![] as Vec<&str>)),
                Arc::new(StringArray::from(vec![] as Vec<&str>)),
            ],
        )
        .map_err(|e| Status::internal(format!("Failed to create batch: {}", e)))?;

        let flight_data = Self::record_batch_to_flight_data(&batch)
            .map_err(|e| Status::internal(format!("Failed to convert batch: {}", e)))?;

        let stream = futures::stream::iter(vec![Ok(flight_data)]);
        Ok(Response::new(Box::pin(PeekableFlightDataStream::new(Box::pin(stream)))))
    }

    async fn do_get_sql_info(
        &self,
        _query: arrow_flight::sql::CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as arrow_flight::flight_service_server::FlightService>::DoGetStream>, Status> {
        // 返回基本的 SQL 信息 - 这里需要返回一个流
        Err(Status::unimplemented("SQL info not yet fully implemented"))
    }

    async fn register_sql_info(&self, _id: i32, _info: &arrow_flight::sql::SqlInfo) {
        // 注册 SQL 信息
    }
}


/// 启动 Flight SQL 服务器
pub async fn start_flight_sql_server(
    server: Arc<DobbyDBServer>,
    addr: std::net::SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let flight_sql_service = DobbyDBFlightSqlService::new(server);
    let svc = FlightServiceServer::new(flight_sql_service);

    println!("Starting DobbyDB Flight SQL server on {}", addr);

    tonic::transport::Server::builder()
        .add_service(svc)
        .serve(addr)
        .await?;

    Ok(())
}
