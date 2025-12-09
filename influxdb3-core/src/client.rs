use std::collections::HashMap;

use async_compression::tokio::write::GzipEncoder;
use futures::StreamExt as _;
use url::Url;
use reqwest::Client as HttpClient;
use reqwest::{header, StatusCode};
use tokio::io::AsyncWriteExt as _;
use arrow_flight::Ticket;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use serde::{Deserialize, Serialize};
use futures::stream::{BoxStream, TryStreamExt as _};

use crate::{ClientBuilder, FromPoint, InfluxDBError, Point, PointStream, TagMap, TimestampPrecision, ToPoint, batch_writer};

pub struct Client {
    pub(crate) api_url: Url,

    pub(crate) gzip_threshold: usize,
    pub(crate) no_sync: bool,
    pub(crate) precision: TimestampPrecision,
    pub(crate) org: String,
    pub(crate) database: String,

    pub(crate) http_client: HttpClient,
    pub(crate) flight_client: FlightServiceClient<tonic::transport::Channel>,
    pub(crate) authorization: String,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub async fn query(&self, query: &str) -> Result<BoxStream<'_, Result<Point, InfluxDBError>>, InfluxDBError> {
        Ok(_query(self, query, None).await?.boxed())
    }

    pub async fn query_as<T>(&self, query: &str) -> Result<BoxStream<'_, Result<T, InfluxDBError>>, InfluxDBError>
    where
        T: FromPoint,
    {
        Ok(
            _query(self, query, None).await?
                .map(|p| {
                    match p {
                        Ok(point) => T::from_point(point),
                        Err(e) => Err(e),
                    }
                }).boxed()
        )
    }

    pub async fn query_with_params(&self, query: &str, params: HashMap<&str, &str>) -> Result<BoxStream<'_, Result<Point, InfluxDBError>>, InfluxDBError> {
        Ok(_query(self, query, Some(params)).await?.boxed())
    }

    pub async fn query_with_params_as<T>(&self, query: &str, params: HashMap<&str, &str>) -> Result<BoxStream<'_, Result<T, InfluxDBError>>, InfluxDBError>
    where
        T: FromPoint,
    {
        Ok(
            _query(self, query, Some(params)).await?
                .map(|p| {
                    match p {
                        Ok(point) => T::from_point(point),
                        Err(e) => Err(e),
                    }
                }).boxed()
        )
    }

    pub async fn write_points<I, T>(&self, points: I) -> Result<(), InfluxDBError>
    where
        T: ToPoint,
        I: IntoIterator<Item = T>,
    {
        self.write_points_with_tags(points, &HashMap::new()).await
    }

    pub async fn write_points_with_tags<I, T>(&self, points: I, default_tags: &TagMap) -> Result<(), InfluxDBError>
    where
        T: ToPoint,
        I: IntoIterator<Item = T>,
    {
        let mut batcher = batch_writer::Batcher::new(self.precision, default_tags);
        batcher.add_points(points)?;

        let uri = self.api_url.join("/api/v3/write_lp")?;
        let mut params = vec![];
        let headers = header::HeaderMap::new();

        params.push(("org", self.org.as_str()));
        params.push(("db", self.database.as_str()));
        params.push(("precision", self.precision.v2_str()));
        params.push(("no_sync", if self.no_sync { "true" } else { "false" }));
        

        for mut buf in batcher.finalize() {
            let mut headers = headers.clone();
            if self.gzip_threshold > 0 && buf.len() > self.gzip_threshold {
                let mut encoder = GzipEncoder::new(Vec::new()); 
                encoder.write_all(&buf).await?;
                encoder.shutdown().await?;
                buf = encoder.into_inner();
                headers.insert(header::CONTENT_ENCODING, "gzip".parse().unwrap());
            }

            let req = self.http_client.post(uri.clone())
                .headers(headers)
                .query(&params)
                .body(buf)
                .send()
                .await?;

            if self.no_sync && req.status() == StatusCode::METHOD_NOT_ALLOWED {
                return Err(InfluxDBError::V3NotSupported);
            }

            if !req.status().is_success() {
                return handle_http_err(req).await;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct ErrorInternal {
    #[serde(default)]
    pub error_message: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ServerError {
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub error: String,
    #[serde(default)]
    pub data: ErrorInternal,
}

#[derive(Debug, Serialize)]
struct TicketData<'a> {
    database: &'a str,
    sql_query: &'a str,
    query_type: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<HashMap<&'a str, &'a str>>,
}

async fn handle_http_err(resp: reqwest::Response) -> Result<(), InfluxDBError> {
    if resp.status().is_success() {
        Ok(())
    } else {
        if let Some(retry_after) =resp.headers().get(header::RETRY_AFTER) {
            let retry_after = retry_after.to_str().unwrap_or("0").parse::<u64>().unwrap_or(0);
            return Err(InfluxDBError::RateLimited(retry_after));
        }

        let status = resp.status();
        let mut message;
        let content_type = resp.headers().get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()).unwrap_or("");
        if content_type.starts_with("application/json") {
            let error: ServerError = resp.json().await?;
            if error.message.is_empty() && error.code.is_empty() {
                message = error.data.error_message;
            } else {
                message = error.error;
            }
        } else {
            message = resp.text().await.unwrap_or_default();
        }
        if message.is_empty() {
            message = format!("HTTP error: {}", status);
        }
        Err(InfluxDBError::ApiError(message))
    }
}

async fn _query(client: &Client, query: &str, params: Option<HashMap<&str, &str>>) -> Result<PointStream, InfluxDBError> {
    let ticket_data = TicketData {
        database: &client.database,
        sql_query: query,
        query_type: "sql",
        params,
    };
    let ticket_json = serde_json::to_vec(&ticket_data)?;
    let ticket = Ticket { ticket: ticket_json.into() };
    let mut request = tonic::Request::new(ticket);
    request.metadata_mut().insert("authorization", client.authorization.parse().unwrap());

    let stream = client.flight_client.clone().do_get(request).await?.into_inner();
    let reader = FlightRecordBatchStream::new_from_flight_data(stream.map_err(|e| e.into()));

    Ok(PointStream::new(reader))
}