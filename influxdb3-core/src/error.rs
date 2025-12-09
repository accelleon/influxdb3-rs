use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum InfluxDBError {
    #[error("Flight Error: {0}")]
    FlightError(#[from] arrow_flight::error::FlightError),

    #[error("Json error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Missing authentication token")]
    MissingToken,

    #[error("Rate limited. Retry after {0} seconds")]
    RateLimited(u64),

    #[error("API error: {0}")]
    ApiError(String),

    #[error("Invalid URI: {0}")]
    InvalidUri(#[from] url::ParseError),

    #[error("Invalid Scheme: {0}")]
    InvalidScheme(String),

    #[error("Invalid paramter: {0} value: {1}")]
    InvalidParameter(String, String),

    #[error("Invalid timestamp precision: {0}")]
    InvalidTimestampPrecision(String),

    #[error("Invalid tag name: {0}")]
    InvalidTagName(String),

    #[error("SSL Certificate error: {0}")]
    SSLCertificateError(String),

    #[error("Tonic transport error: {0}")]
    TonicTransportError(#[from] tonic::transport::Error),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("HTTP error: {0}")]
    HttpError(#[from] http::Error),

    #[error("GRPC error: {0}")]
    GrpcError(#[from] tonic::Status),

    #[error("Server does not support v3 API")]
    V3NotSupported,

    #[error("Reqwest error: {0}")]
    ReqwestError(#[from] reqwest::Error),

    #[error("Invalid point value conversion: {0}")]
    InvalidPointValueConversion(String),

    #[error("Invalid point value type received: {0} {1}")]
    InvalidPointValue(String, String),

    #[error("Other error: {0}")]
    Other(String),
}