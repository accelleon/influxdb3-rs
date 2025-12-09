pub use std::time::Duration;

use arrow_flight::flight_service_client::FlightServiceClient;
use url::Url;
use reqwest::ClientBuilder as ReqwestClientBuilder;
use tonic::transport::Endpoint;

use crate::{InfluxDBError, TimestampPrecision, Client};

const USER_AGENT: &str = "influxdb3-rs/0.1";

fn parse_bool(s: &str) -> Result<bool, &str> {
    match s.to_lowercase().as_str() {
        "true" | "1" | "t" => Ok(true),
        "false" | "0" | "f" => Ok(false),
        _ => Err(s),
    }
}

pub struct ClientBuilder {
    host: Url,
    token: String,
    auth_scheme: String,
    organization: String,
    database: String,
    timeout: Duration,
    query_timeout: Option<Duration>,
    idle_timeout: Option<Duration>,
    max_idle_connections: usize,
    default_headers: http::HeaderMap,
    ssl_root_certificates: Option<String>,
    proxy: Option<Url>,
    precision: TimestampPrecision,
    gzip_threshold: usize,
    no_sync: bool,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        let mut headers = http::HeaderMap::new();
        headers.insert("User-Agent", USER_AGENT.parse().unwrap());

        Self {
            host: "http://localhost:8086".parse().unwrap(),
            token: String::new(),
            auth_scheme: "Bearer".to_string(),
            organization: String::new(),
            database: String::new(),
            timeout: Duration::from_secs(10),
            query_timeout: None,
            idle_timeout: Some(Duration::from_secs(90)),
            max_idle_connections: 100,
            default_headers: http::HeaderMap::new(),
            ssl_root_certificates: None,
            proxy: None,
            precision: TimestampPrecision::Nanoseconds,
            gzip_threshold: 1024,
            no_sync: false,
        }
    }
}

impl ClientBuilder {
    pub fn from_connection_string(s: &str) -> Result<Self, InfluxDBError> {
        let uri: Url = s.parse()?;

        if !matches!(uri.scheme(), "http" | "https") {
            return Err(InfluxDBError::InvalidScheme(uri.scheme().to_string()));
        }

        let mut client = ClientBuilder::default();
        client.host = format!("{}://{}", uri.scheme(), uri.host_str().unwrap()).parse()?;

        for (k, v) in uri.query_pairs() {
            match k.as_ref() {
                "token" => client.token = v.to_string(),
                "authScheme" => client.auth_scheme = v.to_string(),
                "org" => client.organization = v.to_string(),
                "database" => client.database = v.to_string(),
                "precision" => client.precision = TimestampPrecision::try_from(v.as_ref())?,
                "gzipThreshold" => {
                    client.gzip_threshold = v.parse()
                        .map_err(|_| InfluxDBError::InvalidParameter("gzipThreshold".to_string(), v.to_string()))?
                },
                "writeNoSync" => {
                    client.no_sync = parse_bool(&v)
                        .map_err(|s| InfluxDBError::InvalidParameter("writeNoSync".to_string(), s.to_string()))?
                },
                _ => {}
            }
        }

        Ok(client)
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn host(mut self, host: &str) -> Result<Self, InfluxDBError> {
        self.host = host.parse()?;
        Ok(self)
    }

    pub fn token(mut self, token: &str) -> Self {
        self.token = token.to_string();
        self
    }

    pub fn organization(mut self, organization: &str) -> Self {
        self.organization = organization.to_string();
        self
    }

    pub fn database(mut self, database: &str) -> Self {
        self.database = database.to_string();
        self
    }

    pub fn precision(mut self, precision: TimestampPrecision) -> Self {
        self.precision = precision;
        self
    }

    pub fn gzip_threshold(mut self, gzip_threshold: usize) -> Self {
        self.gzip_threshold = gzip_threshold;
        self
    }

    pub fn no_sync(mut self, no_sync: bool) -> Self {
        self.no_sync = no_sync;
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn query_timeout(mut self, query_timeout: Duration) -> Self {
        self.query_timeout = Some(query_timeout);
        self
    }

    pub fn idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = Some(idle_timeout);
        self
    }

    pub fn max_idle_connections(mut self, max_idle_connections: usize) -> Self {
        self.max_idle_connections = max_idle_connections;
        self
    }

    pub fn proxy(mut self, proxy: &str) -> Result<Self, InfluxDBError> {
        self.proxy = Some(proxy.parse()?);
        Ok(self)
    }

    pub fn ssl_root_certificates(mut self, path: &str) -> Self {
        self.ssl_root_certificates = Some(path.to_string());
        self
    }

    pub fn default_header(mut self, key: http::HeaderName, value: &str) -> Result<Self, InfluxDBError> {
        self.default_headers.insert(key, value.parse().unwrap());
        Ok(self)
    }

    pub fn auth_scheme(mut self, scheme: &str) -> Self {
        self.auth_scheme = scheme.to_string();
        self
    }

    pub fn build(mut self) -> Result<Client, InfluxDBError> {
        if self.token.is_empty() {
            return Err(InfluxDBError::MissingToken);
        }

        let authorization = format!("{} {}", self.auth_scheme, self.token);
        self.default_headers.insert("Authorization", authorization.parse().unwrap());

        let mut endpoint = Endpoint::from_shared(self.host.to_string())?
                .user_agent(USER_AGENT)?
                .timeout(self.timeout)
                .connect_timeout(self.timeout)
                .concurrency_limit(self.max_idle_connections);

        // TODO: Proxy support for tonic
        let mut http_builder = ReqwestClientBuilder::new()
            .pool_idle_timeout(self.idle_timeout)
            .pool_max_idle_per_host(self.max_idle_connections)
            .default_headers(self.default_headers)
            .gzip(true);

        if let Some(query_timeout) = self.query_timeout {
            endpoint = endpoint.timeout(query_timeout);
        }
        
        if let Some(proxy_url) = self.proxy {
            http_builder = http_builder.proxy(reqwest::Proxy::all(proxy_url.as_str())?);
        }

        if let Some(cert_path) = self.ssl_root_certificates {
            let cert_data = std::fs::read(cert_path)
                .map_err(|e| InfluxDBError::SSLCertificateError(e.to_string()))?;
            let cert = reqwest::Certificate::from_pem(&cert_data)
                .map_err(|e| InfluxDBError::SSLCertificateError(e.to_string()))?;
            http_builder = http_builder.add_root_certificate(cert);
            endpoint = endpoint.tls_config(
                tonic::transport::ClientTlsConfig::new()
                    .with_native_roots()
                    .ca_certificate(tonic::transport::Certificate::from_pem(cert_data))
            )?;
        }

        Ok(Client {
            api_url: self.host,

            gzip_threshold: self.gzip_threshold,
            no_sync: self.no_sync,
            precision: self.precision,
            org: self.organization,
            database: self.database,

            http_client: http_builder.build()?,
            flight_client: FlightServiceClient::new(endpoint.connect_lazy()),
            authorization,
        })
    }
}