use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, bail};
use http::HeaderName;
use serde_json::Value;
use tracing::warn;

pub const DEFAULT_TIMEOUT_SECS: u64 = 30;
pub const DEFAULT_MAX_RETRIES: u32 = 5;
pub const DEFAULT_INITIAL_RETRY_DELAY: Duration = Duration::from_secs(2);

static ACCEPT: http::HeaderValue = http::HeaderValue::from_static("application/json");
static CONTENT_TYPE: http::HeaderValue =
    http::HeaderValue::from_static("application/json; charset=utf-8");

pub struct HttpClientBuilder {
    user_agent: Option<String>,
    timeout: Duration,
    headers: HashMap<String, String>,
}

impl HttpClientBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn user_agent(mut self, ua: impl Into<String>) -> Self {
        self.user_agent = Some(ua.into());
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    pub fn build(self) -> Result<reqwest::Client> {
        let mut builder = reqwest::Client::builder().timeout(self.timeout);

        if let Some(ua) = &self.user_agent {
            builder = builder.user_agent(ua);
        }

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(http::header::ACCEPT, ACCEPT.clone());
        headers.insert(http::header::CONTENT_TYPE, CONTENT_TYPE.clone());

        for (key, value) in self.headers {
            let key: HeaderName = key.parse().context("parsing header key")?;
            headers.insert(key, value.parse().context("parsing header value")?);
        }

        builder
            .default_headers(headers)
            .build()
            .context("building HTTP client")
    }
}

impl Default for HttpClientBuilder {
    fn default() -> Self {
        Self {
            user_agent: None,
            timeout: Duration::from_secs(DEFAULT_TIMEOUT_SECS),
            headers: HashMap::new(),
        }
    }
}

pub struct RpcRequestBuilder<'a> {
    client: &'a reqwest::Client,
    url: &'a str,
    method: &'a str,
    params: Value,
    id: u64,
    max_retries: u32,
    initial_delay: Duration,
}

impl<'a> RpcRequestBuilder<'a> {
    pub fn new(client: &'a reqwest::Client, url: &'a str) -> Self {
        Self {
            client,
            url,
            method: "",
            params: Value::Array(vec![]),
            id: 1,
            max_retries: 0,
            initial_delay: DEFAULT_INITIAL_RETRY_DELAY,
        }
    }

    pub fn method(mut self, method: &'a str) -> Self {
        self.method = method;
        self
    }

    pub fn params(mut self, params: Value) -> Self {
        self.params = params;
        self
    }

    pub fn id(mut self, id: u64) -> Self {
        self.id = id;
        self
    }

    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    pub async fn call(self) -> Result<Value> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method":  self.method,
            "params":  self.params,
            "id":      self.id,
        });

        let mut delay = self.initial_delay;

        for attempt in 0..=self.max_retries {
            match raw_call(self.client, self.url, &body).await {
                Err(e) if is_rate_limited(&e) && attempt < self.max_retries => {
                    warn!(
                        attempt,
                        secs = delay.as_secs(),
                        "rate limited (429), retrying"
                    );
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
                result => return result,
            }
        }

        unreachable!()
    }

    pub async fn call_and_extract(self) -> Result<Value> {
        let resp = self.call().await?;
        extract_result(resp)
    }
}

async fn raw_call(client: &reqwest::Client, url: &str, body: &Value) -> Result<Value> {
    let resp = client
        .post(url)
        .json(body)
        .send()
        .await
        .context("upstream HTTP request failed")?;

    let status = resp.status();
    let text = resp
        .text()
        .await
        .context("reading upstream response body")?;

    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        bail!("upstream rate limited (429)");
    }
    if !status.is_success() {
        bail!("upstream HTTP {status}: {}", text.trim());
    }
    if text.is_empty() {
        bail!("upstream returned empty body (HTTP {status})");
    }

    serde_json::from_str(&text).context("parsing upstream JSON")
}

fn extract_result(resp: Value) -> Result<Value> {
    if let Some(err) = resp.get("error") {
        bail!("rpc error: {err}");
    }
    Ok(resp["result"].clone())
}

fn is_rate_limited(e: &anyhow::Error) -> bool {
    e.to_string().contains("429")
}
