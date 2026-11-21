//! A session represents a state between the client and the KeyCDN API. It holds
//! the necessary information for making requests, such as the site declaration
//! and any session-specific settings like [`RateLimiter`] information.

use crate::{
    error,
    models::rate_limit::{RateLimit, RateLimiter},
};
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use serde::de::DeserializeOwned;
use url::Url;

/// A session represents a state between the client and the KeyCDN API. It holds
/// the necessary information for making requests, such as the site declaration
/// and any session-specific settings like [`RateLimiter`] information.
///
/// Use [`Session::new`] or [`Session::new_with_rate_limit`] to create a new
/// session.
pub struct Session {
    url: Url,
    headers: HeaderMap,
    rate_limiter: RateLimiter,
    client: tokio::sync::OnceCell<reqwest::Client>,
}

impl Session {
    const BASE_URL: &'static str = "https://tools.keycdn.com/";

    const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
    const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

    /// Creates a new session with the given site ID and default headers.
    pub fn new_with_rate_limit(site: &str, rate_limit: RateLimit) -> error::Result<Self> {
        let url = Url::parse(site).map_err(|_| error::Error::InvalidSiteId(site.to_string()))?;

        let mut headers = HeaderMap::new();
        headers.insert(
            USER_AGENT,
            HeaderValue::from_str(&format!("keycdn-tools:{site}"))?,
        );
        Ok(Self {
            url,
            headers,
            rate_limiter: rate_limit.into(),
            client: tokio::sync::OnceCell::new(),
        })
    }

    /// Creates a new session with the given site ID and default headers and rate limit.
    ///
    /// [`RateLimit`] can be set with [`Session::new_with_rate_limit`].
    pub fn new(site: &str) -> error::Result<Self> {
        Self::new_with_rate_limit(site, RateLimit::default())
    }

    /// Set the site URL which is required for the `User-Agent` header. This is
    /// used to identify the site making requests to the KeyCDN API.
    pub fn set_url(&mut self, url: Url) -> error::Result<()> {
        // Validate that the URL has a host and is either HTTP or HTTPS.
        if !url.has_host() || !matches!(url.scheme(), "http" | "https") {
            return Err(error::Error::InvalidSiteId(url.to_string()));
        }

        self.url = url;
        self.headers.insert(
            USER_AGENT,
            HeaderValue::from_str(&format!("keycdn-tools:{url}", url = self.url.as_str()))?,
        );

        // Drop the existing client if it exists, so that a new one will be
        // created with the updated headers.
        self.client.take();
        Ok(())
    }

    /// Returns a reference to the session's headers.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Build the full endpoint URL by joining the base URL with the provided endpoint path.
    pub fn endpoint(endpoint: &str) -> error::Result<Url> {
        let base_url = Url::parse(Self::BASE_URL)?;
        base_url.join(endpoint).map_err(error::Error::from)
    }

    /// Returns a reference to the session's internal
    /// [`client`](reqwest::Client), initializing it if it hasn't been created
    /// yet.
    pub async fn client(&self) -> error::Result<&reqwest::Client> {
        self.client
            .get_or_try_init(|| async {
                let client = reqwest::Client::builder()
                    .default_headers(self.headers.clone())
                    .connect_timeout(Self::CONNECT_TIMEOUT)
                    .read_timeout(Self::READ_TIMEOUT)
                    .build()?;
                Ok(client)
            })
            .await
    }

    /// Makes a request, handling rate limiting and returning the deserialized
    /// response.
    pub async fn request<T: DeserializeOwned>(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        query: Option<&[(&str, &str)]>,
        headers: Option<HeaderMap>,
        body: Option<serde_json::Value>,
    ) -> error::Result<T> {
        let url = Self::endpoint(endpoint)?;
        let client = self.client().await?;

        let mut builder = client.request(method.clone(), url.clone());

        if let Some(additional_headers) = headers {
            builder = builder.headers(additional_headers);
        }

        if let Some(query_params) = query {
            builder = builder.query(query_params);
        }

        if let Some(body) = body {
            // Reject requests with a body for GET or HEAD methods, as they should not have a body.
            if [reqwest::Method::GET, reqwest::Method::HEAD].contains(&method) {
                return Err(error::Error::InvalidRequest {
                    method,
                    url,
                    body: !body.is_null(), // always true
                });
            }
            builder = builder.json(&body);
        }

        self.rate_limiter
            .acquire_and_execute(async move {
                let response = builder.send().await?;

                if response.status().is_success() {
                    let bytes = response.bytes().await?;
                    serde_json::from_slice::<T>(&bytes).map_err(error::Error::from)
                } else {
                    // This assumes that a failure will still return some sort of response text body;
                    // if the API returns an octat-stream or binary response, this will fail.
                    Err(error::Error::UnexpectedResponse {
                        status: response.status(),
                        body: response.text().await.unwrap_or_else(|err| {
                            format!("Failed to read response body due to: {}", err)
                        }),
                    })
                }
            })
            .await
    }
}
