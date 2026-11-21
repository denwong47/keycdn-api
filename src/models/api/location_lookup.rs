//! Models for the
//! [IP Location Finder API](https://tools.keycdn.com/geo#ip-location-finder-api)
//! (`GET /geo.json?host={IP or hostname}`).
//!
//! A successful response looks like:
//!
//! ```json
//! {
//!   "status": "success",
//!   "description": "Data successfully received.",
//!   "data": { "geo": { "host": "8.8.8.8", "ip": "8.8.8.8", ... } }
//! }
//! ```
//!
//! Internally, the whole envelope is deserialized into a crate-private
//! `LocationLookupResponse`, which is then converted into a
//! [`LocationLookupResult`]: the `data.geo` object, which is what this crate
//! exposes.

use serde::{Deserialize, Serialize};
use std::net::IpAddr;

use crate::error::Error;

/// The `status` value KeyCDN uses for successful responses.
const STATUS_SUCCESS: &str = "success";

/// The full JSON response of the IP Location Finder API.
///
/// Convert it with [`TryFrom`] into a [`LocationLookupResult`], which checks
/// `status` and unwraps `data`.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct LocationLookupResponse {
    /// `"success"` on success. `"error"` on failure. Other values are undocumented.
    pub status: String,
    /// A human-readable description, e.g. `"Data successfully received."`.
    pub description: String,
    /// Only expected when `status` is `"success"`.
    #[serde(default)]
    pub data: Option<LocationLookupData>,
}

/// The `data` object of a [`LocationLookupResponse`], which only wraps `geo`.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct LocationLookupData {
    pub geo: LocationLookupResult,
}

impl TryFrom<LocationLookupResponse> for LocationLookupResult {
    type Error = Error;

    fn try_from(response: LocationLookupResponse) -> Result<Self, Self::Error> {
        if response.status != STATUS_SUCCESS {
            // e.g. {'status': 'error', 'description': 'Hostname or IP is not valid.'}
            return Err(Error::Api {
                status: response.status,
                description: response.description,
            });
        }
        response
            .data
            .map(|data| data.geo)
            .ok_or(Error::MissingData {
                description: response.description,
            })
    }
}

/// The location of an IP address or hostname, as returned by the IP Location
/// Finder API.
///
/// KeyCDN does not document which fields can be `null`; anything that has been
/// observed as `null` (or plausibly could be, e.g. for private or unallocated
/// addresses) is an [`Option`]. Data is sourced from MaxMind GeoLite.
///
/// This struct is serializable and deserializable with `serde`, but the aliases
/// for the field names are different for each direction: `snake_case` for
/// deserialization, `camelCase` for serialization. This is to match the API's
/// documented field names for deserialization.
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all(deserialize = "snake_case", serialize = "camelCase"))]
pub struct LocationLookupResult {
    /// The IP address or hostname that was looked up.
    pub host: String,
    /// The IP address that was located; the resolved address if
    /// [`host`](Self::host) was a hostname.
    pub ip: IpAddr,
    /// The autonomous system number, e.g. `15169` for Google.
    pub asn: Option<u32>,
    /// The name of the internet service provider, e.g. `"Google LLC"`.
    pub isp: Option<String>,
    /// e.g. `"United States"`.
    pub country_name: Option<String>,
    /// ISO 3166-1 alpha-2 country code, e.g. `"US"`.
    pub country_code: Option<String>,
    /// e.g. `"Massachusetts"`.
    pub region_name: Option<String>,
    /// e.g. `"MA"`.
    pub region_code: Option<String>,
    /// e.g. `"Norwell"`.
    pub city: Option<String>,
    /// e.g. `"02061"`.
    pub postal_code: Option<String>,
    /// e.g. `"North America"`.
    pub continent_name: Option<String>,
    /// e.g. `"NA"`.
    pub continent_code: Option<String>,
    /// Latitude in decimal degrees.
    pub latitude: Option<f64>,
    /// Longitude in decimal degrees.
    pub longitude: Option<f64>,
    /// US metro (DMA) code, e.g. `506`.
    pub metro_code: Option<u32>,
    /// IANA time zone name, e.g. `"America/New_York"`.
    pub timezone: Option<String>,
    /// A timestamp as returned by the API, formatted `YYYY-MM-DD HH:MM:SS`
    /// without a UTC offset. KeyCDN does not document what it represents.
    ///
    /// This is not returned by the API as it is not useful for consumers of
    /// this crate, but it is included here for completeness and potential
    /// future use.
    pub datetime: Option<String>,
}

impl crate::models::session::Session {
    /// Look up the location of an IP address or hostname.
    pub async fn location_lookup(&self, host: &str) -> crate::error::Result<LocationLookupResult> {
        let response: LocationLookupResponse = self
            .request(
                reqwest::Method::GET,
                "/geo.json",
                Some(&[("host", host)]),
                None,
                None,
            )
            .await?;
        response.try_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "test-integration")]
    use std::net::{IpAddr, Ipv4Addr};

    fn parse(json: &str) -> Result<LocationLookupResult, Error> {
        serde_json::from_str::<LocationLookupResponse>(json)?.try_into()
    }

    /// A real response captured from the API on 2026-11-21, with `null`s.
    #[test]
    fn deserializes_captured_response() {
        let json = r#"{
            "status": "success",
            "description": "Data successfully received.",
            "data": {
                "geo": {
                    "host": "8.8.8.8",
                    "ip": "8.8.8.8",
                    "asn": 15169,
                    "isp": "Google LLC",
                    "country_name": "United States",
                    "country_code": "US",
                    "region_name": null,
                    "region_code": null,
                    "city": null,
                    "postal_code": null,
                    "continent_name": "North America",
                    "continent_code": "NA",
                    "latitude": 37.751,
                    "longitude": -97.822,
                    "metro_code": null,
                    "timezone": "America/Chicago",
                    "datetime": "2026-11-21 00:31:02"
                }
            }
        }"#;

        let result = parse(json).unwrap();
        assert_eq!(
            result,
            LocationLookupResult {
                host: "8.8.8.8".to_owned(),
                ip: "8.8.8.8".parse().unwrap(),
                asn: Some(15169),
                isp: Some("Google LLC".to_owned()),
                country_name: Some("United States".to_owned()),
                country_code: Some("US".to_owned()),
                region_name: None,
                region_code: None,
                city: None,
                postal_code: None,
                continent_name: Some("North America".to_owned()),
                continent_code: Some("NA".to_owned()),
                latitude: Some(37.751),
                longitude: Some(-97.822),
                metro_code: None,
                timezone: Some("America/Chicago".to_owned()),
                datetime: Some("2026-11-21 00:31:02".to_owned()),
            }
        );
    }

    /// The example from KeyCDN's documentation: a hostname lookup with every
    /// field populated.
    #[test]
    fn deserializes_documented_example() {
        let json = r#"{"status":"success","description":"Data successfully received.","data":{"geo":{"host":"www.example.com","ip":"93.184.216.34","asn":15133,"isp":"MCI Communications Services, Inc. d/b/a Verizon Business","country_name":"United States","country_code":"US","region_name":"Massachusetts","region_code":"MA","city":"Norwell","postal_code":"02061","continent_name":"North America","continent_code":"NA","latitude":42.1596,"longitude":-70.8217,"metro_code":506,"timezone":"America/New_York","datetime":"2019-06-01 00:00:00"}}}"#;

        let result = parse(json).unwrap();
        assert_eq!(result.host, "www.example.com");
        assert_eq!(result.ip, "93.184.216.34".parse::<IpAddr>().unwrap());
        assert_eq!(result.city.as_deref(), Some("Norwell"));
        assert_eq!(result.metro_code, Some(506));
    }

    #[test]
    fn missing_optional_fields_are_none() {
        let json =
            r#"{"status":"success","description":"ok","data":{"geo":{"host":"::1","ip":"::1"}}}"#;

        let result = parse(json).unwrap();
        assert_eq!(result.ip, "::1".parse::<IpAddr>().unwrap());
        assert_eq!(result.asn, None);
        assert_eq!(result.timezone, None);
    }

    /// KeyCDN's error shape is undocumented; this only assumes `status` and
    /// `description` are present.
    #[test]
    fn non_success_status_is_api_error() {
        let json = r#"{"status":"error","description":"Invalid host."}"#;

        match parse(json) {
            Err(Error::Api {
                status,
                description,
            }) => {
                assert_eq!(status, "error");
                assert_eq!(description, "Invalid host.");
            }
            other => panic!("expected Error::Api, got {other:?}"),
        }
    }

    #[test]
    fn success_without_data_is_missing_data_error() {
        let json = r#"{"status":"success","description":"Data successfully received."}"#;

        assert!(matches!(parse(json), Err(Error::MissingData { .. })));
    }

    #[test]
    fn malformed_body_is_json_error() {
        assert!(matches!(parse("<html>"), Err(Error::Json(_))));
    }

    macro_rules! create_integration_test {
        ($name:ident($host:literal) => { $($field:ident: $expected_value:expr,)* $(,)? } ) => {
            #[tokio::test]
            #[cfg(feature = "test-integration")]
            async fn $name() {
                let session = crate::models::session::Session::new("https://github.com/denwong47").unwrap();
                let result = session.location_lookup($host).await.expect("location lookup failed");

                $(
                    assert_eq!(result.$field, $expected_value);
                )*
            }
        }
    }

    create_integration_test!(
        lookup_quad_8(
            "8.8.8.8"
        ) => {
            ip: IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
            country_code: Some("US".to_owned()),
            country_name: Some("United States".to_owned()),
            continent_code: Some("NA".to_owned()),
        }
    );
}
