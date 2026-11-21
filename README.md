# keycdn-api

A small async Rust client for the [KeyCDN](https://www.keycdn.com/) tools API.

It currently supports only the
[IP Location Finder API](https://tools.keycdn.com/geo#ip-location-finder-api).
Look up an IP address or hostname and get its country, region, city, ASN, ISP,
coordinates and time zone. Requests are rate limited to stay within KeyCDN's
limits.

## Installation

The crate is not published on crates.io, so add it as a git dependency. You
also need [Tokio](https://tokio.rs/), because requests are made through
`reqwest`:

```toml
[dependencies]
keycdn-api = { git = "https://github.com/denwong47/keycdn-api" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

### Features

| Feature            | Default | Description                                                    |
| ------------------ | ------- | -------------------------------------------------------------- |
| `rustls`           | yes     | Use `rustls` for TLS.                                          |
| `native-tls`       | no      | Use the platform's native TLS instead. Disable default features to use it on its own. |
| `test-integration` | no      | Enable tests that call the live KeyCDN API.                    |

## Usage

```rust
use keycdn_api::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let session = Session::new("https://www.example.com")?;

    let location = session.location_lookup("8.8.8.8").await?;
    println!("{} is in {:?}", location.ip, location.country_name);

    Ok(())
}
```

### The site URL

KeyCDN requires every request to identify the site using it, in a `User-Agent`
header of the form `keycdn-tools:https://www.example.com`. The URL you pass to
`Session::new` is used for that header. Requests without a valid header are
dropped by KeyCDN without a response. You can change the URL later with
`Session::set_url`.

### Results

`location_lookup` returns a `LocationLookupResult`:

| Field                                 | Type             | Example              |
| ------------------------------------- | ---------------- | -------------------- |
| `host`                                | `String`         | `"www.example.com"`  |
| `ip`                                  | `IpAddr`         | `93.184.216.34`      |
| `asn`                                 | `Option<u32>`    | `15133`              |
| `isp`                                 | `Option<String>` | `"Verizon Business"` |
| `country_name` / `country_code`       | `Option<String>` | `"United States"` / `"US"` |
| `region_name` / `region_code`         | `Option<String>` | `"Massachusetts"` / `"MA"` |
| `city`                                | `Option<String>` | `"Norwell"`          |
| `postal_code`                         | `Option<String>` | `"02061"`            |
| `continent_name` / `continent_code`   | `Option<String>` | `"North America"` / `"NA"` |
| `latitude` / `longitude`              | `Option<f64>`    | `42.1596` / `-70.8217` |
| `metro_code`                          | `Option<u32>`    | `506`                |
| `timezone`                            | `Option<String>` | `"America/New_York"` |
| `datetime`                            | `Option<String>` | `"2019-06-01 00:00:00"` |

KeyCDN does not document which fields can be `null`, so every field apart from
`host` and `ip` is an `Option`. The data comes from MaxMind GeoLite, so many
addresses will have no city or region.

The struct implements `serde::Serialize` and `Deserialize`. It deserializes
from KeyCDN's `snake_case` field names. It serializes to `camelCase` and leaves
out `None` fields.

## Rate limiting

KeyCDN allows 3 requests per second. If you go over the limit, KeyCDN does
not send an HTTP 429. It closes the connection, and that shows up as
`Error::Http`, which looks the same as a network failure.

To avoid this, each `Session` has its own rate limiter. By default it allows
**one request every 350 ms**, counted from when the previous request
*finished*. The doc comment on `RateLimit::KEYCDN` explains how this value was
chosen.

All requests made through one session share its limiter. To make concurrent
lookups, share a single session (by reference or in an `Arc`) instead of
creating several:

```rust
use std::sync::Arc;

let session = Arc::new(Session::new("https://www.example.com")?);

let handles: Vec<_> = ["1.1.1.1", "9.9.9.9", "example.com"]
    .into_iter()
    .map(|host| {
        let session = Arc::clone(&session);
        tokio::spawn(async move { session.location_lookup(host).await })
    })
    .collect();

for handle in handles {
    match handle.await.expect("task panicked") {
        Ok(location) => println!("{}: {:?}", location.host, location.city),
        Err(err) => eprintln!("lookup failed: {err}"),
    }
}
```

To use a different limit, pass a `RateLimit`:

```rust
use std::num::NonZeroU32;
use keycdn_api::models::rate_limit::RateLimit;

let session = Session::new_with_rate_limit(
    "https://www.example.com",
    RateLimit::new_per_minute(NonZeroU32::new(60).unwrap()),
)?;
```

`RateLimit::new(max_requests, duration)` and `RateLimit::new_per_hour` are also
available.

## Errors

Every function returns `keycdn_api::Result<T>`. The error type is
`keycdn_api::Error`:

| Variant              | Cause                                                                  |
| -------------------- | ---------------------------------------------------------------------- |
| `InvalidSiteId`      | The site URL could not be parsed, or (in `set_url`) has no host or is not HTTP(S). |
| `InvalidUrl`         | An endpoint URL could not be built.                                    |
| `InvalidHeaderValue` | The site URL cannot be used in a `User-Agent` header.                  |
| `InvalidRequest`     | A body was given for a `GET` or `HEAD` request.                        |
| `Http`               | Connection error or timeout. This includes going over the rate limit.  |
| `UnexpectedResponse` | KeyCDN responded with a non-2xx status.                                |
| `Json`               | The response body was not the expected JSON.                           |
| `Api`                | KeyCDN reported an error, e.g. `"Hostname or IP is not valid."`        |
| `MissingData`        | KeyCDN reported success but returned no data.                          |

`Error` is `#[non_exhaustive]`, so `match` statements on it need a wildcard
arm.

## Development

```sh
cargo test                                # unit tests only
cargo test --features test-integration    # also call the live KeyCDN API
```

The rate limiter tests run on Tokio's paused clock, so they finish instantly
and give the same result every run. Set `RUST_LOG` to adjust the trace output
from tests.

CI runs `cargo fmt --check`, `cargo clippy -D warnings` and `cargo test` with
`--all-features`.

## AI/LLM Involvement Declaration

This crate had been designed and coded by hand, except:

- model definitions
    - `LocationLookup*` structs, and
    - `Error` model

These were created by LLM to save time. Manual adjustments were made to both.

LLM was used to review and check the code, and applied some fixes to `rate_limit.rs`.

This `README.md` was written by LLM, except this section.