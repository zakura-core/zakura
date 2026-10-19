//! A minimal HTTPS client for downloading the managed `zcashd` binary.
//!
//! Hyper handles HTTP/1.1 framing and rustls handles TLS. This module connects them, follows
//! redirects, and tunnels through the proxies configured in the environment, which it reads
//! with the same matcher `reqwest` uses.

use std::{net::IpAddr, sync::Arc};

use bytes::Bytes;
use eyre::{eyre, Report};
use http_body_util::Empty;
use hyper::{
    body::Incoming,
    client::conn::http1,
    header::{HOST, LOCATION, PROXY_AUTHORIZATION, USER_AGENT},
    Request, Response, Uri,
};
use hyper_util::{
    client::proxy::matcher::{Intercept, Matcher},
    rt::TokioIo,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tokio_rustls::{
    rustls::{pki_types::ServerName, ClientConfig},
    TlsConnector,
};
use url::{Position, Url};

/// The `User-Agent` of download requests.
const DOWNLOAD_USER_AGENT: &str = concat!("zakura/", env!("CARGO_PKG_VERSION"));

/// A connection to a server, which may be tunneled through a proxy and encrypted.
trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}

/// Sends a `GET` request for `url`, following up to `max_redirects` redirects to URLs that
/// `is_allowed` accepts, and returns the first response that isn't a redirect.
///
/// Requests go through the proxy that `proxies` selects for each URL.
pub(super) async fn get_following_redirects(
    url: Url,
    tls: Arc<ClientConfig>,
    proxies: &Matcher,
    max_redirects: usize,
    is_allowed: impl Fn(&Url) -> bool,
) -> Result<Response<Incoming>, Report> {
    let tls = TlsConnector::from(tls);
    let mut url = url;
    let mut redirects = 0;

    loop {
        let response = get(&url, &tls, proxies).await?;
        if !response.status().is_redirection() {
            return Ok(response);
        }

        let location = response
            .headers()
            .get(LOCATION)
            .and_then(|location| location.to_str().ok())
            .ok_or_else(|| {
                eyre!(
                    "redirect from {url} with status {} has no valid Location header",
                    response.status()
                )
            })?;
        let next = url
            .join(location)
            .map_err(|err| eyre!("invalid redirect location '{location}' from {url}: {err}"))?;

        redirects += 1;
        if redirects > max_redirects {
            return Err(eyre!("more than {max_redirects} redirects"));
        }
        if !is_allowed(&next) {
            return Err(eyre!("redirected to non-https URL {next}"));
        }
        url = next;
    }
}

/// Sends a `GET` request for `url` on a new connection.
async fn get(
    url: &Url,
    tls: &TlsConnector,
    proxies: &Matcher,
) -> Result<Response<Incoming>, Report> {
    let host = url
        .host_str()
        .ok_or_else(|| eyre!("URL {url} has no host"))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| eyre!("URL {url} has no port"))?;
    // The host and port, with IPv6 addresses in brackets.
    let authority = &url[Position::BeforeHost..Position::AfterPort];

    let uri: Uri = url
        .as_str()
        .parse()
        .map_err(|err| eyre!("invalid URL {url}: {err}"))?;
    let stream = match proxies.intercept(&uri) {
        Some(proxy) => connect_through_proxy(&proxy, authority, tls).await?,
        None => Box::new(connect(host, port).await?) as Box<dyn Stream>,
    };
    let stream = match url.scheme() {
        "https" => Box::new(
            tls.connect(server_name(host)?, stream)
                .await
                .map_err(|err| eyre!("TLS handshake with {host} failed: {err}"))?,
        ) as Box<dyn Stream>,
        _ => stream,
    };

    let mut sender = handshake(stream, false).await?;
    let request = Request::get(&url[Position::BeforePath..Position::AfterQuery])
        .header(HOST, authority)
        .header(USER_AGENT, DOWNLOAD_USER_AGENT)
        .body(Empty::<Bytes>::new())
        .map_err(|err| eyre!("invalid request for {url}: {err}"))?;
    sender
        .send_request(request)
        .await
        .map_err(|err| eyre!("request to {url} failed: {err}"))
}

/// Opens a tunnel to `authority` through `proxy` with an HTTP `CONNECT` request.
async fn connect_through_proxy(
    proxy: &Intercept,
    authority: &str,
    tls: &TlsConnector,
) -> Result<Box<dyn Stream>, Report> {
    let proxy_uri = proxy.uri();
    let proxy_host = proxy_uri
        .host()
        .ok_or_else(|| eyre!("proxy URL {proxy_uri} has no host"))?;
    let stream: Box<dyn Stream> = match proxy_uri.scheme_str() {
        Some("http") => Box::new(connect(proxy_host, proxy_uri.port_u16().unwrap_or(80)).await?),
        Some("https") => {
            let stream = connect(proxy_host, proxy_uri.port_u16().unwrap_or(443)).await?;
            Box::new(
                tls.connect(server_name(proxy_host)?, stream)
                    .await
                    .map_err(|err| eyre!("TLS handshake with proxy {proxy_host} failed: {err}"))?,
            )
        }
        _ => {
            return Err(eyre!(
                "unsupported proxy URL {proxy_uri}: use an http or https proxy"
            ))
        }
    };

    let mut sender = handshake(stream, true).await?;
    let mut request = Request::connect(authority)
        .header(HOST, authority)
        .header(USER_AGENT, DOWNLOAD_USER_AGENT)
        .body(Empty::<Bytes>::new())
        .map_err(|err| eyre!("invalid proxy request for {authority}: {err}"))?;
    if let Some(auth) = proxy.basic_auth() {
        request
            .headers_mut()
            .insert(PROXY_AUTHORIZATION, auth.clone());
    }

    let response = sender
        .send_request(request)
        .await
        .map_err(|err| eyre!("proxy request to {proxy_host} failed: {err}"))?;
    if !response.status().is_success() {
        return Err(eyre!(
            "proxy {proxy_host} refused to connect to {authority} with HTTP status {}",
            response.status()
        ));
    }
    let tunnel = hyper::upgrade::on(response)
        .await
        .map_err(|err| eyre!("proxy {proxy_host} tunnel to {authority} failed: {err}"))?;
    Ok(Box::new(TokioIo::new(tunnel)))
}

/// Opens a TCP connection to `host` and `port`.
async fn connect(host: &str, port: u16) -> Result<TcpStream, Report> {
    // URLs put IPv6 addresses in brackets, which address resolution doesn't accept.
    let host = host.trim_start_matches('[').trim_end_matches(']');
    TcpStream::connect((host, port))
        .await
        .map_err(|err| eyre!("failed to connect to {host}:{port}: {err}"))
}

/// Starts an HTTP/1.1 connection on `stream`, and drives it in a new task.
///
/// The task ends when the connection closes, or when the request sender and the response
/// body are dropped. Connections that tunnel to a server need `with_upgrades`.
async fn handshake(
    stream: Box<dyn Stream>,
    with_upgrades: bool,
) -> Result<http1::SendRequest<Empty<Bytes>>, Report> {
    let (sender, connection) = http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|err| eyre!("HTTP handshake failed: {err}"))?;
    if with_upgrades {
        tokio::spawn(connection.with_upgrades());
    } else {
        tokio::spawn(connection);
    }
    Ok(sender)
}

/// Returns the TLS server name for `host`, which may be an IP address.
fn server_name(host: &str) -> Result<ServerName<'static>, Report> {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    match host.parse::<IpAddr>() {
        Ok(ip) => Ok(ServerName::IpAddress(ip.into())),
        Err(_) => ServerName::try_from(host.to_owned())
            .map_err(|err| eyre!("invalid TLS server name {host}: {err}")),
    }
}
