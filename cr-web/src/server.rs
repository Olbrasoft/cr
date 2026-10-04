//! Connection deadlines belong at the HTTP layer, not around whole requests:
//! downloads and slow handlers must be allowed to outlive the keep-alive timeout.

use std::{future::Future, sync::Arc, time::Duration};

use axum::{Router, serve::Listener};
use hyper_util::{
    rt::{TokioExecutor, TokioIo, TokioTimer},
    server::{conn::auto::Builder, graceful::GracefulShutdown},
    service::TowerToHyperService,
};
use tokio::{net::TcpListener, sync::Notify};
use tower::ServiceExt;

const HEADER_TIMEOUT: Duration = Duration::from_secs(60);

pub async fn serve(listener: TcpListener, app: Router, shutdown: impl Future<Output = ()>) {
    serve_with_timeout(listener, app, shutdown, HEADER_TIMEOUT).await;
}

async fn serve_with_timeout(
    mut listener: TcpListener,
    app: Router,
    shutdown: impl Future<Output = ()>,
    header_timeout: Duration,
) {
    let graceful = GracefulShutdown::new();
    tokio::pin!(shutdown);

    loop {
        let (stream, peer) = tokio::select! {
            biased;
            _ = &mut shutdown => break,
            accepted = Listener::accept(&mut listener) => accepted,
        };
        let app = app.clone();
        // Subscribe before spawning so shutdown cannot miss a newly accepted connection.
        let watcher = graceful.watcher();
        tokio::spawn(async move {
            let first_request = Arc::new(Notify::new());
            let received = first_request.clone();
            let service = tower::service_fn(move |request| {
                received.notify_one();
                app.clone().oneshot(request)
            });
            let mut builder = Builder::new(TokioExecutor::new());
            builder
                .http1()
                .timer(TokioTimer::new())
                .header_read_timeout(header_timeout);
            builder
                .http2()
                .timer(TokioTimer::new())
                .keep_alive_interval(header_timeout)
                .keep_alive_timeout(Duration::from_secs(20));
            let connection = watcher.watch(builder.serve_connection_with_upgrades(
                TokioIo::new(stream),
                TowerToHyperService::new(service),
            ));
            tokio::pin!(connection);

            // Auto protocol detection happens before Hyper's HTTP/1 header timer.
            // Bound that initial wait as well, but stop timing once a request is
            // dispatched: a handler or response body may legitimately take longer.
            let result = tokio::select! {
                result = &mut connection => result,
                _ = first_request.notified() => connection.await,
                _ = tokio::time::sleep(header_timeout) => {
                    tracing::debug!(%peer, "closing connection before first request: header timeout");
                    return;
                }
            };
            if let Err(error) = result {
                tracing::debug!(%peer, %error, "HTTP connection closed");
            }
        });
    }

    drop(listener);
    graceful.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, routing::get};
    use std::{convert::Infallible, net::SocketAddr};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        sync::oneshot,
        task::JoinHandle,
        time::{sleep, timeout},
    };

    const DEADLINE: Duration = Duration::from_millis(250);
    const TEST_TIMEOUT: Duration = Duration::from_secs(5);
    const REQUEST: &[u8] = b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n";

    struct Server {
        addr: SocketAddr,
        stop: Option<oneshot::Sender<()>>,
        task: JoinHandle<()>,
    }

    impl Server {
        async fn start(app: Router) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (tx, rx) = oneshot::channel();
            let task = tokio::spawn(serve_with_timeout(
                listener,
                app,
                async {
                    let _ = rx.await;
                },
                DEADLINE,
            ));
            Self {
                addr,
                stop: Some(tx),
                task,
            }
        }

        async fn stop(mut self) {
            self.stop.take().unwrap().send(()).unwrap();
            timeout(TEST_TIMEOUT, &mut self.task)
                .await
                .unwrap()
                .unwrap();
        }
    }

    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn app() -> Router {
        Router::new().route("/", get(|| async { "OK" }))
    }

    async fn response(stream: &mut TcpStream) -> Vec<u8> {
        // All non-streaming test responses have a two-byte body. Read exactly
        // one response so a second request proves that reuse remains supported.
        timeout(TEST_TIMEOUT, async {
            let mut data = Vec::new();
            loop {
                let byte = stream.read_u8().await.unwrap();
                data.push(byte);
                if data.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let mut body = [0; 2];
            stream.read_exact(&mut body).await.unwrap();
            assert_eq!(&body, b"OK");
            assert!(data.starts_with(b"HTTP/1.1 200"));
            data
        })
        .await
        .unwrap()
    }

    async fn assert_closed(stream: &mut TcpStream) {
        let mut data = Vec::new();
        timeout(TEST_TIMEOUT, stream.read_to_end(&mut data))
            .await
            .unwrap()
            .unwrap();
        // Hyper may write a 408 for an incomplete request before closing.
        assert!(data.is_empty() || data.starts_with(b"HTTP/1.1 408"));
    }

    #[tokio::test]
    async fn closes_connection_without_any_request() {
        let server = Server::start(app()).await;
        let mut stream = TcpStream::connect(server.addr).await.unwrap();
        assert_closed(&mut stream).await;
        server.stop().await;
    }

    #[tokio::test]
    async fn closes_incomplete_initial_headers() {
        let server = Server::start(app()).await;
        let mut stream = TcpStream::connect(server.addr).await.unwrap();
        stream.write_all(b"GET / HTTP/1.1\r\nHost:").await.unwrap();
        assert_closed(&mut stream).await;
        server.stop().await;
    }

    #[tokio::test]
    async fn expires_idle_keep_alive_after_response() {
        let server = Server::start(app()).await;
        let mut stream = TcpStream::connect(server.addr).await.unwrap();
        stream.write_all(REQUEST).await.unwrap();
        response(&mut stream).await;
        assert_closed(&mut stream).await;
        server.stop().await;
    }

    #[tokio::test]
    async fn expires_incomplete_headers_on_reused_connection() {
        let server = Server::start(app()).await;
        let mut stream = TcpStream::connect(server.addr).await.unwrap();
        stream.write_all(REQUEST).await.unwrap();
        response(&mut stream).await;
        stream.write_all(b"GET / HTTP/1.1\r\nHost:").await.unwrap();
        assert_closed(&mut stream).await;
        server.stop().await;
    }

    #[tokio::test]
    async fn allows_reuse_before_deadline() {
        let server = Server::start(app()).await;
        let mut stream = TcpStream::connect(server.addr).await.unwrap();
        for _ in 0..3 {
            stream.write_all(REQUEST).await.unwrap();
            response(&mut stream).await;
        }
        server.stop().await;
        assert_closed(&mut stream).await;
    }

    #[tokio::test]
    async fn slow_handler_can_outlive_header_deadline() {
        let server = Server::start(Router::new().route(
            "/",
            get(|| async {
                sleep(DEADLINE * 3).await;
                "OK"
            }),
        ))
        .await;
        let mut stream = TcpStream::connect(server.addr).await.unwrap();
        stream.write_all(REQUEST).await.unwrap();
        response(&mut stream).await;
        server.stop().await;
    }

    #[tokio::test]
    async fn streaming_response_can_outlive_header_deadline() {
        let server = Server::start(Router::new().route(
            "/",
            get(|| async {
                Body::from_stream(futures_util::stream::unfold(0, |chunk| async move {
                    if chunk == 4 {
                        return None;
                    }
                    sleep(DEADLINE).await;
                    Some((
                        Ok::<_, Infallible>(if chunk == 3 { "done" } else { "data" }),
                        chunk + 1,
                    ))
                }))
            }),
        ))
        .await;
        let mut stream = TcpStream::connect(server.addr).await.unwrap();
        stream.write_all(REQUEST).await.unwrap();
        let mut data = Vec::new();
        timeout(TEST_TIMEOUT, stream.read_to_end(&mut data))
            .await
            .unwrap()
            .unwrap();
        assert!(data.starts_with(b"HTTP/1.1 200"));
        assert!(data.ends_with(b"0\r\n\r\n"));
        assert!(data.windows(4).any(|w| w == b"done"));
        server.stop().await;
    }

    #[tokio::test]
    async fn http2_remains_usable_after_header_deadline() {
        let server = Server::start(app()).await;
        let client = reqwest::Client::builder()
            .http2_prior_knowledge()
            .timeout(TEST_TIMEOUT)
            .build()
            .unwrap();
        for _ in 0..2 {
            let response = client
                .get(format!("http://{}/", server.addr))
                .send()
                .await
                .unwrap();
            assert_eq!(response.version(), reqwest::Version::HTTP_2);
            assert_eq!(response.text().await.unwrap(), "OK");
            sleep(DEADLINE * 3).await;
        }
        drop(client);
        server.stop().await;
    }

    #[tokio::test]
    async fn reclaims_a_batch_of_idle_connections() {
        let server = Server::start(app()).await;
        let mut clients = Vec::new();
        for _ in 0..32 {
            let mut stream = TcpStream::connect(server.addr).await.unwrap();
            stream.write_all(REQUEST).await.unwrap();
            response(&mut stream).await;
            clients.push(stream);
        }
        for stream in &mut clients {
            assert_closed(stream).await;
        }
        // Reclaimed connections must not prevent new requests from succeeding.
        let mut stream = TcpStream::connect(server.addr).await.unwrap();
        stream.write_all(REQUEST).await.unwrap();
        response(&mut stream).await;
        server.stop().await;
    }

    #[tokio::test]
    async fn graceful_shutdown_closes_connection_before_first_request() {
        let server = Server::start(app()).await;
        let mut stream = TcpStream::connect(server.addr).await.unwrap();
        // Ensure the connection is accepted and awaiting protocol detection.
        stream.write_all(b"G").await.unwrap();
        sleep(Duration::from_millis(20)).await;
        server.stop().await;
        assert_closed(&mut stream).await;
    }

    #[tokio::test]
    async fn graceful_shutdown_drains_active_request() {
        let entered = Arc::new(Notify::new());
        let signal = entered.clone();
        let mut server = Server::start(Router::new().route(
            "/",
            get(move || {
                let signal = signal.clone();
                async move {
                    signal.notify_one();
                    sleep(DEADLINE * 3).await;
                    "OK"
                }
            }),
        ))
        .await;
        let mut stream = TcpStream::connect(server.addr).await.unwrap();
        stream.write_all(REQUEST).await.unwrap();
        timeout(TEST_TIMEOUT, entered.notified()).await.unwrap();
        server.stop.take().unwrap().send(()).unwrap();
        response(&mut stream).await;
        assert_closed(&mut stream).await;
        timeout(TEST_TIMEOUT, &mut server.task)
            .await
            .unwrap()
            .unwrap();
    }
}
