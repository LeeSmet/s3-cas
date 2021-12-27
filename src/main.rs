use hyper::body::HttpBody;
use s3_cas::{cas::CasFS, passthrough::Passthrough};
use s3_server::S3Service;
use s3_server::SimpleAuth;

use std::convert::Infallible;
use std::net::TcpListener;
use std::path::PathBuf;

use anyhow::Result;
use futures::{future, join};
use hyper::server::Server;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response};
use prometheus::{Counter, Encoder, Opts, Registry, TextEncoder};
use structopt::StructOpt;
// use tracing::{debug, info};

#[derive(StructOpt)]
struct Args {
    #[structopt(long, default_value = ".")]
    fs_root: PathBuf,

    #[structopt(long, default_value = ".")]
    meta_root: PathBuf,

    #[structopt(long, default_value = "localhost")]
    host: String,

    #[structopt(long, default_value = "8014")]
    port: u16,

    #[structopt(long, default_value = "localhost")]
    metric_host: String,

    #[structopt(long, default_value = "9100")]
    metric_port: u16,

    #[structopt(long, requires("secret-key"), display_order = 1000)]
    access_key: Option<String>,

    #[structopt(long, requires("access-key"), display_order = 1000)]
    secret_key: Option<String>,
}

// pub fn setup_tracing() {
//     use tracing_error::ErrorLayer;
//     use tracing_subscriber::layer::SubscriberExt;
//     use tracing_subscriber::util::SubscriberInitExt;
//     use tracing_subscriber::{fmt, EnvFilter};
//
//     tracing_subscriber::fmt()
//         .event_format(fmt::format::Format::default().pretty())
//         .with_env_filter(EnvFilter::from_default_env())
//         .with_timer(fmt::time::ChronoLocal::rfc3339())
//         .finish()
//         .with(ErrorLayer::default())
//         .init();
// }

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    // setup_tracing();

    let args: Args = Args::from_args();

    // setup the storage
    // let fs = FileSystem::new(&args.fs_root)?;
    let metrics = s3_cas::metrics::SharedMetrics::new();
    let fs = CasFS::new(args.fs_root, args.meta_root, metrics.clone());
    let fs = s3_cas::metrics::MetricFs::new(fs, metrics);
    // debug!(?fs);

    // setup the service
    // let mut service = S3Service::new(Passthrough::new(fs));
    let mut service = S3Service::new(fs);

    if let (Some(access_key), Some(secret_key)) = (args.access_key, args.secret_key) {
        let mut auth = SimpleAuth::new();
        auth.register(access_key, secret_key);
        // debug!(?auth);
        service.set_auth(auth);
    }

    let server = {
        let service = service.into_shared();
        let listener = TcpListener::bind((args.host.as_str(), args.port))?;
        let make_service: _ =
            make_service_fn(move |_| future::ready(Ok::<_, anyhow::Error>(service.clone())));
        Server::from_tcp(listener)?.serve(make_service)
    };

    async fn serve_metrics(req: Request<Body>) -> Result<Response<Body>, Infallible> {
        let mut response = Response::new(Body::empty());
        match (req.method(), req.uri().path()) {
            (&hyper::Method::GET, "/metrics") => {
                let mut buffer = Vec::new();
                let encoder = TextEncoder::new();

                let metric_families = prometheus::gather();
                encoder.encode(&metric_families, &mut buffer).unwrap();
                *response.body_mut() = Body::from(buffer);
            }
            _ => *response.status_mut() = hyper::StatusCode::NOT_FOUND,
        }
        Ok(response)
    }

    let metric_server = {
        let listener = TcpListener::bind((args.metric_host.as_str(), args.metric_port))?;
        let make_service: _ = make_service_fn(move |_| {
            future::ready(Ok::<_, anyhow::Error>(service_fn(serve_metrics)))
        });
        Server::from_tcp(listener)?.serve(make_service)
    };

    //info!("server is running at http://{}:{}/", args.host, args.port);
    println!("server is running at http://{}:{}/", args.host, args.port);
    println!(
        "metric server is running at http://{}:{}",
        args.metric_host, args.metric_port
    );
    //server.await?;
    join!(metric_server, server);

    Ok(())
}
