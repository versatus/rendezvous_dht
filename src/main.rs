use clap::Parser;
use flexi_logger::{Age, Cleanup, Criterion, FileSpec, Logger, Naming};
use log::info;
use rendezvous_dht::{Key, Node, NodeData};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

///To start as bootstrap RDHT
///cargo run -- --is-bootstrap --port 8080 --rendzevous-addr 127.0.0.1:9090

///Non Bootstrap RDHT
///cargo run -- --is-bootstrap --port 8081 --rendzevous-addr 127.0.0.1:9090 --bootstrap-key 88858dcf8febb47deaa9717c739e1f0d6a1345b13d18b903244e6be2bd139a98

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, short, action)]
    is_bootstrap: bool,

    #[arg(short, long)]
    bootstrap_key: Option<String>,

    #[arg(short, long)]
    port: u16,

    #[arg(short, long)]
    rendzevous_addr: String,
}
fn main() {
    // Configure the logger with a log file and rotation settings
    Logger::try_with_str("info")
        .unwrap()
        .rotate(
            Criterion::AgeOrSize(Age::Day, 10_000_000), // Rotate logs when they reach 10 MB
            Naming::Timestamps,
            Cleanup::KeepLogFiles(7), // Keep the last 7 log files
        )
        .log_to_file(FileSpec::default().directory("logs").discriminant("app"))
        .log_to_stdout()
        .format(flexi_logger::colored_with_thread)
        .start()
        .unwrap();

    let ip_address = "127.0.0.1";

    // Parse the IP address string into an IpAddr
    let ip = ip_address.parse::<Ipv4Addr>().unwrap();
    let args = Args::parse();
    let socket_addr = SocketAddr::new(IpAddr::V4(ip), args.port as u16);
    if args.is_bootstrap {
        let n = Node::new(socket_addr, None, args.rendzevous_addr.parse().unwrap()).unwrap();
        let key = n.node_data().id.0;
        println!("Bootstrap Key :{:?}", hex::encode(key));
    } else {
        let data = hex::decode(&args.bootstrap_key.unwrap()).unwrap();
        let key: rendezvous_dht::Key = Key::try_from(data).unwrap();
        let socket_addr = SocketAddr::new(
            IpAddr::V4(ip_address.parse::<Ipv4Addr>().unwrap()),
            args.port as u16,
        );
        let node_data = NodeData::new(socket_addr, key);
        Node::new(
            SocketAddr::new(
                IpAddr::V4(ip_address.parse::<Ipv4Addr>().unwrap()),
                args.port as u16,
            ),
            Some(node_data),
            args.rendzevous_addr.parse().unwrap(),
        )
        .unwrap();
    }
    info!("Started Rendzevous Node");
    info!("Serving rendzevous request");
    loop {}
}
