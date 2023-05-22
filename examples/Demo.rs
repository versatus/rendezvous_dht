use clap::Parser;
use rendezvous_dht::{Key, Node, NodeData};
use sha3::{Digest, Sha3_256};
use std::io;
use std::process::exit;
use std::thread;

//First Terminal Run
// cargo run  --example Demo -- --port 8080 --is-bootstrap --rendzevous-addr '127.0.0.1:9090
//Output
//Node Key is ED3B11D7B0EFF352ECEA93D1DA40E2B533BF13BD2F906B25E8675F06470A2225
//Please choose a command: (0 Get Value /1 RegisterNS/3 Register QuorumPeer)

//Second Terminal Run
//cargo run  --example Demo  -- --port 8081 --bootstrap-key ED3B11D7B0EFF352ECEA93D1DA40E2B533BF13BD2F906B25E8675F06470A2225 --rendzevous-addr '127.0.0.1:9091
//Please choose a command: (0 Get Value /1 RegisterNS/3 Register QuorumPeer)

//Third Terminal Run
// cargo run  --example Demo -- --port 8082 --bootstrap-key ED3B11D7B0EFF352ECEA93D1DA40E2B533BF13BD2F906B25E8675F06470A2225 --rendzevous-addr '127.0.0.1:9092
//Please choose a command: (0 Get Value /1 RegisterNS/3 Register QuorumPeer)

/// This is a simple program
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, short, action)]
    is_bootstrap: bool,

    #[arg(short, long)]
    bootstrap_key: Option<String>,

    #[arg(short, long)]
    port: i16,

    #[arg(short, long)]
    rendzevous_addr: String,
}
fn clone_into_array<A, T>(slice: &[T]) -> A
where
    A: Sized + Default + AsMut<[T]>,
    T: Clone,
{
    let mut a = Default::default();
    <A as AsMut<[T]>>::as_mut(&mut a).clone_from_slice(slice);
    a
}

fn get_key(key: &[u8]) -> Key {
    let mut hasher = Sha3_256::default();
    hasher.input(key);
    Key(clone_into_array(hasher.result().as_slice()))
}

fn main() {
    let args = Args::parse();
    let mut node = if args.is_bootstrap {
        let n = Node::new(
            "127.0.0.1",
            args.port.to_string().as_str(),
            None,
            args.rendzevous_addr.parse().unwrap(),
        );
        let k = n.node_data().id.0;
        println!("Key {:?}", hex::encode(k.to_vec()));
        println!("Node Key is {:?}", n.node_data().id);
        n
    } else {
        let data = hex::decode(&args.bootstrap_key.unwrap()).unwrap();
        println!("Key is {:?}", data);
        let key: rendezvous_dht::Key = Key::try_from(data).unwrap();
        let node_data = NodeData::new(
            String::from("127.0.0.1"),
            args.port.to_string(),
            format!("{}:{}", "127.0.0.1", "8080".to_string()),
            key,
        );
        Node::new(
            "127.0.0.1",
            args.port.to_string().as_str(),
            Some(node_data),
            args.rendzevous_addr.parse().unwrap(),
        )
    };

    let c = thread::spawn(move || loop {
        println!("Please choose a command: (0 Get Namespace /1 RegisterNS/2 Register Peer/3 Fetch QuorumPeer)");

        let mut command = String::new();
        io::stdin()
            .read_line(&mut command)
            .expect("Failed to read line");

        let command = command.trim();
        if command.starts_with("0") {
            let data: Vec<String> = command
                .split_whitespace()
                .into_iter()
                .map(|x| x.to_string())
                .collect();
            println!("{:?}", data);
            let key = get_key(data.get(1).unwrap().as_bytes());
            let value = node.get_namespaces(&key);
            if let Some(val) = value {
                for v in val.iter() {
                    println!("Value for GET {:?} : {:?}", key, v);
                }
            }
        } else if command.starts_with("3") {
            let data: Vec<String> = command
                .split_whitespace()
                .into_iter()
                .map(|x| x.to_string())
                .collect();
            let key = get_key(&hex::decode(data.get(1).unwrap()).unwrap());
            let value = node.get(&key, vec![]);
            if let Some((val, filter)) = value {
                println!("Filter fetched :{:?}", val);

                for v in val.iter() {
                    println!("Value for GET {:?} : {:?}", key, v);
                }
            }
        } else if command.starts_with("1") {
            println!("Performing Put Key operation");
            let data: Vec<String> = command
                .split_whitespace()
                .into_iter()
                .map(|x| x.to_string())
                .collect();
            let key = get_key(data.get(1).unwrap().as_bytes());
            let _ = node.register_namespace(key, hex::decode(data.get(2).unwrap()).unwrap());
        } else if command.starts_with("2") {
            println!("Performing Put Key operation");
            let data: Vec<String> = command
                .split_whitespace()
                .into_iter()
                .map(|x| x.to_string())
                .collect();
            println!("Data {:?},{:?}", data.len(), data);
            println!("Raptor Port :{:?}", data.get(7).unwrap().clone());
            println!("Quic Port :{:?}", data.get(8).unwrap().clone());
            println!("Node Type  :{:?}", data.get(9).unwrap().clone());

            let quorum_key = hex::decode(data.get(1).unwrap().clone()).unwrap();
            let name_space_type = data.get(2).unwrap().clone().as_bytes().to_vec();
            let pk_share = hex::decode(data.get(3).unwrap().clone()).unwrap();
            let sig_share = hex::decode(data.get(4).unwrap().clone()).unwrap();
            let payload = data.get(5).unwrap().clone().as_bytes().to_vec();
            let address = data.get(6).unwrap().clone();
            let raptor_port = data.get(7).unwrap().parse::<u16>().unwrap();
            let quic_port = data.get(8).unwrap().parse::<u16>().unwrap();
            let node_type = data.get(9).unwrap().clone();

            println!("Quorum Key :{:?}", String::from_utf8_lossy(&quorum_key));
            println!(
                "Name Space Type  :{:?}",
                String::from_utf8_lossy(&name_space_type)
            );
            println!("PK Share  :{:?}", String::from_utf8_lossy(&pk_share));
            println!("Sig Share  :{:?}", String::from_utf8_lossy(&sig_share));
            println!("Payload  :{:?}", String::from_utf8_lossy(&payload));
            println!("Address  :{:?}", &address);
            let status = node.register_peer(
                get_key(&hex::decode(data.get(1).unwrap()).unwrap()),
                quorum_key,
                name_space_type,
                pk_share,
                sig_share,
                payload,
                address,
                raptor_port,
                quic_port,
                node_type,
            );
            println!("Status {:?}", status);
        } else if command == "Print" {
            println!("Performing Print operation");
            let c = node
                .routing_table
                .lock()
                .unwrap()
                .get_closest_nodes(&node.node_data().id, 3);
            println!("Neighbours of node {:?}", c);
        } else if command == "exit" {
            exit(1)
        } else {
            println!("Error: unknown command");
        }
    });
    let _ = c.join();
}
