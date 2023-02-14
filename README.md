  
# Rendezvous DHT  
## Overview
  

The Rendezvous DHT (distributed hash table) protocol based on threshold cryptography is a protocol for creating a secure, decentralized network in which nodes can communicate and share information of the Quorum Peers without relying on a centralized authority.  

The basic idea behind Rendezvous DHT is to use a distributed hash table to store information about the network and to use DKG Key Shares to ensure that this information is secure and cannot be tampered with by malicious actors (Hence Malicious actors cannot participate as peers)

The protocol works as follows:

-  Nodes of Quorum go through DKG process, register Quorum Key as Namespace
    
- Each node then goes through Peer Registration by sending a Signature (signing an arbitrary payload using its secret key share), Its identity and public Key share
    
- The rendezvous uses a distributed hash table (DHT) to store information about the Quorum Peers, When it receives registration it validates and verifies the Signature, Only after successful verification, its register the peer into its NameSpace in DHT
    
- Any node willing to have information about Quorum, connect to the Rendezvous node and gets Quorum Peers with the Quorum Key namespace
    
- Rendezevous Node will ping for all quorum peers for liveness

 

Threshold cryptography is used to ensure the security of the network. In threshold cryptography, multiple parties must work together to generate a key, and no individual party can access the key on its own. This means that even if some nodes in the network are compromised, the network as a whole remains secure. Here Quorum DHT will be secured

Overall, the Rendezvous DHT protocol based on threshold cryptography provides a secure and decentralized way for nodes to communicate and share information about Quorum Peers in a network.  

## 

1. Clone Repo run.
	For Single Rendezvous Node

```
	RUST_FLAGS=-Awarnings  cargo  run  --example  Demo  --  --port  8080  --is-bootstrap  --rendzevous-addr  127.0.0.1:9090
```

For Multiple Rendezvous Node

```
RUST_FLAGS=-Awarnings  cargo  run  --example  Demo  --  --port  8080  --is-bootstrap  --rendzevous-addr  127.0.0.1:9090
 
RUST_FLAGS=-Awarnings cargo  run  --example  Demo  --  --port  8081  --bootstrap-key  cf21ff99b6cc25d981354c8efe5fc85ac5f7feee944badca76a050732ed89956  
--rendzevous-addr  127.0.0.1:9091

RUST_FLAGS=-Awarnings cargo  run  --example  Demo  --  --port  8082  --bootstrap-key  cf21ff99b6cc25d981354c8efe5fc85ac5f7feee944badca76a050732ed89956    
--rendzevous-addr  127.0.0.1:9092
```

2. To Test: 

	Run 
	
	1. For AutoTest
	```
	  cargo  run  --example  test
	```
	  
	2. For Manual Testing

	```
	  cargo  run  --example  Demo
	```
