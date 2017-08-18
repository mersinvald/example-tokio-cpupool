extern crate rand;
extern crate futures;
extern crate futures_cpupool;
extern crate tokio_core;

use std::time::Duration;
use std::time::Instant;
use std::thread;
use std::thread::sleep;
use std::collections::HashMap;

use futures::sync::mpsc as future_mpsc;
use std::sync::mpsc;

use futures::sink::Sink;
use futures::future;
use futures::stream::Stream;
use futures_cpupool::CpuPool;
use futures::future::Future;
use rand::Rng;

// Using custom non-copy structure to take all the slaps from borrowck
#[derive(PartialEq, Eq)]
struct Data {
    id: i32,
    data: u64
}

fn server(rx: future_mpsc::Receiver<Data>, mut clients: HashMap<i32, mpsc::Sender<Data>>) {
    let mut core = tokio_core::reactor::Core::new().unwrap();
    let cpupool = CpuPool::new(4);
    let handle = core.handle();

    let server_future = rx.for_each(|request| {
        println!("Server: received {} from client {}", request.data, request.id);

        let server_tx = clients.remove(&request.id).unwrap();

        // Spawn the hard computing task onto a cpupool
        let request_handler = cpupool.spawn_fn(|| compute_task(request))
            // And sent the result to the client as computation finishes
            .and_then(move |data| server_tx.send(data).map_err(|_|()));
        
        // Spawn computing task onto a reactor
        // and proceed to the next request in the main loop
        handle.spawn(request_handler);
        
        Ok(())
    });

    core.run(server_future).unwrap();
}

// Some hard blocking computing task
fn compute_task(request: Data) -> future::FutureResult<Data, ()> {
    println!("Spawned task {} from client {} on the thread pool", request.data, request.id);
    sleep(Duration::from_secs(request.data));
    future::ok(request)
}

fn client(id: i32, tx: future_mpsc::Sender<Data>, rx: mpsc::Receiver<Data>) {
    let mut rand = rand::thread_rng();
     
    // Sleep before sending a request
    sleep(Duration::from_secs(rand.gen_range(0, 5)));
    
    // Send request
    let request_data = rand.gen_range(1, 10); 
    let request = Data {
        id: id, 
        data: request_data
    };

    tx.send(request).wait().unwrap();
    println!("Client {}: sent {:?}", id, request_data);

    // Wait for response
    let (response, response_time) = stopwatch(|| rx.recv().unwrap());
    println!("Client {}: received {:?} in {} seconds", id, response.data, response_time.as_secs());

    // Assert correctness of responce and absence of delay
    assert_eq!(request_data, response.data);
    assert_eq!(request_data, response_time.as_secs());
}

fn main() {
    // Using mpsc::chanel to imitate a socket
    let (client_tx, server_rx) = future_mpsc::channel(4);

    // Spawn some client threads and construct hashmap id -> server_to_client_tx
    let clients : HashMap<_, _> = (0..4).into_iter().map(|id| {
        // Create "duplex" connection
        let (server_tx, client_rx) = mpsc::channel();
        let client_tx = client_tx.clone();

        thread::spawn(move || client(id, client_tx, client_rx));
        
        (id, server_tx)
    }).collect();

    // Launch server
    server(server_rx, clients);
}

fn stopwatch<F: Fn() -> Data>(closure: F) -> (Data, Duration) {
    let start = Instant::now();
    let data = closure();
    let elapsed = start.elapsed();
    (data, elapsed)
}