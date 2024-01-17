use bytes::Bytes;
use dashmap::DashMap;
use mini_redis::{Connection, Frame};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};

// Using std::sync::Mutex or tokio::sync::Mutex ?
// std::sync::Mutex will block current thread when it is trying to
//  acquire a lock which is already hold by other threads.
// tokio::sync::Mutex will not block current thread, instead it will
//  yield the execution to other tasks.
// Using block in async/await situation should be avoided if the lock
//  is hold across await.
// Another concern is tokio::sync::Mutex has larger performance overhead

// Using bytes as it is arc wrapped immutable data stored in heap,
// when calling clone, it will only increase the reference counting
// It is more effecient to use in network sync situation
// type Db = Arc<Mutex<HashMap<String, Bytes>>>;

// // Sharding for fine-grained lock
// type ShardDb = Arc<Vec<Mutex<HashMap<String, Vec<u8>>>>>;
// fn new_shard_db(shard_count: usize) -> ShardDb {
//     let mut shards = Vec::with_capacity(shard_count);
//     for _ in 0..shard_count {
//         shards.push(Mutex::new(HashMap::new()));
//     }
//     Arc::new(shards)
// }

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    // let db = Arc::new(Mutex::new(HashMap::new()));
    let shard_db = Arc::new(DashMap::with_shard_amount(32));
    println!("create sharding db");

    loop {
        println!("listening...");
        let (socket, _) = listener.accept().await.unwrap();
        let db = shard_db.clone(); // Only increase the reference counting
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}

async fn process(socket: TcpStream, db: Arc<DashMap<String, Bytes>>) {
    use mini_redis::Command::{self, Get, Set};

    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                db.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                if let Some(value) = db.get(cmd.key()) {
                    Frame::Bulk(value.clone().into())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("unimplemented {:?}", cmd),
        };

        connection.write_frame(&response).await.unwrap();
    }
}
