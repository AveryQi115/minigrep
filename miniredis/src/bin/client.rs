use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use mini_redis::client;

// tokio provide multiple modes for producer/consumer model:
// mpsc         -- multiple producer, single consumer
// oneshot      -- single producer, single consumer
// broadcast    -- multiple producer, multiple consumer, one message will be consumed by all consumers
// watch        -- single producer, multiple consumer, only store latest message, commonly used in watching settings

// Apart from the above modes, there are also other crates:
// async-channel        -- multiple producer, multiple consumer, each message send to specific consumer
// crossbeam-channel    -- synchronous channel, sending block current threads

type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;

#[derive(Debug)]
enum Command {
    Get {
        key: String,
        resp: Responder<Option<Bytes>>,
    },
    Set {
        key: String,
        val: Bytes,
        resp: Responder<()>,
    },
}

#[tokio::main]
async fn main(){
    // create a channel with capacity 32
    let (tx, mut rx) = mpsc::channel(32);
    let tx2 = tx.clone();

    // manager task is the consumer
    let manager = tokio::spawn(async move {
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();
    
        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::Get { key, resp } => {
                    let res = client.get(&key).await;
                    let _ = resp.send(res);
                }
                Command::Set { key, val, resp } => {
                    let res = client.set(&key, val).await;
                    let _ = resp.send(res);
                }
            }
        }
    });

    let sender_task = tokio::spawn(async move{
        let (resp_tx, resp_rx) = oneshot::channel();

        tx2.send(Command::Set {
            key: "hello".to_string(),
            val: "world".into(),
            resp: resp_tx,
        }).await.unwrap();

        let res = resp_rx.await;
        println!("Set 'hello': 'world' Got={:?}", res);
    });

    let getter_task = tokio::spawn(async move{
        let (resp_tx, resp_rx) = oneshot::channel();

        tx.send(Command::Get {
            key: "hello".to_string(),
            resp: resp_tx,
        }).await.unwrap();

        let res = resp_rx.await;
        println!("Send 'hello', GOT = {:?}", res);
    });

    manager.await.unwrap();
    sender_task.await.unwrap();
    getter_task.await.unwrap();
}
