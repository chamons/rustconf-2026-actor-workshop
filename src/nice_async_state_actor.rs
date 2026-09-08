use std::sync::Arc;

use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

enum ExampleCommand {
    Increment,
    Retrieve { reply: oneshot::Sender<u32> },
}

#[derive(Clone)]
struct Counter {
    tx: Sender<ExampleCommand>,
    handle: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
}

impl Counter {
    pub async fn new() -> Counter {
        let (tx, handle) = spawn_actor().await;
        Self {
            tx,
            handle: Arc::new(std::sync::Mutex::new(Some(handle))),
        }
    }

    pub async fn increment(&self) -> eyre::Result<()> {
        self.tx.send(ExampleCommand::Increment).await?;
        Ok(())
    }

    pub async fn read(&self) -> eyre::Result<u32> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(ExampleCommand::Retrieve { reply: tx }).await;
        Ok(rx.await?)
    }

    pub async fn shutdown(self) -> eyre::Result<()> {
        let Counter { tx, handle } = self;
        drop(tx);
        if let Some(handle) = handle.lock().expect("poisoned").take() {
            handle.await?;
        }
        Ok(())
    }
}

async fn spawn_actor() -> (Sender<ExampleCommand>, JoinHandle<()>) {
    let (tx, rx) = channel(64);
    let handle = tokio::task::spawn(async move { run_actor(rx).await });
    (tx, handle)
}

async fn run_actor(mut rx: Receiver<ExampleCommand>) {
    let mut counter = 0;
    while let Some(msg) = rx.recv().await {
        match msg {
            ExampleCommand::Increment => counter += 1,
            ExampleCommand::Retrieve { reply } => {
                let _ = reply.send(counter);
            }
        }
    }
}

async fn read(actor: &Sender<ExampleCommand>) -> eyre::Result<u32> {
    let (tx, rx) = oneshot::channel();
    let _ = actor.send(ExampleCommand::Retrieve { reply: tx }).await;
    Ok(rx.await?)
}

pub async fn run() -> eyre::Result<()> {
    let counter = Counter::new().await;
    counter.increment().await?;
    counter.increment().await?;
    let value = counter.read().await?;
    println!("{value}");
    counter.shutdown().await?;

    Ok(())
}
