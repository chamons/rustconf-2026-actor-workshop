use tokio::sync::mpsc::{Sender, channel};
use tokio::sync::oneshot;

enum ExampleCommand {
    Increment,
    Retrieve { reply: oneshot::Sender<u32> },
}

async fn spawn_actor() -> Sender<ExampleCommand> {
    let (tx, mut rx) = channel(64);
    tokio::task::spawn(async move {
        let mut counter = 0;
        while let Some(msg) = rx.recv().await {
            match msg {
                ExampleCommand::Increment => counter += 1,
                ExampleCommand::Retrieve { reply } => {
                    let _ = reply.send(counter);
                }
            }
        }
        println!("Closing actor");
    });
    tx
}

async fn read(actor: &Sender<ExampleCommand>) -> eyre::Result<u32> {
    let (tx, rx) = oneshot::channel();
    let _ = actor.send(ExampleCommand::Retrieve { reply: tx }).await;
    Ok(rx.await?)
}

pub async fn run() -> eyre::Result<()> {
    let tx = spawn_actor().await;

    tx.send(ExampleCommand::Increment).await?;
    tx.send(ExampleCommand::Increment).await?;

    tx.send(ExampleCommand::Increment).await?;

    let value = read(&tx).await;
    println!("{value:?}");

    Ok(())
}
