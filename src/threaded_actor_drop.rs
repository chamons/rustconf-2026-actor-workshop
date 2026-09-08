use std::sync::mpsc::{Receiver, Sender, channel};

enum Command {
    Increment(u32),
    Inspect { reply: oneshot::Sender<u32> },
}

fn spawn() -> Sender<Command> {
    let (tx, rx) = channel();

    std::thread::spawn(|| actor(rx));
    tx
}

fn actor(rx: Receiver<Command>) {
    let mut state: u32 = 0;

    while let Ok(message) = rx.recv() {
        match message {
            Command::Increment(n) => state += n,
            Command::Inspect { reply } => {
                let _ = reply.send(state);
            }
        }
    }
    println!("Closing actor");
}

pub async fn run() -> eyre::Result<()> {
    let actor = spawn();

    let mut threads = vec![];
    for i in 0..5 {
        let actor = actor.clone();
        threads.push(std::thread::spawn(move || {
            let _ = actor.send(Command::Increment(i));
        }));
    }
    for t in threads {
        t.join().unwrap();
    }

    let (response_tx, response_rx) = oneshot::channel();
    actor.send(Command::Inspect { reply: response_tx })?;

    let response = response_rx.recv()?;
    println!("{response}");

    Ok(())
}
