use std::{collections::HashMap, time::Duration};

use tokio::{
    select,
    sync::mpsc::{Receiver, Sender, channel},
    time::interval,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct User(pub String);

#[derive(Clone, Debug)]
pub struct ChatMessage {
    pub sender: User,
    pub message: String,
}

#[derive(Clone, Debug)]
pub struct SubscribeMessage {
    pub user: User,
    pub sender: Sender<ChatMessage>,
}

enum ChatCommand {
    Subscribe(SubscribeMessage),
    SendMessage(ChatMessage),
}

#[derive(Clone)]
pub struct ChatRoom {
    tx: Sender<ChatCommand>,
}

impl ChatRoom {
    pub fn new() -> Self {
        let (tx, rx) = channel(64);
        tokio::task::spawn(async { ChatRoom::run(rx).await });
        Self { tx }
    }

    async fn run(mut rx: Receiver<ChatCommand>) {
        let mut members = HashMap::new();

        while let Some(msg) = rx.recv().await {
            match msg {
                ChatCommand::SendMessage(message) => {
                    Self::send_internal(message, &mut members).await
                }
                ChatCommand::Subscribe(message) => {
                    members.insert(message.user, message.sender);
                }
            }
        }
    }

    pub async fn send(&self, message: ChatMessage) {
        if let Err(e) = self.tx.send(ChatCommand::SendMessage(message)).await {
            println!("Error sending: {e:?}");
        }
    }

    pub async fn subscribe(&self, subscribe: SubscribeMessage) {
        if let Err(e) = self.tx.send(ChatCommand::Subscribe(subscribe)).await {
            println!("Error subscribe: {e:?}");
        }
    }

    async fn send_internal(message: ChatMessage, members: &mut HashMap<User, Sender<ChatMessage>>) {
        let dead_users: Vec<(User, Sender<ChatMessage>)> = members
            .extract_if(|_, notify| notify.try_send(message.clone()).is_err())
            .collect();

        for user in dead_users {
            println!("Processing Dead: {}", user.0.0);
            let message = ChatMessage {
                sender: User("System".to_string()),
                message: format!("{} has left", user.0.0),
            };
            for member in &*members {
                let _ = member.1.try_send(message.clone());
            }
        }
    }
}

async fn spawn_client(user: User, time_offset: u64, room: ChatRoom) {
    let (tx, mut rx) = channel::<ChatMessage>(100);

    {
        let user = user.clone();
        let room = room.clone();
        tokio::task::spawn(async move {
            let mut interval = interval(Duration::from_millis(time_offset));
            let mut i = 0;
            loop {
                select! {
                    _ = interval.tick() => {
                        let _ = room.send(ChatMessage { sender: user.clone(), message: format!("{i}") }).await;
                        i += 1;
                    }
                    Some(msg) = rx.recv() => {
                        println!("Client [{}] = [{}] {} ", user.0, msg.sender.0, msg.message);
                    }
                }
            }
        });
    }

    room.subscribe(SubscribeMessage {
        user: user.clone(),
        sender: tx,
    })
    .await;
}

pub async fn run() -> eyre::Result<()> {
    let room = ChatRoom::new();

    spawn_client(User("First".to_string()), 200, room.clone()).await;
    spawn_client(User("Second".to_string()), 50, room.clone()).await;

    tokio::time::sleep(Duration::from_secs(10)).await;

    println!("Shutting Down Chatroom Simulation");

    Ok(())
}
