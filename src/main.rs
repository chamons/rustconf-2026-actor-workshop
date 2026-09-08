#[allow(dead_code)]
mod threaded_actor;

#[allow(dead_code)]
mod threaded_actor_drop;

#[allow(dead_code)]
mod async_state_actor;

#[allow(dead_code)]
mod nice_async_state_actor;

#[allow(dead_code)]
mod chat_room;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    nice_async_state_actor::run().await?;

    Ok(())
}
