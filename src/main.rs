#[allow(dead_code)]
mod threaded_actor;

#[allow(dead_code)]
mod threaded_actor_drop;

#[allow(dead_code)]
mod async_state_actor;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    async_state_actor::run().await?;

    Ok(())
}
