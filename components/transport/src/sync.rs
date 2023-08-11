#[cfg(test)]
mod tests {
    use std::error::Error;

    use log::trace;

    #[monoio::test]
    async fn test_oneshot() -> Result<(), Box<dyn Error>> {
        ulog::try_init_log();
        let (tx, rx) = local_sync::oneshot::channel();
        monoio::spawn(async move {
            monoio::time::sleep(std::time::Duration::from_secs(1)).await;
            tx.send(1).unwrap();
        });
        trace!("Received {}", rx.await?);
        Ok(())
    }

    #[monoio::test]
    async fn test_mpsc() -> Result<(), Box<dyn Error>> {
        let (tx, mut rx) = local_sync::mpsc::unbounded::channel();
        for i in 0..10 {
            let tx = tx.clone();
            monoio::spawn(async move {
                tx.send(i).expect("Channel should not be closed");
            });
        }

        for _ in 0..10 {
            trace!(
                "Received {}",
                rx.recv().await.expect("Should receive a value")
            );
        }

        Ok(())
    }
}
