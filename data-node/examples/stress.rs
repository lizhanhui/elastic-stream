use std::{cell::RefCell, error::Error, rc::Rc};

use bytes::BytesMut;
use monoio::fs::OpenOptions;

/// Size of the each write call.
const BLOCK_SIZE: u64 = 512;

///
/// By default, `monoio::fs::File` wraps buffered IO in a fully async way.
/// File#write_all_at returns once it writes buffer into system pagecache.
/// Observe system metric to verify this statement. Generally, this is good as
/// OS will help conduct write-merge operation where possible
///
#[monoio::main(entries = 1024)]
async fn main() -> Result<(), Box<dyn Error>> {
    let file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open("/data/1")
        .await?;

    let file = Rc::new(file);

    let pos = RefCell::new(0);

    let handles: Vec<_> = (0..1024)
        .into_iter()
        .map(|idx| {
            let f = Rc::clone(&file);
            (f, idx)
        })
        .map(|(f, idx)| {
            let file = f;
            let pos = pos.clone();
            monoio::spawn(async move {
                let mut buf = BytesMut::zeroed(BLOCK_SIZE as usize).freeze();
                loop {
                    let position = *pos.borrow();
                    {
                        *pos.borrow_mut() += BLOCK_SIZE;
                    }
                    let (res, buf_) = file.write_all_at(buf, position).await;
                    buf = buf_;
                    res.unwrap();
                    if 0 == idx {
                        file.sync_data().await.unwrap();
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.await;
    }

    Ok(())
}
