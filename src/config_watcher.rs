use crate::Config;
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info, instrument};

#[cfg(feature = "notify")]
use notify::{EventKind, RecursiveMode, Watcher};

/// Notify on every modification of `path`. Watches the parent directory so
/// that editors doing rename-replace are caught as well.
#[cfg(feature = "notify")]
pub fn watch_file(path: &Path) -> Result<mpsc::UnboundedReceiver<()>> {
    let (tx, rx) = mpsc::unbounded_channel();
    let path = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let parent_path = path
        .parent()
        .expect("watched file should have a parent dir")
        .to_owned();
    let path_clone = path.clone();
    let tx_clone = tx.clone();
    let mut watcher =
        notify::recommended_watcher(move |res: Result<notify::Event, _>| match res {
            Ok(e) => {
                if matches!(e.kind, EventKind::Modify(_))
                    && e.paths
                        .iter()
                        .map(|x| x.file_name())
                        .any(|x| x == path_clone.file_name())
                {
                    let _ = tx_clone.send(());
                }
            }
            Err(e) => error!("watch error: {:#}", e),
        })?;
    watcher.watch(&parent_path, RecursiveMode::NonRecursive)?;
    info!("Start watching {:?}", path);
    // Keep the watcher alive until the receiver is dropped
    tokio::spawn(async move {
        let _w = watcher;
        tx.closed().await;
    });
    Ok(rx)
}

/// Without `hot-reload` the receiver never fires
#[cfg(not(feature = "notify"))]
pub fn watch_file(_path: &Path) -> Result<mpsc::UnboundedReceiver<()>> {
    let (tx, rx) = mpsc::unbounded_channel();
    std::mem::forget(tx);
    Ok(rx)
}

pub struct ConfigWatcherHandle {
    pub event_rx: mpsc::UnboundedReceiver<Config>,
}

impl ConfigWatcherHandle {
    pub async fn new(path: &Path, shutdown_rx: broadcast::Receiver<bool>) -> Result<Self> {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let origin_cfg = Config::from_file(path).await?;

        // Initial start
        event_tx.send(origin_cfg.clone()).unwrap();

        tokio::spawn(config_watcher(
            path.to_owned(),
            shutdown_rx,
            event_tx,
            origin_cfg,
        ));

        Ok(ConfigWatcherHandle { event_rx })
    }
}

#[instrument(skip(shutdown_rx, event_tx, old))]
async fn config_watcher(
    path: PathBuf,
    mut shutdown_rx: broadcast::Receiver<bool>,
    event_tx: mpsc::UnboundedSender<Config>,
    mut old: Config,
) -> Result<()> {
    let mut fevent_rx = watch_file(&path)?;

    loop {
        tokio::select! {
          e = fevent_rx.recv() => {
            match e {
              Some(_) => {
                    info!("Rescan the configuration");
                    let new = match Config::from_file(&path).await.with_context(|| "The changed configuration is invalid. Ignored") {
                      Ok(v) => v,
                      Err(e) => {
                        error!("{:#}", e);
                        // If the config is invalid, just ignore it
                        continue;
                      }
                    };

                    if new != old {
                        event_tx.send(new.clone())?;
                        old = new;
                    }
              },
              None => break
            }
          },
          _ = shutdown_rx.recv() => break
        }
    }

    info!("Config watcher exiting");

    Ok(())
}
