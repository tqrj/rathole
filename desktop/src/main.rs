#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use rathole::{Cli, ClientState, Config};
use std::path::PathBuf;
use tauri::{Emitter, Manager, State};
use tokio::sync::broadcast;

const TEMPLATE: &str = r#"# Written by the rathole desktop app. Edit and save from the UI.
[client]
remote_addr = "myserver.com:2333"
user = "alice"
key = "alice_secret"
"#;

struct ConfigPath(PathBuf);

#[tauri::command]
fn get_state() -> ClientState {
    rathole::client_state().borrow().clone()
}

#[tauri::command]
fn set_port(port: u16, enabled: bool) {
    rathole::set_port_enabled(port, enabled)
}

#[tauri::command]
fn config_path(path: State<ConfigPath>) -> String {
    path.0.display().to_string()
}

#[tauri::command]
fn read_config(path: State<ConfigPath>) -> Result<String, String> {
    std::fs::read_to_string(&path.0).map_err(|e| e.to_string())
}

#[tauri::command]
fn save_config(path: State<ConfigPath>, text: String) -> Result<(), String> {
    Config::parse(&text).map_err(|e| format!("{:#}", e))?;
    std::fs::write(&path.0, text).map_err(|e| e.to_string())
}

/// `rathole-desktop [client.toml]`. Without an argument: `./client.toml` if it
/// exists (started from a terminal), else the per-user app config dir. An app
/// bundle started from Finder/Explorer has an arbitrary, often read-only, cwd.
fn resolve_config_path(app: &tauri::AppHandle) -> PathBuf {
    if let Some(p) = std::env::args().nth(1) {
        return PathBuf::from(p);
    }
    let cwd = PathBuf::from("client.toml");
    if cwd.exists() {
        return cwd;
    }
    match app.path().app_config_dir() {
        Ok(dir) => {
            let _ = std::fs::create_dir_all(&dir);
            dir.join("client.toml")
        }
        Err(_) => cwd,
    }
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::from("info")),
        )
        .init();

    tauri::Builder::default()
        .setup(move |app| {
            let path = resolve_config_path(app.handle());
            if !path.exists() {
                if let Err(e) = std::fs::write(&path, TEMPLATE) {
                    eprintln!("Failed to write {:?}: {}", path, e);
                }
            }
            app.manage(ConfigPath(path.clone()));

            // The client runs for the lifetime of the app; the sender is kept in
            // the app state so the shutdown channel stays open
            let (shutdown_tx, shutdown_rx) = broadcast::channel::<bool>(1);
            app.manage(shutdown_tx);
            let cli = Cli {
                config_path: Some(path),
                client: true,
                ..Default::default()
            };
            tauri::async_runtime::spawn(async move {
                if let Err(e) = rathole::run(cli, shutdown_rx).await {
                    eprintln!("{:#}", e);
                }
            });

            // Push every state change to the webview
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                let mut rx = rathole::client_state();
                loop {
                    let st = rx.borrow_and_update().clone();
                    let _ = handle.emit("state", st);
                    if rx.changed().await.is_err() {
                        break;
                    }
                }
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            get_state,
            set_port,
            config_path,
            read_config,
            save_config
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
