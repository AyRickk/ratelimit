//! The `ratelimit` service.

use ratelimit::settings::Settings;

fn environment(key: &str) -> Option<String> {
    std::env::var_os(key).map(|v| v.to_string_lossy().into_owned())
}

#[tokio::main]
async fn main() {
    let result = match Settings::from_lookup(&environment) {
        Ok(settings) => ratelimit::runner::run(settings, &environment).await,
        Err(e) => Err(e.to_string()),
    };
    if let Err(e) = result {
        eprintln!("{}", e.trim_end());
        std::process::exit(1);
    }
}
