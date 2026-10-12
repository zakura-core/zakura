//! Validate experiment configuration without starting services.
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path = std::env::args_os().nth(1).ok_or("expected a config path")?;
    let config = zakurad::config::ZakuradConfig::load(Some(path.into()))?;
    if config.network.zakura_node_secret_key.is_some() {
        config.network.zakura_secret_key()?;
    }
    println!("configuration accepted");
    Ok(())
}
