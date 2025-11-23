use rusteron_client::*;
use std::error;
use std::ffi::CStr;
use std::time::Duration;

pub fn create_aeron_publisher(
    stream_id: i32,
    mut channel: String,
) -> Result<AeronPublication, Box<dyn error::Error>> {
    if !channel.ends_with("\0") {
        channel += "\0";
    }
    let mut channel_bytes = channel.into_bytes();
    let channel = CStr::from_bytes_with_nul(&channel_bytes)?;
    let ctx = AeronContext::new()?;
    let aeron: Aeron = Aeron::new(&ctx)?;
    aeron.start()?;
    let publication = aeron
        .async_add_publication(channel, stream_id)?
        .poll_blocking(Duration::from_secs(1))?;
    Ok(publication)
}
