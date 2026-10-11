#![no_main]

use libfuzzer_sys::fuzz_target;

// `read_resume_data` parses untrusted on-disk state; it must validate
// or reject without panic, and valid params must re-encode.
fuzz_target!(|data: &[u8]| {
    let limits = bitdaemon_core::torrent_info::LoadTorrentLimits::default();
    if let Ok(atp) = bitdaemon_core::resume_data::read_resume_data(data, &limits) {
        let _ = bitdaemon_core::resume_data::write_resume_data_buf(&atp);
    }
});
