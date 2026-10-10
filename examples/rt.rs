use bitdaemon_bencode::{bdecode, bencode, Entry};
fn main() {
    let bytes = std::fs::read("D:/Projet/libtorrent/libtorrent/test/test_torrents/duplicate_files2.torrent").unwrap();
    let node = bdecode(&bytes);
    assert!(node.error().is_none());
    let re = bencode(&Entry::from_node(&node));
    for i in 0..bytes.len().min(re.len()) {
        if bytes[i] != re[i] {
            println!("first diff at {i}");
            println!("orig: {}", String::from_utf8_lossy(&bytes[i.saturating_sub(40)..(i+40).min(bytes.len())]));
            println!("re-enc: {}", String::from_utf8_lossy(&re[i.saturating_sub(40)..(i+40).min(re.len())]));
            return;
        }
    }
    println!("len {} vs {}", bytes.len(), re.len());
}
