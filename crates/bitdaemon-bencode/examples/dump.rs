fn main() {
    for f in std::env::args().skip(1) {
        let buf = std::fs::read(&f).unwrap();
        let n = bitdaemon_bencode::bdecode(&buf);
        if n.error().is_some() {
            eprintln!("decode error in {}", f);
            std::process::exit(1);
        }
        println!("=== {} ===", f);
        println!("{}", bitdaemon_bencode::print_node(&n, false, 0));
    }
}
