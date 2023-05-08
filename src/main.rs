fn main() {
    let mut buf = String::new();
    loop {
        std::io::stdin()
            .read_line(&mut buf)
            .expect("Failed to read line");
        if buf.trim() == ".exit" {
            break;
        }
    }
}
