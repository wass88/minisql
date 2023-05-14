#[derive(Debug)]
pub enum SqlError {
    UnknownCommand(String),
    InvalidArgs,
    TooLargeString,
    NotNumber(String),
    IOError(std::io::Error, String),
    TableFull,
    CorruptFile,
    DuplicateKey,
}
