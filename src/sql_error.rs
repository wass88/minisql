#[derive(Debug)]
pub enum SqlError {
    UnknownCommand(String),
    InvalidArgs,
    TooLargeString,
    NotNumber(String),
}
