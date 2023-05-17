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
    NoData,
}

pub type SqlResult<T> = Result<T, SqlError>;
