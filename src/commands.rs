use crate::cursor::Cursor;
use crate::sql_error::SqlError;
use crate::string_utils::copy_null_terminated;
use crate::table::{Row, Table};

#[derive(Debug)]
pub enum Statement {
    Insert(u64, [u8; 32], [u8; 255]),
    Select(u64),
}

pub fn prepare_statement(buf: &str) -> Result<Statement, SqlError> {
    if buf.starts_with("insert") {
        let cmds = buf.split(" ").collect::<Vec<&str>>();
        if cmds.len() != 4 {
            return Err(SqlError::InvalidArgs);
        }
        let id = cmds[1]
            .parse::<u64>()
            .map_err(|_| SqlError::NotNumber(cmds[1].to_string()))?;
        if cmds[2].len() > 32 - 1 {
            return Err(SqlError::TooLargeString);
        }
        if cmds[3].len() > 255 - 1 {
            return Err(SqlError::TooLargeString);
        }
        let mut name = [0u8; 32];
        copy_null_terminated(&mut name, cmds[2]);
        let mut email = [0u8; 255];
        copy_null_terminated(&mut email, cmds[3]);
        return Ok(Statement::Insert(id, name, email));
    }
    if buf.starts_with("select") {
        let cmds = buf.split(" ").collect::<Vec<&str>>();
        if cmds.len() != 2 {
            return Err(SqlError::InvalidArgs);
        }
        let i = cmds[1]
            .parse::<u64>()
            .map_err(|_| SqlError::NotNumber(cmds[1].to_string()))?;
        return Ok(Statement::Select(i as u64));
    }
    Err(SqlError::UnknownCommand(buf.to_string()))
}

impl Statement {
    pub fn execute(&self, table: &mut Table) -> Result<Row, SqlError> {
        match self {
            Statement::Insert(id, name, email) => {
                let row = Row {
                    id: *id,
                    name: *name,
                    email: *email,
                };
                let mut cursor = table.find(*id as u64)?;

                if cursor.has_cell() && cursor.get()?.get_key() == *id as u64 {
                    return Err(SqlError::DuplicateKey);
                }
                cursor.insert(row.id, row.serialize())?;
                Ok(row)
            }
            Statement::Select(i) => {
                let cursor = table.find(*i)?;
                let row = cursor.get()?;
                let row = Row::deserialize(&row.get_value());
                Ok(row)
            }
        }
    }
}
