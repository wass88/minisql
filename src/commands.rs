use crate::sql_error::{SqlError, SqlResult};
use crate::string_utils::copy_null_terminated;
use crate::table::{Row, Table};

#[derive(Debug)]
pub enum Statement {
    Insert(u64, [u8; 32], [u8; 255]),
    Update(u64, [u8; 32], [u8; 255]),
    Select(u64),
    Delete(u64),
    SelectAll(),
}

pub fn prepare_statement(buf: &str) -> SqlResult<Statement> {
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
    if buf.starts_with("update") {
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
        return Ok(Statement::Update(id, name, email));
    }
    if buf.starts_with("select") {
        let cmds = buf.split(" ").collect::<Vec<&str>>();
        if cmds.len() == 1 {
            return Ok(Statement::SelectAll());
        }
        if cmds.len() != 2 {
            return Err(SqlError::InvalidArgs);
        }
        let i = cmds[1]
            .parse::<u64>()
            .map_err(|_| SqlError::NotNumber(cmds[1].to_string()))?;
        return Ok(Statement::Select(i as u64));
    }
    if buf.contains("delete") {
        let cmds = buf.split(" ").collect::<Vec<&str>>();
        if cmds.len() != 2 {
            return Err(SqlError::InvalidArgs);
        }
        let i = cmds[1]
            .parse::<u64>()
            .map_err(|_| SqlError::NotNumber(cmds[1].to_string()))?;
        return Ok(Statement::Delete(i as u64));
    }
    Err(SqlError::UnknownCommand(buf.to_string()))
}

impl Statement {
    pub fn execute(&self, table: &mut Table) -> SqlResult<Vec<Row>> {
        match self {
            Statement::Insert(id, name, email) => {
                let row = Row {
                    id: *id,
                    name: *name,
                    email: *email,
                };
                let cursor = table.find(*id)?;

                if cursor.has_cell()? && cursor.get()?.get_key() == *id as u64 {
                    return Err(SqlError::DuplicateKey);
                }
                cursor.insert(row.id, row.serialize())?;
                Ok(vec![row])
            }
            Statement::Update(id, name, email) => {
                let cursor = table.find(*id)?;
                if !cursor.check_key(*id)? {
                    return Err(SqlError::NoData);
                }
                let row = Row {
                    id: *id,
                    name: *name,
                    email: *email,
                };
                cursor.update(row.serialize())?;
                Ok(vec![row])
            }
            Statement::Select(i) => {
                let cursor = table.find(*i)?;
                if !cursor.check_key(*i)? {
                    return Err(SqlError::NoData);
                }
                let row = cursor.get()?;
                let row = Row::deserialize(&row.get_value());
                Ok(vec![row])
            }
            Statement::SelectAll() => {
                let mut cursor = table.start()?;
                let mut rows = Vec::new();
                while !cursor.end_of_table {
                    let row = cursor.get()?;
                    let row = Row::deserialize(&row.get_value());
                    rows.push(row);
                    cursor.advance()?;
                }
                Ok(rows)
            }
            Statement::Delete(i) => {
                let cursor = table.find(*i)?;
                if !cursor.has_cell()? || cursor.get()?.get_key() != *i as u64 {
                    return Err(SqlError::NoData);
                }
                cursor.remove()?;
                Ok(vec![])
            }
        }
    }
}
