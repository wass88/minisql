mod commands;
mod sql_error;
mod string_utils;
mod table;

use commands::*;
use sql_error::SqlError;
use table::Table;

fn main() {
    let mut table = Table::new();
    loop {
        let mut buf = String::new();
        if let Err(e) = std::io::stdin().read_line(&mut buf) {
            println!("Error reading input: {}", e);
            continue;
        }
        let buf = buf.trim();
        match exec_buf(buf, &mut table) {
            Ok(_) => {}
            Err(e) => {
                println!("Error: {:?}", e);
                continue;
            }
        }
    }
}

fn exec_buf(buf: &str, table: &mut Table) -> Result<(), SqlError> {
    if buf.starts_with(".") {
        meta_command(buf)?;
    }
    let statement = prepare_statement(buf)?;
    let row = statement.execute(table)?;
    println!("{}", row);
    Ok(())
}

fn meta_command(buf: &str) -> Result<(), SqlError> {
    match buf {
        ".exit" => {
            std::process::exit(0);
        }
        _ => {
            return Err(SqlError::UnknownCommand(buf.to_string()));
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn insert_select() {
        let mut table = Table::new();

        let statement = prepare_statement("insert 1 wass wass@example.com").unwrap();
        let row = statement.execute(&mut table).unwrap();
        assert_eq!(row.id, 0);

        let statement = prepare_statement("insert 21 nnna nnna@example.com").unwrap();
        let row = statement.execute(&mut table).unwrap();
        assert_eq!(row.id, 1);

        let statement = prepare_statement("select 0").unwrap();
        let row = statement.execute(&mut table).unwrap();
        assert_eq!(row.id, 0);
        assert_eq!(row.age, 1);
        assert_eq!(string_utils::to_string_null_terminated(&row.name), "wass");
        assert_eq!(
            string_utils::to_string_null_terminated(&row.email),
            "wass@example.com"
        );
    }
}
