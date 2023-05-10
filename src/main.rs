mod b_tree;
mod commands;
mod cursor;
mod pager;
mod sql_error;
mod string_utils;
mod table;

use commands::*;
use sql_error::SqlError;
use table::Table;

fn main() {
    let filename = std::env::args().nth(1).expect("minisql <db filename>");
    let mut table = Table::open(&filename).unwrap();
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
        return meta_command(buf, table);
    }
    let statement = prepare_statement(buf)?;
    let row = statement.execute(table)?;
    println!("{}", row);
    Ok(())
}

fn meta_command(buf: &str, table: &mut Table) -> Result<(), SqlError> {
    match buf {
        ".exit" => {
            table.close()?;
            std::process::exit(0);
        }
        ".btree" => {
            print!("{}", table.pager.node(0)?);
            return Ok(());
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
        let mut table = init_test_db();

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
    #[test]
    fn close_db() {
        let mut table = init_test_db();

        let statement = prepare_statement("insert 1 wass wass@example.com").unwrap();
        let row = statement.execute(&mut table).unwrap();
        assert_eq!(row.id, 0);

        table.close().unwrap();

        let mut table = Table::open("./test.db").unwrap();
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
    #[test]
    fn tough_insert() {
        let mut table = init_test_db();

        let rows = 42;
        for i in 0..rows {
            let statement = prepare_statement(&format!("insert {} name{} {}@a", i, i, i)).unwrap();
            statement.execute(&mut table).unwrap();
        }
        table.close().unwrap();

        let mut table = Table::open("./test.db").unwrap();
        for i in 0..rows {
            let statement = prepare_statement(&format!("select {}", i)).unwrap();
            let row = statement.execute(&mut table).unwrap();
            assert_eq!(row.age, i);
        }
    }
    fn init_test_db() -> Table {
        match std::fs::remove_file("./test.db") {
            Ok(_) => {}
            Err(_) => {}
        }
        Table::open("./test.db").unwrap()
    }
}
