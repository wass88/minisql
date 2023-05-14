mod commands;
mod cursor;
mod node;
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
    let rows = statement.execute(table)?;
    for row in rows {
        println!("{}", row);
    }
    Ok(())
}

fn meta_command(buf: &str, table: &mut Table) -> Result<(), SqlError> {
    match buf {
        ".exit" => {
            table.close()?;
            std::process::exit(0);
        }
        ".btree" => {
            println!("{}", table);
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
        let db = "insert_select";
        let mut table = init_test_db(db);

        let statement = prepare_statement("insert 1 wass wass@example.com").unwrap();
        let row = &statement.execute(&mut table).unwrap()[0];
        assert_eq!(row.id, 1);

        let statement = prepare_statement("insert 2 nnna nnna@example.com").unwrap();
        let row = &statement.execute(&mut table).unwrap()[0];
        assert_eq!(row.id, 2);

        let statement = prepare_statement("select 1").unwrap();
        let row = &statement.execute(&mut table).unwrap()[0];
        assert_eq!(row.id, 1);
        assert_eq!(string_utils::to_string_null_terminated(&row.name), "wass");
        assert_eq!(
            string_utils::to_string_null_terminated(&row.email),
            "wass@example.com"
        );
    }
    #[test]
    fn close_db() {
        let db = "close_db";
        let mut table = init_test_db(db);

        let statement = prepare_statement("insert 1 wass wass@example.com").unwrap();
        let row = &statement.execute(&mut table).unwrap()[0];
        assert_eq!(row.id, 1);

        table.close().unwrap();

        let mut table = reopen_test_db(db);
        let statement = prepare_statement("select 0").unwrap();
        let row = &statement.execute(&mut table).unwrap()[0];
        assert_eq!(row.id, 1);
        assert_eq!(string_utils::to_string_null_terminated(&row.name), "wass");
        assert_eq!(
            string_utils::to_string_null_terminated(&row.email),
            "wass@example.com"
        );
    }
    #[test]
    fn tough_insert() {
        let db = "tough_insert";
        let mut table = init_test_db(db);

        let rows = 14;
        for i in 0..rows {
            let statement = prepare_statement(&format!("insert {} name{} {}@a", i, i, i)).unwrap();
            statement.execute(&mut table).unwrap();
            println!("\n##### {} #####\n{}", i, table);
        }
        table.close().unwrap();

        let mut table = reopen_test_db(db);
        for i in 0..rows {
            println!("\n##### {} #####\n{}", i, table);
            let statement = prepare_statement(&format!("select {}", i)).unwrap();
            let row = &statement.execute(&mut table).unwrap()[0];
            println!("{}", row);
            assert_eq!(row.id, i);
        }
    }

    #[test]
    fn select_all() {
        let db = "select_all";
        let mut table = init_test_db(db);

        let num_rows = 12;
        for i in 0..num_rows {
            let statement = prepare_statement(&format!("insert {} name{} {}@a", i, i, i)).unwrap();
            statement.execute(&mut table).unwrap();
        }
        table.close().unwrap();

        let mut table = reopen_test_db(db);
        println!("{}", table);
        let statement = prepare_statement("select").unwrap();
        let rows = statement.execute(&mut table).unwrap();
        assert_eq!(rows.len(), num_rows);
        for i in 0..num_rows {
            let row = &rows[i];
            println!("{}", row);
            assert_eq!(row.id, i as u64);
        }
    }

    #[test]
    fn random_insert() {
        let db = "random_insert";
        let mut table = init_test_db(db);
        let order = vec![9, 17, 3, 2, 6, 8, 11, 1, 7, 21, 4, 15, 12, 14, 20, 13];
        for i in &order {
            let statement = prepare_statement(&format!("insert {} name{} {}@a", i, i, i)).unwrap();
            statement.execute(&mut table).unwrap();
            println!("##### {} #####\n{}", i, table);
        }

        for i in &order {
            let statement = prepare_statement(&format!("select {}", i)).unwrap();
            let row = &statement.execute(&mut table).unwrap()[0];
            assert_eq!(row.id, *i);
        }
    }

    fn db_name(prefix: &str) -> String {
        format!("./forTest/{}.db", prefix)
    }
    pub fn init_test_db(prefix: &str) -> Table {
        match std::fs::remove_file(db_name(prefix)) {
            Ok(_) => {}
            Err(_) => {}
        }
        Table::open(&db_name(prefix)).unwrap()
    }
    pub fn reopen_test_db(prefix: &str) -> Table {
        Table::open(&db_name(prefix)).unwrap()
    }
}
