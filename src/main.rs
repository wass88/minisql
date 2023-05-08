fn main() {
    loop {
        let mut buf = String::new();
        if let Err(e) = std::io::stdin().read_line(&mut buf) {
            println!("Error reading input: {}", e);
            continue;
        }

        let buf = buf.trim();
        if buf.starts_with(".") {
            match meta_command(buf) {
                Ok(_) => continue,
                Err(_) => {
                    println!("Unrecognized command '{}'.", buf);
                    continue;
                }
            }
        }
        let statement = prepare_statement(buf);
        if let Err(_) = statement {
            println!("Unrecognized keyword at start of '{}'.", buf);
            continue;
        }
        let statement = statement.unwrap();
        if let Err(_) = statement.execute() {
            println!("Error executing statement.");
            continue;
        }
    }
}

fn meta_command(buf: &str) -> Result<(), ()> {
    match buf {
        ".exit" => {
            std::process::exit(0);
        }
        _ => {
            return Err(());
        }
    }
}

#[derive(Debug)]
enum Statement {
    Insert,
    Select,
}

fn prepare_statement(buf: &str) -> Result<Statement, ()> {
    if buf.starts_with("insert") {
        return Ok(Statement::Insert);
    }
    if buf.starts_with("select") {
        return Ok(Statement::Select);
    }
    Err(())
}

impl Statement {
    fn execute(&self) -> Result<(), ()> {
        match self {
            Statement::Insert => {
                println!("This is where we would do an insert.");
                Ok(())
            }
            Statement::Select => {
                println!("This is where we would do a select.");
                Ok(())
            }
        }
    }
}
