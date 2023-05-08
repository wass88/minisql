use crate::{pager::Pager, sql_error::SqlError, string_utils::to_string_null_terminated};
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub struct Row {
    pub id: u64,
    pub age: i64,
    pub name: [u8; 32],
    pub email: [u8; 255],
}

impl Display for Row {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Row {{ id: {}, age: {}, name: {}, email: {} }}",
            self.id,
            self.age,
            to_string_null_terminated(&self.name),
            to_string_null_terminated(&self.email)
        )
    }
}
pub const ROW_SIZE: usize = 303;

impl Row {
    pub fn serialize(&self) -> [u8; ROW_SIZE] {
        let mut buf = [0u8; 303];
        buf[0..8].copy_from_slice(&self.id.to_le_bytes());
        buf[8..16].copy_from_slice(&self.age.to_le_bytes());
        buf[16..48].copy_from_slice(&self.name);
        buf[48..303].copy_from_slice(&self.email);
        buf
    }

    pub fn deserialize(buf: &[u8]) -> Self {
        let mut id_bytes = [0; 8];
        id_bytes.copy_from_slice(&buf[0..8]);
        let mut age_bytes = [0; 8];
        age_bytes.copy_from_slice(&buf[8..16]);
        let mut name_bytes = [0; 32];
        name_bytes.copy_from_slice(&buf[16..48]);
        let mut email_bytes = [0; 255];
        email_bytes.copy_from_slice(&buf[48..303]);
        Row {
            id: u64::from_le_bytes(id_bytes),
            age: i64::from_le_bytes(age_bytes),
            name: name_bytes,
            email: email_bytes,
        }
    }
}

use crate::pager::PAGE_SIZE;
pub const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;

pub struct Table {
    pub num_rows: usize,
    pub pager: Pager,
}

impl Table {
    pub fn open(filename: &str) -> Result<Self, SqlError> {
        Ok(Table {
            num_rows: 0,
            pager: Pager::open(filename)?,
        })
    }

    pub fn close(&mut self) -> Result<(), SqlError> {
        let num_full_pages = self.num_rows / ROWS_PER_PAGE;
        for i in 0..num_full_pages {
            if self.pager.pages[i].is_none() {
                continue;
            }
            self.pager.flush(i, PAGE_SIZE)?;
            self.pager.drop(i);
        }
        let remains_rows = self.num_rows % ROWS_PER_PAGE;
        if remains_rows > 0 {
            self.pager.flush(num_full_pages, remains_rows * ROW_SIZE)?;
            self.pager.drop(num_full_pages);
        }
        Ok(())
    }
}
