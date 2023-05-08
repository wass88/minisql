use crate::{
    sql_error::SqlError,
    table::{Table, ROWS_PER_PAGE, ROW_SIZE},
};

pub struct Cursor<'a> {
    pub table: &'a mut Table,
    row_num: usize,
    pub end_of_table: bool,
}

impl<'a> Cursor<'a> {
    pub fn table_start(table: &'a mut Table) -> Self {
        Cursor {
            table,
            row_num: 0,
            end_of_table: false,
        }
    }
    pub fn table_end(table: &'a mut Table) -> Self {
        let num_rows = table.num_rows;
        Cursor {
            table,
            row_num: num_rows,
            end_of_table: true,
        }
    }
    pub fn value(&'a mut self) -> Result<&'a mut [u8], SqlError> {
        let page_num = self.row_num / ROWS_PER_PAGE;
        let page = self.table.pager.get_page(page_num)?;
        let row_offset = self.row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        Ok(&mut page[byte_offset..byte_offset + ROW_SIZE])
    }
    pub fn advance(&mut self) {
        self.row_num += 1;
        // TODO end_of_table
    }
}
