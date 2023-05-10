use std::cell;

use crate::{
    b_tree::Node,
    sql_error::SqlError,
    table::{Table, ROW_SIZE},
};

pub struct Cursor<'a> {
    pub table: &'a mut Table,
    page_num: usize,
    pub cell_num: usize,
    pub end_of_table: bool,
}

impl<'a> Cursor<'a> {
    pub fn table_start(table: &'a mut Table) -> Result<Self, SqlError> {
        let num_rows = *table.pager.node(0)?.num_cells();
        Ok(Cursor {
            table,
            page_num: 0,
            cell_num: 0,
            end_of_table: num_rows == 0,
        })
    }
    pub fn table_end(table: &'a mut Table) -> Result<Self, SqlError> {
        let root_page_num = table.root_page_num;
        let cell_num = *table.pager.node(root_page_num)?.num_cells() as usize;
        Ok(Cursor {
            table,
            page_num: root_page_num,
            cell_num: cell_num,
            end_of_table: true,
        })
    }
    pub fn value(&'a mut self) -> Result<&'a mut [u8], SqlError> {
        let node = self.table.pager.node(self.page_num)?;
        Ok(node.value(self.cell_num))
    }
    pub fn advance(&mut self) {
        // TODD
    }
    pub fn insert(&mut self, key: u64, value: [u8; ROW_SIZE]) -> Result<(), SqlError> {
        // TODO Split page
        let node = self.table.pager.node(self.page_num)?;
        let num_cells = *node.num_cells();
        node.set_key(self.cell_num, key);
        node.value(self.cell_num).copy_from_slice(value.as_ref());
        *node.num_cells() = num_cells + 1;
        Ok(())
    }
}
