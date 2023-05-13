use crate::{
    cursor::Cursor, pager::Pager, sql_error::SqlError, string_utils::to_string_null_terminated,
};
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub struct Row {
    pub id: u64,
    pub name: [u8; 32],
    pub email: [u8; 255],
}

impl Display for Row {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Row {{ id: {}, name: {}, email: {} }}",
            self.id,
            to_string_null_terminated(&self.name),
            to_string_null_terminated(&self.email)
        )
    }
}
pub const ROW_SIZE: usize = 295;

impl Row {
    pub fn serialize(&self) -> [u8; ROW_SIZE] {
        let mut buf = [0u8; ROW_SIZE];
        buf[0..8].copy_from_slice(&self.id.to_le_bytes());
        buf[8..40].copy_from_slice(&self.name);
        buf[40..295].copy_from_slice(&self.email);
        buf
    }

    pub fn deserialize(buf: &[u8]) -> Self {
        let mut id_bytes = [0; 8];
        id_bytes.copy_from_slice(&buf[0..8]);
        let mut name_bytes = [0; 32];
        name_bytes.copy_from_slice(&buf[8..40]);
        let mut email_bytes = [0; 255];
        email_bytes.copy_from_slice(&buf[40..295]);
        Row {
            id: u64::from_le_bytes(id_bytes),
            name: name_bytes,
            email: email_bytes,
        }
    }
}

pub struct Table {
    pub pager: Pager,
    pub root_page_num: usize,
}

impl Table {
    pub fn open(filename: &str) -> Result<Self, SqlError> {
        Ok(Table {
            pager: Pager::open(filename)?,
            root_page_num: 0,
        })
    }

    pub fn close(&mut self) -> Result<(), SqlError> {
        for i in 0..self.pager.num_pages.get() {
            if self.pager.pages.borrow()[i].is_none() {
                continue;
            }
            self.pager.flush(i)?;
            self.pager.drop(i);
        }
        Ok(())
    }

    pub fn find(&mut self, key: u64) -> Result<Cursor, SqlError> {
        let root_node = self.pager.node(self.root_page_num)?;
        if root_node.borrow().is_leaf() {
            self.find_leaf(self.root_page_num, key)
        } else {
            self.find_internal(self.root_page_num, key)
        }
    }
    pub fn find_internal(&mut self, page_num: usize, key: u64) -> Result<Cursor, SqlError> {
        let node = self.pager.node(page_num)?;
        let node = node.borrow();
        let num_keys = node.get_num_cells();
        let mut min_index = 0usize;
        let mut max_index = num_keys;
        while min_index < max_index {
            let index = (min_index + max_index) / 2;
            let key_at_index = node.get_key(index);
            if key < key_at_index {
                max_index = index;
            } else {
                min_index = index + 1;
            }
        }
        let child = node.get_child_at(min_index);
        let child_node = self.pager.node(child)?;
        drop(node);
        if child_node.borrow().is_leaf() {
            drop(child_node);
            self.find_leaf(child, key)
        } else {
            drop(child_node);
            self.find_internal(child, key)
        }
    }
    pub fn find_leaf(&mut self, page_num: usize, key: u64) -> Result<Cursor, SqlError> {
        let node = self.pager.node(page_num)?;
        let mut min_cell = 0usize;
        let mut max_cell = node.borrow().get_num_cells() as usize;
        while min_cell < max_cell {
            let mid_cell = (min_cell + max_cell) / 2;
            let mid_key = node.borrow().get_key(mid_cell);
            if key == mid_key {
                return Ok(Cursor {
                    table: self,
                    page_num,
                    cell_num: mid_cell,
                    end_of_table: false,
                });
            }
            if key < mid_key {
                max_cell = mid_cell;
            } else {
                min_cell = mid_cell + 1;
            }
        }
        Ok(Cursor {
            table: self,
            page_num,
            cell_num: min_cell,
            end_of_table: false,
        })
    }
}

impl Display for Table {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fn print_table(f: &mut Formatter<'_>, table: &Table, node_num: usize) -> std::fmt::Result {
            let node = table.pager.node(node_num).unwrap();
            let node = node.borrow();
            write!(f, "Node[{}] {}", node_num, node)?;
            if node.is_internal() {
                for i in 0..=node.get_num_keys() {
                    print_table(f, table, node.get_child_at(i))?;
                }
            }
            Ok(())
        }
        writeln!(f, "Table {{ root_page_num: {} }}", self.root_page_num)?;
        print_table(f, self, self.root_page_num)?;
        Ok(())
    }
}
