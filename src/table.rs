use crate::{
    cursor::Cursor, node::NodeRef, pager::Pager, sql_error::SqlError,
    string_utils::to_string_null_terminated,
};
use std::{
    fmt::{Display, Formatter},
    write,
};

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

    pub fn start(&mut self) -> Result<Cursor, SqlError> {
        let mut cursor = self.find(0)?;
        if !cursor.has_cell() {
            cursor.end_of_table = true;
        }
        Ok(cursor)
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
        let node = node.internal_node();
        let num_keys = node.get_num_keys();
        let mut min_index = 0usize;
        let mut max_index = num_keys;
        while min_index < max_index {
            let index = (min_index + max_index) / 2;
            let key_at_index = node.get_key_at(index);
            if key_at_index >= key {
                max_index = index;
            } else {
                min_index = index + 1;
            }
        }
        let child = node.get_child_at(max_index);
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
        let mut max_cell = node.borrow().leaf_node().get_num_cells() as usize;
        while min_cell < max_cell {
            let mid_cell = (min_cell + max_cell) / 2;
            let mid_key = node.borrow().leaf_node().get_key(mid_cell);
            if mid_key >= key {
                max_cell = mid_cell;
            } else {
                min_cell = mid_cell + 1;
            }
        }
        Ok(Cursor {
            table: self,
            page_num,
            cell_num: max_cell,
            end_of_table: false,
        })
    }
}

impl Display for Table {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fn indent(buf: &str, indent_size: usize) -> String {
            let mut buf = buf.to_owned();
            let indent = " ".repeat(indent_size);
            if buf.ends_with("\n") {
                buf.pop();
            }
            format!(
                "{}{}\n",
                indent,
                buf.replace("\n", &format!("\n{}", indent))
            )
        }
        fn print_table(
            f: &mut Formatter<'_>,
            table: &Table,
            node_num: usize,
            visited: &mut Vec<bool>,
            indent_size: usize,
        ) -> std::fmt::Result {
            if visited[node_num] {
                write!(f, "Node[{}] <visited>\n", node_num)?;
                return Ok(());
            }
            visited[node_num] = true;
            let node = table.pager.node(node_num).unwrap();
            let node = node.borrow();
            let buf = format!("Node {} {}", node_num, node);
            let buf = indent(&buf, indent_size);
            write!(f, "{}", buf)?;
            if let NodeRef::Internal(internal) = node.as_typed() {
                for i in 0..=internal.get_num_keys() {
                    print_table(f, table, internal.get_child_at(i), visited, indent_size + 2)?;
                }
            }
            Ok(())
        }
        writeln!(f, "Table {{ root_page_num: {} }}", self.root_page_num)?;
        let mut visited = vec![false; self.pager.num_pages.get()];
        print_table(f, self, self.root_page_num, &mut visited, 0)?;
        Ok(())
    }
}
