use crate::{
    cursor::Cursor,
    meta::{MetaMut, MetaRef, META_NODE_NUM},
    node::{InternalMut, InternalRef, LeafMut, LeafRef, NodeRef, NodeType},
    pager::Pager,
    sql_error::SqlResult,
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
}

impl Table {
    pub fn open(filename: &str) -> SqlResult<Self> {
        Ok(Table {
            pager: Pager::open(filename)?,
        })
    }

    pub fn close(&mut self) -> SqlResult<()> {
        for i in 0..self.pager.num_pages.get() {
            if self.pager.pages.borrow()[i].is_none() {
                continue;
            }
            self.pager.flush(i)?;
            self.pager.drop(i);
        }
        Ok(())
    }

    pub fn start(&mut self) -> SqlResult<Cursor> {
        let mut cursor = self.find(0)?;
        if !cursor.has_cell()? {
            cursor.end_of_table = true;
        }
        Ok(cursor)
    }

    pub fn find(&mut self, key: u64) -> SqlResult<Cursor> {
        let root_node = self.pager.node(self.get_root_num()?)?;
        match root_node.get_type() {
            NodeType::Leaf => self.find_leaf(self.get_root_num()?, key),
            NodeType::Internal => self.find_internal(self.get_root_num()?, key),
        }
    }
    pub fn find_internal(&mut self, page_num: usize, key: u64) -> SqlResult<Cursor> {
        let node = self.internal_ref(page_num)?;
        let index = match node.find_key(key) {
            Some(index) => index,
            None => 0,
        };
        let child = node.get_child_at(index);
        let child_node = self.pager.node(child)?;
        match child_node.get_type() {
            NodeType::Leaf => self.find_leaf(child, key),
            NodeType::Internal => self.find_internal(child, key),
        }
    }
    pub fn find_leaf(&mut self, page_num: usize, key: u64) -> SqlResult<Cursor> {
        let node = self.leaf_ref(page_num)?;
        let mut min_cell = 0usize;
        let mut max_cell = node.get_num_cells() as usize;
        while min_cell < max_cell {
            let mid_cell = (min_cell + max_cell) / 2;
            let mid_key = node.get_key(mid_cell);
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

    pub fn internal_mut(&self, page_num: usize) -> SqlResult<InternalMut> {
        let node = self.pager.node(page_num)?;
        Ok(node.internal_node_mut())
    }
    pub fn leaf_mut(&self, page_num: usize) -> SqlResult<LeafMut> {
        let node = self.pager.node(page_num)?;
        Ok(node.leaf_node_mut())
    }
    pub fn leaf_ref(&self, page_num: usize) -> SqlResult<LeafRef> {
        let node = self.pager.node(page_num)?;
        Ok(node.leaf_node())
    }
    pub fn internal_ref(&self, page_num: usize) -> SqlResult<InternalRef> {
        let node = self.pager.node(page_num)?;
        Ok(node.internal_node())
    }

    // Meta
    pub fn meta_mut(&self) -> SqlResult<MetaMut> {
        let node = self.pager.node(META_NODE_NUM)?;
        Ok(node.meta_node_mut())
    }
    pub fn meta_ref(&self) -> SqlResult<MetaRef> {
        let node = self.pager.node(META_NODE_NUM)?;
        Ok(node.meta_node())
    }
    pub fn get_root_num(&self) -> SqlResult<usize> {
        let meta = self.meta_ref()?;
        Ok(meta.get_root_num())
    }
    pub fn set_root_num(&self, root_num: usize) -> SqlResult<()> {
        let mut meta = self.meta_mut()?;
        meta.set_root_num(root_num);
        Ok(())
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
            let buf = format!("Node {} {}", node_num, node);
            let buf = indent(&buf, indent_size);
            write!(f, "{}", buf)?;
            if let NodeRef::Internal(internal) = node.as_typed() {
                for i in 0..internal.get_num_keys() {
                    print_table(f, table, internal.get_child_at(i), visited, indent_size + 2)?;
                }
            }
            Ok(())
        }
        writeln!(
            f,
            "Table {{ root_page_num: {} }}",
            self.get_root_num().unwrap()
        )?;
        let mut visited = vec![false; self.pager.num_pages.get()];
        print_table(f, self, self.get_root_num().unwrap(), &mut visited, 0)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::test::init_test_db;

    #[test]
    fn find_leaf() {
        let db = "find_leaf";
        let mut table = init_test_db(db);
        let node = table.leaf_mut(0).unwrap();
        node.set_key(0, 2);
        node.set_key(1, 3);
        node.set_key(2, 5);
        node.set_num_cells(3);
        println!("{}", node.node_ref.node);
        assert_eq!(table.find_leaf(0, 1).unwrap().cell_num, 0);
        assert_eq!(table.find_leaf(0, 2).unwrap().cell_num, 0);
        assert_eq!(table.find_leaf(0, 3).unwrap().cell_num, 1);
        assert_eq!(table.find_leaf(0, 5).unwrap().cell_num, 2);
    }
}
