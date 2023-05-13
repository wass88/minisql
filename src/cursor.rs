use crate::{
    node::{Node, LEAF_NODE_LEFT_SPLIT_COUNT, LEAF_NODE_MAX_CELLS, LEAF_NODE_RIGHT_SPLIT_COUNT},
    sql_error::SqlError,
    table::{Table, ROW_SIZE},
};
use std::{
    cell::{RefCell, RefMut},
    rc::Rc,
};

pub struct Cursor<'a> {
    pub table: &'a mut Table,
    pub page_num: usize,
    pub cell_num: usize,
    pub end_of_table: bool,
}

pub struct CursorValue {
    node: Rc<RefCell<Box<Node>>>,
    cell_num: usize,
}
impl CursorValue {
    pub fn get_key(&self) -> u64 {
        self.node.borrow().get_key(self.cell_num)
    }
    pub fn get_value(&self) -> RefMut<[u8]> {
        RefMut::map(self.node.borrow_mut(), |node| node.value(self.cell_num))
    }
}

impl<'a> Cursor<'a> {
    pub fn table_start(table: &'a mut Table) -> Result<Self, SqlError> {
        let num_rows = table.pager.node(0)?.borrow().get_num_cells();
        Ok(Cursor {
            table,
            page_num: 0,
            cell_num: 0,
            end_of_table: num_rows == 0,
        })
    }
    pub fn get(&self) -> Result<CursorValue, SqlError> {
        let node = self.table.pager.node(self.page_num)?;
        Ok(CursorValue {
            node,
            cell_num: self.cell_num,
        })
    }
    pub fn advance(&mut self) {
        self.cell_num += 1;
    }
    pub fn insert(&mut self, key: u64, value: [u8; ROW_SIZE]) -> Result<(), SqlError> {
        let num_cells = self
            .table
            .pager
            .node(self.page_num)?
            .borrow()
            .get_num_cells();
        if num_cells >= LEAF_NODE_MAX_CELLS {
            return self.split_and_insert(key, value);
        }
        dbg!(self.cell_num, num_cells);
        if self.cell_num < num_cells {
            for i in self.cell_num..num_cells {
                let node = self.table.pager.node(self.page_num)?;
                let mut node = node.borrow_mut();
                let cell = node.cell(i).to_owned(); // TODO Slow own
                node.cell(i + 1).copy_from_slice(&cell);
            }
        }
        let node = self.table.pager.node(self.page_num)?;
        let mut node = node.borrow_mut();
        node.set_key(self.cell_num, key);
        node.value(self.cell_num).copy_from_slice(value.as_ref());
        node.set_num_cells(num_cells + 1);
        println!("DONE");
        Ok(())
    }
    pub fn split_and_insert(&mut self, key: u64, value: [u8; ROW_SIZE]) -> Result<(), SqlError> {
        println!("Split");
        // cursor_page -> old_node
        //             -> new_node
        //             -> root_node
        //
        // Create New Leaf Node
        let new_page_num = self.table.pager.new_page_num();
        let old_node = self.table.pager.node(self.page_num)?;
        let mut old_node = old_node.borrow_mut();
        let new_node = self.table.pager.node(new_page_num)?;
        let mut new_node = new_node.borrow_mut();
        new_node.init_leaf();

        // Move the rows to the old node to the new node
        for i in (0..=LEAF_NODE_MAX_CELLS).rev() {
            let j = i % LEAF_NODE_LEFT_SPLIT_COUNT;
            if i == self.cell_num {
                if i >= LEAF_NODE_LEFT_SPLIT_COUNT {
                    println!("i: {} self new[{}]", i, j);
                    new_node.set_key(j, key);
                    new_node.value(j).copy_from_slice(value.as_ref());
                } else {
                    println!("i: {} self old[{}]", i, j);
                    old_node.set_key(j, key);
                    old_node.value(j).copy_from_slice(value.as_ref());
                };
            } else {
                let g = if i > self.cell_num { i - 1 } else { i };
                let key = old_node.get_key(g);
                let value = old_node.get_value(g).to_owned();
                if i >= LEAF_NODE_LEFT_SPLIT_COUNT {
                    println!("i: {} move new[{}]", i, j);
                    new_node.set_key(j, key);
                    new_node.value(j).copy_from_slice(&value);
                } else {
                    println!("i: {} move old[{}]", i, j);
                    old_node.set_key(j, key);
                    old_node.value(j).copy_from_slice(&value);
                };
            }
        }
        new_node.set_num_cells(LEAF_NODE_RIGHT_SPLIT_COUNT);
        old_node.set_num_cells(LEAF_NODE_LEFT_SPLIT_COUNT);
        println!("old: {}", old_node);
        println!("new: {}", new_node);

        dbg!(old_node.is_root());
        if old_node.is_root() {
            drop(old_node);
            drop(new_node);
            self.create_new_root(new_page_num)
        } else {
            Ok(())
        }
    }
    fn create_new_root(&mut self, right_child_num: usize) -> Result<(), SqlError> {
        println!("New Root");
        let root = self.table.pager.node(self.table.root_page_num)?;
        let mut root = root.borrow_mut();
        let right_child = self.table.pager.node(right_child_num)?;
        let right_child = right_child.borrow();
        let left_child_num = self.table.pager.new_page_num();
        let left_child = self.table.pager.node(left_child_num)?;
        let mut left_child = left_child.borrow_mut();
        dbg!(self.table.root_page_num, right_child_num, left_child_num);

        // root ?
        // right ->
        // _     -> left

        left_child.buf.copy_from_slice(&root.buf);

        left_child.set_root(false);
        root.init_internal();
        root.set_root(true);
        root.set_num_keys(1);
        root.set_child_at(0, left_child_num);
        root.set_key_at(0, left_child.get_max_key());
        root.set_right_child(right_child_num);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::init_test_db;

    #[test]
    fn test_insert() {
        let mut table = init_test_db();
        let mut cursor = Cursor::table_start(&mut table).unwrap();
        println!("{}", cursor.table.pager.node(0).unwrap().borrow());
        cursor.insert(1, [1; ROW_SIZE]).unwrap();
        println!("{}", cursor.table.pager.node(0).unwrap().borrow());
        cursor.insert(2, [2; ROW_SIZE]).unwrap();
        println!("{}", cursor.table.pager.node(0).unwrap().borrow());

        let cursor = Cursor::table_start(&mut table).unwrap();
        let cursor_value = cursor.get().unwrap();
        assert_eq!(cursor_value.get_key(), 2);
        assert_eq!(cursor_value.get_value().to_vec(), vec![2; ROW_SIZE]);
    }

    #[test]
    fn test_split() {
        let mut table = init_test_db();
        let mut cursor = Cursor::table_start(&mut table).unwrap();
        let skip = 4;
        for i in 0..=LEAF_NODE_MAX_CELLS {
            if i == skip {
                continue;
            }
            cursor.insert(i as u64, [i as u8; ROW_SIZE]).unwrap();
            cursor.advance();
        }
        println!("{}", cursor.table.pager.node(0).unwrap().borrow());
        cursor.cell_num = skip;
        cursor
            .insert(skip as u64, [LEAF_NODE_MAX_CELLS as u8; ROW_SIZE])
            .unwrap();
        let node0 = cursor.table.pager.node(0).unwrap();
        let node0 = node0.borrow();
        let node1 = cursor.table.pager.node(1).unwrap();
        let node1 = node1.borrow();
        let node2 = cursor.table.pager.node(2).unwrap();
        let node2 = node2.borrow();

        println!("0 {}", node0);
        println!("1 {}", node1);
        println!("2 {}", node2);

        assert!(node0.is_internal());
        assert_eq!(node0.get_right_child(), 1);
        assert_eq!(node0.get_num_keys(), 1);
        assert_eq!(node0.get_key_at(0), LEAF_NODE_LEFT_SPLIT_COUNT as u64 - 1);
        assert_eq!(node0.get_child_at(0), 2);

        assert_eq!(cursor.table.pager.node(2).unwrap().borrow().is_leaf(), true);
        assert_eq!(
            cursor.table.pager.node(2).unwrap().borrow().get_num_cells(),
            LEAF_NODE_LEFT_SPLIT_COUNT
        );

        assert_eq!(cursor.table.pager.node(1).unwrap().borrow().is_leaf(), true);
        assert_eq!(
            cursor.table.pager.node(1).unwrap().borrow().get_num_cells(),
            LEAF_NODE_RIGHT_SPLIT_COUNT
        );
    }
}
