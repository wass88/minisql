use crate::{
    node::{
        Node, NodeRef, INTERNAL_NODE_LEFT_SPLIT_COUNT, INTERNAL_NODE_MAX_CELLS,
        INTERNAL_NODE_RIGHT_SPLIT_COUNT, LEAF_NODE_LEFT_SPLIT_COUNT, LEAF_NODE_MAX_CELLS,
        LEAF_NODE_RIGHT_SPLIT_COUNT,
    },
    sql_error::SqlError,
    table::{Table, ROW_SIZE},
};
use std::{cell::RefCell, rc::Rc};

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
        self.node.borrow().leaf_node().get_key(self.cell_num)
    }
    pub fn get_value<T, F: FnOnce(&mut [u8]) -> T>(&self, callback: F) -> T {
        let mut node = self.node.borrow_mut();
        let mut node = node.leaf_node_mut();
        callback(node.value(self.cell_num))
    }
}

impl<'a> Cursor<'a> {
    pub fn get(&self) -> Result<CursorValue, SqlError> {
        let node = self.table.pager.node(self.page_num)?;
        Ok(CursorValue {
            node,
            cell_num: self.cell_num,
        })
    }
    pub fn advance(&mut self) {
        self.cell_num += 1;
        let page = self.table.pager.node(self.page_num).unwrap();
        let leaf = page.borrow();
        let leaf = leaf.leaf_node();
        let num_page_cells = leaf.get_num_cells();
        let next_leaf = leaf.get_next_leaf();
        if self.cell_num >= num_page_cells {
            if next_leaf == 0 {
                self.end_of_table = true;
            } else {
                self.page_num = next_leaf;
                self.cell_num = 0;
            }
        }
    }
    pub fn has_cell(&self) -> bool {
        let node = self.table.pager.node(self.page_num).unwrap();
        let node = node.borrow();
        let node = node.leaf_node();
        self.cell_num < node.get_num_cells()
    }
    pub fn insert(&mut self, key: u64, value: [u8; ROW_SIZE]) -> Result<(), SqlError> {
        let num_cells = self
            .table
            .pager
            .node(self.page_num)?
            .borrow()
            .leaf_node()
            .get_num_cells();
        if num_cells >= LEAF_NODE_MAX_CELLS {
            return self.split_and_insert(key, value);
        }
        for i in (self.cell_num..num_cells).rev() {
            let node = self.table.pager.node(self.page_num)?;
            let mut node = node.borrow_mut();
            let mut node = node.leaf_node_mut();
            let cell = node.cell(i).to_owned(); // TODO Slow own
            node.cell(i + 1).copy_from_slice(&cell);
        }
        let node = self.table.pager.node(self.page_num)?;
        let mut node = node.borrow_mut();
        let mut node = node.leaf_node_mut();

        node.set_key(self.cell_num, key);
        node.value(self.cell_num).copy_from_slice(value.as_ref());
        node.set_num_cells(num_cells + 1);
        Ok(())
    }

    pub fn split_and_insert(&mut self, key: u64, value: [u8; ROW_SIZE]) -> Result<(), SqlError> {
        // cursor_page -> old_node
        //             -> new_node
        //             -> root_node
        //
        // Create New Leaf Node
        let old_node = self.table.pager.node(self.page_num)?;
        let mut old_node_ref = old_node.borrow_mut();
        let mut old_node = old_node_ref.leaf_node_mut();

        let new_page_num = self.table.pager.new_page_num();
        let new_node = self.table.pager.node(new_page_num)?;
        let mut new_node_ref = new_node.borrow_mut();
        let mut new_node = new_node_ref.init_leaf();

        println!("Split Leaf old:{} new:{}", self.page_num, new_page_num);
        // Move the rows to the old node to the new node
        for i in (0..=LEAF_NODE_MAX_CELLS).rev() {
            let n = if i >= LEAF_NODE_LEFT_SPLIT_COUNT {
                i - LEAF_NODE_LEFT_SPLIT_COUNT
            } else {
                i
            };
            if i == self.cell_num {
                if i >= LEAF_NODE_LEFT_SPLIT_COUNT {
                    new_node.set_key(n, key);
                    new_node.value(n).copy_from_slice(value.as_ref());
                } else {
                    old_node.set_key(i, key);
                    old_node.value(i).copy_from_slice(value.as_ref());
                };
            } else {
                let g = if i > self.cell_num { i - 1 } else { i };
                let key = old_node.view().get_key(g);
                let value = old_node.view().get_value(g).to_owned();
                if i >= LEAF_NODE_LEFT_SPLIT_COUNT {
                    new_node.set_key(n, key);
                    new_node.value(n).copy_from_slice(&value);
                } else {
                    old_node.set_key(i, key);
                    old_node.value(i).copy_from_slice(&value);
                };
            }
        }
        let old_node_next = old_node.view().get_next_leaf();
        old_node.set_next_leaf(new_page_num);
        old_node.set_num_cells(LEAF_NODE_LEFT_SPLIT_COUNT);

        new_node.set_next_leaf(old_node_next);
        new_node.set_num_cells(LEAF_NODE_RIGHT_SPLIT_COUNT);
        new_node.node.set_parent(old_node.node.get_parent());

        let old_max = old_node.node.get_max_key();
        let new_max = new_node.node.get_max_key();
        let parent_num = old_node.node.get_parent();
        let old_is_root = old_node.node.is_root();
        drop(old_node_ref);
        drop(new_node_ref);

        self.update_parent(old_is_root, parent_num, old_max, new_max, new_page_num)
    }

    fn update_parent(
        &mut self,
        old_is_root: bool,
        parent_num: usize,
        old_key: u64,
        new_key: u64,
        new_node: usize,
    ) -> Result<(), SqlError> {
        if old_is_root {
            self.create_new_root(new_node)
        } else {
            self.update_internal_node_key(parent_num, old_key, new_key)?;
            self.insert_internal_node(parent_num, new_node)
        }
    }

    fn create_new_root(&mut self, right_child_num: usize) -> Result<(), SqlError> {
        println!("Create New Root root:{}", self.table.root_page_num);

        let right_child = self.table.pager.node(right_child_num)?;
        let mut right_child = right_child.borrow_mut();
        let left_child_num = self.table.pager.new_page_num();
        let left_child = self.table.pager.node(left_child_num)?;
        let mut left_child = left_child.borrow_mut();

        // current root is moved to left
        // new root has left and right

        let root = self.table.pager.node(self.table.root_page_num)?;
        let mut root = root.borrow_mut();

        left_child.buf.copy_from_slice(&root.buf);

        left_child.set_root(false);
        left_child.set_parent(self.table.root_page_num);
        right_child.set_parent(self.table.root_page_num);

        let mut root = root.init_internal();
        root.node.set_root(true);
        root.set_num_keys(1);
        root.set_child_at(0, left_child_num);
        root.set_right_child(right_child_num);

        drop(left_child);
        drop(right_child);
        let key_right_most = self.get_key_right_most(left_child_num);
        root.set_key_at(0, key_right_most);

        Ok(())
    }

    fn get_key_right_most(&self, node_num: usize) -> u64 {
        let node = self.table.pager.node(node_num).unwrap();
        let node = node.borrow();
        println!("{}", node);
        match node.as_typed() {
            NodeRef::Leaf(node) => node.get_key(node.get_num_cells() - 1),
            NodeRef::Internal(node) => self.get_key_right_most(node.get_right_child()),
        }
    }

    fn update_internal_node_key(
        &mut self,
        parent_num: usize,
        old_key: u64,
        new_key: u64,
    ) -> Result<(), SqlError> {
        let parent = self.table.pager.node(parent_num)?;
        let mut parent = parent.borrow_mut();
        let mut parent = parent.internal_node_mut();
        let old_index = parent.view().find_key(old_key);
        parent.set_key_at(old_index, new_key);
        Ok(())
    }

    fn insert_internal_node(
        &mut self,
        parent_num: usize,
        child_num: usize,
    ) -> Result<(), SqlError> {
        println!("insert internal node {} <- {}", parent_num, child_num);
        let parent = self.table.pager.node(parent_num)?;
        let mut parent_ref = parent.borrow_mut();
        let mut parent = parent_ref.internal_node_mut();
        let child = self.table.pager.node(child_num)?;
        let child = child.borrow();
        let child_max = child.get_max_key();
        let index = parent.view().find_key(child_max);

        let original_num_keys = parent.view().get_num_keys();

        let right_child_num = parent.view().get_right_child();
        drop(child);
        let right_max = self.get_key_right_most(right_child_num);

        if original_num_keys >= INTERNAL_NODE_MAX_CELLS {
            drop(parent_ref);
            return self.split_and_insert_internal_node(
                parent_num, child_num, child_max, index, right_max,
            );
        }

        parent.set_num_keys(original_num_keys + 1);

        if child_max > right_max {
            /* Replace right child */
            parent.set_key_at(original_num_keys, right_max);
            parent.set_child_at(original_num_keys, right_child_num);
            parent.set_right_child(child_num);
        } else {
            /* Move cells to make room */
            for i in (index..original_num_keys).rev() {
                let key = parent.view().get_key_at(i);
                let child_num = parent.view().get_child_at(i);
                parent.set_key_at(i + 1, key);
                parent.set_child_at(i + 1, child_num);
            }
            parent.set_key_at(index, child_max);
            parent.set_child_at(index, child_num);
        }
        Ok(())
    }

    fn split_and_insert_internal_node(
        &mut self,
        node_num: usize,
        child_num: usize,
        child_max: u64,
        child_index: usize,
        right_max: u64,
    ) -> Result<(), SqlError> {
        let old_node = self.table.pager.node(node_num)?;
        let mut old_node_ref = old_node.borrow_mut();
        let mut old_node = old_node_ref.internal_node_mut();
        let new_node_num = self.table.pager.new_page_num();
        let new_node = self.table.pager.node(new_node_num)?;
        let mut new_node_ref = new_node.borrow_mut();
        let mut new_node = new_node_ref.init_internal();

        let num_keys = old_node.view().get_num_keys();
        // old[0] [1] [a]      [2] [3] [X]
        // old[0] [1] [X]  new [0] [1] [X]
        println!("Split internal old: {}, new: {}", node_num, new_node_num);
        new_node.set_num_keys(INTERNAL_NODE_RIGHT_SPLIT_COUNT);

        let child_is_last = right_max < child_max;
        for i in (0..=num_keys + 1).rev() {
            let (key, num) = if i == num_keys + 1 {
                if child_is_last {
                    print!("i: {}, child", i);
                    (child_max, child_num)
                } else {
                    print!("i: {}, old_right", i);
                    (right_max, old_node.view().get_right_child())
                }
            } else if i == child_index as usize {
                if !child_is_last {
                    print!("i: {}, child", i);
                    (child_max, child_num)
                } else {
                    print!("i: {}, [-]", i);
                    (right_max, old_node.view().get_right_child())
                }
            } else if i >= child_index as usize {
                print!("i: {}, [{}]", i, i - 1);
                (
                    old_node.view().get_key_at(i - 1),
                    old_node.view().get_child_at(i - 1),
                )
            } else {
                print!("i: {}, [{}]", i, i);
                (
                    old_node.view().get_key_at(i),
                    old_node.view().get_child_at(i),
                )
            };
            print!(" key:{}, page:{} ", key, num);
            if i < INTERNAL_NODE_LEFT_SPLIT_COUNT {
                println!(" -> old[{}]", i);
                old_node.set_key_at(i, key);
                old_node.set_child_at(i, num);
            } else if i == INTERNAL_NODE_LEFT_SPLIT_COUNT {
                println!(" -> old_right");
                old_node.set_right_child(num);
            } else if i - INTERNAL_NODE_LEFT_SPLIT_COUNT - 1 < INTERNAL_NODE_RIGHT_SPLIT_COUNT {
                println!(" -> new[{}]", i - INTERNAL_NODE_LEFT_SPLIT_COUNT - 1);
                new_node.set_key_at(i - INTERNAL_NODE_LEFT_SPLIT_COUNT - 1, key);
                new_node.set_child_at(i - INTERNAL_NODE_LEFT_SPLIT_COUNT - 1, num);
            } else if i - INTERNAL_NODE_LEFT_SPLIT_COUNT - 1 == INTERNAL_NODE_RIGHT_SPLIT_COUNT {
                println!(" -> new_right");
                new_node.set_right_child(num);
            } else {
                println!("->error");
                panic!("Invalid index, i: {}", i);
            }
        }

        old_node.set_num_keys(INTERNAL_NODE_LEFT_SPLIT_COUNT);

        let parent_num = old_node.node.get_parent();
        let old_max = old_node.node.get_max_key();
        let new_max = new_node.node.get_max_key();
        let old_is_root = old_node.node.is_root();

        drop(old_node_ref);
        drop(new_node_ref);

        self.update_parent(old_is_root, parent_num, old_max, new_max, new_node_num)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::init_test_db;

    #[test]
    fn test_insert() {
        let db = "test_insert";
        let mut table = init_test_db(db);
        let mut cursor = table.start().unwrap();
        println!("{}", cursor.table.pager.node(0).unwrap().borrow());
        cursor.insert(1, [1; ROW_SIZE]).unwrap();
        println!("{}", cursor.table.pager.node(0).unwrap().borrow());
        cursor.insert(2, [2; ROW_SIZE]).unwrap();
        println!("{}", cursor.table.pager.node(0).unwrap().borrow());

        let cursor = table.start().unwrap();
        let cursor_value = cursor.get().unwrap();
        assert_eq!(cursor_value.get_key(), 2);
        assert_eq!(cursor_value.get_value(|v| v.to_vec()), vec![2; ROW_SIZE]);
    }

    #[test]
    fn test_split() {
        let db = "test_split";
        let mut table = init_test_db(db);
        let mut cursor = table.start().unwrap();
        let skip = 2;
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
        let node0 = node0.internal_node();
        let node1 = cursor.table.pager.node(1).unwrap();
        let node1 = node1.borrow();
        let node1 = node1.leaf_node();
        let node2 = cursor.table.pager.node(2).unwrap();
        let node2 = node2.borrow();
        let node2 = node2.leaf_node();

        assert_eq!(node0.get_right_child(), 1);
        assert_eq!(node0.get_num_keys(), 1);
        assert_eq!(node0.get_key_at(0), LEAF_NODE_LEFT_SPLIT_COUNT as u64 - 1);
        assert_eq!(node0.get_child_at(0), 2);

        assert_eq!(node2.get_num_cells(), LEAF_NODE_LEFT_SPLIT_COUNT);
        assert_eq!(node1.get_num_cells(), LEAF_NODE_RIGHT_SPLIT_COUNT);
    }
}
