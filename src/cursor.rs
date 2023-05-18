use crate::{
    node::{
        LeafRef, NodeRef, INTERNAL_NODE_LEFT_SPLIT_COUNT, INTERNAL_NODE_MAX_CELLS,
        INTERNAL_NODE_RIGHT_SPLIT_COUNT, LEAF_NODE_LEFT_SPLIT_COUNT, LEAF_NODE_MAX_CELLS,
        LEAF_NODE_RIGHT_SPLIT_COUNT,
    },
    sql_error::{SqlError, SqlResult},
    table::{Table, ROW_SIZE},
};
use std::{cell::Ref, unimplemented};

pub struct Cursor<'a> {
    pub table: &'a mut Table,
    pub page_num: usize,
    pub cell_num: usize,
    pub end_of_table: bool,
}

pub struct CursorValue {
    node: LeafRef,
    cell_num: usize,
}
impl CursorValue {
    pub fn get_key(&self) -> u64 {
        self.node.get_key(self.cell_num)
    }
    pub fn get_value(&self) -> Ref<[u8]> {
        self.node.get_value(self.cell_num)
    }
}

impl<'a> Cursor<'a> {
    /// Get values from the cursorS
    pub fn get(&self) -> SqlResult<CursorValue> {
        let node = self.table.leaf_ref(self.page_num)?;
        Ok(CursorValue {
            node,
            cell_num: self.cell_num,
        })
    }

    /// Go to the next cell
    pub fn advance(&mut self) -> SqlResult<()> {
        self.cell_num += 1;
        let leaf = self.table.leaf_ref(self.page_num)?;
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
        Ok(())
    }

    /// Check if the cursor has a cell
    pub fn has_cell(&self) -> SqlResult<bool> {
        let node = self.table.leaf_ref(self.page_num)?;
        Ok(self.cell_num < node.get_num_cells())
    }

    /// Insert at the position of the cursor
    pub fn insert(&mut self, key: u64, value: [u8; ROW_SIZE]) -> SqlResult<()> {
        let num_cells = self.table.leaf_ref(self.page_num)?.get_num_cells();
        if num_cells >= LEAF_NODE_MAX_CELLS {
            // When the node is full, split it
            return self.split_and_insert(key, value);
        }
        for i in (self.cell_num..num_cells).rev() {
            let node = self.table.leaf_mut(self.page_num)?;
            let cell = node.cell(i).to_owned(); // TODO Slow own
            node.cell(i + 1).copy_from_slice(&cell);
        }
        let node = self.table.leaf_mut(self.page_num)?;

        node.set_key(self.cell_num, key);
        node.value(self.cell_num).copy_from_slice(value.as_ref());
        node.set_num_cells(num_cells + 1);
        Ok(())
    }

    fn split_and_insert(&mut self, key: u64, value: [u8; ROW_SIZE]) -> SqlResult<()> {
        // max cursor_page -> old_node
        //                 -> new_node

        let old_node = self.table.leaf_mut(self.page_num)?;

        // Create New Leaf Node
        let new_page_num = self.table.pager.new_page_num();
        let new_node = self.table.pager.node(new_page_num)?.init_leaf();

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
                let key = old_node.get_key(g);
                let value = old_node.get_value(g).to_owned();
                if i >= LEAF_NODE_LEFT_SPLIT_COUNT {
                    new_node.set_key(n, key);
                    new_node.value(n).copy_from_slice(&value);
                } else {
                    old_node.set_key(i, key);
                    old_node.value(i).copy_from_slice(&value);
                };
            }
        }

        // Node properties
        let old_node_next = old_node.get_next_leaf();
        old_node.set_next_leaf(new_page_num);
        old_node.set_num_cells(LEAF_NODE_LEFT_SPLIT_COUNT);

        new_node.set_next_leaf(old_node_next);
        new_node.set_num_cells(LEAF_NODE_RIGHT_SPLIT_COUNT);
        new_node.node.set_parent(old_node.node.get_parent());

        // Update parent key
        let old_max = old_node.node.get_max_key();
        let new_max = new_node.node.get_max_key();
        let parent_num = old_node.node.get_parent();
        let old_is_root = old_node.node.is_root();
        self.update_parent(old_is_root, parent_num, old_max, new_max, new_page_num)
    }

    /// update parent node after splitting
    fn update_parent(
        &mut self,
        old_is_root: bool,
        parent_num: usize,
        old_key: u64,
        new_key: u64,
        new_node: usize,
    ) -> SqlResult<()> {
        if old_is_root {
            self.create_new_root(new_node)
        } else {
            self.update_internal_node_key(parent_num, old_key, new_key)?;
            self.insert_internal_node(parent_num, new_node)
        }
    }

    /// When root_node is splitted, create new root
    fn create_new_root(&mut self, right_child_num: usize) -> SqlResult<()> {
        println!("Create New Root root:{}", self.table.root_page_num);

        let right_child = self.table.pager.node(right_child_num)?;
        let left_child_num = self.table.pager.new_page_num();
        let left_child = self.table.pager.node(left_child_num)?;

        // current root is moved to left
        let root = self.table.pager.node(self.table.root_page_num)?;
        left_child.raw_buf().copy_from_slice(&root.raw_buf());
        left_child.set_root(false);

        // new root has left and right
        let root = root.init_internal();
        root.node.set_root(true);
        root.set_num_keys(1);
        root.set_child_at(0, left_child_num);
        root.set_right_child(right_child_num);

        left_child.set_parent(self.table.root_page_num);
        right_child.set_parent(self.table.root_page_num);

        let left_key = self.get_key_right_most(left_child_num)?;
        root.set_key_at(0, left_key);

        Ok(())
    }

    /// Search right most max key
    fn get_key_right_most(&self, node_num: usize) -> SqlResult<u64> {
        let node = self.table.pager.node(node_num)?;
        Ok(match node.as_typed() {
            NodeRef::Leaf(node) => node.get_key(node.get_num_cells() - 1),
            NodeRef::Internal(node) => self.get_key_right_most(node.get_right_child())?,
        })
    }

    /// After node is splitted, update new key to parent
    fn update_internal_node_key(
        &mut self,
        parent_num: usize,
        old_key: u64,
        new_key: u64,
    ) -> SqlResult<()> {
        let parent = self.table.internal_mut(parent_num)?;
        let old_index = parent.find_key(old_key);
        parent.set_key_at(old_index, new_key);
        Ok(())
    }

    /// After node is splitted, insert new node to parent
    fn insert_internal_node(&mut self, parent_num: usize, child_num: usize) -> SqlResult<()> {
        let parent = self.table.internal_mut(parent_num)?;
        let child = self.table.pager.node(child_num)?;
        let child_max = child.get_max_key();
        let index = parent.find_key(child_max);

        let original_num_keys = parent.get_num_keys();

        let right_child_num = parent.get_right_child();
        let right_max = self.get_key_right_most(right_child_num)?;

        if original_num_keys >= INTERNAL_NODE_MAX_CELLS {
            return self.split_and_insert_internal_node(
                parent_num, child_num, child_max, index, right_max,
            );
        }

        parent.set_num_keys(original_num_keys + 1);

        if child_max > right_max {
            // Replace right child
            parent.set_key_at(original_num_keys, right_max);
            parent.set_child_at(original_num_keys, right_child_num);
            parent.set_right_child(child_num);
        } else {
            // Move cells to make room
            for i in (index..original_num_keys).rev() {
                let key = parent.get_key_at(i);
                let child_num = parent.get_child_at(i);
                parent.set_key_at(i + 1, key);
                parent.set_child_at(i + 1, child_num);
            }
            parent.set_key_at(index, child_max);
            parent.set_child_at(index, child_num);
        }
        Ok(())
    }

    /// When internal node is overflowed, split to new internal node
    fn split_and_insert_internal_node(
        &mut self,
        node_num: usize,
        child_num: usize,
        child_max: u64,
        child_index: usize,
        right_max: u64,
    ) -> SqlResult<()> {
        let old_node = self.table.internal_mut(node_num)?;
        let new_node_num = self.table.pager.new_page_num();
        let new_node = self.table.pager.node(new_node_num)?.init_internal();

        let num_keys = old_node.get_num_keys();
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
                    (right_max, old_node.get_right_child())
                }
            } else if i == child_index as usize {
                if !child_is_last {
                    print!("i: {}, child", i);
                    (child_max, child_num)
                } else {
                    print!("i: {}, [-]", i);
                    (right_max, old_node.get_right_child())
                }
            } else if i >= child_index as usize {
                print!("i: {}, [{}]", i, i - 1);
                (old_node.get_key_at(i - 1), old_node.get_child_at(i - 1))
            } else {
                print!("i: {}, [{}]", i, i);
                (old_node.get_key_at(i), old_node.get_child_at(i))
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

        self.update_parent(old_is_root, parent_num, old_max, new_max, new_node_num)
    }

    /// Remove cell from leaf node
    pub fn remove(&mut self) -> SqlResult<()> {
        if !self.has_cell()? {
            return Err(SqlError::NoData);
        }

        let leaf_num = self.page_num;
        let leaf = self.table.leaf_mut(leaf_num)?;

        let leaf_max = leaf.node.get_max_key();

        // Remove Element
        let num_cells = leaf.get_num_cells();
        for i in self.cell_num..(num_cells - 1) {
            let cell = leaf.cell(i + 1).to_owned();
            leaf.cell(i).copy_from_slice(&cell);
        }
        leaf.set_num_cells(num_cells - 1);
        let num_cells = leaf.get_num_cells();

        if leaf.node.is_root() {
            // Not need to merge
            return Ok(());
        }

        if num_cells >= LEAF_NODE_RIGHT_SPLIT_COUNT {
            // No need to balance
            return Ok(());
        }

        println!("Balance leaf node: {}", leaf_num);
        let next_leaf = leaf.get_next_leaf();
        if next_leaf == 0 {
            // Merge to right node
            let left_num = self.previous_leaf(leaf_num)?.unwrap();
            let left = self.table.leaf_mut(left_num)?;

            if left.get_num_cells() + leaf.get_num_cells() <= LEAF_NODE_MAX_CELLS {
                // Merge leaves
                self.merge_and_remove(left_num, leaf_num)?;
            } else {
                // Shift from left
                let num_leaf = leaf.get_num_cells();
                let num_left = left.get_num_cells();
                for i in (0..num_leaf).rev() {
                    let cell = leaf.cell(i).to_owned();
                    leaf.cell(i + 1).copy_from_slice(&cell);
                }
                {
                    let left_last = left.cell(num_left - 1);
                    leaf.cell(0).copy_from_slice(&left_last);
                }
                leaf.set_num_cells(num_leaf + 1);
                left.set_num_cells(num_left - 1);
                // Update parent key
                let parent_num = left.node.get_parent();
                let parent = self.table.internal_ref(parent_num)?;
                let num_left = left.get_num_cells();
                let left_last_key = left.get_key(num_left - 1);
                let index = parent.find_node_index(left_num, left_last_key);
                self.update_key(parent_num, index, left_last_key)?;
            }

            return Ok(());
        }

        // Pick from right
        let right_index = next_leaf;
        let right = self.table.leaf_mut(right_index)?;

        if right.get_num_cells() + leaf.get_num_cells() <= LEAF_NODE_MAX_CELLS {
            // Merge leaves
            self.merge_and_remove(leaf_num, right_index);
        } else {
            let leaf_nums = leaf.get_num_cells();
            // Shift from right
            let right_0_key = right.get_key(0);
            {
                let right_0 = right.cell(0);
                leaf.cell(leaf_nums).copy_from_slice(&right_0);
            }
            leaf.set_num_cells(leaf_nums + 1);
            for i in 0..right.get_num_cells() - 1 {
                let cell = right.cell(i + 1).to_owned(); // TODO slow owned
                right.cell(i).copy_from_slice(&cell);
            }
            let right_nums = right.get_num_cells();
            right.set_num_cells(right_nums - 1);

            // Update parent key
            let parent_num = leaf.node.get_parent();
            let parent = self.table.internal_mut(parent_num)?;
            let index = parent.find_node_index(leaf_num, leaf_max);
            self.update_key(parent_num, index, right_0_key)?;
        }
        Ok(())
    }

    fn previous_leaf(&self, leaf_num: usize) -> SqlResult<Option<usize>> {
        // Back traverse
        let leaf = self.table.leaf_ref(leaf_num)?;
        let leaf_max = leaf.node.get_max_key();
        if leaf.is_root() {
            return Ok(None);
        }
        let parent_num = leaf.node.get_parent();
        let parent = self.table.internal_ref(parent_num)?;
        let index = parent.find_node_index(leaf_num, leaf_max);
        if index == 0 {
            // Recursive upper
            if parent.is_root() {
                return Ok(None);
            }
            let previous_parent_num = self.prev_internal(parent_num)?;
            let previous_parent_num = match previous_parent_num {
                None => return Ok(None),
                Some(n) => n,
            };
            let previous_parent = self.table.internal_ref(previous_parent_num)?;
            let node_num = previous_parent.get_right_child();
            Ok(Some(node_num))
        } else {
            let left_num = parent.get_child_at(index - 1);
            return Ok(Some(left_num));
        }
    }

    fn prev_internal(&self, node_num: usize) -> SqlResult<Option<usize>> {
        let node = self.table.internal_ref(node_num)?;
        if node.is_root() {
            return Ok(None);
        }
        let parent_num = node.node.get_parent();
        let parent = self.table.internal_ref(parent_num)?;
        let node_key = self.get_key_right_most(node_num)?;
        let index = parent.find_node_index(node_num, node_key);
        if index == 0 {
            // Recursive upper
            if parent.is_root() {
                return Ok(None);
            }
            let previous_parent_num = self.prev_internal(parent_num)?;
            let previous_parent_num = match previous_parent_num {
                None => return Ok(None),
                Some(n) => n,
            };

            let previous_parent = self.table.internal_ref(previous_parent_num)?;
            let node_num = previous_parent.get_right_child();
            return Ok(Some(node_num));
        }
        let left_num = parent.get_child_at(index - 1);
        Ok(Some(left_num))
    }

    fn next_internal(&self, node_num: usize) -> SqlResult<Option<usize>> {
        let node = self.table.internal_ref(node_num)?;
        if node.is_root() {
            return Ok(None);
        }
        let parent_num = node.node.get_parent();
        let parent = self.table.internal_ref(parent_num)?;

        let node_key = self.get_key_right_most(node_num)?;
        let index = parent.find_node_index(node_num, node_key);

        if index == parent.get_num_keys() {
            // Recursive upper
            if parent.is_root() {
                return Ok(None);
            }
            let next_parent_num = self.next_internal(parent_num)?;
            let next_parent_num = match next_parent_num {
                None => return Ok(None),
                Some(n) => n,
            };
            let next_parent = self.table.internal_ref(next_parent_num)?;
            let node_num = next_parent.get_child_at(0);
            return Ok(Some(node_num));
        }
        let right_num = parent.get_child_at(index + 1);
        Ok(Some(right_num))
    }

    fn update_key(&mut self, node_num: usize, index: usize, key: u64) -> SqlResult<()> {
        println!("Update Node{}[{}] = key {}", node_num, index, key);
        let node = self.table.internal_mut(node_num)?;

        let num_key = node.get_num_keys();
        if num_key == index {
            // right child key is not need to update
            return Ok(());
        }
        node.set_key_at(index, key);

        // recursive update
        if node.node.is_root() {
            return Ok(());
        }
        let parent_num = node.node.get_parent();
        let parent = self.table.internal_mut(parent_num)?;
        let node_key = node.get_max_key();
        let node_index = parent.find_node_index(node_num, node_key);
        self.update_key(parent_num, node_index, key)?;
        Ok(())
    }

    fn merge_and_remove(&mut self, left_num: usize, right_num: usize) -> SqlResult<()> {
        println!("Merge Node{} and Node{}", left_num, right_num);
        let left = self.table.leaf_mut(left_num)?;
        let right = self.table.leaf_mut(right_num)?;

        let left_key = left.get_max_key();
        let right_key = right.get_max_key();

        let left_cells = left.get_num_cells();
        let right_cells = right.get_num_cells();

        assert!(left_cells + right_cells <= LEAF_NODE_MAX_CELLS);

        for i in 0..right_cells {
            let cell = right.cell(i).to_owned(); // TODO: slow owned
            left.cell(left_cells + i).copy_from_slice(&cell);
        }
        left.set_next_leaf(right.get_next_leaf());
        left.set_num_cells(left_cells + right_cells);
        // TODO: right_cells is already not used.

        let parent_num = left.node.get_parent();
        let parent = self.table.internal_mut(parent_num)?;

        let left_index = parent.find_node_index(left_num, left_key);
        self.update_key(parent_num, left_index, left_key)?;

        let right_index = parent.find_node_index(right_num, right_key);
        self.remove_key_from_internal(parent_num, right_index)
    }

    fn remove_key_from_internal(&mut self, parent_num: usize, key_index: usize) -> SqlResult<()> {
        let parent = self.table.internal_mut(parent_num)?;

        let num_keys = parent.get_num_keys();
        if key_index == num_keys {
            let right_child = parent.get_child_at(key_index - 1);
            parent.set_right_child(right_child);
        } else {
            for i in key_index..num_keys {
                let key = parent.get_key_at(i + 1);
                parent.set_key_at(i, key);
            }
            for i in key_index..(num_keys - 1) {
                let child = parent.get_child_at(i + 1);
                parent.set_child_at(i, child);
            }
        }
        parent.set_num_keys(num_keys - 1);

        let num_keys = parent.get_num_keys();
        if parent.node.is_root() {
            if num_keys == 0 {
                let single = parent.get_child_at(0);
                let node = self.table.pager.node(single)?;
                parent.raw_buf().copy_from_slice(&node.raw_buf());
                parent.set_root(true);
                // TODO: node is not used anymore
            }
            return Ok(());
        }

        self.balance_internal(parent_num)
    }

    fn balance_internal(&mut self, node_num: usize) -> SqlResult<()> {
        let node = self.table.internal_mut(node_num).unwrap();
        let num_keys = node.get_num_keys();
        if num_keys >= INTERNAL_NODE_RIGHT_SPLIT_COUNT {
            return Ok(());
        }

        if node.is_root() {
            if num_keys == 0 {
                let single = node.get_child_at(0);
                let single = self.table.pager.node(single)?;
                node.raw_buf().copy_from_slice(&single.raw_buf());
                node.set_root(true);
            }
            return Ok(());
        }

        let right_num = self.next_internal(node_num)?;
        if right_num.is_none() {
            let left_num = self.prev_internal(node_num)?;
            if left_num.is_none() {
                panic!("node {} is singleton?", node_num);
            }

            let left_num = left_num.unwrap();
            let left = self.table.internal_ref(left_num)?;
            let left_num_keys = left.get_num_keys();

            if num_keys + left_num_keys + 2 <= INTERNAL_NODE_MAX_CELLS + 1 {
                return self.merge_and_remove_internal(node_num, right_num);
            }
            // Shift Left ---> Node
            for i in (1..left_num_keys).rev() {
                let key = node.get_key_at(i - 1);
                let child = node.get_child_at(i - 1);
                node.set_key_at(i, key);
                node.set_child_at(i, child);
            }
            let left_max = self.get_key_right_most(left_num)?;
            let left_child = left.get_child_at(left_num_keys - 1);
            node.set_key_at(0, left_max);
            node.set_child_at(0, left_child);

            let parent_num = node.get_parent();
            let parent = self.table.internal_ref(parent_num)?;
            let node_index = parent.find_node_index(node_num, left_max);
            let new_right_key = self.get_key_right_most(left_num_keys)?;
            self.update_key(parent_num, node_index, new_right_key)?;
        }

        let right_num = right_num.unwrap();
        let right = self.table.internal_mut(right_num)?;
        let right_num_keys = right.get_num_keys();
        if num_keys + right_num_keys + 2 <= INTERNAL_NODE_MAX_CELLS + 1 {
            return self.merge_and_remove_internal(node_num, right_num);
        }

        let node_max = self.get_key_right_most(node_num)?;
        // Shift node <-- right
        node.set_num_keys(num_keys + 1);
        node.set_key_at(num_keys, node_max);
        node.set_child_at(num_keys, node.get_right_child());
        node.set_right_child(right.get_child_at(0));
        for i in 1..right_num_keys {
            let key = right.get_key_at(i);
            let child = right.get_child_at(i);
            right.set_key_at(i - 1, key);
            right.set_child_at(i - 1, child);
        }

        let parent_num = node.get_parent();
        let parent = self.table.internal_ref(parent_num)?;
        let node_index = parent.find_node_index(node_num, node_max);
        let new_node_max = self.get_key_right_most(node_num)?;
        self.update_key(parent_num, node_index, new_node_max)
    }

    fn merge_and_remove_internal(&self, left_num: usize, right_num: usize) -> SqlResult<()> {
        let left = self.table.internal_mut(left_num)?;
        let right = self.table.internal_mut(right_num)?;
        let left_num_keys = left.get_num_keys();
        let right_num_keys = right.get_num_keys();

        let parent_num = left.get_parent();
        let parent = self.table.internal_mut(parent_num)?;
        let left_max = self.get_key_right_most(left_num)?;
        let left_index = parent.find_node_index(left_num, left_max);

        let right_parent_num = right.get_parent();
        let right_parent = self.table.internal_mut(right_parent_num)?;
        let right_max = self.get_key_right_most(right_num)?;
        let right_index = right_parent.find_node_index(right_num, right_max);

        // move right to left
        left.set_num_keys(left_num_keys + 1 + right_num_keys);
        let left_last_num = left.get_right_child();
        let left_last_key = self.get_key_right_most(left_last_num)?;
        left.set_child_at(left_num_keys, left_last_num);
        left.set_key_at(left_num_keys, left_last_key);

        for i in 0..right_num_keys {
            let key = right.get_key_at(i);
            let child = right.get_child_at(i);
            left.set_key_at(left_num_keys + 1 + i, key);
            left.set_child_at(left_num_keys + 1 + i, child);
        }
        left.set_right_child(right.get_right_child());
        // TODO: right is not freed
        Ok(())
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
        println!("{}", cursor.table.pager.node(0).unwrap());
        cursor.insert(1, [1; ROW_SIZE]).unwrap();
        println!("{}", cursor.table.pager.node(0).unwrap());
        cursor.insert(2, [2; ROW_SIZE]).unwrap();
        println!("{}", cursor.table.pager.node(0).unwrap());

        let cursor = table.start().unwrap();
        let cursor_value = cursor.get().unwrap();
        assert_eq!(cursor_value.get_key(), 2);
        assert_eq!(*cursor_value.get_value(), vec![2; ROW_SIZE]);
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
            cursor.advance().unwrap();
        }
        println!("{}", cursor.table.pager.node(0).unwrap());
        cursor.cell_num = skip;
        cursor
            .insert(skip as u64, [LEAF_NODE_MAX_CELLS as u8; ROW_SIZE])
            .unwrap();
        let node0 = cursor.table.internal_ref(0).unwrap();
        let node1 = cursor.table.leaf_ref(1).unwrap();
        let node2 = cursor.table.leaf_ref(2).unwrap();

        assert_eq!(node0.get_right_child(), 1);
        assert_eq!(node0.get_num_keys(), 1);
        assert_eq!(node0.get_key_at(0), LEAF_NODE_LEFT_SPLIT_COUNT as u64 - 1);
        assert_eq!(node0.get_child_at(0), 2);

        assert_eq!(node2.get_num_cells(), LEAF_NODE_LEFT_SPLIT_COUNT);
        assert_eq!(node1.get_num_cells(), LEAF_NODE_RIGHT_SPLIT_COUNT);
    }
    #[test]
    fn small_remove() {
        let db = "small_remove";
        let mut table = init_test_db(db);
        let mut cursor = table.start().unwrap();
        cursor.insert(0, [1; ROW_SIZE]).unwrap();
        cursor.advance();
        cursor.insert(1, [1; ROW_SIZE]).unwrap();
        cursor.advance();
        cursor.insert(2, [1; ROW_SIZE]).unwrap();
        let mut cursor = table.start().unwrap();
        cursor.advance();
        cursor.remove();
        assert_eq!(cursor.get().unwrap().get_key(), 2);
        assert_eq!(cursor.table.leaf_ref(0).unwrap().get_num_cells(), 2);
        let mut cursor = table.start().unwrap();
        cursor.remove();
        cursor.remove();
        println!("{}", cursor.table);
    }
    #[test]
    fn leaf_balance() {
        let db = "leaf_balance";
        let mut table = init_test_db(db);
        let rows = vec![0, 4, 5, 6, 3, 2, 1];
        for i in rows {
            table
                .find(i as u64)
                .unwrap()
                .insert(i as u64, [i as u8; ROW_SIZE])
                .unwrap();
        }
        println!("{}", table);

        let removes = vec![1, 2, 5, 6, 3];
        for i in removes {
            table.find(i as u64).unwrap().remove().unwrap();
            println!("### {} ###\n{}", i, table);
        }
    }
}
