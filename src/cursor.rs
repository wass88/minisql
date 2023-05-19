use crate::{
    node::{
        LeafRef, INTERNAL_NODE_LEFT_SPLIT_COUNT, INTERNAL_NODE_MAX_CELLS,
        INTERNAL_NODE_RIGHT_SPLIT_COUNT, LEAF_NODE_LEFT_SPLIT_COUNT, LEAF_NODE_MAX_CELLS,
        LEAF_NODE_RIGHT_SPLIT_COUNT, MISSING_NODE,
    },
    sql_error::{SqlError, SqlResult},
    table::{Table, ROW_SIZE},
};
use std::cell::Ref;

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

    /// Update value
    pub fn update(&self, value: [u8; ROW_SIZE]) -> SqlResult<()> {
        println!(
            "[Update] node {}[{}] key: {}",
            self.page_num,
            self.cell_num,
            self.get()?.get_key(),
        );
        let node = self.table.leaf_mut(self.page_num)?;
        node.value(self.cell_num).copy_from_slice(value.as_ref());
        Ok(())
    }

    /// Insert at the position of the cursor
    pub fn insert(&self, key: u64, value: [u8; ROW_SIZE]) -> SqlResult<()> {
        println!(
            "[Insert] node {}[{}] key: {}",
            self.page_num, self.cell_num, key,
        );
        let node = self.table.leaf_mut(self.page_num)?;
        let num_cells = node.get_num_cells();

        let key_before = node.get_first_key();
        if self.cell_num == 0 {
            self.update_key_rec(self.page_num, key_before, key)?;
        }

        if num_cells >= LEAF_NODE_MAX_CELLS {
            // When the node is full, split it
            return self.split_and_insert(key, value);
        }
        // Shift the cells to the right
        for i in (self.cell_num..num_cells).rev() {
            let node = self.table.leaf_mut(self.page_num)?;
            let cell = node.cell(i).to_owned(); // TODO Slow own
            node.cell(i + 1).copy_from_slice(&cell);
        }
        node.set_key(self.cell_num, key);
        node.value(self.cell_num).copy_from_slice(value.as_ref());
        node.set_num_cells(num_cells + 1);

        Ok(())
    }

    /// Update parents with the first key recursively to root;
    fn update_key_rec(&self, node_num: usize, key_before: u64, key_after: u64) -> SqlResult<()> {
        let node = self.table.pager.node(node_num)?;
        if node.is_root() {
            return Ok(());
        }
        let parent_num = node.get_parent();
        let parent = self.table.internal_mut(parent_num)?;
        let index = parent.find_key(key_before).unwrap();
        parent.set_key_at(index, key_after);
        self.update_key_rec(parent_num, key_before, key_after)
    }

    /// Insert to full cell
    fn split_and_insert(&self, key: u64, value: [u8; ROW_SIZE]) -> SqlResult<()> {
        // max cursor_page -> old_node
        //                 -> new_node
        let old_num = self.page_num;
        let old_node = self.table.leaf_mut(old_num)?;

        // Create New Leaf Node
        let new_page_num = self.table.pager.new_page_num();
        let new_node = self.table.pager.node(new_page_num)?.init_leaf();

        println!("Split Leaf old:{} new:{}", old_num, new_page_num);

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
        new_node.set_parent(old_node.get_parent());

        // Update parent key
        let old_is_root = old_node.is_root();
        self.update_parent(old_is_root, new_page_num)
    }

    /// update parent node after splitting
    fn update_parent(&self, old_is_root: bool, new_num: usize) -> SqlResult<()> {
        if old_is_root {
            self.create_new_root(new_num)
        } else {
            self.insert_internal_node(new_num)
        }
    }

    /// When root_node is splitted, create new root
    fn create_new_root(&self, right_child_num: usize) -> SqlResult<()> {
        let old_root_num = self.table.get_root_num()?;
        let new_root_num = self.table.pager.new_page_num();
        println!(
            "Create New Root old root->left: {}, right: {}, new root: {}",
            old_root_num, right_child_num, new_root_num
        );

        let left_num = old_root_num;
        let left_child = self.table.pager.node(left_num)?;
        let right_child = self.table.pager.node(right_child_num)?;
        let root = self.table.pager.node(new_root_num)?;

        left_child.set_root(false);
        // new root has left and right
        let root = root.init_internal();
        root.set_root(true);
        root.set_num_keys(2);
        root.set_key_at(0, left_child.get_first_key());
        root.set_child_at(0, left_num);
        root.set_key_at(1, right_child.get_first_key());
        root.set_child_at(1, right_child_num);
        self.table.set_root_num(new_root_num);

        println!(
            "root{}: {}\nleft{} [{}]: {}\nright{} j[{}]: {}",
            self.table.get_root_num()?,
            root.node_ref.node,
            left_num,
            left_child.get_first_key(),
            left_child,
            right_child_num,
            right_child.get_first_key(),
            right_child
        );

        left_child.set_parent(new_root_num);
        right_child.set_parent(new_root_num);

        Ok(())
    }

    /// After node is splitted, insert new node to parent
    fn insert_internal_node(&self, child_num: usize) -> SqlResult<()> {
        let child = self.table.pager.node(child_num)?;
        let node_num = child.get_parent();
        println!("Insert internal node {} <- child {}", node_num, child_num);

        let node = self.table.internal_mut(node_num)?;

        let num_keys = node.get_num_keys();
        if num_keys >= INTERNAL_NODE_MAX_CELLS {
            return self.split_and_insert_internal_node(node_num, child_num);
        }

        let child_key = child.get_first_key();
        let index = node.find_key(child_key).unwrap() + 1;

        node.set_num_keys(num_keys + 1);
        for i in (index..num_keys).rev() {
            let key = node.get_key_at(i);
            node.set_key_at(i + 1, key);
            let child_num = node.get_child_at(i);
            node.set_child_at(i + 1, child_num);
        }
        node.set_key_at(index, child_key);
        node.set_child_at(index, child_num);
        Ok(())
    }

    /// When internal node is overflowed, split to new internal node
    fn split_and_insert_internal_node(&self, node_num: usize, child_num: usize) -> SqlResult<()> {
        let old_node = self.table.internal_mut(node_num)?;
        let new_node_num = self.table.pager.new_page_num();
        let new_node = self.table.pager.node(new_node_num)?.init_internal();
        let num_keys = old_node.get_num_keys();

        let child = self.table.pager.node(child_num)?;
        let child_key = child.get_first_key();
        let child_index = old_node.find_key(child_key).unwrap() + 1;

        // old[0] [1] [a]      [2] [3] [4]
        // old[0] [1] [2]  new [0] [1] [2]
        println!("Split internal old: {}, new: {}", node_num, new_node_num);

        for i in (0..num_keys + 1).rev() {
            let (key, num) = if i == child_index as usize {
                print!("i: {}, child", i);
                (child_key, child_num)
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
            } else if i - INTERNAL_NODE_LEFT_SPLIT_COUNT < INTERNAL_NODE_RIGHT_SPLIT_COUNT {
                println!(" -> new[{}]", i - INTERNAL_NODE_LEFT_SPLIT_COUNT);
                new_node.set_key_at(i - INTERNAL_NODE_LEFT_SPLIT_COUNT, key);
                new_node.set_child_at(i - INTERNAL_NODE_LEFT_SPLIT_COUNT, num);
            } else {
                println!("->error");
                panic!("Invalid index, i: {}", i);
            }
        }

        old_node.set_num_keys(INTERNAL_NODE_LEFT_SPLIT_COUNT);
        new_node.set_num_keys(INTERNAL_NODE_RIGHT_SPLIT_COUNT);
        new_node.set_parent(old_node.get_parent());

        // Update right_child's parent;
        for i in 0..INTERNAL_NODE_RIGHT_SPLIT_COUNT {
            let child_num = new_node.get_child_at(i);
            let child = self.table.pager.node(child_num)?;
            child.set_parent(new_node_num);
        }

        let old_is_root = old_node.node.is_root();
        self.update_parent(old_is_root, new_node_num)
    }

    /// Remove cell from leaf node
    pub fn remove(&self) -> SqlResult<()> {
        println!("[Remove] page: {}, cell: {}", self.page_num, self.cell_num);

        if !self.has_cell()? {
            return Err(SqlError::NoData);
        }

        let leaf_num = self.page_num;
        let leaf = self.table.leaf_mut(leaf_num)?;

        if self.cell_num == 0 {
            let before = leaf.get_key(0);
            let after = leaf.get_key(1); // INFO: LEAF MIN >= 2
            self.update_key_rec(leaf_num, before, after)?;
        }

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
        if next_leaf == MISSING_NODE {
            // Merge to left node
            let left_num = self.previous_leaf(leaf_num)?.unwrap();
            let left = self.table.leaf_mut(left_num)?;

            if left.get_num_cells() + leaf.get_num_cells() <= LEAF_NODE_MAX_CELLS {
                // Merge leaves
                self.merge_and_remove(left_num, leaf_num)?;
            } else {
                // Shift left --> leaf
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

                let leaf_after_key = leaf.get_key(0);
                let leaf_before_key = left.get_key(1);
                self.update_key_rec(left_num, leaf_before_key, leaf_after_key)?;
            }

            return Ok(());
        }

        // Pick from right
        let right_index = next_leaf;
        let right = self.table.leaf_mut(right_index)?;

        if right.get_num_cells() + leaf.get_num_cells() <= LEAF_NODE_MAX_CELLS {
            // Merge leaves
            self.merge_and_remove(leaf_num, right_index)?;
        } else {
            let leaf_num = leaf.get_num_cells();
            let right_num = right.get_num_cells();

            let right_before = right.get_key(0);
            let right_after = right.get_key(1);
            self.update_key_rec(leaf_num, right_before, right_after)?;

            // Shift leaf <-- right
            {
                let right_0 = right.cell(0);
                leaf.cell(leaf_num).copy_from_slice(&right_0);
            }
            for i in 0..(right.get_num_cells() - 1) {
                let cell = right.cell(i + 1).to_owned(); // TODO slow owned
                right.cell(i).copy_from_slice(&cell);
            }
            leaf.set_num_cells(leaf_num + 1);
            right.set_num_cells(right_num - 1);
        }
        Ok(())
    }

    fn previous_leaf(&self, leaf_num: usize) -> SqlResult<Option<usize>> {
        // Back traverse
        let leaf = self.table.leaf_ref(leaf_num)?;
        let leaf_key = leaf.get_first_key();
        if leaf.is_root() {
            return Ok(None);
        }
        let parent_num = leaf.node.get_parent();
        let parent = self.table.internal_ref(parent_num)?;
        let index = parent.find_key(leaf_key).unwrap();
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
            let node_num = previous_parent.get_child_at(previous_parent.get_num_keys() - 1);
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
        let node_key = node.get_first_key();
        let index = parent.find_key(node_key).unwrap();
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
            let node_num = previous_parent.get_child_at(previous_parent.get_num_keys() - 1);
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

        let node_key = node.get_first_key();
        let index = parent.find_key(node_key).unwrap();

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

    fn merge_and_remove(&self, left_num: usize, right_num: usize) -> SqlResult<()> {
        println!("Merge Node{} and Node{}", left_num, right_num);
        let left = self.table.leaf_mut(left_num)?;
        let right = self.table.leaf_mut(right_num)?;
        let right_key = right.get_first_key();
        let parent_num = right.get_parent();
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

        self.remove_key_from_internal(parent_num, right_key)
    }

    fn remove_key_from_internal(&self, parent_num: usize, key: u64) -> SqlResult<()> {
        println!("remove key {} from Node{}", key, parent_num);
        let parent = self.table.internal_mut(parent_num)?;
        let index = parent.find_key(key).unwrap();

        if index == 0 {
            let before = parent.get_key_at(0);
            let after = parent.get_key_at(1); // INFO: MIN KEYS >= 2
            self.update_key_rec(parent_num, before, after)?;
        }

        let num_keys = parent.get_num_keys();
        for i in (index..num_keys - 1).rev() {
            let key = parent.get_key_at(i + 1);
            parent.set_key_at(i, key);
            let child = parent.get_child_at(i + 1);
            parent.set_child_at(i, child);
        }
        parent.set_num_keys(num_keys - 1);

        self.balance_internal(parent_num)
    }

    fn balance_internal(&self, node_num: usize) -> SqlResult<()> {
        println!("balance internal node {}", node_num);
        let node = self.table.internal_mut(node_num).unwrap();
        let num_keys = node.get_num_keys();
        if num_keys >= INTERNAL_NODE_RIGHT_SPLIT_COUNT {
            return Ok(());
        }

        if node.is_root() {
            if num_keys == 1 {
                let single_num = node.get_child_at(0);
                self.table.set_root_num(single_num)?;
                let single = self.table.pager.node(single_num)?;
                single.set_parent(MISSING_NODE);
                single.set_root(true);
                // TODO: original root is not used anymore
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
            let left = self.table.internal_mut(left_num)?;
            let left_num_keys = left.get_num_keys();

            if left_num_keys + num_keys <= INTERNAL_NODE_MAX_CELLS {
                return self.merge_and_remove_internal(left_num, node_num);
            }
            // Shift Left ---> Node
            for i in (1..left_num_keys).rev() {
                let key = node.get_key_at(i - 1);
                let child = node.get_child_at(i - 1);
                node.set_key_at(i, key);
                node.set_child_at(i, child);
            }
            let left_key = left.get_first_key();
            let left_child = left.get_child_at(left_num_keys - 1);
            node.set_key_at(0, left_key);
            node.set_child_at(0, left_child);

            node.set_num_keys(num_keys + 1);
            left.set_num_keys(left_num_keys - 1);

            let before = node.get_key_at(1);
            let after = node.get_key_at(0);
            self.update_key_rec(node_num, before, after)?;
        }

        let right_num = right_num.unwrap();
        let right = self.table.internal_mut(right_num)?;
        let right_num_keys = right.get_num_keys();
        if num_keys + right_num_keys <= INTERNAL_NODE_MAX_CELLS {
            return self.merge_and_remove_internal(node_num, right_num);
        }

        // Shift node <-- right
        let before = right.get_key_at(0);
        let after = right.get_key_at(1);
        self.update_key_rec(right_num, before, after)?;

        node.set_key_at(num_keys, right.get_key_at(0));
        node.set_child_at(num_keys, right.get_child_at(0));
        for i in 1..right_num_keys {
            let key = right.get_key_at(i);
            let child = right.get_child_at(i);
            right.set_key_at(i - 1, key);
            right.set_child_at(i - 1, child);
        }
        node.set_num_keys(num_keys + 1);
        right.set_num_keys(right_num_keys - 1);
        Ok(())
    }

    fn merge_and_remove_internal(&self, left_num: usize, right_num: usize) -> SqlResult<()> {
        let left = self.table.internal_mut(left_num)?;
        let right = self.table.internal_mut(right_num)?;
        let left_num_keys = left.get_num_keys();
        let right_num_keys = right.get_num_keys();

        let right_key = right.get_first_key();
        let parent_num = right.get_parent();

        // move right to left
        left.set_num_keys(left_num_keys + right_num_keys);
        for i in 0..right_num_keys {
            let key = right.get_key_at(i);
            let child = right.get_child_at(i);
            left.set_key_at(left_num_keys + i, key);
            left.set_child_at(left_num_keys + i, child);
        }
        // TODO: right is not freed

        self.remove_key_from_internal(parent_num, right_key)
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
    fn small_remove() {
        let db = "small_remove";
        let mut table = init_test_db(db);
        let mut cursor = table.start().unwrap();
        cursor.insert(0, [1; ROW_SIZE]).unwrap();
        cursor.advance().unwrap();
        cursor.insert(1, [1; ROW_SIZE]).unwrap();
        cursor.advance().unwrap();
        cursor.insert(2, [1; ROW_SIZE]).unwrap();
        println!("{}", cursor.table);
        let mut cursor = table.start().unwrap();
        cursor.advance().unwrap();
        cursor.remove().unwrap();
        println!("{}", cursor.table);
        assert_eq!(cursor.get().unwrap().get_key(), 2);
        assert_eq!(
            cursor
                .table
                .leaf_ref(cursor.table.get_root_num().unwrap())
                .unwrap()
                .get_num_cells(),
            2
        );
        let cursor = table.start().unwrap();
        cursor.remove().unwrap();
        cursor.remove().unwrap();
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
