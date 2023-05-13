use std::fmt::Display;

use crate::{
    pager::PAGE_SIZE,
    table::{Row, ROW_SIZE},
};

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum NodeType {
    Internal = 0,
    Leaf,
}

const POINTER_SIZE: usize = std::mem::size_of::<usize>();

// COMMON_NODE_HEADER:
//   NODE_TYPE, IS_ROOT, PARENT_POINTER
const NODE_TYPE_SIZE: usize = 1;
const NODE_TYPE_OFFSET: usize = 0;
const IS_ROOT_SIZE: usize = 1;
const IS_ROOT_OFFSET: usize = NODE_TYPE_OFFSET + NODE_TYPE_SIZE;
const PARENT_POINTER_SIZE: usize = POINTER_SIZE;
const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// LEAF NODE HEADER
//   COMMON_NODE_HEADER, NUM_CELLS
const LEAF_NODE_NUM_CELLS_SIZE: usize = POINTER_SIZE;
const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const LEAF_NODE_NEXT_LEAF_OFFSET: usize = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const LEAF_NODE_NEXT_LEAF_SIZE: usize = POINTER_SIZE;
const LEAF_NODE_HEADER_SIZE: usize =
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;

// LEAF NODE BODY
//  {NODE_KEY, NODE_VALUE}...
const LEAF_NODE_KEY_SIZE: usize = 8;
const LEAF_NODE_KEY_OFFSET: usize = 0;
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
const LEAF_NODE_VALUE_OFFSET: usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
// pub const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
pub const LEAF_NODE_MAX_CELLS: usize = 4; // DEBUG: 4 for testing

// INTERNAL NODE HEADER
const INTERNAL_NODE_NUM_KEYS_SIZE: usize = POINTER_SIZE;
const INTERNAL_NODE_NUM_KEYS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const INTERNAL_NODE_RIGHT_CHILD_SIZE: usize = POINTER_SIZE;
const INTERNAL_NODE_RIGHT_CHILD_OFFSET: usize =
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const INTERNAL_NODE_HEADER_SIZE: usize =
    COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

// INTERNAL NODE BODY
//   {INTERNAL_NODE_CHILD, INTERNAL_NODE_KEY}...
const INTERNAL_NODE_CHILD_SIZE: usize = POINTER_SIZE;
const INTERNAL_NODE_KEY_SIZE: usize = 8;
const INTERNAL_NODE_CELL_SIZE: usize = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
pub const INTERNAL_NODE_MAX_CELLS: usize = 3; // DEBUG: 3 for testing

// Node Splitting
pub const LEAF_NODE_LEFT_SPLIT_COUNT: usize = (LEAF_NODE_MAX_CELLS + 1) / 2;
pub const LEAF_NODE_RIGHT_SPLIT_COUNT: usize = LEAF_NODE_MAX_CELLS + 1 - LEAF_NODE_LEFT_SPLIT_COUNT;

#[derive(Debug, Clone)]
pub struct Node {
    pub buf: [u8; PAGE_SIZE],
}
impl Node {
    pub fn new(buf: [u8; PAGE_SIZE]) -> Self {
        Node { buf }
    }

    // Root Node
    pub fn set_root(&mut self, is_root: bool) {
        self.buf[IS_ROOT_OFFSET] = is_root as u8;
    }
    pub fn is_root(&self) -> bool {
        self.buf[IS_ROOT_OFFSET] == 1
    }

    // Parent Node
    pub fn set_parent(&mut self, parent: usize) {
        self.buf[PARENT_POINTER_OFFSET..PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE]
            .copy_from_slice(&parent.to_le_bytes())
    }
    pub fn get_parent(&self) -> usize {
        usize::from_le_bytes(
            self.buf[PARENT_POINTER_OFFSET..PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE]
                .try_into()
                .unwrap(),
        )
    }

    // Leaf Node
    pub fn init_leaf(&mut self) {
        self.buf[NODE_TYPE_OFFSET] = NodeType::Leaf as u8;
        self.set_root(false);
        self.set_num_cells(0);
        self.set_next_leaf(0); // 0 represents no sibling
    }
    pub fn is_leaf(&self) -> bool {
        self.buf[NODE_TYPE_OFFSET] == NodeType::Leaf as u8
    }

    // Leaf Node: Cells
    pub fn set_num_cells(&mut self, num_cells: usize) {
        let start = LEAF_NODE_NUM_CELLS_OFFSET;
        self.buf[start..start + LEAF_NODE_NUM_CELLS_SIZE].copy_from_slice(&num_cells.to_le_bytes())
    }
    pub fn get_num_cells(&self) -> usize {
        let start = LEAF_NODE_NUM_CELLS_OFFSET;
        usize::from_le_bytes(
            self.buf[start..start + LEAF_NODE_NUM_CELLS_SIZE]
                .try_into()
                .unwrap(),
        )
    }

    // Leaf Node: Next Leaf
    pub fn set_next_leaf(&mut self, next_leaf: usize) {
        self.buf[LEAF_NODE_NEXT_LEAF_OFFSET..LEAF_NODE_NEXT_LEAF_OFFSET + LEAF_NODE_NEXT_LEAF_SIZE]
            .copy_from_slice(&next_leaf.to_le_bytes())
    }
    pub fn get_next_leaf(&self) -> usize {
        usize::from_le_bytes(
            self.buf
                [LEAF_NODE_NEXT_LEAF_OFFSET..LEAF_NODE_NEXT_LEAF_OFFSET + LEAF_NODE_NEXT_LEAF_SIZE]
                .try_into()
                .unwrap(),
        )
    }
    pub fn cell(&mut self, cell: usize) -> &mut [u8] {
        let start = LEAF_NODE_HEADER_SIZE + cell * LEAF_NODE_CELL_SIZE;
        &mut self.buf[start..start + LEAF_NODE_CELL_SIZE]
    }

    // Leaf Node: Cells : Key
    pub fn set_key(&mut self, cell: usize, key: u64) {
        let start = LEAF_NODE_HEADER_SIZE + cell * LEAF_NODE_CELL_SIZE;
        self.buf[start..start + LEAF_NODE_KEY_SIZE].copy_from_slice(&key.to_le_bytes())
    }
    pub fn get_key(&self, cell: usize) -> u64 {
        let start = LEAF_NODE_HEADER_SIZE + cell * LEAF_NODE_CELL_SIZE;
        u64::from_le_bytes(
            self.buf[start..start + LEAF_NODE_KEY_SIZE]
                .try_into()
                .unwrap(),
        )
    }

    // Leaf Node: Cells : Value
    pub fn value(&mut self, cell: usize) -> &mut [u8] {
        let start = LEAF_NODE_HEADER_SIZE + cell * LEAF_NODE_CELL_SIZE + LEAF_NODE_KEY_SIZE;
        &mut self.buf[start..start + LEAF_NODE_VALUE_SIZE]
    }
    pub fn get_value(&self, cell: usize) -> &[u8] {
        let start = LEAF_NODE_HEADER_SIZE + cell * LEAF_NODE_CELL_SIZE + LEAF_NODE_KEY_SIZE;
        &self.buf[start..start + LEAF_NODE_VALUE_SIZE]
    }

    // Internal Node
    pub fn init_internal(&mut self) {
        self.buf[NODE_TYPE_OFFSET] = NodeType::Internal as u8;
        self.set_root(false);
        self.set_num_keys(0);
    }
    pub fn is_internal(&self) -> bool {
        self.buf[NODE_TYPE_OFFSET] == NodeType::Internal as u8
    }
    pub fn set_num_keys(&mut self, num_keys: usize) {
        self.buf[INTERNAL_NODE_NUM_KEYS_OFFSET..INTERNAL_NODE_NUM_KEYS_OFFSET + 8]
            .copy_from_slice(&num_keys.to_le_bytes())
    }
    pub fn get_num_keys(&self) -> usize {
        usize::from_le_bytes(
            self.buf[INTERNAL_NODE_NUM_KEYS_OFFSET..INTERNAL_NODE_NUM_KEYS_OFFSET + 8]
                .try_into()
                .unwrap(),
        )
    }

    // Internal Node: Right Child
    pub fn set_right_child(&mut self, right_child: usize) {
        self.buf[INTERNAL_NODE_RIGHT_CHILD_OFFSET..INTERNAL_NODE_RIGHT_CHILD_OFFSET + 8]
            .copy_from_slice(&right_child.to_le_bytes())
    }
    pub fn get_right_child(&self) -> usize {
        usize::from_le_bytes(
            self.buf[INTERNAL_NODE_RIGHT_CHILD_OFFSET..INTERNAL_NODE_RIGHT_CHILD_OFFSET + 8]
                .try_into()
                .unwrap(),
        )
    }

    // Internal Node: Keys
    pub fn get_key_at(&self, cell: usize) -> u64 {
        let start =
            INTERNAL_NODE_HEADER_SIZE + cell * INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_CHILD_SIZE;
        u64::from_le_bytes(
            self.buf[start..start + INTERNAL_NODE_KEY_SIZE]
                .try_into()
                .unwrap(),
        )
    }
    pub fn set_key_at(&mut self, cell: usize, key: u64) {
        let start =
            INTERNAL_NODE_HEADER_SIZE + cell * INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_CHILD_SIZE;
        self.buf[start..start + INTERNAL_NODE_KEY_SIZE].copy_from_slice(&key.to_le_bytes())
    }

    // Internal Node: Children
    pub fn set_child_at(&mut self, cell: usize, child: usize) {
        if cell == self.get_num_cells() {
            self.set_right_child(child);
            return;
        }
        let start = INTERNAL_NODE_HEADER_SIZE + cell * INTERNAL_NODE_CELL_SIZE;
        self.buf[start..start + INTERNAL_NODE_CHILD_SIZE].copy_from_slice(&child.to_le_bytes())
    }
    pub fn get_child_at(&self, cell: usize) -> usize {
        if cell == self.get_num_cells() {
            return self.get_right_child();
        }
        let start = INTERNAL_NODE_HEADER_SIZE + cell * INTERNAL_NODE_CELL_SIZE;
        usize::from_le_bytes(
            self.buf[start..start + INTERNAL_NODE_CHILD_SIZE]
                .try_into()
                .unwrap(),
        )
    }

    // Max Key (internal and leaf)
    pub fn get_max_key(&self) -> u64 {
        if self.is_leaf() {
            self.get_key(self.get_num_cells() - 1)
        } else {
            self.get_key_at(self.get_num_keys() - 1)
        }
    }

    // Find key
    pub fn find_key(&self, key: u64) -> usize {
        let mut min_index = 0;
        let mut max_index = self.get_num_keys();
        while min_index < max_index {
            let index = (min_index + max_index) / 2;
            let key_at_index = self.get_key_at(index);
            if key < key_at_index {
                max_index = index;
            } else {
                min_index = index + 1;
            }
        }
        min_index
    }
}

impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let node_type = match self.buf[NODE_TYPE_OFFSET] {
            0 => "Internal",
            1 => "Leaf",
            _ => "Unknown",
        };
        let is_root = match self.buf[IS_ROOT_OFFSET] {
            0 => "No",
            1 => "Yes",
            _ => "Unknown",
        };
        let parent_page = usize::from_le_bytes(
            self.buf[PARENT_POINTER_OFFSET..PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE]
                .try_into()
                .unwrap(),
        );
        writeln!(
            f,
            "NodeType: {}, IsRoot: {}, Parent: {}",
            node_type, is_root, parent_page
        )?;
        if self.is_leaf() {
            let num_cells = self.get_num_cells();
            writeln!(
                f,
                "  NumCells: {}, NextLeaf {}",
                num_cells,
                self.get_next_leaf()
            )?;
            for i in 0..num_cells as usize {
                let key = self.get_key(i);
                let value = self.get_value(i);
                let row = Row::deserialize(value);
                writeln!(f, "- Key: {}, Value: {}", key, row)?;
            }
        } else {
            let num_keys = self.get_num_keys();
            writeln!(f, "  NumKeys: {}", num_keys)?;
            let right_child = self.get_right_child();
            for i in 0..num_keys as usize {
                let child = self.get_child_at(i);
                let key = self.get_key_at(i);
                writeln!(f, "- Key: {}, Child: {}", key, child)?;
            }
            writeln!(f, "- RightChild: {}", right_child)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf() {
        let buf = [0u8; PAGE_SIZE];
        let mut node = Node::new(buf);
        node.init_leaf();
        assert_eq!(node.is_leaf(), true);
        assert_eq!(node.is_internal(), false);
        assert_eq!(node.get_num_cells(), 0);
        node.set_num_cells(1);
        assert_eq!(node.get_num_cells(), 1);
        node.set_key(0, 1);
        assert_eq!(node.get_key(0), 1);
        let row = [2u8; ROW_SIZE];
        node.value(0).copy_from_slice(&row);
        assert_eq!(node.get_value(0), row);
        node.set_next_leaf(1);
        assert_eq!(node.get_next_leaf(), 1);
    }
    #[test]
    fn test_internal() {
        let buf = [0u8; PAGE_SIZE];
        let mut node = Node::new(buf);
        node.set_root(true);
        assert_eq!(node.is_root(), true);
        node.init_internal();
        assert_eq!(node.is_leaf(), false);
        assert_eq!(node.is_internal(), true);
        assert_eq!(node.get_num_keys(), 0);
        node.set_num_keys(1);
        assert_eq!(node.get_num_keys(), 1);
        node.set_key_at(0, 1);
        assert_eq!(node.get_key_at(0), 1);
        node.set_child_at(0, 2);
        assert_eq!(node.get_child_at(0), 2);
        node.set_right_child(3);
        assert_eq!(node.get_right_child(), 3);
        assert_eq!(node.get_child_at(1), 3);
    }
}
