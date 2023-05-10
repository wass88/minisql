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

// COMMON_NODE_HEADER:
//   NODE_TYPE, IS_ROOT, PARENT_POINTER
const NODE_TYPE_SIZE: usize = 1;
const NODE_TYPE_OFFSET: usize = 0;
const IS_ROOT_SIZE: usize = 1;
const IS_ROOT_OFFSET: usize = NODE_TYPE_OFFSET + NODE_TYPE_SIZE;
const PARENT_POINTER_SIZE: usize = 4;
const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// LEAF NODE HEADER
//   COMMON_NODE_HEADER, NUM_CELLS
const LEAF_NODE_NUM_CELLS_SIZE: usize = 4;
const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// LEAF NODE BODY
//  {NODE_KEY, NODE_VALUE}...
const LEAF_NODE_KEY_SIZE: usize = 8;
const LEAF_NODE_KEY_OFFSET: usize = 0;
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
const LEAF_NODE_VALUE_OFFSET: usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

#[derive(Debug, Clone)]
pub struct Node {
    pub buf: [u8; PAGE_SIZE],
}
impl Node {
    pub fn new(buf: [u8; PAGE_SIZE]) -> Self {
        Node { buf }
    }
    pub fn num_cells(&mut self) -> &mut u8 {
        &mut self.buf[LEAF_NODE_CELL_SIZE]
    }
    pub fn get_num_cells(&self) -> u8 {
        self.buf[LEAF_NODE_CELL_SIZE]
    }
    pub fn cell(&mut self, cell: usize) -> &mut [u8] {
        let start = LEAF_NODE_HEADER_SIZE + cell * LEAF_NODE_CELL_SIZE;
        &mut self.buf[start..start + LEAF_NODE_CELL_SIZE]
    }
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
    pub fn value(&mut self, cell: usize) -> &mut [u8] {
        let start = LEAF_NODE_HEADER_SIZE + cell * LEAF_NODE_CELL_SIZE + LEAF_NODE_KEY_SIZE;
        &mut self.buf[start..start + LEAF_NODE_VALUE_SIZE]
    }
    pub fn get_value(&self, cell: usize) -> &[u8] {
        let start = LEAF_NODE_HEADER_SIZE + cell * LEAF_NODE_CELL_SIZE + LEAF_NODE_KEY_SIZE;
        &self.buf[start..start + LEAF_NODE_VALUE_SIZE]
    }
    pub fn init_leaf(&mut self) {
        self.buf[NODE_TYPE_OFFSET] = NodeType::Leaf as u8;
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
        let parent_pointer = u32::from_le_bytes(
            self.buf[PARENT_POINTER_OFFSET..PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE]
                .try_into()
                .unwrap(),
        );
        let num_cells = self.get_num_cells();
        write!(
            f,
            "NodeType: {}, IsRoot: {}, ParentPointer: {}, NumCells: {}\n",
            node_type, is_root, parent_pointer, num_cells
        )?;
        for i in 0..num_cells as usize {
            let key = self.get_key(i);
            let value = self.get_value(i);
            let row = Row::deserialize(value);
            write!(f, "Key: {}, Value: {}\n", key, row)?;
        }
        Ok(())
    }
}
