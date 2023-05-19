use std::{
    cell::{Ref, RefMut},
    fmt::Display,
    ops::Deref,
};

use crate::{
    meta::{MetaMut, MetaRef},
    pager::{Page, PageBuffer, PAGE_SIZE},
    table::{Row, ROW_SIZE},
};

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum NodeType {
    Internal = 0,
    Leaf,
}

pub const POINTER_SIZE: usize = std::mem::size_of::<usize>();

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
#[allow(dead_code)]
const LEAF_NODE_KEY_OFFSET: usize = 0;
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
#[allow(dead_code)]
const LEAF_NODE_VALUE_OFFSET: usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
#[allow(dead_code)]
const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
// pub const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
pub const LEAF_NODE_MAX_CELLS: usize = 4; // DEBUG: 4 for testing

// INTERNAL NODE HEADER
const INTERNAL_NODE_NUM_KEYS_SIZE: usize = POINTER_SIZE;
const INTERNAL_NODE_NUM_KEYS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const INTERNAL_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE;

// INTERNAL NODE BODY
//   {INTERNAL_NODE_CHILD, INTERNAL_NODE_KEY}...
const INTERNAL_NODE_CHILD_SIZE: usize = POINTER_SIZE;
const INTERNAL_NODE_KEY_SIZE: usize = 8;
const INTERNAL_NODE_CELL_SIZE: usize = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
pub const INTERNAL_NODE_MAX_CELLS: usize = 4; // DEBUG: 4 for testing

// Node Splitting
pub const LEAF_NODE_LEFT_SPLIT_COUNT: usize = (LEAF_NODE_MAX_CELLS + 2) / 2;
pub const LEAF_NODE_RIGHT_SPLIT_COUNT: usize = LEAF_NODE_MAX_CELLS + 1 - LEAF_NODE_LEFT_SPLIT_COUNT;

pub const INTERNAL_NODE_LEFT_SPLIT_COUNT: usize = (INTERNAL_NODE_MAX_CELLS + 2) / 2;
pub const INTERNAL_NODE_RIGHT_SPLIT_COUNT: usize =
    INTERNAL_NODE_MAX_CELLS + 1 - INTERNAL_NODE_LEFT_SPLIT_COUNT;

#[derive(Debug, Clone)]
pub struct Node {
    pub page: Page,
}

#[derive(Debug, Clone)]
pub struct InternalRef {
    pub node: Node,
}
#[derive(Debug, Clone)]
pub struct LeafRef {
    pub node: Node,
}

#[derive(Debug, Clone)]
pub enum NodeRef {
    Internal(InternalRef),
    Leaf(LeafRef),
}
#[derive(Debug, Clone)]
pub struct InternalMut {
    pub node_ref: InternalRef,
}
#[derive(Debug, Clone)]
pub struct LeafMut {
    pub node_ref: LeafRef,
}

#[derive(Debug, Clone)]
pub enum NodeMut {
    Internal(InternalMut),
    Leaf(LeafMut),
}

impl Node {
    pub fn new(page: Page) -> Self {
        Self { page }
    }
    pub fn raw_buf(&self) -> RefMut<[u8]> {
        RefMut::map(self.page.borrow_mut(), |page| &mut page.buf[..])
    }
    // Leaf Node
    pub fn init_leaf(&self) -> LeafMut {
        self.set_type(NodeType::Leaf);
        self.set_root(false);
        let leaf = self.leaf_node_mut();
        leaf.set_num_cells(0);
        leaf.set_next_leaf(0); // 0 represents no sibling
        leaf
    }
    pub fn leaf_node_mut(&self) -> LeafMut {
        assert!(self.is_leaf());
        LeafMut {
            node_ref: self.leaf_node(),
        }
    }
    pub fn leaf_node(&self) -> LeafRef {
        assert!(self.is_leaf());
        LeafRef { node: self.clone() }
    }

    // Internal Node
    pub fn init_internal(&self) -> InternalMut {
        self.set_type(NodeType::Internal);
        self.set_root(false);
        let internal = self.internal_node_mut();
        internal.set_num_keys(0);
        internal
    }
    pub fn internal_node_mut(&self) -> InternalMut {
        assert!(self.is_internal());
        InternalMut {
            node_ref: self.internal_node(),
        }
    }
    pub fn internal_node(&self) -> InternalRef {
        assert!(self.is_internal());
        InternalRef { node: self.clone() }
    }

    // Common Node
    pub fn set_root(&self, is_root: bool) {
        self.page.borrow_mut().buf[IS_ROOT_OFFSET] = is_root as u8;
    }
    pub fn is_root(&self) -> bool {
        self.page.borrow().buf[IS_ROOT_OFFSET] == 1
    }
    pub fn set_type(&self, node_type: NodeType) {
        self.page.borrow_mut().buf[NODE_TYPE_OFFSET] = node_type as u8;
    }
    pub fn get_type(&self) -> NodeType {
        match self.page.borrow().buf[NODE_TYPE_OFFSET] {
            0 => NodeType::Internal,
            1 => NodeType::Leaf,
            _ => panic!("Unknown node type"),
        }
    }
    pub fn is_leaf(&self) -> bool {
        self.page.borrow().buf[NODE_TYPE_OFFSET] == NodeType::Leaf as u8
    }
    pub fn is_internal(&self) -> bool {
        self.page.borrow().buf[NODE_TYPE_OFFSET] == NodeType::Internal as u8
    }
    pub fn as_typed(&self) -> NodeRef {
        if self.is_leaf() {
            NodeRef::Leaf(self.leaf_node())
        } else {
            NodeRef::Internal(self.internal_node())
        }
    }
    pub fn as_typed_mut(&mut self) -> NodeMut {
        if self.is_leaf() {
            NodeMut::Leaf(self.leaf_node_mut())
        } else {
            NodeMut::Internal(self.internal_node_mut())
        }
    }

    // Parent Node
    pub fn set_parent(&self, parent: usize) {
        self.page.borrow_mut().buf
            [PARENT_POINTER_OFFSET..PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE]
            .copy_from_slice(&parent.to_le_bytes())
    }
    pub fn get_parent(&self) -> usize {
        usize::from_le_bytes(
            self.page.borrow().buf
                [PARENT_POINTER_OFFSET..PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE]
                .try_into()
                .unwrap(),
        )
    }

    // Max Key (internal and leaf)
    pub fn get_first_key(&self) -> u64 {
        match self.as_typed() {
            NodeRef::Internal(internal) => internal.get_key_at(0),
            NodeRef::Leaf(leaf) => leaf.get_key(0),
        }
    }

    // Borrow Map
    pub fn borrow_map<T, F>(&self, f: F) -> Ref<T>
    where
        F: FnOnce(&Box<PageBuffer>) -> &T,
        T: ?Sized,
    {
        Ref::map(self.page.borrow(), f)
    }
    pub fn borrow_mut_map<T, F>(&self, f: F) -> RefMut<T>
    where
        F: FnOnce(&mut Box<PageBuffer>) -> &mut T,
        T: ?Sized,
    {
        RefMut::map(self.page.borrow_mut(), f)
    }

    // Meta
    pub fn meta_node(&self) -> MetaRef {
        MetaRef::new(self.clone())
    }
    pub fn meta_node_mut(&self) -> MetaMut {
        MetaMut::new(self.clone())
    }
    pub fn init_meta(&self) -> MetaMut {
        let meta = MetaMut::new(self.clone());
        meta.init();
        meta
    }
}

impl LeafRef {
    pub fn get_cell(&self, cell: usize) -> Ref<[u8]> {
        let start = LEAF_NODE_HEADER_SIZE + cell * LEAF_NODE_CELL_SIZE;
        self.node
            .borrow_map(|page| &page.buf[start..start + LEAF_NODE_CELL_SIZE])
    }
    pub fn get_num_cells(&self) -> usize {
        let start = LEAF_NODE_NUM_CELLS_OFFSET;
        usize::from_le_bytes(
            self.node.page.borrow().buf[start..start + LEAF_NODE_NUM_CELLS_SIZE]
                .try_into()
                .unwrap(),
        )
    }
    pub fn get_key(&self, cell: usize) -> u64 {
        let start = LEAF_NODE_HEADER_SIZE + cell * LEAF_NODE_CELL_SIZE;
        u64::from_le_bytes(
            self.node.page.borrow().buf[start..start + LEAF_NODE_KEY_SIZE]
                .try_into()
                .unwrap(),
        )
    }
    pub fn get_value(&self, cell: usize) -> Ref<[u8]> {
        let start = LEAF_NODE_HEADER_SIZE + cell * LEAF_NODE_CELL_SIZE + LEAF_NODE_KEY_SIZE;
        self.node
            .borrow_map(|page| &page.buf[start..start + LEAF_NODE_VALUE_SIZE])
    }
    pub fn get_next_leaf(&self) -> usize {
        usize::from_le_bytes(
            self.node.page.borrow().buf
                [LEAF_NODE_NEXT_LEAF_OFFSET..LEAF_NODE_NEXT_LEAF_OFFSET + LEAF_NODE_NEXT_LEAF_SIZE]
                .try_into()
                .unwrap(),
        )
    }
}

impl LeafMut {
    pub fn set_num_cells(&self, num_cells: usize) {
        let start = LEAF_NODE_NUM_CELLS_OFFSET;
        self.node.page.borrow_mut().buf[start..start + LEAF_NODE_NUM_CELLS_SIZE]
            .copy_from_slice(&num_cells.to_le_bytes())
    }
    pub fn set_next_leaf(&self, next_leaf: usize) {
        self.node.page.borrow_mut().buf
            [LEAF_NODE_NEXT_LEAF_OFFSET..LEAF_NODE_NEXT_LEAF_OFFSET + LEAF_NODE_NEXT_LEAF_SIZE]
            .copy_from_slice(&next_leaf.to_le_bytes())
    }
    pub fn set_key(&self, cell: usize, key: u64) {
        let start = LEAF_NODE_HEADER_SIZE + cell * LEAF_NODE_CELL_SIZE;
        self.node.page.borrow_mut().buf[start..start + LEAF_NODE_KEY_SIZE]
            .copy_from_slice(&key.to_le_bytes())
    }
    pub fn cell(&self, cell: usize) -> RefMut<[u8]> {
        let start = LEAF_NODE_HEADER_SIZE + cell * LEAF_NODE_CELL_SIZE;
        self.node
            .borrow_mut_map(|page| &mut page.buf[start..start + LEAF_NODE_CELL_SIZE])
    }
    pub fn value(&self, cell: usize) -> RefMut<[u8]> {
        let start = LEAF_NODE_HEADER_SIZE + cell * LEAF_NODE_CELL_SIZE + LEAF_NODE_KEY_SIZE;
        self.node
            .borrow_mut_map(|page| &mut page.buf[start..start + LEAF_NODE_VALUE_SIZE])
    }
}

impl InternalRef {
    pub fn get_num_keys(&self) -> usize {
        usize::from_le_bytes(
            self.node.page.borrow().buf
                [INTERNAL_NODE_NUM_KEYS_OFFSET..INTERNAL_NODE_NUM_KEYS_OFFSET + 8]
                .try_into()
                .unwrap(),
        )
    }
    pub fn get_key_at(&self, cell: usize) -> u64 {
        let start =
            INTERNAL_NODE_HEADER_SIZE + cell * INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_CHILD_SIZE;
        u64::from_le_bytes(
            self.node.page.borrow().buf[start..start + INTERNAL_NODE_KEY_SIZE]
                .try_into()
                .unwrap(),
        )
    }
    pub fn get_child_at(&self, cell: usize) -> usize {
        let start = INTERNAL_NODE_HEADER_SIZE + cell * INTERNAL_NODE_CELL_SIZE;
        usize::from_le_bytes(
            self.node.page.borrow().buf[start..start + INTERNAL_NODE_CHILD_SIZE]
                .try_into()
                .unwrap(),
        )
    }
    // Find key
    pub fn find_key(&self, key: u64) -> Option<usize> {
        let mut min_index = 0;
        let mut max_index = self.get_num_keys();
        println!("min {} max {}; node{}", min_index, max_index, self.node);
        while min_index < max_index {
            let index = (min_index + max_index) / 2;
            let key_at_index = self.get_key_at(index);
            if key_at_index > key {
                max_index = index;
            } else {
                min_index = index + 1;
            }
        }
        if min_index == 0 {
            return None;
        }
        Some(min_index - 1 as usize)
    }
}

impl InternalMut {
    pub fn set_num_keys(&self, num_keys: usize) {
        self.node.page.borrow_mut().buf
            [INTERNAL_NODE_NUM_KEYS_OFFSET..INTERNAL_NODE_NUM_KEYS_OFFSET + 8]
            .copy_from_slice(&num_keys.to_le_bytes())
    }
    pub fn set_key_at(&self, cell: usize, key: u64) {
        let start =
            INTERNAL_NODE_HEADER_SIZE + cell * INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_CHILD_SIZE;
        self.node.page.borrow_mut().buf[start..start + INTERNAL_NODE_KEY_SIZE]
            .copy_from_slice(&key.to_le_bytes())
    }

    pub fn set_child_at(&self, cell: usize, child: usize) {
        let start = INTERNAL_NODE_HEADER_SIZE + cell * INTERNAL_NODE_CELL_SIZE;
        self.node.page.borrow_mut().buf[start..start + INTERNAL_NODE_CHILD_SIZE]
            .copy_from_slice(&child.to_le_bytes())
    }
}

impl Deref for InternalMut {
    type Target = InternalRef;
    fn deref(&self) -> &Self::Target {
        &self.node_ref
    }
}
impl Deref for LeafMut {
    type Target = LeafRef;
    fn deref(&self) -> &Self::Target {
        &self.node_ref
    }
}
impl Deref for LeafRef {
    type Target = Node;
    fn deref(&self) -> &Self::Target {
        &self.node
    }
}
impl Deref for InternalRef {
    type Target = Node;
    fn deref(&self) -> &Self::Target {
        &self.node
    }
}

impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let node_type = match self.get_type() {
            NodeType::Internal => "Internal",
            NodeType::Leaf => "Leaf",
        };
        let is_root = if self.is_root() { "Yes" } else { "No" };
        let parent_page = self.get_parent();
        write!(
            f,
            "NodeType: {}, IsRoot: {}, Parent: {}",
            node_type, is_root, parent_page
        )?;
        match self.as_typed() {
            NodeRef::Leaf(leaf) => {
                let num_cells = leaf.get_num_cells();
                writeln!(
                    f,
                    " ( NumCells: {}, NextLeaf {} ) ",
                    num_cells,
                    leaf.get_next_leaf()
                )?;
                for i in 0..num_cells as usize {
                    let key = leaf.get_key(i);
                    let value = leaf.get_value(i);
                    let row = Row::deserialize(&value);
                    writeln!(f, "[{}] {}", key, row)?;
                }
            }
            NodeRef::Internal(internal) => {
                let num_keys = internal.get_num_keys();
                writeln!(f, " ( NumKeys: {} )", num_keys)?;
                for i in 0..num_keys as usize {
                    let child = internal.get_child_at(i);
                    let key = internal.get_key_at(i);
                    write!(f, "[{}] {} ", key, child)?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_eq;

    use crate::pager::new_page;

    use super::*;

    #[test]
    fn test_leaf() {
        let node = Node::new(new_page());
        let leaf = node.init_leaf();
        assert_eq!(leaf.node.is_leaf(), true);
        assert_eq!(leaf.node.is_internal(), false);
        assert_eq!(leaf.get_num_cells(), 0);
        leaf.set_num_cells(1);
        assert_eq!(leaf.get_num_cells(), 1);
        leaf.set_key(0, 1);
        assert_eq!(leaf.get_key(0), 1);
        let row = [2u8; ROW_SIZE];
        leaf.value(0).copy_from_slice(&row);
        assert_eq!(*leaf.get_value(0), row);
        leaf.set_next_leaf(1);
        assert_eq!(leaf.get_next_leaf(), 1);
    }
    #[test]
    fn test_internal() {
        let node = Node::new(new_page());
        let internal = node.init_internal();
        internal.node.set_root(true);
        assert_eq!(internal.node.is_root(), true);
        assert_eq!(internal.node.is_leaf(), false);
        assert_eq!(internal.node.is_internal(), true);
        assert_eq!(internal.get_num_keys(), 0);
        internal.set_num_keys(1);
        assert_eq!(internal.get_num_keys(), 1);
        internal.set_key_at(0, 1);
        assert_eq!(internal.get_key_at(0), 1);
        internal.set_child_at(0, 2);
        assert_eq!(internal.get_child_at(0), 2);
    }
    #[test]
    fn find_key() {
        let node = Node::new(new_page());
        let internal = node.init_internal();
        internal.set_num_keys(3);
        internal.set_key_at(0, 1);
        internal.set_key_at(1, 3);
        internal.set_key_at(2, 5);
        assert_eq!(internal.find_key(0), None);
        assert_eq!(internal.find_key(1), Some(0));
        assert_eq!(internal.find_key(2), Some(0));
        assert_eq!(internal.find_key(3), Some(1));
        assert_eq!(internal.find_key(4), Some(1));
        assert_eq!(internal.find_key(5), Some(2));
    }
}
