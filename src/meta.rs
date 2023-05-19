use crate::node::{Node, POINTER_SIZE};

pub struct MetaRef {
    pub node: Node,
}
pub struct MetaMut {
    pub node_erf: MetaRef,
}

pub const META_NODE_NUM: usize = 0;
pub const DEFAULT_ROOT_NUM: usize = 1;
const META_ROOT_NODE_SIZE: usize = POINTER_SIZE;
const MEAT_ROOT_OFFSET: usize = 0;

impl MetaRef {
    pub fn new(node: Node) -> Self {
        Self { node }
    }
    pub fn get_root_num(&self) -> usize {
        usize::from_le_bytes(
            self.node.page.borrow().buf[MEAT_ROOT_OFFSET..MEAT_ROOT_OFFSET + META_ROOT_NODE_SIZE]
                .try_into()
                .unwrap(),
        )
    }
}
impl MetaMut {
    pub fn new(node: Node) -> Self {
        Self {
            node_erf: MetaRef::new(node),
        }
    }
    pub fn init(&self) {
        self.set_root_num(DEFAULT_ROOT_NUM);
    }
    pub fn set_root_num(&self, root_num: usize) {
        self.node_erf.node.page.borrow_mut().buf
            [MEAT_ROOT_OFFSET..MEAT_ROOT_OFFSET + META_ROOT_NODE_SIZE]
            .copy_from_slice(&root_num.to_le_bytes());
    }
}

#[cfg(test)]
mod test {
    use crate::pager::new_page;

    use super::*;
    #[test]
    fn test_meta() {
        let node = Node::new(new_page());
        let meta = node.init_meta();
        assert_eq!(meta.node_erf.get_root_num(), DEFAULT_ROOT_NUM);
        meta.set_root_num(2);
        assert_eq!(meta.node_erf.get_root_num(), 2);
    }
}
