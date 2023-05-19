use array_macro::array;
use std::{
    cell::{Cell, RefCell},
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    rc::Rc,
};

use crate::{
    meta::{DEFAULT_ROOT_NUM, META_NODE_NUM},
    node::Node,
    sql_error::{SqlError, SqlResult},
};

pub const PAGE_SIZE: usize = 4096;
pub const MAX_PAGES: usize = 100;

#[derive(Debug, Clone)]
pub struct PageBuffer {
    pub buf: [u8; PAGE_SIZE],
}
impl PageBuffer {
    fn new() -> Self {
        Self {
            buf: [0; PAGE_SIZE],
        }
    }
    fn from_buf(buf: [u8; PAGE_SIZE]) -> Self {
        Self { buf }
    }
    fn to_page(&self) -> Page {
        Rc::new(RefCell::new(Box::new(self.clone())))
    }
}
pub type Page = Rc<RefCell<Box<PageBuffer>>>;

#[allow(dead_code)]
pub fn new_page() -> Page {
    PageBuffer::new().to_page()
}

type PageContainer = RefCell<Box<[Option<Page>; MAX_PAGES]>>;
pub struct Pager {
    pub file: RefCell<File>,
    pub file_length: usize,
    pub num_pages: Cell<usize>,
    pub pages: PageContainer,
}

impl Pager {
    pub fn open(filename: &str) -> SqlResult<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)
            .map_err(|e| SqlError::IOError(e, "Failed to open file".to_string()))?;

        let file_length = file.metadata().unwrap().len() as usize;
        let num_pages = file_length / PAGE_SIZE;
        if file_length % PAGE_SIZE != 0 {
            return Err(SqlError::CorruptFile);
        }
        let pages = array![None; MAX_PAGES];
        let pager = Pager {
            file: RefCell::new(file),
            file_length,
            num_pages: Cell::new(num_pages),
            pages: RefCell::new(Box::new(pages)),
        };
        if pager.num_pages.get() == 0 {
            pager.init_db()?
        }
        Ok(pager)
    }
    fn init_db(&self) -> SqlResult<()> {
        let page = self.node(META_NODE_NUM)?;
        page.init_meta();
        let page = self.node(DEFAULT_ROOT_NUM)?;
        page.init_leaf();
        page.set_root(true);
        Ok(())
    }
    pub fn node(&self, page_num: usize) -> SqlResult<Node> {
        if page_num >= MAX_PAGES {
            return Err(SqlError::TableFull);
        }
        let mut pages = self.pages.borrow_mut();
        let page = &pages[page_num];
        if page.is_none() {
            let mut buf = [0u8; PAGE_SIZE];
            let num_pages: usize = (self.file_length + PAGE_SIZE - 1) / PAGE_SIZE;
            if page_num < num_pages {
                self.file
                    .borrow_mut()
                    .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                    .map_err(|e| SqlError::IOError(e, "Failed to seek to read".to_string()))?;
                self.file
                    .borrow_mut()
                    .read(&mut buf)
                    .map_err(|e| SqlError::IOError(e, "Failed to read".to_string()))?;
            }
            pages[page_num] = Some(PageBuffer::from_buf(buf).to_page());
            if page_num >= self.num_pages.get() {
                self.num_pages.set(page_num + 1);
            }
        }
        Ok(Node::new(pages[page_num].as_ref().unwrap().to_owned()))
    }
    pub fn flush(&self, page_num: usize) -> SqlResult<()> {
        if self.pages.borrow()[page_num].is_none() {
            return Ok(());
        }
        self.file
            .borrow_mut()
            .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
            .map_err(|e| SqlError::IOError(e, "Failed to seek to write".to_string()))?;
        let pages = self.pages.borrow();
        let buf = &pages[page_num].as_ref().unwrap().borrow().buf;

        self.file
            .borrow_mut()
            .write_all(buf.as_slice())
            .map_err(|e| SqlError::IOError(e, "Failed to write".to_string()))?;
        Ok(())
    }
    pub fn drop(&mut self, page_num: usize) {
        self.pages.borrow_mut()[page_num] = None;
    }
    pub fn new_page_num(&self) -> usize {
        self.num_pages.get()
    }
}
