use array_macro::array;
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
};

use crate::sql_error::SqlError;

pub const PAGE_SIZE: usize = 4096;
pub const MAX_PAGES: usize = 100;

pub struct Pager {
    pub file: File,
    pub file_length: usize,
    pub num_pages: usize,
    pub pages: [Option<Box<[u8; PAGE_SIZE]>>; MAX_PAGES],
}

impl Pager {
    pub fn open(filename: &str) -> Result<Self, SqlError> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)
            .map_err(|e| SqlError::IOError(e, "Failed to open file".to_string()))?;

        let file_length = file.metadata().unwrap().len() as usize;
        let num_pages = 0;
        let pages = array![None; MAX_PAGES];
        Ok(Pager {
            file,
            file_length,
            num_pages,
            pages,
        })
    }
    pub fn get_page(&mut self, page_num: usize) -> Result<&mut Box<[u8; PAGE_SIZE]>, SqlError> {
        if page_num >= MAX_PAGES {
            return Err(SqlError::TableFull);
        }
        let page = &mut self.pages[page_num];
        if page.is_none() {
            let mut buf = [0u8; PAGE_SIZE];
            let num_pages: usize = (self.file_length + PAGE_SIZE - 1) / PAGE_SIZE;
            if page_num < num_pages {
                self.file
                    .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                    .map_err(|e| SqlError::IOError(e, "Failed to seek to read".to_string()))?;
                self.file
                    .read(&mut buf)
                    .map_err(|e| SqlError::IOError(e, "Failed to read".to_string()))?;
            }
            self.pages[page_num] = Some(Box::new(buf));
        }
        Ok(self.pages[page_num].as_mut().unwrap())
    }
    pub fn flush(&mut self, page_num: usize, size: usize) -> Result<(), SqlError> {
        if self.pages[page_num].is_none() {
            return Ok(());
        }
        self.file
            .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
            .map_err(|e| SqlError::IOError(e, "Failed to seek to write".to_string()))?;
        self.file
            .write_all(&self.pages[page_num].as_ref().unwrap()[0..size])
            .map_err(|e| SqlError::IOError(e, "Failed to write".to_string()))?;
        Ok(())
    }
    pub fn drop(&mut self, page_num: usize) {
        self.pages[page_num] = None;
    }
}
