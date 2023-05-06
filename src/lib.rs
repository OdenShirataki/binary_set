use std::{cmp::Ordering, io, path::Path};

use idx_sized::{anyhow::Result, Found, IdxSized};
use various_data_file::{DataAddress, VariousDataFile};

pub struct IdxBinary {
    index: IdxSized<DataAddress>,
    data: VariousDataFile,
}
impl IdxBinary {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref();
        let file_name = if let Some(file_name) = path.file_name() {
            file_name.to_string_lossy()
        } else {
            "".into()
        };
        Ok(IdxBinary {
            index: IdxSized::new({
                let mut path = path.to_path_buf();
                path.set_file_name(&(file_name.to_string() + ".i"));
                path
            })?,
            data: VariousDataFile::new({
                let mut path = path.to_path_buf();
                path.set_file_name(&(file_name.into_owned() + ".d"));
                path
            })?,
        })
    }
    pub unsafe fn bytes(&self, row: u32) -> &'static [u8] {
        match self.index.value(row) {
            Some(ref word) => self.data.bytes(word),
            None => b"",
        }
    }
    fn search(&self, target: &[u8]) -> Found {
        self.index
            .triee()
            .search(|s| unsafe { self.data.bytes(s) }.cmp(target))
    }
    pub fn find_row(&self, target: &[u8]) -> Option<u32> {
        let found = self.search(target);
        let found_row = found.row();
        if found.ord() == Ordering::Equal && found_row != 0 {
            Some(found_row)
        } else {
            None
        }
    }

    pub fn entry(&mut self, target: &[u8]) -> Result<u32> {
        let found = self.search(target);
        let found_row = found.row();
        if found.ord() == Ordering::Equal && found_row != 0 {
            Ok(found_row)
        } else {
            Ok(self
                .index
                .insert_unique(self.data.insert(target)?.address().clone(), found)?)
        }
    }
}
