use std::{cmp::Ordering, path::Path};

use idx_file::{Found, IdxFile};
use various_data_file::{DataAddress, VariousDataFile};

pub struct BinarySet {
    index: IdxFile<DataAddress>,
    data_file: VariousDataFile,
}
impl BinarySet {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let path = path.as_ref();
        let file_name = if let Some(file_name) = path.file_name() {
            file_name.to_string_lossy()
        } else {
            "".into()
        };
        Self {
            index: IdxFile::new({
                let mut path = path.to_path_buf();
                path.set_file_name(&(file_name.to_string() + ".i"));
                path
            }),
            data_file: VariousDataFile::new({
                let mut path = path.to_path_buf();
                path.set_file_name(&(file_name.into_owned() + ".d"));
                path
            }),
        }
    }
    pub unsafe fn bytes(&self, row: u32) -> &'static [u8] {
        match self.index.value(row) {
            Some(ref word) => self.data_file.bytes(word),
            None => b"",
        }
    }

    fn search_end(&self, target: &[u8]) -> Found {
        self.index
            .search_end(|s| unsafe { self.data_file.bytes(s) }.cmp(target))
    }

    pub fn row(&self, target: &[u8]) -> Option<u32> {
        let found = self.search_end(target);
        let found_row = found.row();
        if found.ord() == Ordering::Equal && found_row != 0 {
            Some(found_row)
        } else {
            None
        }
    }
    pub fn row_or_insert(&mut self, content: &[u8]) -> u32 {
        let found = self.search_end(content);
        let found_row = found.row();
        if found.ord() == Ordering::Equal && found_row != 0 {
            found_row
        } else {
            let row = self.index.new_row(0);
            let value = self.data_file.insert(content);
            unsafe {
                self.index
                    .insert_unique(row, value.address().clone(), found);
            }
            row
        }
    }
}
