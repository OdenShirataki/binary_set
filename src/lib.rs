use std::{cmp::Ordering, num::NonZeroU32, path::Path};

use idx_file::{Found, IdxFile};
use various_data_file::{DataAddress, VariousDataFile};

pub struct BinarySet {
    index: IdxFile<DataAddress>,
    data_file: VariousDataFile,
}
impl BinarySet {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let path = path.as_ref();
        let file_name = path.file_name().map_or("".into(), |f| f.to_string_lossy());
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

    #[inline(always)]
    pub unsafe fn bytes(&self, row: u32) -> &'static [u8] {
        self.index
            .value(row)
            .map_or(b"", |v| self.data_file.bytes(v))
    }

    #[inline(always)]
    fn search_end(&self, target: &[u8]) -> Found {
        self.index
            .search_end(|v| unsafe { self.data_file.bytes(v) }.cmp(target))
    }

    #[inline(always)]
    pub fn row(&self, target: &[u8]) -> Option<u32> {
        let found = self.search_end(target);
        let found_row = found.row();
        (found.ord() == Ordering::Equal && found_row != 0).then_some(found_row)
    }

    #[inline(always)]
    pub fn row_or_insert(&mut self, content: &[u8]) -> NonZeroU32 {
        let found = self.search_end(content);
        let found_row = found.row();
        if found.ord() == Ordering::Equal && found_row != 0 {
            unsafe { NonZeroU32::new_unchecked(found_row) }
        } else {
            let row = self.index.create_row();
            unsafe {
                self.index.insert_unique(
                    row.get(),
                    self.data_file.insert(content).address().clone(),
                    found,
                );
            }
            row
        }
    }
}
