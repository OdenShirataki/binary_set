use std::{cmp::Ordering, num::NonZeroU32, path::Path};

use idx_file::{Found, IdxFile};
use various_data_file::{DataAddress, VariousDataFile};

pub struct BinarySet {
    index: IdxFile<DataAddress>,
    data_file: VariousDataFile,
}
impl BinarySet {
    /// Opens the file and creates the BinarySet.
    /// /// # Arguments
    /// * `path` - Path of file to save data
    /// * `allocation_lot` - Extends the specified size when the file size becomes insufficient due to data addition.
    /// If you expect to add a lot of data, specifying a larger size will improve performance.
    pub fn new<P: AsRef<Path>>(path: P, allocation_lot: u32) -> Self {
        let path = path.as_ref();
        let file_name = path.file_name().map_or("".into(), |f| f.to_string_lossy());
        Self {
            index: IdxFile::new(
                {
                    let mut path = path.to_path_buf();
                    path.set_file_name(&(file_name.to_string() + ".i"));
                    path
                },
                allocation_lot,
            ),
            data_file: VariousDataFile::new({
                let mut path = path.to_path_buf();
                path.set_file_name(&(file_name.into_owned() + ".d"));
                path
            }),
        }
    }

    /// Returns the value of the specified row. Returns None if the row does not exist.
    pub fn bytes(&self, row: NonZeroU32) -> Option<&'static [u8]> {
        self.index.value(row).map(|v| self.data_file.bytes(v))
    }

    fn search_end(&self, target: &[u8]) -> Found {
        self.index
            .search_end(|v| self.data_file.bytes(v).cmp(target))
    }

    /// Search for a sequence of bytes.
    pub fn row(&self, target: &[u8]) -> Option<NonZeroU32> {
        let found = self.search_end(target);
        let found_row = found.row();
        (found.ord() == Ordering::Equal && found_row != 0)
            .then_some(unsafe { NonZeroU32::new_unchecked(found_row) })
    }

    /// Finds a sequence of bytes, inserts it if it doesn't exist, and returns a row.
    pub fn row_or_insert(&mut self, content: &[u8]) -> NonZeroU32 {
        let found = self.search_end(content);
        let found_row = found.row();
        if found.ord() == Ordering::Equal && found_row != 0 {
            unsafe { NonZeroU32::new_unchecked(found_row) }
        } else {
            let row = self.index.create_row();
            unsafe {
                self.index.insert_unique(
                    row,
                    self.data_file.insert(content).address().clone(),
                    found,
                );
            }
            row
        }
    }
}
