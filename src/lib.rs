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
    pub fn bytes(&self, row: NonZeroU32) -> Option<&[u8]> {
        self.index.get(row).map(|v| self.data_file.bytes(v))
    }

    /// Search for a sequence of bytes.
    pub fn row(&self, content: &[u8]) -> Option<NonZeroU32> {
        let found = self.search(content);
        if found.ord() == Ordering::Equal {
            Some(found.row().unwrap())
        } else {
            None
        }
    }

    /// Finds a sequence of bytes, inserts it if it doesn't exist, and returns a row.
    pub fn row_or_insert(&mut self, content: &[u8]) -> NonZeroU32 {
        let found = self.search(content);
        let found_row = found.row();
        if found.ord() == Ordering::Equal && found_row.is_some() {
            found_row.unwrap()
        } else {
            let row = unsafe { NonZeroU32::new_unchecked(self.index.rows_count() + 1) };
            unsafe {
                self.index.insert_unique_unchecked(
                    row,
                    self.data_file.insert(content).address().clone(),
                    found,
                );
            }
            row
        }
    }

    fn search(&self, target: &[u8]) -> Found {
        self.index.search(|v| self.data_file.bytes(v).cmp(target))
    }
}
