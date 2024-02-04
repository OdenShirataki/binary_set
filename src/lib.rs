use std::{cmp::Ordering, num::NonZeroU32, ops::Deref, path::Path};

use idx_file::{AvltrieeOrd, IdxFile, IdxFileAllocator, IdxFileAvlTriee};
use various_data_file::{DataAddress, VariousDataFile};

type BinaryIdxFile = IdxFile<DataAddress, [u8]>;

pub struct BinarySet {
    index: BinaryIdxFile,
    data_file: VariousDataFile,
}

impl Deref for BinarySet {
    type Target = BinaryIdxFile;
    fn deref(&self) -> &Self::Target {
        &self.index
    }
}

impl AsRef<IdxFileAvlTriee<DataAddress, [u8]>> for BinarySet {
    fn as_ref(&self) -> &IdxFileAvlTriee<DataAddress, [u8]> {
        self
    }
}

impl AvltrieeOrd<DataAddress, [u8], IdxFileAllocator<DataAddress>> for BinarySet {
    fn cmp(&self, left: &DataAddress, right: &[u8]) -> Ordering {
        self.data_file.bytes(left).cmp(right)
    }
}

impl BinarySet {
    /// Opens the file and creates the BinarySet.
    /// /// # Arguments
    /// * `path` - Path of file to save data
    /// * `allocation_lot` - Extends the specified size when the file size becomes insufficient due to data addition.
    /// If you expect to add a lot of data, specifying a larger size will improve performance.
    pub fn new<P: AsRef<Path>>(path: P, allocation_lot: u32) -> Self {
        let path = path.as_ref();
        Self {
            index: IdxFile::new(path.with_extension("i"), allocation_lot),
            data_file: VariousDataFile::new(path.with_extension("d")),
        }
    }

    /// Returns the value of the specified row. Returns None if the row does not exist.
    pub fn bytes(&self, row: NonZeroU32) -> Option<&[u8]> {
        self.index.get(row).map(|v| self.data_file.bytes(v))
    }

    /// Search for a sequence of bytes.
    pub fn row(&self, content: &[u8]) -> Option<NonZeroU32> {
        let found = self.search_edge(self, content);
        (found.ord() == Ordering::Equal)
            .then(|| found.row())
            .flatten()
    }

    /// Finds a sequence of bytes, inserts it if it doesn't exist, and returns a row.
    pub fn row_or_insert(&mut self, content: &[u8]) -> NonZeroU32 {
        let found = self.search_edge(self, content);
        if let (Ordering::Equal, Some(found_row)) = (found.ord(), found.row()) {
            found_row
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
}
