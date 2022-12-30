use idx_sized::IdxSized;
use std::{cmp::Ordering, io, path::Path};
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
    pub unsafe fn bytes(&self, row: u32) -> &[u8] {
        match self.index.triee().value(row) {
            Some(word) => self.data.bytes(word),
            None => b"",
        }
    }
    pub unsafe fn str(&self, row: u32) -> &str {
        std::str::from_utf8(self.bytes(row)).unwrap()
    }
    fn search(&self, target: &[u8]) -> (Ordering, u32) {
        self.index
            .triee()
            .search_cb(|s| -> Ordering { target.cmp(unsafe { self.data.bytes(s) }) })
    }
    pub fn find_row(&self, target: &[u8]) -> Option<u32> {
        let (ord, found_row) = self.search(target);
        if ord == Ordering::Equal && found_row != 0 {
            Some(found_row)
        } else {
            None
        }
    }
    pub fn entry(&mut self, target: &[u8]) -> io::Result<u32> {
        let (ord, found_row) = self.search(target);
        if ord == Ordering::Equal && found_row != 0 {
            Ok(found_row)
        } else {
            let data = self.data.insert(target)?;
            self.index
                .insert_unique(data.address().clone(), found_row, ord, 0)
        }
    }
}

#[test]
fn test() {
    let dir = "./ib-test";
    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
        std::fs::create_dir_all(dir).unwrap();
    } else {
        std::fs::create_dir_all(dir).unwrap();
    }
    if let Ok(mut s) = IdxBinary::new(&(dir.to_owned() + "/test")) {
        s.entry(b"US").unwrap();
        s.entry(b"US").unwrap();

        s.entry(b"US").unwrap();
        s.entry(b"US").unwrap();

        s.entry(b"UK").unwrap();

        s.entry(b"US").unwrap();
        s.entry(b"US").unwrap();
        s.entry(b"UK").unwrap();
    }
}
