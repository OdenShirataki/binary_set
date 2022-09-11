use std::cmp::Ordering;
use indexed_data_file::*;
use strings_set_file::*;

pub struct IndexedStringFile{
    index: IndexedDataFile<WordAddress>
    ,strings:StringsSetFile
}
impl IndexedStringFile{
    pub fn new(path_prefix:&str) -> Result<IndexedStringFile,std::io::Error>{
        let index=IndexedDataFile::new(&(path_prefix.to_string()+".i"))?;
        let strings=StringsSetFile::new(&(path_prefix.to_string()+".d"))?;
        Ok(IndexedStringFile{
            index
            ,strings
        })
    }
    pub fn into_string(&self,id:u32)->String{
        match self.index.triee().entity_value(id){
            Some(word)=>self.strings.to_str(word).to_string()
            ,None=>"".to_owned()
        }
    }
    fn search(&self,target: &str)->(Ordering,u32){
        let target_cstring=std::ffi::CString::new(target).unwrap();
        self.index.triee().search_cb(|s|->Ordering{
            let cmp=unsafe{libc::strcmp(
                target_cstring.as_ptr()
                ,self.strings.offset(s.offset() as isize)
            ) as isize};
            if cmp<0{
                Ordering::Less
            }else if cmp>0{
                Ordering::Greater
            }else{
                Ordering::Equal
            }
        })
    }
    pub fn id(&self,target: &str) -> Option<u32>{
        let (ord,found_id)=self.search(target);
        if ord==Ordering::Equal && found_id!=0{
            Some(found_id)
        }else{
            None
        }
    }
    pub fn entry(&mut self,target: &str) -> Option<u32>{
        let (ord,found_id)=self.search(target);
        if ord==Ordering::Equal && found_id!=0{
            Some(found_id)
        }else{
            if let Some(ystr)=self.strings.insert(target){
                self.index.add_new(ystr.address(),found_id,ord)
            }else{
                None
            }
        }
    }
}