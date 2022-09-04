use std::cmp::Ordering;
use indexed_data_file::*;
use strings_set_file::*;

pub struct IndexedString{
    index: IndexedDataFile<WordAddress>
    ,strings:StringsSetFile
}
impl IndexedString{
    pub fn new(path_prefix:&str) -> Result<IndexedString,std::io::Error>{
        let index=IndexedDataFile::new(&(path_prefix.to_string()+".i"))?;
        let strings=StringsSetFile::new(&(path_prefix.to_string()+".d"))?;
        Ok(IndexedString{
            index
            ,strings
        })
    }
    pub fn into_string(&self,id:i64)->String{
        match self.index.tree().entity_data(id){
            Some(word)=>self.strings.to_str(word).to_string()
            ,None=>"".to_owned()
        }
    }
    fn search(&self,target: &str)->(Ordering,i64){
        let target_cstring=std::ffi::CString::new(target).unwrap();
        self.index.tree().search_cb(|s|->Ordering{
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
    pub fn id(&self,target: &str) -> Option<i64>{
        let (ord,found_id)=self.search(target);
        if ord==Ordering::Equal && found_id!=0{
            Some(found_id)
        }else{
            None
        }
    }
    pub fn entry(&mut self,target: &str) -> Option<i64>{
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