use std::cmp::Ordering;
use idx_sized::IdxSized;
use various_data_file::{
    DataAddress
    ,VariousDataFile
};

pub struct IdxBinary{
    index: IdxSized<DataAddress>
    ,strings:VariousDataFile
}
impl IdxBinary{
    pub fn new(path_prefix:&str) -> Result<IdxBinary,std::io::Error>{
        let index=IdxSized::new(&(path_prefix.to_string()+".i"))?;
        let strings=VariousDataFile::new(&(path_prefix.to_string()+".d"))?;
        Ok(IdxBinary{
            index
            ,strings
        })
    }
    pub fn str(&self,row:u32)->&str{
        match self.index.triee().value(row){
            Some(word)=>{
                std::str::from_utf8(self.strings.slice(word)).unwrap()
            }
            ,None=>""
        }
    }
    pub fn string(&self,row:u32)->String{
        self.str(row).to_string()
    }
    fn search(&self,target: &[u8])->(Ordering,u32){
        self.index.triee().search_cb(|s|->Ordering{
            target.cmp(self.strings.slice(s))
        })
    }
    pub fn row(&self,target: &[u8]) -> Option<u32>{
        let (ord,found_row)=self.search(target);
        if ord==Ordering::Equal && found_row!=0{
            Some(found_row)
        }else{
            None
        }
    }
    pub fn entry(&mut self,target: &[u8]) -> Option<u32>{
        let (ord,found_row)=self.search(target);
        if ord==Ordering::Equal && found_row!=0{
            Some(found_row)
        }else{
            if let Some(ystr)=self.strings.insert(target){
                self.index.insert_unique(ystr.address(),found_row,ord,0)
            }else{
                None
            }
        }
    }
}