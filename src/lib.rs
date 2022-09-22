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
    pub fn into_string(&self,id:u32)->String{
        match self.index.triee().entity_value(id){
            Some(word)=>{
                std::str::from_utf8(self.strings.slice(word)).unwrap().to_string()
            }
            ,None=>"".to_owned()
        }
    }
    fn search(&self,target: &[u8])->(Ordering,u32){
        self.index.triee().search_cb(|s|->Ordering{
            target.cmp(self.strings.slice(s))
        })
    }
    pub fn id(&self,target: &[u8]) -> Option<u32>{
        let (ord,found_id)=self.search(target);
        if ord==Ordering::Equal && found_id!=0{
            Some(found_id)
        }else{
            None
        }
    }
    pub fn entry(&mut self,target: &[u8]) -> Option<u32>{
        let (ord,found_id)=self.search(target);
        if ord==Ordering::Equal && found_id!=0{
            Some(found_id)
        }else{
            if let Some(ystr)=self.strings.insert(target){
                self.index.insert_unique(ystr.address(),found_id,ord,0)
            }else{
                None
            }
        }
    }
}