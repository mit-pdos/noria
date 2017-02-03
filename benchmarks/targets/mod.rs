pub trait Backend {
    type P: Putter;
    type G: Getter;

    fn putter(&mut self) -> Self::P;
    fn getter(&mut self) -> Self::G;
    fn migrate(&mut self, usize) -> (Self::P, Vec<Self::G>);
}

pub trait Putter: Send {
    fn article<'a>(&'a mut self) -> Box<FnMut(i64, String) + 'a>;
    fn vote<'a>(&'a mut self) -> Box<FnMut(i64, i64) + 'a>;
}

pub trait Getter: Send {
    fn get<'a>(&'a mut self) -> Box<FnMut(i64) -> Result<Option<(i64, String, i64)>, ()> + 'a>;
}

pub mod soup;
#[cfg(feature="b_mysql")]
pub mod mysql;
#[cfg(feature="b_postgresql")]
pub mod postgres;
#[cfg(feature="b_netsoup")]
pub mod netsoup;
#[cfg(feature="b_memcached")]
pub mod memcached;
