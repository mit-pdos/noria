use prelude::*;

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Ingress {
    /// Parent domain
    src: Option<DomainIndex>,
}

impl Ingress {
    pub fn new() -> Ingress {
        Ingress::default()
    }

    pub fn set_src(&mut self, src: DomainIndex) {
        assert!(self.src.is_none());
        self.src = Some(src);
    }

    pub fn src(&self) -> DomainIndex {
        self.src.expect("ingress should have a parent domain")
    }

    pub(in crate::node) fn take(&mut self) -> Self {
        Clone::clone(self)
    }
}
