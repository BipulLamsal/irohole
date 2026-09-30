use anyhow::bail;
use irohole_proto::{Plugin, WireId};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default, Clone)]
pub struct Registry {
    inner: HashMap<WireId, Arc<dyn Plugin>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<P: Plugin>(&mut self, plugin: P) -> anyhow::Result<()> {
        let id = plugin.wire_id();
        if self.inner.contains_key(&id) {
            bail!("wire id {:?} already registered", id.as_u8());
        }
        self.inner.insert(id, Arc::new(plugin));
        Ok(())
    }

    pub fn get(&self, wire_id: WireId) -> Option<Arc<dyn Plugin>> {
        self.inner.get(&wire_id).cloned()
    }

    pub fn ids(&self) -> Vec<WireId> {
        self.inner.keys().copied().collect()
    }
}
