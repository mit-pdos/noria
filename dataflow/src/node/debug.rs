use std::fmt;
use node::{Node, NodeType};
use prelude::*;

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.inner {
            NodeType::Dropped => write!(f, "dropped node"),
            NodeType::Source => write!(f, "source node"),
            NodeType::Ingress => write!(f, "ingress node"),
            NodeType::Egress { .. } => write!(f, "egress node"),
            NodeType::Sharder { .. } => write!(f, "sharder"),
            NodeType::Reader(..) => write!(f, "reader node"),
            NodeType::Internal(ref i) => write!(f, "internal {} node", i.description()),
        }
    }
}

impl Node {
    pub fn describe(
        &self,
        idx: NodeIndex,
        materialization_status: MaterializationStatus,
    ) -> String {
        let mut s = String::new();
        let border = match self.sharded_by {
            Sharding::ByColumn(_, _) | Sharding::Random(_) => "filled,dashed",
            _ => "filled",
        };
        s.push_str(&format!(
            " [style=\"{}\", fillcolor={}, label=\"",
            border,
            self.domain
                .map(|d| -> usize { d.into() })
                .map(|d| format!("\"/set312/{}\"", (d % 12) + 1))
                .unwrap_or("white".into())
        ));

        let materialized = match materialization_status {
            MaterializationStatus::Not => "",
            MaterializationStatus::Partial => "| ▓",
            MaterializationStatus::Full => "| █",
        };

        let addr = match self.index {
            Some(ref idx) => if idx.has_local() {
                format!("{} / {}", idx.as_global().index(), **idx)
            } else {
                format!("{} / -", idx.as_global().index())
            },
            None => format!("{} / -", idx.index()),
        };

        match self.inner {
            NodeType::Source => s.push_str("(source)"),
            NodeType::Dropped => s.push_str("✗"),
            NodeType::Ingress => s.push_str(&format!(
                "{{ {{ {} {} }} | (ingress) }}",
                addr,
                materialized
            )),
            NodeType::Egress { .. } => s.push_str(&format!("{{ {} | (egress) }}", addr)),
            NodeType::Sharder { .. } => s.push_str(&format!("{{ {} | (sharder) }}", addr)),
            NodeType::Reader(ref r) => {
                let key = match r.key() {
                    None => String::from("none"),
                    Some(k) => format!("{}", k),
                };
                s.push_str(&format!(
                    "{{ {{ {} / {} {} }} | (reader / ⚷: {}) }}",
                    addr,
                    Self::escape(self.name()),
                    materialized,
                    key,
                ))
            }
            NodeType::Internal(ref i) => {
                s.push_str(&format!("{{"));

                // Output node name and description. First row.
                s.push_str(&format!(
                    "{{ {} / {} | {} {} }}",
                    addr,
                    Self::escape(self.name()),
                    Self::escape(&i.description()),
                    materialized
                ));

                // Output node outputs. Second row.
                s.push_str(&format!(" | {}", self.fields().join(", \\n")));

                // Maybe output node's HAVING conditions. Optional third row.
                // TODO
                // if let Some(conds) = n.node().unwrap().having_conditions() {
                //     let conds = conds.iter()
                //         .map(|c| format!("{}", c))
                //         .collect::<Vec<_>>()
                //         .join(" ∧ ");
                //     write!(f, " | σ({})", escape(&conds))?;
                // }

                s.push_str(" }}")
            }
        };

        s.push_str("\"]\n");

        s
    }

    fn escape(s: &str) -> String {
        use regex::Regex;

        Regex::new("([\"|{}])")
            .unwrap()
            .replace_all(s, "\\$1")
            .to_string()
    }
}
