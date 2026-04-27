//! Paths into manifest bodies.
//!
//! The differ reports diffs by *path* into the YAML value tree. A
//! path is a sequence of segments — either a map key, a list index,
//! or a list-map key (the matched value of a list-map's identifying
//! field). Rendering a path produces a JSON-pointer-ish string that
//! is stable for diffing and human-readable in operator output.
//!
//! Example: `spec.template.spec.containers[name=app].image`.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSegment {
    /// Field of a YAML mapping (object) — e.g. `spec`, `metadata`.
    Field(String),
    /// Element of a positional list — e.g. `args[2]`. Used when the
    /// list has no list-map key registered.
    Index(usize),
    /// Element of a list-map (a list semantically keyed by one or
    /// more fields). `keys` is a key/value list rendered in the same
    /// order each time so the path is stable. `containers` keyed by
    /// `name=app` renders as `containers[name=app]`.
    Keyed(Vec<(String, String)>),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ValuePath {
    pub segments: Vec<PathSegment>,
}

impl ValuePath {
    pub fn root() -> Self {
        Self::default()
    }

    pub fn push(&self, seg: PathSegment) -> Self {
        let mut next = self.clone();
        next.segments.push(seg);
        next
    }

    pub fn field(&self, name: impl Into<String>) -> Self {
        self.push(PathSegment::Field(name.into()))
    }

    pub fn index(&self, i: usize) -> Self {
        self.push(PathSegment::Index(i))
    }

    pub fn keyed(&self, keys: Vec<(String, String)>) -> Self {
        self.push(PathSegment::Keyed(keys))
    }

    pub fn is_root(&self) -> bool {
        self.segments.is_empty()
    }
}

impl fmt::Display for ValuePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.segments.is_empty() {
            return f.write_str(".");
        }
        for (i, seg) in self.segments.iter().enumerate() {
            match seg {
                PathSegment::Field(name) => {
                    if i > 0 {
                        f.write_str(".")?;
                    }
                    f.write_str(name)?;
                }
                PathSegment::Index(idx) => write!(f, "[{idx}]")?,
                PathSegment::Keyed(keys) => {
                    f.write_str("[")?;
                    for (j, (k, v)) in keys.iter().enumerate() {
                        if j > 0 {
                            f.write_str(",")?;
                        }
                        write!(f, "{k}={v}")?;
                    }
                    f.write_str("]")?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_renders_as_dot() {
        assert_eq!(ValuePath::root().to_string(), ".");
    }

    #[test]
    fn field_chain_renders_dotted() {
        let p = ValuePath::root().field("spec").field("replicas");
        assert_eq!(p.to_string(), "spec.replicas");
    }

    #[test]
    fn index_renders_with_brackets() {
        let p = ValuePath::root().field("args").index(2);
        assert_eq!(p.to_string(), "args[2]");
    }

    #[test]
    fn keyed_renders_with_key_value() {
        let p = ValuePath::root()
            .field("spec")
            .field("containers")
            .keyed(vec![("name".into(), "app".into())]);
        assert_eq!(p.to_string(), "spec.containers[name=app]");
    }

    #[test]
    fn keyed_with_multiple_keys_joined_by_commas() {
        let p = ValuePath::root().field("ports").keyed(vec![
            ("containerPort".into(), "80".into()),
            ("protocol".into(), "TCP".into()),
        ]);
        assert_eq!(p.to_string(), "ports[containerPort=80,protocol=TCP]");
    }
}
