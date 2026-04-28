//! Structural diff for Kubernetes manifests.
//!
//! The reconciler compares *desired* manifests (rendered from git)
//! against *live* manifests (read from the cluster). A naive YAML
//! equality check produces false positives: the API server fills in
//! defaults, mutating webhooks rewrite fields, and controllers add
//! status. This crate's job is to take a server-normalized desired
//! manifest (obtained via [`DryRunApplier`]) and report only the
//! fields that genuinely differ from live.
//!
//! # Tier 1 of the Smart Diff Engine (xje.1)
//!
//! - [`DryRunApplier`]: trait abstracting "ask the API server to
//!   normalize this manifest without persisting it." Implementors
//!   call `PATCH /…?dryRun=All` with server-side apply semantics.
//!   This crate ships only the trait — a `kube`-rs implementation
//!   lands in a follow-up bead.
//! - [`compare::diff`]: pure structural compare, list-map aware.
//!   Given two `serde_yaml_ng::Value`s and a [`ListMapKeys`]
//!   schema, produces a [`Diff`] whose entries point at the exact
//!   leaf paths that differ.
//! - [`ListMapKeys`]: registry telling the differ which list-typed
//!   fields are really keyed maps (e.g.
//!   `spec.template.spec.containers` is keyed by `name`). Without
//!   this, reordering a containers list would look like a sweeping
//!   change. Built-in defaults cover the well-known core/apps lists.
//!
//! Subsequent tiers — controller-aware ignore (xje.2), user-defined
//! ignore (xje.3), SSA field-ownership filter (xje.4) — are layered
//! on top: each consumes a [`Diff`] and prunes entries before the
//! reconciler decides "drift" vs "noop".

pub mod applier;
pub mod compare;
pub mod ignore;
pub mod listmap;
pub mod path;

pub use applier::{DryRunApplier, DryRunError};
pub use compare::{diff, manifests_equivalent, Change, Diff};
pub use ignore::{path_to_json_pointer, IgnoreRule, IgnoreRules};
pub use listmap::{ListMapKeys, ListMapKind};
pub use path::{PathSegment, ValuePath};
