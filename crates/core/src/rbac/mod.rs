//! Role-Based Access Control (RBAC) for ExchangeDB.
//!
//! Provides user/role management, permission checking, and persistent
//! storage under the `_security/` directory within the database root.

pub mod model;
pub mod store;

pub use model::{Permission, Role, SecurityContext, User};
pub use store::{hash_password, needs_rehash, verify_password, RbacStore};
