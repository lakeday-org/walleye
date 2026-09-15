//! Stable namespace and object identity framing retained for existing Bitr streams.
use crate::{WalBackendError, WalResult};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
pub const RECORDS_DATASET_NAME: &str = "records";
const SHARD_UUID_NAMESPACE: Uuid = Uuid::from_u128(0x6a6f_1c2d_9b2e_4d4e_8c37_0e48_3f72_5a11);
/// A namespace's shared Lance dataset and identity domain.
///
/// The dataset URI identifies one tenant namespace bucket.  It is never
/// rewritten per object; object identity is represented by the shard and
/// stream derived by [`Self::do_identity`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceConfig {
    tenant: String,
    namespace: String,
    dataset_uri: String,
}

impl NamespaceConfig {
    /// Creates the tenant's default (`default`) Lance namespace.
    pub fn new(tenant: impl Into<String>, dataset_uri: impl Into<String>) -> WalResult<Self> {
        Self::for_namespace(tenant, "default", dataset_uri)
    }

    /// Creates a named Lance namespace under one tenant.
    pub fn for_namespace(
        tenant: impl Into<String>,
        namespace: impl Into<String>,
        dataset_uri: impl Into<String>,
    ) -> WalResult<Self> {
        let tenant = validate_identity_component("tenant", tenant.into())?;
        let namespace = validate_identity_component("namespace", namespace.into())?;
        let dataset_uri = dataset_uri.into();
        if dataset_uri.trim().is_empty() {
            return Err(WalBackendError::InvalidDatasetUri(
                "dataset URI must not be empty".to_owned(),
            ));
        }
        if dataset_uri.chars().any(char::is_control) {
            return Err(WalBackendError::InvalidDatasetUri(
                "dataset URI must not contain control characters".to_owned(),
            ));
        }
        Ok(Self {
            tenant,
            namespace,
            dataset_uri: dataset_uri.trim_end_matches('/').to_owned(),
        })
    }

    /// Returns the tenant identity that owns this namespace.
    #[must_use]
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// Returns the logical namespace name.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns the configured shared namespace bucket URI.
    #[must_use]
    pub fn dataset_uri(&self) -> &str {
        &self.dataset_uri
    }

    /// Returns the one physical `records` Lance dataset URI for this namespace.
    #[must_use]
    pub fn records_uri(&self) -> String {
        format!("{}/{}", self.dataset_uri, RECORDS_DATASET_NAME)
    }

    /// Creates a full identity for one Durable Object in this namespace.
    pub fn do_identity(
        &self,
        worker: impl Into<String>,
        binding: impl Into<String>,
        object: impl Into<String>,
    ) -> WalResult<DoIdentity> {
        DoIdentity::new(
            self.tenant.clone(),
            self.namespace.clone(),
            worker,
            binding,
            object,
        )
    }

    /// Creates the physical identity of one Durable Object namespace/object.
    /// Worker and binding aliases are intentionally absent so routing aliases
    /// cannot split one durable object into multiple shards.
    pub fn do_identity_for_namespace(
        &self,
        do_namespace_id: impl Into<String>,
        object: impl Into<String>,
    ) -> WalResult<DoIdentity> {
        DoIdentity::for_namespace_id(
            self.tenant.clone(),
            self.namespace.clone(),
            do_namespace_id,
            object,
        )
    }

    /// Derives the deterministic MemWAL shard UUID for one full DO identity.
    pub fn shard_for_do(&self, identity: &DoIdentity) -> WalResult<Uuid> {
        self.validate_identity(identity)?;
        Ok(Uuid::new_v5(
            &SHARD_UUID_NAMESPACE,
            identity.full_key().as_bytes(),
        ))
    }

    /// Derives the canonical Bitr stream key for one full DO identity.
    ///
    /// This is intentionally the same length-prefixed representation used by
    /// the control-plane `durableObjectStreamId` helper.  The stream key and
    /// the Arrow `owner_do_id` are separate values: the former is a transport
    /// coordinate, while the latter remains [`DoIdentity::full_key`].
    pub fn stream_for_do(&self, identity: &DoIdentity) -> WalResult<String> {
        self.validate_identity(identity)?;
        Ok(format!(
            "{}/do:{}:{}{}",
            self.tenant,
            js_length(&self.namespace),
            self.namespace,
            identity.shard_key(),
        ))
    }

    /// Rejects an identity from another tenant or namespace before any write.
    fn validate_identity(&self, identity: &DoIdentity) -> WalResult<()> {
        if identity.tenant != self.tenant || identity.namespace != self.namespace {
            return Err(WalBackendError::OwnerMismatch {
                expected: format!("{}/{}", self.tenant, self.namespace),
                received: format!("{}/{}", identity.tenant, identity.namespace),
            });
        }
        Ok(())
    }
}

/// Immutable identity of one Durable Object writer.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct DoIdentity {
    tenant: String,
    namespace: String,
    worker: String,
    binding: String,
    object: String,
    #[serde(default)]
    physical_shard_key: Option<String>,
}

impl DoIdentity {
    /// Creates and validates a full tenant/namespace/worker/binding/object key.
    pub fn new(
        tenant: impl Into<String>,
        namespace: impl Into<String>,
        worker: impl Into<String>,
        binding: impl Into<String>,
        object: impl Into<String>,
    ) -> WalResult<Self> {
        Ok(Self {
            tenant: validate_identity_component("tenant", tenant.into())?,
            namespace: validate_identity_component("namespace", namespace.into())?,
            worker: validate_identity_component("worker", worker.into())?,
            binding: validate_identity_component("binding", binding.into())?,
            object: validate_identity_component("object", object.into())?,
            physical_shard_key: None,
        })
    }

    /// Creates an identity from the provider's immutable Durable Object
    /// namespace plus object name, independent of Worker routing aliases.
    pub fn for_namespace_id(
        tenant: impl Into<String>,
        namespace: impl Into<String>,
        do_namespace_id: impl Into<String>,
        object: impl Into<String>,
    ) -> WalResult<Self> {
        let do_namespace_id =
            validate_identity_component("do_namespace_id", do_namespace_id.into())?;
        let object = validate_identity_component("object", object.into())?;
        let physical_shard_key = format!(
            "{}:{}{}",
            js_length(&do_namespace_id),
            do_namespace_id,
            object
        );
        Ok(Self {
            tenant: validate_identity_component("tenant", tenant.into())?,
            namespace: validate_identity_component("namespace", namespace.into())?,
            worker: do_namespace_id,
            binding: "DO".to_owned(),
            object,
            physical_shard_key: Some(physical_shard_key),
        })
    }

    /// Returns the canonical identity used in owner metadata and shard UUIDs.
    #[must_use]
    pub fn full_key(&self) -> String {
        if let Some(shard_key) = &self.physical_shard_key {
            return format!("{}/{}/{}", self.tenant, self.namespace, shard_key);
        }
        format!(
            "{}/{}/{}/{}/{}",
            self.tenant, self.namespace, self.worker, self.binding, self.object
        )
    }

    /// Returns the tenant component.
    #[must_use]
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// Returns the namespace component.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns the worker component.
    #[must_use]
    pub fn worker(&self) -> &str {
        &self.worker
    }

    /// Returns the binding/class component.
    #[must_use]
    pub fn binding(&self) -> &str {
        &self.binding
    }

    /// Returns the object name component.
    #[must_use]
    pub fn object(&self) -> &str {
        &self.object
    }

    /// Returns the length-prefixed worker/binding/object key used by the
    /// control-plane shard descriptor.
    #[must_use]
    pub fn shard_key(&self) -> String {
        if let Some(shard_key) = &self.physical_shard_key {
            return shard_key.clone();
        }
        format!(
            "{}:{}{}:{}{}",
            js_length(&self.worker),
            self.worker,
            js_length(&self.binding),
            self.binding,
            self.object,
        )
    }
}

/// JavaScript's `String.length` counts UTF-16 code units.  The control plane
/// uses that value in its stable length-prefixed resource and stream IDs, so
/// Rust must use the same metric for non-ASCII identity components.
fn js_length(value: &str) -> usize {
    value.encode_utf16().count()
}

/// Validates identity components shared by namespace and DO keys.
fn validate_identity_component(field: &str, value: String) -> WalResult<String> {
    if value.trim().is_empty() {
        return Err(WalBackendError::InvalidIdentity {
            field: field.to_owned(),
            reason: "value must not be empty".to_owned(),
        });
    }
    if value != value.trim() {
        return Err(WalBackendError::InvalidIdentity {
            field: field.to_owned(),
            reason: "leading/trailing whitespace is not allowed".to_owned(),
        });
    }
    if value
        .chars()
        .any(|character| character == '/' || character == '\0' || character.is_control())
    {
        return Err(WalBackendError::InvalidIdentity {
            field: field.to_owned(),
            reason: "slashes, NUL, and control characters are not allowed".to_owned(),
        });
    }
    Ok(value)
}
