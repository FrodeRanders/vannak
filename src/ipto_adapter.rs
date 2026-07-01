/*
 * Copyright (C) 2026 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Ipto writer adapter backed by `ipto_rust::RepoService`.
//!
//! Enabled with the `ipto-writer` feature flag. This module depends on
//! `ipto_rust`, `serde_json`, and `uuid` — the dependency-free core modules
//! remain usable without these.
//!
//! ## Usage
//!
//! ```text
//! let backend: Arc<dyn ipto_rust::backend::Backend> =
//!     Arc::new(ipto_rust::backends::postgres::PostgresBackend::new());
//! let repo = ipto_rust::repo::RepoService::new(backend);
//!
//! let mut writer = IptoRepoWriter::new(repo, tenant_id);
//! writer.configure_sdl(PROV_O_SDL)?;
//!
//! outbox.deliver_next_pending(&mut writer);
//! ```

use crate::data::{IdempotencyKey, MetadataValue};
use crate::ipto::{
    IptoAttributeName, IptoWriteError, IptoWritePayload, IptoWriter, MetadataOutbox,
    MetadataOutboxDeliveryResult,
};
use ipto_rust::repo::RepoService;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Minimal PROV-O SDL schema declaring provenance attributes in Ipto.
///
/// Registers the core PROV-O properties as Ipto attributes plus a
/// `ProvEntity` unit template. Call `configure_sdl()` to persist these
/// to the connected Ipto backend.
pub const PROV_O_SDL: &str = "\
enum DataTypes @datatypeRegistry {
    STRING  @datatype(id: 1)
    TIME    @datatype(id: 2)
    INTEGER @datatype(id: 3)
    LONG    @datatype(id: 4)
    DOUBLE  @datatype(id: 5)
    BOOLEAN @datatype(id: 6)
}

enum Attributes @attributeRegistry {
    prov_generatedAtTime   @attribute(datatype: TIME,   array: false, name: \"prov:generatedAtTime\",   uri: \"http://www.w3.org/ns/prov#generatedAtTime\")
    prov_startedAtTime     @attribute(datatype: TIME,   array: false, name: \"prov:startedAtTime\",     uri: \"http://www.w3.org/ns/prov#startedAtTime\")
    prov_endedAtTime       @attribute(datatype: TIME,   array: false, name: \"prov:endedAtTime\",       uri: \"http://www.w3.org/ns/prov#endedAtTime\")
    prov_invalidatedAtTime @attribute(datatype: TIME,   array: false, name: \"prov:invalidatedAtTime\", uri: \"http://www.w3.org/ns/prov#invalidatedAtTime\")
    prov_wasAttributedTo   @attribute(datatype: STRING, array: false, name: \"prov:wasAttributedTo\",   uri: \"http://www.w3.org/ns/prov#wasAttributedTo\")
    prov_wasGeneratedBy    @attribute(datatype: STRING, array: false, name: \"prov:wasGeneratedBy\",    uri: \"http://www.w3.org/ns/prov#wasGeneratedBy\")
    prov_used              @attribute(datatype: STRING, array: false, name: \"prov:used\",              uri: \"http://www.w3.org/ns/prov#used\")
    prov_wasDerivedFrom    @attribute(datatype: STRING, array: false, name: \"prov:wasDerivedFrom\",    uri: \"http://www.w3.org/ns/prov#wasDerivedFrom\")
    prov_value             @attribute(datatype: STRING, array: false, name: \"prov:value\",             uri: \"http://www.w3.org/ns/prov#value\")
    prov_location          @attribute(datatype: STRING, array: false, name: \"prov:location\",          uri: \"http://www.w3.org/ns/prov#location\")
    prov_type              @attribute(datatype: STRING, array: false, name: \"prov:type\",              uri: \"http://www.w3.org/ns/prov#type\")

    rdfs_label             @attribute(datatype: STRING, array: false, name: \"rdfs:label\",             uri: \"http://www.w3.org/2000/01/rdf-schema#label\")
    rdfs_comment           @attribute(datatype: STRING, array: false, name: \"rdfs:comment\",           uri: \"http://www.w3.org/2000/01/rdf-schema#comment\")

    vannak_data_individual @attribute(datatype: STRING, array: false, name: \"vannak:dataIndividualId\")
    vannak_process_instance @attribute(datatype: STRING, array: false, name: \"vannak:processInstanceId\")
    vannak_activity_id     @attribute(datatype: STRING, array: false, name: \"vannak:activityId\")
    vannak_pipeline_id     @attribute(datatype: STRING, array: false, name: \"vannak:pipelineId\")
    vannak_tenant_id       @attribute(datatype: STRING, array: false, name: \"vannak:tenantId\")
    vannak_environment_id  @attribute(datatype: STRING, array: false, name: \"vannak:environmentId\")
}

type ProvEntity @template(name: \"ProvEntity\") {
    label:             String @use(attribute: rdfs_label)
    comment:           String @use(attribute: rdfs_comment)
    generatedAtTime:   String @use(attribute: prov_generatedAtTime)
    invalidatedAtTime: String @use(attribute: prov_invalidatedAtTime)
    wasAttributedTo:   String @use(attribute: prov_wasAttributedTo)
    value:             String @use(attribute: prov_value)
    location:          String @use(attribute: prov_location)
    type:              String @use(attribute: prov_type)
    dataIndividualId:  String @use(attribute: vannak_data_individual)
}

type ProvGeneration @template(name: \"ProvGeneration\") {
    wasGeneratedBy: String @use(attribute: prov_wasGeneratedBy)
    label:          String @use(attribute: rdfs_label)
}

type ProvUsage @template(name: \"ProvUsage\") {
    used:  String @use(attribute: prov_used)
    label: String @use(attribute: rdfs_label)
}

type ProvDerivation @template(name: \"ProvDerivation\") {
    wasDerivedFrom: String @use(attribute: prov_wasDerivedFrom)
    label:          String @use(attribute: rdfs_label)
}
";

/// An `IptoWriter` implementation that persists metadata through
/// `ipto_rust::RepoService` on a PostgreSQL backend.
///
/// Uses the correlation-id-based idempotency pattern: checks for an existing
/// unit by `corrid` (derived from Vannak's `IdempotencyKey`) before creating
/// a new unit.
pub struct IptoRepoWriter {
    repo: Arc<RepoService>,
    tenant_id: i64,
    attr_ids: HashMap<IptoAttributeName, (i64, String)>,
    sdl_configured: bool,
}

impl IptoRepoWriter {
    /// Create a new writer bound to the given RepoService and tenant.
    ///
    /// The tenant must already exist in the Ipto instance (typically tenant
    /// id 1 = `SCRATCH` from `boot.sql`).
    pub fn new(repo: Arc<RepoService>, tenant_id: i64) -> Self {
        Self {
            repo,
            tenant_id,
            attr_ids: HashMap::new(),
            sdl_configured: false,
        }
    }

    /// Configure PROV-O attribute metadata from the built-in SDL schema.
    ///
    /// Call once at startup before any writes. This persists attribute
    /// definitions and unit/record templates into the Ipto backend.
    /// Subsequently calls resolve attribute names to their numeric IDs
    /// for efficient payload construction.
    ///
    /// Safe to call multiple times — subsequent calls are no-ops if SDL
    /// has already been configured.
    pub fn configure_sdl(&mut self) -> Result<(), IptoWriteError> {
        if self.sdl_configured {
            return Ok(());
        }

        self.repo
            .configure_graphql_sdl(PROV_O_SDL)
            .map_err(|e| IptoWriteError::retryable(format!("SDL configuration failed: {e}")))?;

        let attribute_names = [
            ("prov:generatedAtTime", "time"),
            ("prov:startedAtTime", "time"),
            ("prov:endedAtTime", "time"),
            ("prov:invalidatedAtTime", "time"),
            ("prov:wasAttributedTo", "string"),
            ("prov:wasGeneratedBy", "string"),
            ("prov:used", "string"),
            ("prov:wasDerivedFrom", "string"),
            ("prov:value", "string"),
            ("prov:location", "string"),
            ("prov:type", "string"),
            ("rdfs:label", "string"),
            ("rdfs:comment", "string"),
            ("vannak:dataIndividualId", "string"),
            ("vannak:processInstanceId", "string"),
            ("vannak:activityId", "string"),
            ("vannak:pipelineId", "string"),
            ("vannak:tenantId", "string"),
            ("vannak:environmentId", "string"),
        ];

        for (qualname, attr_type) in attribute_names {
            let id = self
                .repo
                .attribute_name_to_id(qualname)
                .map_err(|e| IptoWriteError::retryable(format!("id lookup for {qualname}: {e}")))?
                .ok_or_else(|| {
                    IptoWriteError::permanent(format!(
                        "attribute {qualname} not found after SDL configuration"
                    ))
                })?;

            self.attr_ids.insert(
                IptoAttributeName::from(qualname),
                (id, attr_type.to_string()),
            );
        }

        self.sdl_configured = true;
        Ok(())
    }

    fn corrid_for_key(&self, key: &IdempotencyKey) -> String {
        fn hash_bytes(input: &[u8], seed: u8) -> u64 {
            let mut state = 0xcbf2_9ce4_8422_2325u64;
            state ^= u64::from(seed);
            state = state.wrapping_mul(0x0000_0100_0000_01b3);
            for byte in input {
                state ^= u64::from(*byte);
                state = state.wrapping_mul(0x0000_0100_0000_01b3);
            }
            state
        }

        let bytes = key.as_str().as_bytes();
        let hi = hash_bytes(bytes, 0);
        let lo = hash_bytes(bytes, 1);

        format!(
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            (hi >> 32) as u32,
            (hi >> 16) as u32 & 0xFFFF,
            0x7000 | (hi as u32 & 0x0FFF),
            0x8000 | (lo as u32 >> 16 & 0x3FFF),
            lo & 0xFFFF_FFFF,
        )
    }

    fn metadata_value_to_json(&self, value: &MetadataValue) -> Value {
        match value {
            MetadataValue::String(s) => Value::Array(vec![Value::String(s.clone())]),
            MetadataValue::Integer(i) => serde_json::json!([i]),
            MetadataValue::Boolean(b) => Value::Array(vec![Value::Bool(*b)]),
            MetadataValue::Timestamp(t) => {
                Value::Array(vec![Value::String(t.as_str().to_string())])
            }
            MetadataValue::StringList(v) => {
                Value::Array(v.iter().map(|s| Value::String(s.clone())).collect())
            }
        }
    }

    fn build_unit_payload(&self, payload: &IptoWritePayload) -> Result<Value, IptoWriteError> {
        let corrid = self.corrid_for_key(&payload.idempotency_key);

        let mut attributes: Vec<Value> = Vec::new();
        for (attr_name, value) in &payload.attributes {
            if let Some(&(attr_id, ref attr_type)) = self.attr_ids.get(attr_name) {
                let attr_type_num = match attr_type.as_str() {
                    "string" => 1,
                    "time" | "instant" | "timestamp" | "datetime" => 2,
                    "int" | "integer" => 3,
                    "long" => 4,
                    "double" | "float" => 5,
                    "bool" | "boolean" => 6,
                    "data" | "bytes" | "blob" => 7,
                    _ => 1,
                };
                attributes.push(serde_json::json!({
                    "attrid": attr_id,
                    "attrtype": attr_type_num,
                    "value": self.metadata_value_to_json(value),
                }));
            }
        }

        let unit = serde_json::json!({
            "tenantid": self.tenant_id,
            "corrid": corrid,
            "status": 30,
            "attributes": attributes,
        });

        Ok(unit)
    }
}

impl IptoWriter for IptoRepoWriter {
    fn write(&mut self, payload: &IptoWritePayload) -> Result<(), IptoWriteError> {
        if !self.sdl_configured {
            return Err(IptoWriteError::permanent(
                "IptoRepoWriter: SDL not configured — call configure_sdl() first",
            ));
        }

        let corrid = self.corrid_for_key(&payload.idempotency_key);

        match self
            .repo
            .get_unit_by_corrid_json(&corrid)
            .map_err(|e| IptoWriteError::retryable(format!("corrid lookup failed: {e}")))?
        {
            Some(_existing) => {}
            None => {
                let unit = self.build_unit_payload(payload)?;
                self.repo
                    .store_unit_json(unit)
                    .map_err(|e| IptoWriteError::retryable(format!("store unit failed: {e}")))?;
            }
        }

        self.write_relation_units(payload)?;

        Ok(())
    }
}

impl IptoRepoWriter {
    /// Detects sentinel PROV-O relation attributes and writes relation units.
    ///
    /// Sentinel attributes use the prefix `vannak:relation:`:
    /// - `vannak:relation:wasGeneratedBy` → `ProvGeneration` unit
    /// - `vannak:relation:wasDerivedFrom` → `ProvDerivation` unit
    /// - `vannak:relation:used`            → `ProvUsage` unit
    ///
    /// Each relation unit gets a derived idempotency key so that replay is
    /// safe across partial writes of entity + relation units.
    fn write_relation_units(
        &mut self,
        payload: &IptoWritePayload,
    ) -> Result<(), IptoWriteError> {
        for (attr_name, value) in &payload.attributes {
            let relation_key = attr_name.as_str().strip_prefix("vannak:relation:");
            let Some(relation_key) = relation_key else {
                continue;
            };

            let target_attr = match relation_key {
                "wasGeneratedBy" => "prov:wasGeneratedBy",
                "wasDerivedFrom" => "prov:wasDerivedFrom",
                "used" => "prov:used",
                _ => continue,
            };

            let target_value = match value {
                MetadataValue::String(s) => s.clone(),
                other => format!("{other:?}"),
            };

            let relation_corrid_key = format!(
                "{}:relation:{}:{}",
                payload.idempotency_key.as_str(),
                relation_key,
                target_value,
            );
            let corrid = self.corrid_for_key(&IdempotencyKey::from(&*relation_corrid_key));

            if self
                .repo
                .get_unit_by_corrid_json(&corrid)
                .map_err(|e| IptoWriteError::retryable(format!("corrid lookup failed: {e}")))?
                .is_some()
            {
                continue;
            }

            let unit = self.build_relation_unit(target_attr, &target_value, &corrid)?;
            self.repo
                .store_unit_json(unit)
                .map_err(|e| IptoWriteError::retryable(format!("store relation unit failed: {e}")))?;
        }

        Ok(())
    }

    fn build_relation_unit(
        &self,
        attr_qualname: &str,
        value: &str,
        corrid: &str,
    ) -> Result<Value, IptoWriteError> {
        let (attr_id, attr_type) = self.attr_ids.get(&IptoAttributeName::from(attr_qualname))
            .ok_or_else(|| IptoWriteError::permanent(
                format!("relation attribute {attr_qualname} not found in SDL configuration"),
            ))?;

        let attr_type_num = match attr_type.as_str() {
            "string" => 1,
            "time" | "instant" | "timestamp" | "datetime" => 2,
            "int" | "integer" => 3,
            "long" => 4,
            "double" | "float" => 5,
            "bool" | "boolean" => 6,
            "data" | "bytes" | "blob" => 7,
            _ => 1,
        };

        let label_attr = if let Some(&(label_id, _)) = self.attr_ids.get(&IptoAttributeName::from("rdfs:label")) {
            Some(serde_json::json!({
                "attrid": label_id,
                "attrtype": 1,
                "value": [format!("{attr_qualname} → {value}")],
            }))
        } else {
            None
        };

        let mut attributes: Vec<Value> = vec![serde_json::json!({
            "attrid": attr_id,
            "attrtype": attr_type_num,
            "value": [value.to_string()],
        })];
        if let Some(label) = label_attr {
            attributes.push(label);
        }

        Ok(serde_json::json!({
            "tenantid": self.tenant_id,
            "corrid": corrid.to_string(),
            "status": 30,
            "attributes": attributes,
        }))
    }
}

/// Convenience: deliver the next pending outbox entry through an IptoRepoWriter.
pub fn deliver_next_pending_ipto(
    outbox: &mut MetadataOutbox,
    writer: &mut IptoRepoWriter,
) -> MetadataOutboxDeliveryResult {
    crate::ipto::deliver_next_pending(outbox, writer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corrid_is_deterministic() {
        let backend = Arc::new(ipto_rust::backends::postgres::PostgresBackend::new());
        let writer = IptoRepoWriter::new(Arc::new(RepoService::new(backend)), 1);
        let key = IdempotencyKey::from("data-1:metadata-event-1");

        let a = writer.corrid_for_key(&key);
        let b = writer.corrid_for_key(&key);
        assert_eq!(a, b);
        assert_eq!(a.len(), 36);
        assert!(a.contains('-'));
    }

    #[test]
    fn corrid_differs_for_different_keys() {
        let backend = Arc::new(ipto_rust::backends::postgres::PostgresBackend::new());
        let writer = IptoRepoWriter::new(Arc::new(RepoService::new(backend)), 1);
        let a = writer.corrid_for_key(&IdempotencyKey::from("data-1:event-1"));
        let b = writer.corrid_for_key(&IdempotencyKey::from("data-1:event-2"));
        assert_ne!(a, b);
    }

    #[test]
    fn build_unit_payload_resolves_known_attributes() {
        let backend: Arc<dyn ipto_rust::backend::Backend> =
            Arc::new(ipto_rust::backends::postgres::PostgresBackend::new());
        let repo = Arc::new(RepoService::new(backend));
        let mut writer = IptoRepoWriter::new(repo, 1);

        writer
            .attr_ids
            .insert(IptoAttributeName::from("rdfs:label"), (10, "1".into()));

        let payload = IptoWritePayload {
            target: crate::ipto::IptoInstanceId::from("ignored"),
            shard_id: crate::data::DataIndividualShardId(0),
            idempotency_key: IdempotencyKey::from("test"),
            mapping_version: "v1".into(),
            attributes: {
                let mut m = std::collections::BTreeMap::new();
                m.insert(
                    IptoAttributeName::from("rdfs:label"),
                    MetadataValue::String("test-label".into()),
                );
                m
            },
        };

        let unit = writer.build_unit_payload(&payload).unwrap();
        let attrs = unit["attributes"].as_array().unwrap();
        assert_eq!(attrs.len(), 1);
        assert_eq!(attrs[0]["attrid"], 10);
        assert_eq!(attrs[0]["attrtype"], 1);
        assert_eq!(attrs[0]["value"][0], "test-label");
    }
}
