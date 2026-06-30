<!--
Copyright (C) 2026 Frode Randers
All rights reserved

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# PROV-O and DCAT/DCAT-AP Feasibility on Ipto

## Status

Draft analysis, 2026-06-30.

## Summary

Both the W3C PROV Ontology (PROV-O) and the Data Catalog Vocabulary
(DCAT / DCAT-AP) can be modeled on top of Ipto. Ipto's Entity-Attribute-Value
(EAV) metadata core, combined with typed internal relations and a Neo4j graph
backend, maps naturally to ontology-based metadata. The sibling projects (Ipto,
Durga, Raft) provide independent building blocks; the remaining work is
composition through Vannak, not greenfield construction.

## 1. Sibling Project Inventory

### Ipto (`../ipto`)

Auto-configured metadata management platform. Core concepts:

- **EAV storage**: Attributes are typed (string, time, int, long, double, bool,
  data, record) and stored in dedicated value-vector tables
  (`repo_string_vector`, `repo_time_vector`, etc.). New attribute types do not
  require schema migration of existing units.
- **GraphQL SDL schema**: Structure is declared via SDL directives —
  `@attributeRegistry`, `@attribute`, `@template`, `@record`, `@use`.
  `configure_graphql_sdl()` parses and persists attribute/template/record
  shape metadata.
- **Versioned units**: Every unit is `(tenant_id, unit_id, unit_version)`.
  Attribute values are range-tracked per version interval.
  `correlation_id` (UUID v7) provides an idempotency anchor for external
  writers.
- **Internal relations**: Typed `(left_unit --[rel_type]--> right_unit)` edges
  in `repo_internal_relation`. Supports parent-child (type 1), replacement
  (type 3), and arbitrary numeric types.
- **External associations**: `(unit --[assoc_type]--> reference_string)` for
  URI/reference links.
- **Dual backend**: PostgreSQL + Neo4j. Neo4j backend is available for graph
  traversal queries.
- **Rust crate** at `ipto/implementations/rust/`: compiles as both `rlib` and
  `cdylib` (Python via PyO3). Key public types: `Backend` trait (30+ storage
  methods), `RepoService` (high-level semantic layer), SDL parser, constrained
  GraphQL runtime.

### Durga (`../durga`)

BPMN-to-Kafka-native process runtime. Key facts:

- **ProcessEvent record**: `processInstanceId`, `processId`, `activityId`,
  `tokenId`, `correlationId`, `payload: Map<String,Object>`, `status`, `error`,
  `eventType`, `processVersion`, `businessKey`, `timestamp`.
- **Event types**: `PROCESS_STARTED`, `ACTIVITY_ENTERED`, `ACTIVITY_COMPLETED`,
  `ACTIVITY_ESCALATED`, `ACTIVITY_CANCELLED`, `GATEWAY_TAKEN`,
  `PROCESS_COMPLETED`, `PROCESS_FAILED`.
- **Kafka topics**: One per process (`process-events-{processId}`), plus
  `process-state` (compacted), monitoring projections.
- **Plugin system**: 15 plugins (Mask, JsonTransform, KvEnricher, etc.).
  `KvEnricher` shows the enrichment pattern and has a documented Kafka-topic
  source mode for future metadata lookups.
- **DataHandle**: `(name, uri, mediaType, schema, metadata: Map<String,Object>)`
  — structured reference to data assets, embeddable in ProcessEvent payload.
- **Monitoring**: Kafka Streams topology with materialized state stores,
  Quarkus REST API, Svelte dashboard.
- **Integration surface**: `ProcessEvent.payload` is the extensibility point.
  There is no existing Ipto or Vannak integration.

### Raft (`../raft/graft-rust/`)

Transport-agnostic Rust Raft consensus implementation. Key facts:

- **`StateMachine` trait**: `apply(term, command)`, `apply_with_result()`,
  `snapshot()`, `restore()`, optional `query()` for linearizable reads.
  Application state machine receives opaque `Vec<u8>` commands and returns
  opaque `Vec<u8>` results. Raft owns membership; snapshots wrap domain bytes
  in a Raft membership envelope.
- **`QueryableStateMachine` trait**: `query(request) -> Vec<u8>` for reads.
  Raft handles read leases/barriers before calling.
- **Storage**: `FileLogStore` + `FilePersistentStateStore` or in-memory variants.
- **Transport**: Protobuf over varint32-framed TCP. Cross-language compatible
  with Java and C++ peers.
- **Snapshot pipeline**: Auto-compaction, chunked InstallSnapshot RPC, JSON
  snapshot envelope with base64-encoded application payload.
- **Membership**: JOIN → JOINT → FINALIZE commands. Joint consensus with
  split-majority checks.
- **Hardening**: Jepsen-tested for safety.

### Vannak (this repository)

Currently at dependency-free core types and reducers. See `ARCHITECTURE.md`
for the full phased plan. The codebase defines the data-individual metadata
model, Ipto placement, durable outbox, segment storage, cluster control state,
and the `IptoWriter` trait boundary — all without external dependencies.

## 2. Why Ipto Fits Ontology-Based Metadata

Ipto's EAV model was built for semantic metadata, not generic CRUD. The four
primitives that make it well-suited for ontology profiles:

**Attributes as properties.** Each RDF property becomes a globally registered
Ipto attribute. `@attribute(datatype: X, name: "prefix:localName")` mirrors
qualified names. The attribute registry enforces type stability — once an
attribute is used by units, it cannot be mutated.

**Templates as classes.** `@template(name: "ClassName")` binds a set of
attributes into a named type. Multiple templates can share attributes (an
`Activity` and an `Agent` can both carry `rdfs:label`). Templates are
non-exclusive — Ipto does not force a single type per unit.

**Internal relations as predicates.** `(subject --[predicate]--> object)` edges
with numeric relation types map to RDF triples. Ipto's internal relations are
binary and directional, matching the subject-predicate-object pattern.

**Records as blank nodes / complex values.** `@record(attribute: X)` defines
nested structure trees within a unit attribute value. Maps to blank nodes or
structured property values that need internal fields.

**Neo4j backend for graph queries.** Transitive closure (`wasDerivedFrom*`),
reverse traversal, and path queries are natural in Neo4j's property graph model.
Ipto already has a functioning Neo4j backend (`backends/neo4j.rs`).

**Sparse attributes cost nothing.** EAV means declaring 100+ DCAT-AP properties
does not create 100 nullable columns or require schema migration. Only the
attributes actually populated for a given unit consume storage.

## 3. PROV-O Feasibility

### Overview

PROV-O defines three core classes and seven core relations:

```
Entity    ← wasGeneratedBy ── Activity   ← wasAssociatedWith ── Agent
  │              │                  │
  └ wasDerivedFrom                 ├ used
                                   └ wasInformedBy
                                              │
                                   Agent ─────┘ wasAttributedTo
```

Additional properties: `generatedAtTime`, `startedAtTime`, `endedAtTime`,
`invalidatedAtTime`, `wasAttributedTo`, `actedOnBehalfOf`, `wasRevisionOf`,
`wasQuotedFrom`, `alternateOf`, `specializationOf`.

### Mapping to Ipto

**Step 1: Declare PROV attributes.**

```graphql
enum Attributes @attributeRegistry {
  prov_generatedAtTime    @attribute(datatype: TIME,   name: "prov:generatedAtTime")
  prov_startedAtTime      @attribute(datatype: TIME,   name: "prov:startedAtTime")
  prov_endedAtTime        @attribute(datatype: TIME,   name: "prov:endedAtTime")
  prov_invalidatedAtTime  @attribute(datatype: TIME,   name: "prov:invalidatedAtTime")
  prov_wasAttributedTo    @attribute(datatype: STRING, name: "prov:wasAttributedTo")
  prov_actedOnBehalfOf    @attribute(datatype: STRING, name: "prov:actedOnBehalfOf")
  prov_value              @attribute(datatype: STRING, name: "prov:value")
  prov_label              @attribute(datatype: STRING, name: "prov:label")
  prov_location           @attribute(datatype: STRING, name: "prov:location")
  prov_role               @attribute(datatype: STRING, name: "prov:role")
  prov_type               @attribute(datatype: STRING, name: "prov:type")

  rdfs_label              @attribute(datatype: STRING, name: "rdfs:label")
  rdfs_comment            @attribute(datatype: STRING, name: "rdfs:comment")

  vannak_data_individual  @attribute(datatype: STRING, name: "vannak:dataIndividualId")
  vannak_process_instance @attribute(datatype: STRING, name: "vannak:processInstanceId")
  vannak_activity_id      @attribute(datatype: STRING, name: "vannak:activityId")
  vannak_pipeline_id      @attribute(datatype: STRING, name: "vannak:pipelineId")
  vannak_tenant_id        @attribute(datatype: STRING, name: "vannak:tenantId")
  vannak_environment      @attribute(datatype: STRING, name: "vannak:environmentId")
}
```

**Step 2: Declare PROV templates.**

```graphql
type ProvEntity @template(name: "ProvEntity") {
  label:              String @use(attribute: rdfs_label)
  comment:            String @use(attribute: rdfs_comment)
  generatedAtTime:    String @use(attribute: prov_generatedAtTime)
  invalidatedAtTime:  String @use(attribute: prov_invalidatedAtTime)
  wasAttributedTo:    String @use(attribute: prov_wasAttributedTo)
  value:              String @use(attribute: prov_value)
  location:           String @use(attribute: prov_location)
  dataIndividualId:   String @use(attribute: vannak_data_individual)
}

type ProvActivity @template(name: "ProvActivity") {
  label:              String @use(attribute: rdfs_label)
  startedAtTime:      String @use(attribute: prov_startedAtTime)
  endedAtTime:        String @use(attribute: prov_endedAtTime)
  processInstanceId:  String @use(attribute: vannak_process_instance)
  activityId:         String @use(attribute: vannak_activity_id)
  pipelineId:         String @use(attribute: vannak_pipeline_id)
  tenantId:           String @use(attribute: vannak_tenant_id)
}

type ProvAgent @template(name: "ProvAgent") {
  label:              String @use(attribute: rdfs_label)
  actedOnBehalfOf:    String @use(attribute: prov_actedOnBehalfOf)
  type:               String @use(attribute: prov_type)
}
```

**Step 3: Define relation type codes.**

| PROV-O Predicate | Ipto Relation Code | Direction |
|---|---|---|
| `wasGeneratedBy` | 10 | Entity → Activity |
| `used` | 11 | Activity → Entity |
| `wasInformedBy` | 12 | Activity → Activity |
| `wasDerivedFrom` | 13 | Entity → Entity |
| `wasAttributedTo` | 14 | Entity → Agent |
| `wasAssociatedWith` | 15 | Activity → Agent |
| `actedOnBehalfOf` | 16 | Agent → Agent |
| `wasRevisionOf` | 17 | Entity → Entity |
| `wasQuotedFrom` | 18 | Entity → Entity |
| `alternateOf` | 19 | Entity → Entity |
| `specializationOf` | 20 | Entity → Entity |

Ipto's `repo_internal_relation` schema: `(left_tenant, left_unit_id, right_tenant, right_unit_id, rel_type)`. Each triple maps to one row.

**Step 4: Vannak writes provenance facts at runtime.**

For each data-individual metadata event, Vannak's `IptoWriter` adapter would:

1. Create a `ProvEntity` unit for the data individual (or upsert via idempotency key).
2. Create or resolve a `ProvActivity` unit for the Durga activity context.
3. Create or resolve a `ProvAgent` unit for the pipeline owner or plugin.
4. Map passive/active metadata fields into prov attributes
   (e.g., `passive_metadata.received_at` → `prov:generatedAtTime`).
5. Create internal relations: `Entity --[wasGeneratedBy]--> Activity`,
   `Activity --[used]--> Entity` (input entity), etc.

**Step 5: Query provenance through Ipto or Neo4j.**

```
# What is the full derivation chain for data item X?
# Neo4j: MATCH path = (e:ProvEntity {id: 'X'})-[:wasDerivedFrom*]->(ancestor) RETURN path

# Which activities used data item Y?
# Vannak query: find InternalRelations by right_unit and rel_type=11 (USED)

# Which agent was responsible for transformation Z?
# Vannak query: find relations of type 15 (wasAssociatedWith) from Activity Z
```

### Feasibility Assessment

**High.**

- All PROV-O concepts have a direct Ipto primitive.
- The EAV pattern means the SDL can be authored once and applied to any Ipto
  instance via `configure_graphql_sdl()`.
- Vannak's existing `IptoMapping` type already maps metadata field names to Ipto
  attribute names. Switching the mapping to use PROV qualified names is a
  configuration change, not a code change.
- Transitive provenance queries (e.g., `wasDerivedFrom*`) are well-supported by
  the Neo4j backend. For PostgreSQL, recursive CTEs or application-side
  traversal would be needed — Neo4j is the recommended backend for
  provenance-heavy placements.

## 4. DCAT/DCAT-AP Feasibility

### Overview

DCAT defines:

| Class | Description |
|---|---|
| `dcat:Catalog` | Curated collection of metadata about datasets |
| `dcat:Dataset` | Logical collection of data |
| `dcat:Distribution` | Specific representation of a dataset |
| `dcat:DataService` | Collection of operations providing access to datasets |

DCAT-AP extends this with mandatory, recommended, and optional properties for
European data portals (~100+ properties across all classes).

### Mapping to Ipto

DCAT uses a containment hierarchy (Catalog → Dataset → Distribution) that maps
directly to Ipto's parent-child relation (relation type 1). Additional
cross-references (e.g., `dcat:contactPoint`) can use typed internal relations.

**Step 1: DCAT attributes (excerpt — full DCAT-AP would be ~100).**

```graphql
enum Attributes @attributeRegistry {
  dcterms_title          @attribute(datatype: STRING, name: "dcterms:title")
  dcterms_description    @attribute(datatype: STRING, name: "dcterms:description")
  dcterms_issued         @attribute(datatype: TIME,   name: "dcterms:issued")
  dcterms_modified       @attribute(datatype: TIME,   name: "dcterms:modified")
  dcterms_publisher      @attribute(datatype: STRING, name: "dcterms:publisher")
  dcterms_identifier     @attribute(datatype: STRING, name: "dcterms:identifier")
  dcterms_language       @attribute(datatype: STRING, name: "dcterms:language")
  dcterms_license        @attribute(datatype: STRING, name: "dcterms:license")
  dcterms_rights         @attribute(datatype: STRING, name: "dcterms:rights")
  dcterms_spatial        @attribute(datatype: STRING, name: "dcterms:spatial")
  dcterms_temporal       @attribute(datatype: STRING, name: "dcterms:temporal")
  dcterms_accrualPeriodicity @attribute(datatype: STRING, name: "dcterms:accrualPeriodicity")
  dcterms_conformsTo     @attribute(datatype: STRING, name: "dcterms:conformsTo")
  dcterms_contactPoint   @attribute(datatype: STRING, name: "dcterms:contactPoint")
  dcterms_keyword        @attribute(datatype: STRING, name: "dcterms:keyword")
  dcterms_theme          @attribute(datatype: STRING, name: "dcterms:theme")
  dcterms_landingPage    @attribute(datatype: STRING, name: "dcterms:landingPage")

  dcat_byteSize          @attribute(datatype: LONG,   name: "dcat:byteSize")
  dcat_accessURL         @attribute(datatype: STRING, name: "dcat:accessURL")
  dcat_downloadURL       @attribute(datatype: STRING, name: "dcat:downloadURL")
  dcat_mediaType         @attribute(datatype: STRING, name: "dcat:mediaType")
  dcat_endpointURL       @attribute(datatype: STRING, name: "dcat:endpointURL")
  dcat_servesDataset     @attribute(datatype: STRING, name: "dcat:servesDataset")

  foaf_name              @attribute(datatype: STRING, name: "foaf:name")

  vannak_pipeline_id     @attribute(datatype: STRING, name: "vannak:pipelineId")
  vannak_tenant_id       @attribute(datatype: STRING, name: "vannak:tenantId")
}
```

**Step 2: DCAT templates.**

```graphql
type DcatCatalog @template(name: "DcatCatalog") {
  title:        String @use(attribute: dcterms_title)
  description:  String @use(attribute: dcterms_description)
  issued:       String @use(attribute: dcterms_issued)
  modified:     String @use(attribute: dcterms_modified)
  publisher:    String @use(attribute: dcterms_publisher)
  language:     String @use(attribute: dcterms_language)
  license:      String @use(attribute: dcterms_license)
  rights:       String @use(attribute: dcterms_rights)
  spatial:      String @use(attribute: dcterms_spatial)
  homepage:     String @use(attribute: dcterms_landingPage)
}

type DcatDataset @template(name: "DcatDataset") {
  title:              String @use(attribute: dcterms_title)
  description:        String @use(attribute: dcterms_description)
  issued:             String @use(attribute: dcterms_issued)
  modified:           String @use(attribute: dcterms_modified)
  identifier:         String @use(attribute: dcterms_identifier)
  keyword:            String @use(attribute: dcterms_keyword)
  theme:              String @use(attribute: dcterms_theme)
  landingPage:        String @use(attribute: dcterms_landingPage)
  accrualPeriodicity: String @use(attribute: dcterms_accrualPeriodicity)
  conformsTo:         String @use(attribute: dcterms_conformsTo)
  contactPoint:       String @use(attribute: dcterms_contactPoint)
  publisher:          String @use(attribute: dcterms_publisher)
  temporal:           String @use(attribute: dcterms_temporal)
  spatial:            String @use(attribute: dcterms_spatial)
  language:           String @use(attribute: dcterms_language)
  pipelineId:         String @use(attribute: vannak_pipeline_id)
  tenantId:           String @use(attribute: vannak_tenant_id)
}

type DcatDistribution @template(name: "DcatDistribution") {
  title:       String @use(attribute: dcterms_title)
  description: String @use(attribute: dcterms_description)
  issued:      String @use(attribute: dcterms_issued)
  modified:    String @use(attribute: dcterms_modified)
  accessURL:   String @use(attribute: dcat_accessURL)
  downloadURL: String @use(attribute: dcat_downloadURL)
  mediaType:   String @use(attribute: dcat_mediaType)
  byteSize:    String @use(attribute: dcat_byteSize)
  license:     String @use(attribute: dcterms_license)
  rights:      String @use(attribute: dcterms_rights)
  conformsTo:  String @use(attribute: dcterms_conformsTo)
}
```

**Step 3: Parent-child hierarchy.**

```text
Catalog  ──[PARENT_CHILD(1)]──► Dataset ──[PARENT_CHILD(1)]──► Distribution
```

Ipto's existing `PARENT_CHILD_RELATION` (type 1) maps to DCAT containment.
Additional cross-references use typed relations:

| DCAT Property | Ipto Relation Type | Direction |
|---|---|---|
| `dcat:dataset` | 1 (parent-child) | Catalog → Dataset |
| `dcat:distribution` | 1 (parent-child) | Dataset → Distribution |
| `dcat:servesDataset` | 5 | DataService → Dataset |
| `dcat:contactPoint` | 6 | Dataset → vCard unit |
| `dcterms:relation` | 7 | Dataset → Dataset |

**Step 4: Vannak writes DCAT metadata.**

When a data pipeline produces a dataset, Vannak writes:

1. Create or upsert a `DcatDataset` unit.
2. Create a `DcatDistribution` unit for each physical representation
   (partition, file, topic, table).
3. Establish parent-child relations.
4. Map Vannak's `PassiveMetadata` fields into DCAT attributes:
   - `format` → `dcterms:format` / `dcat:mediaType`
   - `size` → `dcat:byteSize`
   - `source_system` → `dcterms:source`
   - `schema` → `dcterms:conformsTo`
   - `checksum` → `spdx:checksum` (as a separate attribute or nested record)

### Feasibility Assessment

**High.**

- Ipto already uses DC Terms namespacing (`dcterms:title`) in its documentation
  and examples. The naming convention is established.
- The containment hierarchy maps to Ipto's existing parent-child relation.
  No new relation infrastructure is needed.
- DCAT-AP's ~100 properties are a configuration exercise, not a code change.
  The SDL is declarative.
- DCAT-AP mandatory/recommended/optional distinction is a documentation
  concern — Ipto does not enforce property cardinality at the attribute level,
  only at the template field level (SDL fields can be marked non-null with `!`).

## 5. Integration Architecture

### Full Flow

```text
                     ┌──────────────────────────────────────────────────────┐
                     │                    Raft (graft-rust)                  │
                     │                                                      │
                     │  ┌─────────────────┐  ┌──────────────────────────┐   │
                     │  │ IptoPlacementMap│  │ MetadataOutboxCheckpoint │   │
                     │  │ WriterLease     │  │ SegmentManifests         │   │
                     │  │ ClusterMembers  │  │ OwnershipEpochs          │   │
                     │  └─────────────────┘  └──────────────────────────┘   │
                     └────────▲───────────────────────────┬─────────────────┘
                              │ consensus                  │ cluster view
                              │                            │
  ┌───────────────────────────┴────────────────────────────▼──────────────────┐
  │                              Vannak Node                                  │
  │                                                                           │
  │  ┌──────────────┐   ┌─────────────┐   ┌────────────┐   ┌──────────────┐  │
  │  │ Durga Mirror │   │ Hot Index   │   │ Metadata   │   │ ClusterCtrl  │  │
  │  │ (durga.rs)   │   │ (index.rs)  │   │ Outbox     │   │ (cluster.rs) │  │
  │  │              │   │             │   │ (ipto.rs)  │   │              │  │
  │  │ ProcessEvent │   │ ProcessState│   │            │   │ Applies Raft │  │
  │  │ → PipelineEv │   │ Queries     │   │ Segment-   │   │ commands to  │  │
  │  │              │   │ Snapshots   │   │ Backed     │   │ local state  │  │
  │  └──────▲───────┘   └─────────────┘   │ Enqueue    │   └──────┬───────┘  │
  │         │                             │ Replay     │          │          │
  │         │                             │ Drain      │          │          │
  │  ┌──────┴─────────────────────────────┴────────────┴──────────▼───────┐  │
  │  │                     IptoWriter Adapter                              │  │
  │  │                                                                    │  │
  │  │  impl IptoWriter for IptoRepoWriter {                              │  │
  │  │    fn write(&mut self, payload: &IptoWritePayload) {               │  │
  │  │      // Translate IptoWritePayload into ipto_rust::RepoService     │  │
  │  │      // calls: create_unit(), add_attribute_values(),              │  │
  │  │      // create_internal_relation()                                 │  │
  │  │    }                                                               │  │
  │  │  }                                                                 │  │
  │  └────────────────────────────┬───────────────────────────────────────┘  │
  │                               │                                          │
  └───────────────────────────────┼──────────────────────────────────────────┘
                                  │
                    ┌─────────────▼──────────────┐
                    │    ipto_rust (rlib)         │
                    │                             │
                    │  ┌───────────────────────┐  │
                    │  │ RepoService           │  │
                    │  │  • create_unit()      │  │
                    │  │  • add_attribute()    │  │
                    │  │  • create_relation()  │  │
                    │  │  • search()           │  │
                    │  └──────────┬────────────┘  │
                    │             │               │
                    │  ┌──────────▼────────────┐  │
                    │  │ Backend trait         │  │
                    │  │  ├─ PostgresBackend   │  │
                    │  │  └─ Neo4jBackend      │  │
                    │  └───────────────────────┘  │
                    └─────────────────────────────┘
```

### Durga → Vannak Bridge

Two integration paths, not mutually exclusive:

**Path A: Durga metadata plugin.** A new plugin (following the `KvEnricher`
pattern) that runs inside the Durga process runtime and emits
data-individual metadata alongside process lifecycle events. The plugin
receives the data item payload, the activity context, and a metadata mapping
configuration. It publishes metadata events to a Vannak-dedicated Kafka topic
for ingest.

**Path B: Kafka Streams adapter.** A separate Kafka Streams topology that
consumes Durga `ProcessEvent` topics, enriches them with metadata from an
external cache or Ipto query, and publishes Vannak-formatted metadata events.
This keeps the enrichment logic outside the process runtime and can be
updated independently.

### Vannak → Ipto Writer

The `IptoWriter` trait in `vannak/src/ipto.rs` is already defined. An
implementation against `ipto_rust::RepoService` would:

```rust
struct IptoRepoWriter {
    repo: ipto_rust::RepoService,   // bound to a Backend
    tenant_id: String,
}

impl IptoWriter for IptoRepoWriter {
    fn write(&mut self, payload: &IptoWritePayload) -> Result<(), IptoWriteError> {
        // 1. Create unit with template matching the payload's ontology profile
        // 2. Set attribute values from payload.attributes
        // 3. Create internal relations from payload metadata
        // 4. Use payload.idempotency_key as the Ipto correlation_id
        //    for idempotent upsert
        Ok(())
    }
}
```

The `correlation_id` (UUID v7) on every Ipto unit provides the idempotency
guarantee. Vannak's `IdempotencyKey` maps to `correlation_id` — if the write
fails and is retried, Ipto detects duplicate correlation ids and returns the
existing unit.

## 6. Implementation Path

### Phase A: Ontology SDL Schemas

Author the PROV-O and DCAT SDL files. Apply them to a test Ipto instance via
`configure_graphql_sdl()`. Verify that attributes, templates, and record
shapes are persisted correctly. This is standalone Ipto work, no Vannak
changes needed.

### Phase B: IptoWriter Implementation

Add `ipto_rust` as an optional dependency of Vannak (behind a feature flag
initially). Implement `IptoRepoWriter` against `RepoService`. Write
integration tests that create provenance units and relations from a
`DataIndividualMetadataEvent`.

### Phase C: Vannak Metadata Mapping

Extend Vannak's `IptoMapping` to support ontology-aware field mapping.
Example mapping configuration:

```text
metadata.source_system  → dcterms:source
metadata.received_at    → prov:generatedAtTime
metadata.size_bytes     → dcat:byteSize
metadata.checksum       → spdx:checksum
mask.customer.email     → prov:generatedBy  (reference to Masking activity)
```

The mapping version is already tracked in `IptoWritePayload.mapping_version`.

### Phase D: Durga Integration

Implement a metadata plugin or Kafka Streams topology that extracts
data-individual metadata from Durga activities and publishes them to Vannak's
ingest path. This is primarily Durga-side work.

### Phase E: Raft Cluster Control

Implement `StateMachine` for Vannak's `ClusterControlState` against the
`graft-core::StateMachine` trait. Wire `graft-runtime` for the event loop.
Publish placement maps, writer leases, and checkpoint manifests through Raft.

### Phase F: Query Layer

Extend Vannak's query module with provenance and catalog queries that
delegate to the Ipto backend where needed:

- "Trace derivation chain for data individual X"
- "Which datasets are affected by pipeline failure Y?"
- "What is the DCAT description for dataset Z?"
- "Which pipelines produce datasets with theme T?"

## 7. Risks and Mitigations

### Risk 1: Ipto Rust crate maturity

The Rust crate targets Python via PyO3 and is not yet at full Java parity
(see `FEATURE_COMPLETENESS.md`). The `Backend` trait and `RepoService` are
functional for unit CRUD, attribute management, relations, and search, but
edge cases or performance issues may exist.

**Mitigation**: The Java reference implementation is production-hardened.
Vannak's `IptoWriter` trait abstracts the backend — an initial Java-backed
implementation via a thin HTTP bridge or JNI is possible as a fallback.
Long term, the Rust crate's feature gap is small and closing.

### Risk 2: Neo4j dependency

For graph provenance queries, Neo4j is the natural choice, but it adds
operational complexity compared to PostgreSQL alone.

**Mitigation**: Ipto supports both backends. The PROV-O relation model also
works with recursive PostgreSQL CTEs for moderate-depth traversal. Start
with PostgreSQL, add Neo4j for provenance-heavy placements only when
query patterns demand it.

### Risk 3: Durga integration gap

Durga currently has no Ipto or Vannak integration. The `ProcessEvent.payload`
extensibility is untyped and ad-hoc.

**Mitigation**: The plugin system is the established extensibility pattern
(15 plugins already exist). A metadata plugin following the same pattern is
low-risk. The `DataHandle` type already shows the direction — extend it or
add a sibling type for provenance facts.

### Risk 4: DCAT-AP property volume

~100 properties across four classes is verbose but straightforward. The
maintenance burden is in keeping the SDL aligned with the specification,
not in Ipto's capacity to store it.

**Mitigation**: The SDL is declarative and versionable in source control.
DCAT-AP changes slowly. A CI check that validates the SDL against the
published specification vocabulary is helpful but not blocking.

### Risk 5: Cross-project dependency graph

Vannak currently has zero dependencies. Adding `ipto_rust`, `graft-core`,
and `graft-runtime` plus their transitive deps (postgres, serde, chrono,
uuid, tokio, protobuf, parking_lot, etc.) is a significant expansion.

**Mitigation**: This is by design — Vannak's phased plan explicitly calls
out adding Ipto writer (Phase 5) and Raft control plane (Phase 6) as
separate milestones with new dependencies. The dependency-free core was
intentional to validate the domain model first. Feature-gate the heavy
dependencies so the core types remain usable without them for testing
and standalone validation.

## 8. Open Questions

- Should PROV-O and DCAT profiles live in Vannak's repository (as SDL files
  and mapping configurations) or in Ipto's schema directory?
- What is the boundary between Vannak's `IptoMapping` (field-to-attribute)
  and Ipto's SDL (attribute-to-template)? Should Vannak generate template
  instances, or just attribute values on a generic unit?
- For DCAT cataloging: does Vannak automatically register datasets on first
  observation, or does registration require an explicit API call?
- PROV-O's `Agent` — is this always the pipeline owner, or can it be a
  plugin identity, a user, or an external service?
- Should Vannak's `ClusterControlState` also store the active ontology
  profile version so all nodes agree on what mapping to apply?
- How are transitive provenance queries like `wasDerivedFrom*` exposed
  through Vannak's query model? Direct Neo4j delegation or abstracted
  behind a Vannak query primitive?
- Should the PROV-O and DCAT SDL be split into two independent Ipto
  configurations, or combined into one multi-profile schema?
