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

# Bug: `normalize_attribute_type` does not map numeric type IDs to canonical names

## Location

`/ipto/implementations/rust/src/graphql_sdl.rs:155-170`

## Severity

Medium — breaks `configure_graphql_sdl` idempotency. Calling
`configure_graphql_sdl` a second time (e.g. after a restart) fails with a
spurious validation error, even though nothing changed.

## Reproduction

1. Call `configure_graphql_sdl` with a valid SDL string containing attribute
   declarations (e.g. `@attribute(datatype: TIME, ...)`). The first call
   succeeds and creates attributes in the database.
2. Call `configure_graphql_sdl` again with the same SDL string.

**Expected:** Second call is a no-op — existing attributes match the SDL
definition and no error is returned.

**Actual:** Second call fails with:

```
invalid input: existing attribute 'prov_generatedAtTime' does not match SDL
definition (expected ... type='time' ..., got ... type='2' ...)
```

## Root Cause

The type-comparison chain is:

1. **SDL parser** (`graphql_sdl.rs:261`): `attribute_type = normalize_attribute_type("TIME")` → `"time"`.
   Stored in `SdlAttributeSpec.attribute_type`.

2. **Database** stores the type as an integer column `attrtype = 2` (TIME).

3. **`get_attribute_info`** (`backends/postgres.rs:1707`): returns JSON
   `{"type": 2, ...}` — the numeric database value under the JSON key `"type"`.

4. **`attribute_type_from_info`** (`repo.rs:953-963`): extracts `info["type"] = 2`
   and stringifies it to `"2"`.

5. **`normalize_attribute_type("2")`** (`graphql_sdl.rs:156`): detects that
   `"2"` parses as an `i32` and returns it unchanged as `"2"`.

6. **Comparison** in `ensure_attribute_compatible` (`repo.rs:783`):
   `actual_type = "2"` ≠ `expected_type = "time"` → mismatch, returns error.

The fundamental problem is that `normalize_attribute_type` short-circuits on
numeric input, returning the raw string `"2"` instead of mapping it to the
canonical name `"time"`. The function knows about the mapping in one direction
(string → string, line 159-168) but not the reverse (int → string).

## Fix

The early-return guard on `graphql_sdl.rs:156-158` should map numeric type IDs
to their canonical string names instead of passing them through unchanged:

```rust
pub fn normalize_attribute_type(input: &str) -> String {
    if let Ok(n) = input.trim().parse::<i32>() {
        return match n {
            1 => "string",
            2 => "time",
            3 => "int",
            4 => "long",
            5 => "double",
            6 => "bool",
            7 => "data",
            99 => "record",
            other => return other.to_string(),
        }
        .to_string();
    }
    // ... existing match block unchanged
}
```

This makes `normalize_attribute_type` idempotent regardless of whether the
input is a canonical name (`"time"`), a numeric ID (`"2"`), or a raw SDL
datatype (`"TIME"`).

## Affected Code Paths

- `repo.rs:767` — `ensure_attribute_compatible` normalization of the actual
  (database) type.
- `repo.rs:779` — normalization of the expected (SDL) type.
- Any other caller of `normalize_attribute_type` that receives a value
  originating from the database rather than the SDL parser.

## Related

The `attribute_info_from_row` function (`backends/postgres.rs:1694-1710`)
serializes the database column `attrtype` (an integer) under the JSON key
`"type"`. The Neo4j backend (`backends/neo4j.rs:1765`) maps via column index
`["attrtype"]` and may have the same issue depending on its row format.
