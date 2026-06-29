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

use std::fmt;

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self::new(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

string_id!(DatasetId);
string_id!(SchemaId);
string_id!(FieldId);
string_id!(PipelineDefinitionId);
string_id!(MetadataObjectId);
string_id!(LineageEdgeId);
string_id!(DataContractId);
string_id!(OwnerId);
string_id!(ClassificationId);
string_id!(MetadataVersion);

/// Typed reference from a process event to the metadata graph.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MetadataRef {
    Dataset(DatasetId),
    Schema {
        id: SchemaId,
        version: Option<MetadataVersion>,
    },
    Field(FieldId),
    PipelineDefinition {
        id: PipelineDefinitionId,
        version: Option<MetadataVersion>,
    },
    Object(MetadataObjectId),
    LineageEdge(LineageEdgeId),
    DataContract(DataContractId),
    Owner(OwnerId),
    Classification(ClassificationId),
}

impl MetadataRef {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Dataset(_) => "dataset",
            Self::Schema { .. } => "schema",
            Self::Field(_) => "field",
            Self::PipelineDefinition { .. } => "pipeline_definition",
            Self::Object(_) => "object",
            Self::LineageEdge(_) => "lineage_edge",
            Self::DataContract(_) => "data_contract",
            Self::Owner(_) => "owner",
            Self::Classification(_) => "classification",
        }
    }
}
