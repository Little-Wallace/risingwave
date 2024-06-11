// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_pb::catalog::PbSecret;

use crate::catalog::{DatabaseId, OwnedByUserCatalog, SecretId};
use crate::user::UserId;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SecretCatalog {
    pub secret_id: SecretId,
    pub name: String,
    pub database_id: DatabaseId,
    pub value: Vec<u8>,
    pub owner: UserId,
}

impl From<&PbSecret> for SecretCatalog {
    fn from(value: &PbSecret) -> Self {
        Self {
            secret_id: SecretId::new(value.id),
            database_id: value.database_id,
            owner: value.owner,
            name: value.name.clone(),
            value: value.value.clone(),
        }
    }
}

impl OwnedByUserCatalog for SecretCatalog {
    fn owner(&self) -> UserId {
        self.owner
    }
}
