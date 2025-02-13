// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::Arc};

use anyhow::Context as _;
use async_graphql::dataloader::Loader;
use diesel::{BoolExpressionMethods, ExpressionMethods, QueryDsl};
use sui_indexer_alt_schema::{objects::StoredObject, schema::kv_objects};
use sui_kvstore::KeyValueStoreReader;
use sui_types::{base_types::ObjectID, object::Object, storage::ObjectKey};

use super::{
    bigtable_reader::BigtableReader, object_versions::LatestObjectVersionKey, pg_reader::PgReader,
};
use crate::{data::read_error::ReadError, Context};

/// Key for fetching the contents a particular version of an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct VersionedObjectKey(pub ObjectID, pub u64);

#[async_trait::async_trait]
impl Loader<VersionedObjectKey> for PgReader {
    type Value = StoredObject;
    type Error = Arc<ReadError>;

    async fn load(
        &self,
        keys: &[VersionedObjectKey],
    ) -> Result<HashMap<VersionedObjectKey, StoredObject>, Self::Error> {
        use kv_objects::dsl as o;

        if keys.is_empty() {
            return Ok(HashMap::new());
        }

        let mut conn = self.connect().await.map_err(Arc::new)?;

        let mut query = o::kv_objects.into_boxed();

        for VersionedObjectKey(id, version) in keys {
            query = query.or_filter(
                o::object_id
                    .eq(id.into_bytes())
                    .and(o::object_version.eq(*version as i64)),
            );
        }

        let objects: Vec<StoredObject> = conn.results(query).await.map_err(Arc::new)?;

        let key_to_stored: HashMap<_, _> = objects
            .iter()
            .map(|stored| {
                let id = &stored.object_id[..];
                let version = stored.object_version as u64;
                ((id, version), stored)
            })
            .collect();

        Ok(keys
            .iter()
            .filter_map(|key| {
                let slice: &[u8] = key.0.as_ref();
                let stored = *key_to_stored.get(&(slice, key.1))?;
                Some((*key, stored.clone()))
            })
            .collect())
    }
}

#[async_trait::async_trait]
impl Loader<VersionedObjectKey> for BigtableReader {
    type Value = Object;
    type Error = Arc<ReadError>;

    async fn load(
        &self,
        keys: &[VersionedObjectKey],
    ) -> Result<HashMap<VersionedObjectKey, Object>, Self::Error> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }

        let object_keys: Vec<ObjectKey> = keys
            .iter()
            .map(|key| ObjectKey(key.0, key.1.into()))
            .collect();

        let objects: Vec<Object>;

        // let client = self.0.clone();
        objects = self
            .0
            .clone()
            .get_objects(&object_keys)
            .await
            .map_err(|e| Arc::new(ReadError::BigtableRead(e.into())))?;

        let key_to_result: HashMap<_, _> =
            objects.iter().map(|o| ((o.id(), o.version()), o)).collect();

        Ok(keys
            .iter()
            .filter_map(|key| {
                let object = *key_to_result.get(&(key.0, key.1.into()))?;
                Some((*key, object.clone()))
            })
            .collect())
    }
}

/// Load the contents of the latest version of an object, if it exists. This function does not
/// respect deletion and wrapping. If an object is deleted or wrapped, it may return the contents
/// of the object before the deletion or wrapping, or it may return `None` if the object has been
/// fully pruned from the versions table.
pub(crate) async fn load_latest(
    ctx: &Context,
    object_id: ObjectID,
) -> Result<Option<Object>, anyhow::Error> {
    let Some(latest_version) = ctx
        .pg_loader()
        .load_one(LatestObjectVersionKey(object_id))
        .await
        .context("Failed to load latest version")?
    else {
        return Ok(None);
    };

    let object = ctx
        .kv_loader()
        .load_one_object(VersionedObjectKey(
            object_id,
            latest_version.object_version as u64,
        ))
        .await
        .context("Failed to load latest object")?;

    Ok(object)
}
