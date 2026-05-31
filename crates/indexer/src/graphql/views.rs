use async_graphql::{
    Error, Value as GraphqlValue,
    dynamic::{
        Field, FieldFuture, FieldValue, InputValue, Object as DynamicObject, Scalar,
        Schema as DynamicSchema, TypeRef,
    },
};
use serde_json::{Map, Value};

use crate::{GraphqlViewConfig, GraphqlViewFieldConfig, IndexerError, StoreQuery};

use super::{
    IndexerGraphqlDynamicSchema, SharedStore,
    query::{
        EventFilter, bounded_limit, event_filter, graphql_error, insert_string, insert_u64,
        parse_after, string_array, string_field,
    },
};

struct DynamicEventRow {
    payload: Value,
}

pub(super) fn build_dynamic_schema(
    store: SharedStore,
    views: Vec<GraphqlViewConfig>,
) -> Result<IndexerGraphqlDynamicSchema, IndexerError> {
    let mut query = DynamicObject::new("Query").field(dynamic_events_field());
    let mut builder = DynamicSchema::build("Query", None, None)
        .data(store)
        .register(Scalar::new("JSON"))
        .register(dynamic_indexed_event_object());

    for view in views {
        let type_name = format!("{}Row", view.name);
        query = query.field(dynamic_view_field(view.clone(), type_name.clone()));
        builder = builder.register(dynamic_view_object(&type_name, &view.fields));
    }

    builder.register(query).finish().map_err(|error| {
        IndexerError::Config(format!("query.views: build GraphQL schema: {error}"))
    })
}

fn dynamic_events_field() -> Field {
    Field::new("events", TypeRef::named_nn_list_nn("IndexedEvent"), |ctx| {
        let store = match ctx.data::<SharedStore>() {
            Ok(store) => store.clone(),
            Err(_) => {
                return FieldFuture::new(async move {
                    Err::<Option<FieldValue<'static>>, _>(Error::new(
                        "queryable store is not configured",
                    ))
                });
            }
        };
        let dataset = match ctx.args.try_get("dataset").and_then(|value| value.string()) {
            Ok(dataset) => dataset.to_owned(),
            Err(error) => {
                return FieldFuture::new(
                    async move { Err::<Option<FieldValue<'static>>, _>(error) },
                );
            }
        };
        let limit = match ctx
            .args
            .get("limit")
            .map(|value| value.u64())
            .transpose()
            .and_then(bounded_limit)
        {
            Ok(limit) => limit,
            Err(error) => {
                return FieldFuture::new(
                    async move { Err::<Option<FieldValue<'static>>, _>(error) },
                );
            }
        };
        let after = match ctx
            .args
            .get("after")
            .map(|value| value.string().map(str::to_owned))
            .transpose()
            .and_then(parse_after)
        {
            Ok(after) => after,
            Err(error) => {
                return FieldFuture::new(
                    async move { Err::<Option<FieldValue<'static>>, _>(error) },
                );
            }
        };
        let filter = event_filter(EventFilter {
            index_name: dynamic_string_arg(&ctx.args, "indexName"),
            chain: dynamic_string_arg(&ctx.args, "chain"),
            chain_id: dynamic_u64_arg(&ctx.args, "chainId"),
            address: dynamic_string_arg(&ctx.args, "address"),
            event_name: dynamic_string_arg(&ctx.args, "eventName"),
            signature: dynamic_string_arg(&ctx.args, "signature"),
            from_block: dynamic_u64_arg(&ctx.args, "fromBlock"),
            to_block: dynamic_u64_arg(&ctx.args, "toBlock"),
            topic0: dynamic_string_arg(&ctx.args, "topic0"),
            limit,
            after,
        });
        FieldFuture::new(async move {
            let result =
                tokio::task::spawn_blocking(move || store.query(StoreQuery { dataset, filter }))
                    .await
                    .map_err(|error| Error::new(format!("graphql query task failed: {error}")))?
                    .map_err(graphql_error)?;
            Ok(Some(dynamic_rows(result.rows)))
        })
    })
    .argument(InputValue::new(
        "indexName",
        TypeRef::named(TypeRef::STRING),
    ))
    .argument(InputValue::new("chain", TypeRef::named(TypeRef::STRING)))
    .argument(InputValue::new("chainId", TypeRef::named(TypeRef::INT)))
    .argument(InputValue::new(
        "dataset",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("address", TypeRef::named(TypeRef::STRING)))
    .argument(InputValue::new(
        "eventName",
        TypeRef::named(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "signature",
        TypeRef::named(TypeRef::STRING),
    ))
    .argument(InputValue::new("fromBlock", TypeRef::named(TypeRef::INT)))
    .argument(InputValue::new("toBlock", TypeRef::named(TypeRef::INT)))
    .argument(InputValue::new("topic0", TypeRef::named(TypeRef::STRING)))
    .argument(InputValue::new("limit", TypeRef::named(TypeRef::INT)))
    .argument(InputValue::new("after", TypeRef::named(TypeRef::STRING)))
}

fn dynamic_view_field(view: GraphqlViewConfig, type_name: String) -> Field {
    Field::new(
        view.name.clone(),
        TypeRef::named_nn_list_nn(type_name),
        move |ctx| {
            let view = view.clone();
            let store = match ctx.data::<SharedStore>() {
                Ok(store) => store.clone(),
                Err(_) => {
                    return FieldFuture::new(async move {
                        Err::<Option<FieldValue<'static>>, _>(Error::new(
                            "queryable store is not configured",
                        ))
                    });
                }
            };
            let limit = match ctx
                .args
                .get("limit")
                .map(|value| value.u64())
                .transpose()
                .and_then(|limit| bounded_view_limit(limit, &view))
            {
                Ok(limit) => limit,
                Err(error) => {
                    return FieldFuture::new(async move {
                        Err::<Option<FieldValue<'static>>, _>(error)
                    });
                }
            };
            let after = match ctx
                .args
                .get("after")
                .map(|value| value.string().map(str::to_owned))
                .transpose()
                .and_then(parse_after)
            {
                Ok(after) => after,
                Err(error) => {
                    return FieldFuture::new(async move {
                        Err::<Option<FieldValue<'static>>, _>(error)
                    });
                }
            };
            let dataset = view.dataset.clone();
            let filter = view_filter(&view, limit, after);
            FieldFuture::new(async move {
                let result = tokio::task::spawn_blocking(move || {
                    store.query(StoreQuery { dataset, filter })
                })
                .await
                .map_err(|error| Error::new(format!("graphql query task failed: {error}")))?
                .map_err(graphql_error)?;
                Ok(Some(dynamic_rows(result.rows)))
            })
        },
    )
    .argument(InputValue::new("limit", TypeRef::named(TypeRef::INT)))
    .argument(InputValue::new("after", TypeRef::named(TypeRef::STRING)))
}

fn dynamic_indexed_event_object() -> DynamicObject {
    DynamicObject::new("IndexedEvent")
        .field(dynamic_row_field(
            "indexName",
            "index",
            TypeRef::named(TypeRef::STRING),
        ))
        .field(dynamic_row_field(
            "chain",
            "chain",
            TypeRef::named(TypeRef::STRING),
        ))
        .field(dynamic_row_field(
            "chainId",
            "chain_id",
            TypeRef::named(TypeRef::INT),
        ))
        .field(dynamic_row_field(
            "dataset",
            "dataset",
            TypeRef::named(TypeRef::STRING),
        ))
        .field(dynamic_row_field(
            "blockNumber",
            "block_number",
            TypeRef::named(TypeRef::INT),
        ))
        .field(dynamic_row_field(
            "blockHash",
            "block_hash",
            TypeRef::named(TypeRef::STRING),
        ))
        .field(dynamic_row_field(
            "transactionHash",
            "transaction_hash",
            TypeRef::named(TypeRef::STRING),
        ))
        .field(dynamic_row_field(
            "transactionIndex",
            "transaction_index",
            TypeRef::named(TypeRef::INT),
        ))
        .field(dynamic_row_field(
            "eventIndex",
            "log_index",
            TypeRef::named(TypeRef::INT),
        ))
        .field(dynamic_row_field(
            "address",
            "address",
            TypeRef::named(TypeRef::STRING),
        ))
        .field(dynamic_selector_field())
        .field(dynamic_topics_field())
        .field(dynamic_topic0_field())
        .field(dynamic_row_field(
            "signature",
            "signature",
            TypeRef::named(TypeRef::STRING),
        ))
        .field(dynamic_row_field(
            "eventName",
            "event_name",
            TypeRef::named(TypeRef::STRING),
        ))
        .field(dynamic_json_row_field("decoded", "decoded"))
        .field(dynamic_row_field(
            "data",
            "data",
            TypeRef::named(TypeRef::STRING),
        ))
        .field(dynamic_json_payload_field())
        .field(dynamic_row_field(
            "createdAt",
            "created_at",
            TypeRef::named(TypeRef::STRING),
        ))
}

fn dynamic_view_object(type_name: &str, fields: &[GraphqlViewFieldConfig]) -> DynamicObject {
    let mut object = DynamicObject::new(type_name).field(dynamic_json_payload_field());
    for field in fields {
        object = object.field(dynamic_json_row_field(&field.name, &field.path));
    }
    object
}

fn dynamic_row_field(name: &str, path: &str, ty: TypeRef) -> Field {
    let path = path.to_owned();
    Field::new(name, ty, move |ctx| {
        let value = ctx
            .parent_value
            .try_downcast_ref::<DynamicEventRow>()
            .map(|row| value_at_path(&row.payload, &path).cloned())
            .and_then(|value| json_to_graphql(value.unwrap_or(Value::Null)));
        FieldFuture::from_value(value.ok())
    })
}

fn dynamic_json_row_field(name: &str, path: &str) -> Field {
    let path = path.to_owned();
    Field::new(name, TypeRef::named("JSON"), move |ctx| {
        let value = ctx
            .parent_value
            .try_downcast_ref::<DynamicEventRow>()
            .map(|row| {
                value_at_path(&row.payload, &path)
                    .cloned()
                    .unwrap_or(Value::Null)
            })
            .and_then(json_to_graphql);
        FieldFuture::from_value(value.ok())
    })
}

fn dynamic_json_payload_field() -> Field {
    Field::new("payload", TypeRef::named("JSON"), move |ctx| {
        let value = ctx
            .parent_value
            .try_downcast_ref::<DynamicEventRow>()
            .map(|row| row.payload.clone())
            .and_then(json_to_graphql);
        FieldFuture::from_value(value.ok())
    })
}

fn dynamic_selector_field() -> Field {
    Field::new("selector", TypeRef::named(TypeRef::STRING), move |ctx| {
        let value = ctx
            .parent_value
            .try_downcast_ref::<DynamicEventRow>()
            .ok()
            .and_then(|row| {
                string_field(row.payload.as_object()?, "address")
                    .or_else(|| string_field(row.payload.as_object()?, "selector"))
                    .or_else(|| string_field(row.payload.as_object()?, "program"))
                    .or_else(|| string_field(row.payload.as_object()?, "account"))
            })
            .map(GraphqlValue::String);
        FieldFuture::from_value(value)
    })
}

fn dynamic_topics_field() -> Field {
    Field::new(
        "topics",
        TypeRef::named_list_nn(TypeRef::STRING),
        move |ctx| {
            let value = ctx
                .parent_value
                .try_downcast_ref::<DynamicEventRow>()
                .ok()
                .and_then(|row| {
                    row.payload
                        .as_object()
                        .map(|object| string_array(object, "topics"))
                })
                .map(|topics| {
                    GraphqlValue::List(topics.into_iter().map(GraphqlValue::String).collect())
                });
            FieldFuture::from_value(value)
        },
    )
}

fn dynamic_topic0_field() -> Field {
    Field::new("topic0", TypeRef::named(TypeRef::STRING), move |ctx| {
        let value = ctx
            .parent_value
            .try_downcast_ref::<DynamicEventRow>()
            .ok()
            .and_then(|row| {
                row.payload
                    .as_object()
                    .map(|object| string_array(object, "topics"))
            })
            .and_then(|topics| topics.into_iter().next())
            .map(GraphqlValue::String);
        FieldFuture::from_value(value)
    })
}

fn dynamic_rows(rows: Vec<Value>) -> FieldValue<'static> {
    FieldValue::list(
        rows.into_iter()
            .map(|payload| FieldValue::owned_any(DynamicEventRow { payload })),
    )
}

fn dynamic_string_arg(
    args: &async_graphql::dynamic::ObjectAccessor<'_>,
    name: &str,
) -> Option<String> {
    args.get(name)
        .and_then(|value| value.string().ok())
        .map(str::to_owned)
}

fn dynamic_u64_arg(args: &async_graphql::dynamic::ObjectAccessor<'_>, name: &str) -> Option<u64> {
    args.get(name).and_then(|value| value.u64().ok())
}

fn bounded_view_limit(limit: Option<u64>, view: &GraphqlViewConfig) -> async_graphql::Result<u64> {
    let limit = limit.unwrap_or(view.default_limit);
    if limit == 0 {
        return Err(Error::new("limit must be greater than 0"));
    }
    if limit > view.max_limit {
        return Err(Error::new(format!(
            "limit must be less than or equal to {}",
            view.max_limit
        )));
    }
    Ok(limit)
}

fn view_filter(view: &GraphqlViewConfig, limit: u64, after: Option<u64>) -> Value {
    let mut filter = Map::new();
    insert_string(&mut filter, "event_name", view.event_name.clone());
    insert_string(&mut filter, "signature", view.signature.clone());
    insert_u64(&mut filter, "limit", Some(limit));
    insert_u64(&mut filter, "after", after);
    for filter_config in &view.filters {
        filter.insert(
            filter_config.field.clone(),
            Value::String(filter_config.equals.clone()),
        );
    }
    Value::Object(filter)
}

fn value_at_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for segment in path.split('.') {
        current = current.as_object()?.get(segment)?;
    }
    Some(current)
}

fn json_to_graphql(value: Value) -> async_graphql::Result<GraphqlValue> {
    GraphqlValue::from_json(value).map_err(|error| Error::new(error.to_string()))
}
