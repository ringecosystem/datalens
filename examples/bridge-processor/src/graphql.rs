use std::sync::Arc;

use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, InputValue, Object as DynamicObject, Schema as DynamicSchema,
    TypeRef,
};
use datalens_indexer::{
    ApplicationEntityQueryStore, ApplicationEntityReadQuery, ApplicationGraphqlSchemaContext,
    ApplicationGraphqlSchemaHook, IndexerError,
};
use serde_json::Value;

pub struct BridgeGraphqlSchema;

impl ApplicationGraphqlSchemaHook for BridgeGraphqlSchema {
    fn build_schema(
        &self,
        context: ApplicationGraphqlSchemaContext,
    ) -> Result<DynamicSchema, IndexerError> {
        let message = DynamicObject::new("BridgeMessage")
            .field(json_string_field("messageId"))
            .field(json_string_field("sender"))
            .field(json_string_field("recipient"))
            .field(json_i32_field("destinationChain"))
            .field(json_i32_field("amount"))
            .field(json_string_field("status"))
            .field(json_string_field("routeName"));
        let route = DynamicObject::new("BridgeRouteCounter")
            .field(json_i32_field("destinationChain"))
            .field(json_string_field("routeName"))
            .field(json_i32_field("sentCount"))
            .field(json_i32_field("deliveredCount"))
            .field(json_i32_field("totalAmount"));
        let query = DynamicObject::new("Query")
            .field(bridge_messages_field())
            .field(bridge_route_counters_field());

        DynamicSchema::build("Query", None, None)
            .data(context.entity_store())
            .register(query)
            .register(message)
            .register(route)
            .finish()
            .map_err(|error| IndexerError::Config(format!("bridge graphql schema: {error}")))
    }
}

fn bridge_messages_field() -> Field {
    Field::new(
        "bridgeMessages",
        TypeRef::named_nn_list_nn("BridgeMessage"),
        |ctx| {
            let store = ctx
                .data::<Arc<dyn ApplicationEntityQueryStore>>()
                .expect("entity store")
                .clone();
            let account = ctx
                .args
                .try_get("account")
                .ok()
                .and_then(|value| value.string().ok())
                .map(str::to_owned);
            FieldFuture::new(async move {
                let mut query = ApplicationEntityReadQuery::new(
                    r#"
                    SELECT
                        message_id AS messageId,
                        sender,
                        recipient,
                        destination_chain AS destinationChain,
                        amount,
                        status,
                        COALESCE(route_name, '') AS routeName
                    FROM bridge_messages
                    ORDER BY message_id
                    "#,
                );
                if let Some(account) = account {
                    query = ApplicationEntityReadQuery::new(
                        r#"
                        SELECT
                            message_id AS messageId,
                            sender,
                            recipient,
                            destination_chain AS destinationChain,
                            amount,
                            status,
                            COALESCE(route_name, '') AS routeName
                        FROM bridge_messages
                        WHERE sender = ? OR recipient = ?
                        ORDER BY message_id
                        "#,
                    )
                    .bind(account.clone())
                    .bind(account);
                }
                let rows = store
                    .query_json(query)
                    .await
                    .map_err(|error| async_graphql::Error::new(error.to_string()))?;
                Ok(Some(FieldValue::list(
                    rows.into_iter().map(FieldValue::owned_any),
                )))
            })
        },
    )
    .argument(InputValue::new("account", TypeRef::named(TypeRef::STRING)))
}

fn bridge_route_counters_field() -> Field {
    Field::new(
        "bridgeRouteCounters",
        TypeRef::named_nn_list_nn("BridgeRouteCounter"),
        |ctx| {
            let store = ctx
                .data::<Arc<dyn ApplicationEntityQueryStore>>()
                .expect("entity store")
                .clone();
            FieldFuture::new(async move {
                let rows = store
                    .query_json(ApplicationEntityReadQuery::new(
                        r#"
                        SELECT
                            destination_chain AS destinationChain,
                            COALESCE(route_name, '') AS routeName,
                            sent_count AS sentCount,
                            delivered_count AS deliveredCount,
                            total_amount AS totalAmount
                        FROM bridge_route_counters
                        ORDER BY destination_chain
                        "#,
                    ))
                    .await
                    .map_err(|error| async_graphql::Error::new(error.to_string()))?;
                Ok(Some(FieldValue::list(
                    rows.into_iter().map(FieldValue::owned_any),
                )))
            })
        },
    )
}

fn json_string_field(name: &'static str) -> Field {
    Field::new(name, TypeRef::named_nn(TypeRef::STRING), move |ctx| {
        FieldFuture::new(async move {
            Ok(Some(FieldValue::value(json_string(
                ctx.parent_value.try_downcast_ref::<Value>()?,
                name,
            ))))
        })
    })
}

fn json_i32_field(name: &'static str) -> Field {
    Field::new(name, TypeRef::named_nn(TypeRef::INT), move |ctx| {
        FieldFuture::new(async move {
            Ok(Some(FieldValue::value(json_i32(
                ctx.parent_value.try_downcast_ref::<Value>()?,
                name,
            ))))
        })
    })
}

fn json_string(value: &Value, field: &str) -> String {
    value
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn json_i32(value: &Value, field: &str) -> i32 {
    value
        .get(field)
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or_default()
}
