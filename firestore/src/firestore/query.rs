use anyhow::{anyhow, Result};

use super::FValue;
use google_cloud_grpc_proto::firestore::v1::{
    batch_get_documents_response, firestore_client,
    structured_query::{
        self, composite_filter, field_filter, filter, filter::FilterType, unary_filter,
        CollectionSelector, CompositeFilter, Direction, FieldFilter, FieldReference, Filter, Order,
        Projection, UnaryFilter,
    },
    Cursor, Document, StructuredQuery, Value, WriteResult,
};

fn select_projection<F: Into<String>>(fields: Vec<F>) -> Projection {
    Projection {
        fields: fields
            .into_iter()
            .map(|field| field_reference(field))
            .collect(),
    }
}

fn from(collection_id: String, all_descendants: bool) -> CollectionSelector {
    CollectionSelector {
        collection_id,
        all_descendants,
    }
}
fn field_reference<F: Into<String>>(field_path: F) -> FieldReference {
    FieldReference {
        field_path: field_path.into(),
    }
}

fn unary_filter<F: Into<String>>(field: F, op: unary_filter::Operator) -> Filter {
    let operand = unary_filter::OperandType::Field(field_reference(field));

    Filter {
        filter_type: Some(FilterType::UnaryFilter(UnaryFilter {
            op: op as i32,
            operand_type: Some(operand),
        })),
    }
}

fn field_filter<F: Into<String>, V: Into<FValue>>(
    field: F,
    op: field_filter::Operator,
    value: V,
) -> Filter {
    Filter {
        filter_type: Some(FilterType::FieldFilter(FieldFilter {
            field: Some(field_reference(field)),
            op: op as i32,
            value: Some(value.into().to_grpc_value()),
        })),
    }
}

fn merge_filters(mut filters: Vec<Filter>) -> Option<Filter> {
    match filters.len() {
        0 => None,
        1 => filters.pop(),
        _ => Some(Filter {
            filter_type: Some(FilterType::CompositeFilter(CompositeFilter {
                op: composite_filter::Operator::And as i32,
                filters,
            })),
        }),
    }
}

fn order<F: Into<String>>(field: F, direction: Direction) -> Order {
    Order {
        field: Some(field_reference(field)),
        direction: direction as i32,
    }
}

fn str_to_field_op<S: AsRef<str>>(s: S) -> Result<field_filter::Operator> {
    match s.as_ref() {
        "<" => Ok(field_filter::Operator::LessThan),
        "<=" => Ok(field_filter::Operator::LessThanOrEqual),
        "==" => Ok(field_filter::Operator::Equal),
        ">" => Ok(field_filter::Operator::GreaterThan),
        ">=" => Ok(field_filter::Operator::GreaterThanOrEqual),
        "!=" => Ok(field_filter::Operator::NotEqual),
        "array-contains" => Ok(field_filter::Operator::ArrayContains),
        "array-contains-any" => Ok(field_filter::Operator::ArrayContainsAny),
        "in" => Ok(field_filter::Operator::In),
        "not-in" => Ok(field_filter::Operator::NotIn),
        _ => Err(anyhow!("{}", s.as_ref())),
    }
}

fn str_to_unary_op<S: AsRef<str>>(s: S) -> Result<unary_filter::Operator> {
    match s.as_ref() {
        "is-nan" => Ok(unary_filter::Operator::IsNan),
        "is-null" => Ok(unary_filter::Operator::IsNull),
        "is-not-nan" => Ok(unary_filter::Operator::IsNotNan),
        "is-not-null" => Ok(unary_filter::Operator::IsNotNull),
        _ => Err(anyhow!("{}", s.as_ref())),
    }
}

fn str_to_direction<S: AsRef<str>>(s: S) -> Result<Direction> {
    match s.as_ref() {
        "asc" => Ok(Direction::Ascending),
        "desc" => Ok(Direction::Descending),
        _ => Err(anyhow!("not a order :{}", s.as_ref())),
    }
}

pub struct QueryBuilder {
    select: Option<structured_query::Projection>,
    from: Vec<CollectionSelector>,
    filters: Vec<Filter>,
    orders: Vec<Order>,
    offset: i32,
    limit: Option<i32>,
}

impl QueryBuilder {
    pub fn collection(collection_id: String, all_descendants: bool) -> Self {
        let mut colls = Vec::new();
        colls.push(from(collection_id, all_descendants));
        QueryBuilder {
            select: None,
            from: colls,
            filters: Vec::new(),
            orders: Vec::new(),
            offset: 0,
            limit: None,
        }
    }

    pub fn select<F: Into<String>>(mut self, fields: Vec<F>) -> Self {
        self.select = Some(select_projection(fields));
        self
    }

    pub fn filter(mut self, filter: Filter) -> Self {
        self.filters.push(filter);
        self
    }

    /// ### operations
    /// * "<"
    /// * "<="
    /// * "=="
    /// * ">"
    /// * ">="
    /// * "!="
    /// * "array-contains"
    /// * "array-contains-any"
    /// * "in"
    /// * "not-in"
    pub fn filter_bin<F, OP, V>(self, field: F, op: OP, value: V) -> Self
    where
        F: Into<String>,
        OP: AsRef<str>,
        V: Into<FValue>,
    {
        //TODO(tacogips) tobe type safe
        let op = str_to_field_op(op).unwrap_or_else(|e| panic!("invalid field op [{}]", e));
        self.filter(field_filter(field, op, value))
    }

    /// ### operations
    /// * "is-nan"
    /// * "is-null"
    /// * "is-not-nan"
    /// * "is-not-null"
    pub fn filter_una<F, OP, V>(self, field: F, op: OP) -> Self
    where
        F: Into<String>,
        OP: AsRef<str>,
    {
        //TODO(tacogips) tobe type safe
        let op = str_to_unary_op(op).unwrap_or_else(|e| panic!("invalid unary op [{}]", e));
        self.filter(unary_filter(field, op))
    }

    ///
    /// directions
    /// * "asc"
    /// * "desc"
    pub fn order<F, D>(mut self, field: F, direction: D) -> Self
    where
        F: Into<String>,
        D: AsRef<str>,
    {
        //TODO(tacogips) tobe type safe
        let op = str_to_direction(direction).unwrap_or_else(|e| panic!("{:?}", e));

        self.orders.push(order(field, op));
        self
    }

    pub fn offset(mut self, offset: i32) -> Self {
        self.offset = offset;
        self
    }

    pub fn limit(mut self, limit: i32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn build_with_cursor(
        self,
        start_at: Option<Cursor>,
        end_at: Option<Cursor>,
    ) -> StructuredQuery {
        let merged_filter = merge_filters(self.filters);
        StructuredQuery {
            select: self.select,
            from: self.from,
            r#where: merged_filter,
            order_by: self.orders,
            start_at,
            end_at,
            offset: self.offset,
            limit: self.limit,
        }
    }

    pub fn build(self) -> StructuredQuery {
        self.build_with_cursor(None, None)
    }
}
