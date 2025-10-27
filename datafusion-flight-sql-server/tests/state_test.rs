use datafusion_flight_sql_server::state::QueryHandle;

#[test]
fn test_query_handle_with_complex_sql() {
    let query = r#"
        SELECT
            a.id,
            a.name,
            COUNT(b.order_id) as order_count
        FROM customers a
        LEFT JOIN orders b ON a.id = b.customer_id
        WHERE a.created_at > $1 AND a.status = $2
        GROUP BY a.id, a.name
        HAVING COUNT(b.order_id) > $3
        ORDER BY order_count DESC
        LIMIT 100
    "#
    .to_string();

    let handle = QueryHandle::new(query.clone(), None);

    let encoded = handle.clone().encode();
    let decoded = QueryHandle::try_decode(encoded).expect("Should decode");

    assert_eq!(decoded.query(), query);
}

#[test]
fn test_query_handle_empty_query() {
    let query = String::new();
    let handle = QueryHandle::new(query.clone(), None);

    let encoded = handle.clone().encode();
    let decoded = QueryHandle::try_decode(encoded).expect("Should decode");

    assert_eq!(decoded.query(), "");
}
