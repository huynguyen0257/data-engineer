SELECT 
    customer_id,
    DATE_TRUNC('month', transaction_date) AS month,
    SUM(transaction_amount) AS total_transaction_amount,
    COUNT(CASE WHEN status = 'completed' THEN 1 END) AS successful_transaction_count
FROM 
    customer_transactions
WHERE 
    transaction_date BETWEEN '2024-01-01' AND '2025-01-01'
GROUP BY 
    customer_id, month
ORDER BY 
    month, customer_id;
