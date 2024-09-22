SELECT 
    customer_id,
    SUM(transaction_amount) AS total_deposit_amount
FROM 
    customer_transactions
WHERE 
    transaction_date BETWEEN '2024-09-01' AND '2024-10-01'
    AND transaction_type = 'deposit'
GROUP BY 
    customer_id
HAVING 
    total_deposit_amount > 50000
    AND COUNT(CASE WHEN status = 'pending' THEN 1 END) > 0;
