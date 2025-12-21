SELECT
    SUM(transaction_amount)AS total_revenue,
    count(transaction_sk) AS total_transactions
FROM fact.transactions
WHERE status = 'Success';
