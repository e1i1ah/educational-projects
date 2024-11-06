INSERT INTO STV2024060713__DWH.global_metrics
SELECT
    '{{ ds }}'::DATE - INTERVAL '1 day' AS date_update,
    t.currency_code AS currency_from,
    SUM(
        CASE
            WHEN t.currency_code = 420 THEN t.amount
            ELSE t.amount / c.currency_with_div
        END
    ) AS amount_total,
    COUNT(t.operation_id) AS cnt_transactions,
    COUNT(t.operation_id) / NULLIF(COUNT(DISTINCT t.account_number_from), 0) AS avg_transactions_per_account,
    COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
FROM
    STV2024060713__STAGING.transactions t
LEFT JOIN
    (
        SELECT
            t.operation_id,
            c.currency_with_div,
            ROW_NUMBER() OVER (
                PARTITION BY t.operation_id
                ORDER BY c.date_update DESC
            ) AS rn
        FROM
            STV2024060713__STAGING.transactions t
        JOIN
            STV2024060713__STAGING.currencies c
        ON
            t.currency_code = c.currency_code
        WHERE
            c.date_update <= t.transaction_dt
            AND c.currency_code_with = 420
            AND t.transaction_dt::DATE = '{{ ds }}'::DATE - INTERVAL '1 day'
    ) c
ON
    t.operation_id = c.operation_id
    AND c.rn = 1
WHERE
    t.transaction_dt::DATE = '{{ ds }}'::DATE - INTERVAL '1 day'
    AND t.status = 'done'
    AND t.account_number_from >= 0
GROUP BY
    t.currency_code;
