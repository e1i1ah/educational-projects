CREATE TABLE IF NOT EXISTS STV2024060713__DWH.global_metrics (
    date_update DATE NOT NULL,
    currency_from INT NOT NULL,
    amount_total FLOAT NOT NULL,
    cnt_transactions INT NOT NULL,
    avg_transactions_per_account FLOAT NOT NULL,
    cnt_accounts_make_transactions INT NOT NULL,
    PRIMARY KEY (date_update, currency_from)
)
ORDER BY date_update, currency_from
SEGMENTED BY hash(date_update, currency_from) ALL NODES
PARTITION BY date_update::date;