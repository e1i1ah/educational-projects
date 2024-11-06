CREATE TABLE IF NOT EXISTS STV2024060713__STAGING.currencies (
    date_update TIMESTAMP NOT NULL CHECK (date_update <= CURRENT_TIMESTAMP),
    currency_code INT NOT NULL,
    currency_code_with INT NOT NULL,
    currency_with_div FLOAT NOT NULL,
    PRIMARY KEY (date_update, currency_code, currency_code_with) 
)
ORDER BY date_update, currency_code
SEGMENTED BY hash(date_update, currency_code) ALL NODES
PARTITION BY date_update::date;


CREATE TABLE IF NOT EXISTS STV2024060713__STAGING.transactions (
    operation_id UUID NOT NULL,
    account_number_from INT NOT NULL,
    account_number_to INT NOT NULL,
    currency_code INT NOT NULL,
    country VARCHAR(100) NOT NULL,
    status VARCHAR(100) NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    amount FLOAT NOT NULL,
    transaction_dt TIMESTAMP NOT NULL CHECK (transaction_dt <= CURRENT_TIMESTAMP),
    PRIMARY KEY (operation_id, account_number_from, account_number_to, currency_code, country, status, transaction_type, amount, transaction_dt)
)
ORDER BY transaction_dt, operation_id
SEGMENTED BY hash(transaction_dt, operation_id) ALL NODES
PARTITION BY transaction_dt::date;