CREATE TABLE IF NOT EXISTS gold.fct_eventos (
    evento_id            INT,
    user_id              INT,
    plan_name            STRING,
    date_id              DATE,
    total_minutes_used   FLOAT,
    total_messages_used  FLOAT,
    total_mb_used        FLOAT,
    extra_minutes        FLOAT,
    extra_messages       FLOAT,
    extra_mb             FLOAT, 
    monthly_revenue      FLOAT
) PARTITION BY RANGE (date);

-- enero a febrero 2025
CREATE TABLE fct_eventos_enero_febrero_2025
    PARTITION OF gold.fct_eventos
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');  
