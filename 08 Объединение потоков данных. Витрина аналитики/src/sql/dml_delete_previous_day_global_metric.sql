DELETE FROM STV2024060713__DWH.global_metrics
WHERE date_update = '{{ ds }}'::DATE - INTERVAL '1 day';
