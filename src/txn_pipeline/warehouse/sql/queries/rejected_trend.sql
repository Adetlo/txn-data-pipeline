-- Rejected (quarantined) records per rule over time
SELECT quarantined_date, rule_id, severity, stage, COUNT(*) AS rejected_records
FROM fact_quarantine
GROUP BY quarantined_date, rule_id, severity, stage
ORDER BY quarantined_date, rejected_records DESC;
