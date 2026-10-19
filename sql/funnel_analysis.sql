WITH last_event_times AS (
  SELECT
    user_session,
    event_type,
    MIN(event_time)
  FROM eventlog
  GROUP BY user_session, event_type
),

purchased_sessions AS (
  SELECT
    COUNT(*) as number_of_sessions_resulting_in_purchase
  FROM last_event_times
  WHERE event_type = 'purchase'
),

non_purchased_sessions AS (
  SELECT
    COUNT(*) as number_of_sessions_not_resulting_in_purchase
  FROM last_event_times
  WHERE event_type != 'purchase'
)

SELECT
  number_of_sessions_resulting_in_purchase,
  number_of_sessions_not_resulting_in_purchase
FROM purchased_sessions
  CROSS JOIN non_purchased_sessions;