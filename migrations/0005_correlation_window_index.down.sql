-- Drop the correlation window index.
--
-- Rolling this back returns the window query to a full scan of one of the 0001
-- indexes, whose cost grows with the size of the table rather than the size of
-- the window. Correlation keeps working; it just gets steadily slower as
-- telemetry accumulates.
DROP INDEX IF EXISTS telemetry_events_ts_idx;
