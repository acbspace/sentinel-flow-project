-- Drop the table first (it references incidents), then the marker column.
DROP TABLE IF EXISTS notifications;
ALTER TABLE incidents DROP COLUMN IF EXISTS alerted_at;
