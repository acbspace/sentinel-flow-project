-- Drop the table first (it references incidents), then the marker column.
DROP TABLE IF EXISTS remediation_actions;
ALTER TABLE incidents DROP COLUMN IF EXISTS remediated_at;
