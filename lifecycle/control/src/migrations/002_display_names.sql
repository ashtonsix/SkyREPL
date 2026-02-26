-- =============================================================================
-- 002_display_names.sql
-- =============================================================================
-- Add display_name column to instances and manifests.
-- Human-readable adjective-noun names like "serene-otter" for UX display.

ALTER TABLE instances ADD COLUMN display_name TEXT;
ALTER TABLE manifests ADD COLUMN display_name TEXT;

CREATE INDEX idx_instances_display_name ON instances(tenant_id, display_name)
    WHERE display_name IS NOT NULL;
CREATE INDEX idx_manifests_display_name ON manifests(tenant_id, display_name)
    WHERE display_name IS NOT NULL;

UPDATE schema_version SET version = 2;
