-- =============================================================================
-- 002_auth.sql
-- =============================================================================
-- Add authentication support for agent endpoints via bearer tokens.

-- Add registration token hash to instances table
ALTER TABLE instances ADD COLUMN registration_token_hash TEXT;

-- Index for token hash lookups (sparse index - only for instances with tokens)
CREATE INDEX idx_instances_token_hash ON instances(registration_token_hash)
    WHERE registration_token_hash IS NOT NULL;

-- Update schema version
UPDATE schema_version SET version = 2;
