-- Migration to add version column for optimistic locking
-- Run this against your database

ALTER TABLE experts ADD COLUMN version INTEGER DEFAULT 0;

-- Update all existing records to have version 0
UPDATE experts SET version = 0 WHERE version IS NULL;