-- Migration to add availability_errors table for tracking expert availability failures
-- This table maintains only current failures - records are deleted when experts succeed

CREATE TABLE IF NOT EXISTS availability_errors (
    bubble_uid VARCHAR(255) PRIMARY KEY,  -- Same as expert's bubble_uid for uniqueness
    expert_name VARCHAR(255) NOT NULL,
    cronofy_id VARCHAR(255) NOT NULL,
    error_reason VARCHAR(500) NOT NULL,   -- "API error", "Empty availability", etc.
    error_details TEXT,                   -- Additional error context
    unix_timestamp BIGINT NOT NULL,       -- Unix timestamp when error occurred
    melbourne_time VARCHAR(100) NOT NULL, -- Human-readable Melbourne time
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_availability_errors_updated_at ON availability_errors(updated_at);
CREATE INDEX IF NOT EXISTS idx_availability_errors_cronofy_id ON availability_errors(cronofy_id);