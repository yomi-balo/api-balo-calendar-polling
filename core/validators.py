"""Shared validation utilities for the application"""

from typing import List


def validate_and_clean_string(value: str, field_name: str) -> str:
    """Validate and clean string field"""
    if not value or not value.strip():
        raise ValueError(f'{field_name} cannot be empty')
    return value.strip()


def validate_and_clean_calendar_ids(calendar_ids: List[str]) -> List[str]:
    """Validate and clean calendar IDs list"""
    if not calendar_ids:
        raise ValueError('At least one calendar ID is required')
    
    # Remove empty strings and duplicates
    valid_ids = list(set([id.strip() for id in calendar_ids if id and id.strip()]))
    if not valid_ids:
        raise ValueError('At least one valid calendar ID is required')
    
    return valid_ids