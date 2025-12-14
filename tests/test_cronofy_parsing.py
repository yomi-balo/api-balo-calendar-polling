"""Unit tests for CronofyService parsing logic"""

import pytest
from services.cronofy_service import CronofyService


class TestFindEarliestAvailableSlot:
    """Tests for find_earliest_available_slot_from_response"""

    def test_empty_response_returns_none(self):
        """Empty available_slots should return None"""
        response = {"available_slots": []}
        result = CronofyService.find_earliest_available_slot_from_response(
            response, "expert_123"
        )
        assert result is None

    def test_missing_available_slots_returns_none(self):
        """Response without available_slots key should return None"""
        response = {}
        result = CronofyService.find_earliest_available_slot_from_response(
            response, "expert_123"
        )
        assert result is None

    def test_expert_not_in_participants_returns_none(self):
        """Should return None when expert is not in any slot's participants"""
        response = {
            "available_slots": [
                {
                    "start": "2025-01-15T10:00:00Z",
                    "end": "2025-01-15T11:00:00Z",
                    "participants": [
                        {"sub": "other_expert_456"}
                    ]
                }
            ]
        }
        result = CronofyService.find_earliest_available_slot_from_response(
            response, "expert_123"
        )
        assert result is None

    def test_single_slot_returns_correct_timestamp(self):
        """Single matching slot should return its unix timestamp"""
        response = {
            "available_slots": [
                {
                    "start": "2025-01-15T10:00:00Z",
                    "end": "2025-01-15T11:00:00Z",
                    "participants": [
                        {"sub": "expert_123"}
                    ]
                }
            ]
        }
        result = CronofyService.find_earliest_available_slot_from_response(
            response, "expert_123"
        )
        # 2025-01-15T10:00:00Z = 1736935200
        assert result == 1736935200

    def test_multiple_slots_returns_earliest(self):
        """Should return the earliest slot when multiple are available"""
        response = {
            "available_slots": [
                {
                    "start": "2025-01-15T14:00:00Z",  # Later slot
                    "end": "2025-01-15T15:00:00Z",
                    "participants": [{"sub": "expert_123"}]
                },
                {
                    "start": "2025-01-15T10:00:00Z",  # Earlier slot
                    "end": "2025-01-15T11:00:00Z",
                    "participants": [{"sub": "expert_123"}]
                },
                {
                    "start": "2025-01-15T12:00:00Z",  # Middle slot
                    "end": "2025-01-15T13:00:00Z",
                    "participants": [{"sub": "expert_123"}]
                }
            ]
        }
        result = CronofyService.find_earliest_available_slot_from_response(
            response, "expert_123"
        )
        # Should return earliest: 2025-01-15T10:00:00Z = 1736935200
        assert result == 1736935200

    def test_filters_to_correct_expert(self):
        """Should only consider slots where the specified expert is a participant"""
        response = {
            "available_slots": [
                {
                    "start": "2025-01-15T08:00:00Z",  # Earlier but different expert
                    "end": "2025-01-15T09:00:00Z",
                    "participants": [{"sub": "other_expert"}]
                },
                {
                    "start": "2025-01-15T10:00:00Z",  # Our expert's slot
                    "end": "2025-01-15T11:00:00Z",
                    "participants": [{"sub": "expert_123"}]
                }
            ]
        }
        result = CronofyService.find_earliest_available_slot_from_response(
            response, "expert_123"
        )
        # Should return expert_123's slot, not the earlier one
        assert result == 1736935200

    def test_slot_with_multiple_participants(self):
        """Should find expert in slot with multiple participants"""
        response = {
            "available_slots": [
                {
                    "start": "2025-01-15T10:00:00Z",
                    "end": "2025-01-15T11:00:00Z",
                    "participants": [
                        {"sub": "expert_abc"},
                        {"sub": "expert_123"},
                        {"sub": "expert_xyz"}
                    ]
                }
            ]
        }
        result = CronofyService.find_earliest_available_slot_from_response(
            response, "expert_123"
        )
        assert result == 1736935200

    def test_slot_without_start_time_returns_none(self):
        """Slot missing start time should return None"""
        response = {
            "available_slots": [
                {
                    "end": "2025-01-15T11:00:00Z",
                    "participants": [{"sub": "expert_123"}]
                }
            ]
        }
        result = CronofyService.find_earliest_available_slot_from_response(
            response, "expert_123"
        )
        assert result is None

    def test_malformed_timestamp_returns_none(self):
        """Malformed timestamp should return None gracefully"""
        response = {
            "available_slots": [
                {
                    "start": "not-a-valid-timestamp",
                    "end": "2025-01-15T11:00:00Z",
                    "participants": [{"sub": "expert_123"}]
                }
            ]
        }
        result = CronofyService.find_earliest_available_slot_from_response(
            response, "expert_123"
        )
        assert result is None

    def test_empty_participants_list(self):
        """Slot with empty participants list should be skipped"""
        response = {
            "available_slots": [
                {
                    "start": "2025-01-15T10:00:00Z",
                    "end": "2025-01-15T11:00:00Z",
                    "participants": []
                }
            ]
        }
        result = CronofyService.find_earliest_available_slot_from_response(
            response, "expert_123"
        )
        assert result is None

    def test_missing_participants_key(self):
        """Slot without participants key should be handled gracefully"""
        response = {
            "available_slots": [
                {
                    "start": "2025-01-15T10:00:00Z",
                    "end": "2025-01-15T11:00:00Z"
                }
            ]
        }
        result = CronofyService.find_earliest_available_slot_from_response(
            response, "expert_123"
        )
        assert result is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
