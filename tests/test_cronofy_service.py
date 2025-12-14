"""Unit tests for CronofyService request building and batching"""

import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone, timedelta
from freezegun import freeze_time

from services.cronofy_service import CronofyService, CronofyAPIError


# =============================================================================
# Helper to create mock experts
# =============================================================================

def create_mock_expert(cronofy_id: str, calendar_ids: list, name: str = None, bubble_uid: str = None):
    """Create a mock expert object"""
    expert = MagicMock()
    expert.cronofy_id = cronofy_id
    expert.calendar_ids = calendar_ids
    expert.expert_name = name or f"Expert {cronofy_id}"
    expert.bubble_uid = bubble_uid or f"bubble_{cronofy_id}"
    return expert


# =============================================================================
# batch_experts Tests
# =============================================================================

class TestBatchExperts:
    """Tests for batch_experts method"""

    def test_empty_list_returns_empty(self):
        """Empty expert list should return empty batches"""
        result = CronofyService.batch_experts([], batch_size=10)
        assert result == []

    def test_single_expert_single_batch(self):
        """Single expert should return single batch"""
        experts = [create_mock_expert("exp1", ["cal1"])]
        result = CronofyService.batch_experts(experts, batch_size=10)

        assert len(result) == 1
        assert len(result[0]) == 1

    def test_exact_batch_size(self):
        """Experts equal to batch size should return single batch"""
        experts = [create_mock_expert(f"exp{i}", [f"cal{i}"]) for i in range(10)]
        result = CronofyService.batch_experts(experts, batch_size=10)

        assert len(result) == 1
        assert len(result[0]) == 10

    def test_multiple_full_batches(self):
        """Should create multiple full batches"""
        experts = [create_mock_expert(f"exp{i}", [f"cal{i}"]) for i in range(30)]
        result = CronofyService.batch_experts(experts, batch_size=10)

        assert len(result) == 3
        assert all(len(batch) == 10 for batch in result)

    def test_partial_last_batch(self):
        """Last batch can be smaller than batch_size"""
        experts = [create_mock_expert(f"exp{i}", [f"cal{i}"]) for i in range(25)]
        result = CronofyService.batch_experts(experts, batch_size=10)

        assert len(result) == 3
        assert len(result[0]) == 10
        assert len(result[1]) == 10
        assert len(result[2]) == 5

    def test_custom_batch_size(self):
        """Should respect custom batch size"""
        experts = [create_mock_expert(f"exp{i}", [f"cal{i}"]) for i in range(15)]
        result = CronofyService.batch_experts(experts, batch_size=5)

        assert len(result) == 3
        assert all(len(batch) == 5 for batch in result)

    def test_batch_size_larger_than_list(self):
        """Should handle batch size larger than expert count"""
        experts = [create_mock_expert(f"exp{i}", [f"cal{i}"]) for i in range(3)]
        result = CronofyService.batch_experts(experts, batch_size=100)

        assert len(result) == 1
        assert len(result[0]) == 3

    def test_preserves_expert_order(self):
        """Should preserve expert order in batches"""
        experts = [create_mock_expert(f"exp{i}", [f"cal{i}"]) for i in range(15)]
        result = CronofyService.batch_experts(experts, batch_size=5)

        # Flatten and check order
        flattened = [exp for batch in result for exp in batch]
        for i, exp in enumerate(flattened):
            assert exp.cronofy_id == f"exp{i}"


# =============================================================================
# create_availability_request_body Tests
# =============================================================================

class TestCreateAvailabilityRequestBody:
    """Tests for create_availability_request_body method"""

    def test_basic_structure(self):
        """Should create correct basic structure"""
        experts = [create_mock_expert("exp1", ["cal1"])]
        query_periods = [{"start": "2025-01-01T00:00:00Z", "end": "2025-01-31T00:00:00Z"}]

        result = CronofyService.create_availability_request_body(
            experts, query_periods
        )

        assert "participants" in result
        assert "query_periods" in result
        assert "required_duration" in result
        assert "buffer" in result
        assert "max_results" in result
        assert "response_format" in result

    def test_participants_structure(self):
        """Should create correct participants structure"""
        experts = [
            create_mock_expert("exp1", ["cal1", "cal2"]),
            create_mock_expert("exp2", ["cal3"])
        ]
        query_periods = [{"start": "2025-01-01T00:00:00Z", "end": "2025-01-31T00:00:00Z"}]

        result = CronofyService.create_availability_request_body(
            experts, query_periods
        )

        participants = result["participants"]
        assert len(participants) == 1
        assert participants[0]["required"] == 1

        members = participants[0]["members"]
        assert len(members) == 2

        # First member
        assert members[0]["sub"] == "exp1"
        assert members[0]["calendar_ids"] == ["cal1", "cal2"]
        assert members[0]["managed_availability"] is True

        # Second member
        assert members[1]["sub"] == "exp2"
        assert members[1]["calendar_ids"] == ["cal3"]

    def test_default_duration(self):
        """Should use default duration of 60 minutes"""
        experts = [create_mock_expert("exp1", ["cal1"])]
        query_periods = [{"start": "2025-01-01T00:00:00Z", "end": "2025-01-31T00:00:00Z"}]

        result = CronofyService.create_availability_request_body(
            experts, query_periods
        )

        assert result["required_duration"]["minutes"] == 60

    def test_custom_duration(self):
        """Should use custom duration"""
        experts = [create_mock_expert("exp1", ["cal1"])]
        query_periods = [{"start": "2025-01-01T00:00:00Z", "end": "2025-01-31T00:00:00Z"}]

        result = CronofyService.create_availability_request_body(
            experts, query_periods, duration=30
        )

        assert result["required_duration"]["minutes"] == 30

    def test_default_buffers(self):
        """Should use default buffers of 0"""
        experts = [create_mock_expert("exp1", ["cal1"])]
        query_periods = [{"start": "2025-01-01T00:00:00Z", "end": "2025-01-31T00:00:00Z"}]

        result = CronofyService.create_availability_request_body(
            experts, query_periods
        )

        assert result["buffer"]["before"]["minutes"] == 0
        assert result["buffer"]["after"]["minutes"] == 0

    def test_custom_buffers(self):
        """Should use custom buffers"""
        experts = [create_mock_expert("exp1", ["cal1"])]
        query_periods = [{"start": "2025-01-01T00:00:00Z", "end": "2025-01-31T00:00:00Z"}]

        result = CronofyService.create_availability_request_body(
            experts, query_periods, buffer_before=15, buffer_after=10
        )

        assert result["buffer"]["before"]["minutes"] == 15
        assert result["buffer"]["after"]["minutes"] == 10

    def test_query_periods_passed_through(self):
        """Should pass query periods unchanged"""
        experts = [create_mock_expert("exp1", ["cal1"])]
        query_periods = [
            {"start": "2025-01-01T00:00:00Z", "end": "2025-01-15T00:00:00Z"},
            {"start": "2025-01-20T00:00:00Z", "end": "2025-01-31T00:00:00Z"}
        ]

        result = CronofyService.create_availability_request_body(
            experts, query_periods
        )

        assert result["query_periods"] == query_periods

    def test_max_results_is_512(self):
        """Should set max_results to 512"""
        experts = [create_mock_expert("exp1", ["cal1"])]
        query_periods = [{"start": "2025-01-01T00:00:00Z", "end": "2025-01-31T00:00:00Z"}]

        result = CronofyService.create_availability_request_body(
            experts, query_periods
        )

        assert result["max_results"] == 512

    def test_response_format_is_slots(self):
        """Should set response_format to 'slots'"""
        experts = [create_mock_expert("exp1", ["cal1"])]
        query_periods = [{"start": "2025-01-01T00:00:00Z", "end": "2025-01-31T00:00:00Z"}]

        result = CronofyService.create_availability_request_body(
            experts, query_periods
        )

        assert result["response_format"] == "slots"

    def test_empty_expert_list(self):
        """Should handle empty expert list"""
        query_periods = [{"start": "2025-01-01T00:00:00Z", "end": "2025-01-31T00:00:00Z"}]

        result = CronofyService.create_availability_request_body(
            [], query_periods
        )

        assert result["participants"][0]["members"] == []


# =============================================================================
# create_default_query_periods Tests
# =============================================================================

class TestCreateDefaultQueryPeriods:
    """Tests for create_default_query_periods method"""

    @freeze_time("2025-01-15T10:30:45Z")
    def test_creates_single_period(self):
        """Should create a single query period"""
        result = CronofyService.create_default_query_periods(days_ahead=30)

        assert len(result) == 1
        assert "start" in result[0]
        assert "end" in result[0]

    @freeze_time("2025-01-15T10:30:45Z")
    def test_start_time_is_now(self):
        """Start time should be current time (without microseconds)"""
        result = CronofyService.create_default_query_periods(days_ahead=30)

        assert result[0]["start"] == "2025-01-15T10:30:45Z"

    @freeze_time("2025-01-15T10:30:45Z")
    def test_end_time_respects_days_ahead(self):
        """End time should be days_ahead from now"""
        result = CronofyService.create_default_query_periods(days_ahead=30)

        assert result[0]["end"] == "2025-02-14T10:30:45Z"

    @freeze_time("2025-01-15T10:30:45Z")
    def test_custom_days_ahead(self):
        """Should respect custom days_ahead parameter"""
        result = CronofyService.create_default_query_periods(days_ahead=7)

        assert result[0]["end"] == "2025-01-22T10:30:45Z"

    @freeze_time("2025-01-15T10:30:45.123456Z")
    def test_removes_microseconds(self):
        """Should remove microseconds from timestamps"""
        result = CronofyService.create_default_query_periods(days_ahead=1)

        # Should not contain microseconds
        assert ".123456" not in result[0]["start"]
        assert result[0]["start"] == "2025-01-15T10:30:45Z"

    @freeze_time("2025-01-15T10:30:45Z")
    def test_uses_utc_timezone(self):
        """Should use UTC timezone (Z suffix)"""
        result = CronofyService.create_default_query_periods(days_ahead=1)

        assert result[0]["start"].endswith("Z")
        assert result[0]["end"].endswith("Z")


# =============================================================================
# CronofyAPIError Tests
# =============================================================================

class TestCronofyAPIError:
    """Tests for CronofyAPIError exception class"""

    def test_stores_status_code(self):
        """Should store status code"""
        response = MagicMock()
        error = CronofyAPIError("Error message", status_code=422, response=response)

        assert error.status_code == 422

    def test_stores_response(self):
        """Should store response object"""
        response = MagicMock()
        error = CronofyAPIError("Error message", status_code=401, response=response)

        assert error.response is response

    def test_message_accessible(self):
        """Should have accessible error message"""
        response = MagicMock()
        error = CronofyAPIError("Custom error message", status_code=403, response=response)

        assert str(error) == "Custom error message"

    def test_inherits_from_exception(self):
        """Should be catchable as Exception"""
        response = MagicMock()
        error = CronofyAPIError("Error", status_code=422, response=response)

        assert isinstance(error, Exception)


# =============================================================================
# _get_error_details_for_status Tests
# =============================================================================

class TestGetErrorDetailsForStatus:
    """Tests for _get_error_details_for_status helper"""

    def test_401_error(self):
        """Should return auth error details for 401"""
        experts = [create_mock_expert("exp1", ["cal1"])]
        error_reason, error_details = CronofyService._get_error_details_for_status(
            401, experts, "Original error"
        )

        assert "401" in error_reason
        assert "Authentication" in error_reason or "Unauthorized" in error_details

    def test_403_error(self):
        """Should return permission error details for 403"""
        experts = [create_mock_expert("exp1", ["cal1"])]
        error_reason, error_details = CronofyService._get_error_details_for_status(
            403, experts, "Original error"
        )

        assert "403" in error_reason
        assert "Permission" in error_reason or "Forbidden" in error_details

    def test_422_error(self):
        """Should return invalid data error details for 422"""
        experts = [create_mock_expert("exp1", ["cal1"], name="John Doe")]
        error_reason, error_details = CronofyService._get_error_details_for_status(
            422, experts, "Original error"
        )

        assert "422" in error_reason
        assert "Invalid" in error_reason or "Unprocessable" in error_details
        assert "John Doe" in error_details  # Should include expert name

    def test_429_error(self):
        """Should return rate limit error details for 429"""
        experts = [create_mock_expert("exp1", ["cal1"])]
        error_reason, error_details = CronofyService._get_error_details_for_status(
            429, experts, "Original error"
        )

        assert "429" in error_reason
        assert "Rate limit" in error_reason or "Too Many" in error_details

    def test_unknown_status_code(self):
        """Should return generic error for unknown status codes"""
        experts = [create_mock_expert("exp1", ["cal1"])]
        error_reason, error_details = CronofyService._get_error_details_for_status(
            418, experts, "I'm a teapot"
        )

        assert "418" in error_reason
        assert "I'm a teapot" in error_details

    def test_empty_experts_list(self):
        """Should handle empty experts list"""
        error_reason, error_details = CronofyService._get_error_details_for_status(
            422, [], "Original error"
        )

        assert "422" in error_reason
        # Should not crash, may show "Unknown" for expert name


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
