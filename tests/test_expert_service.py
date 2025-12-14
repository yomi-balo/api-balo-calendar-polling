"""Unit tests for ExpertService"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from services.expert_service import ExpertService, BatchResults
from schemas.availability import AvailabilityResult, AvailabilityData


# =============================================================================
# BatchResults Tests
# =============================================================================

class TestBatchResults:
    """Tests for BatchResults dataclass"""

    def test_default_values(self):
        """BatchResults should initialize with zeros and empty list"""
        results = BatchResults()
        assert results.processed == 0
        assert results.failed == 0
        assert results.algolia_updates == []

    def test_custom_values(self):
        """BatchResults should accept custom values"""
        results = BatchResults(processed=5, failed=2, algolia_updates=[{"id": 1}])
        assert results.processed == 5
        assert results.failed == 2
        assert results.algolia_updates == [{"id": 1}]

    def test_merge_combines_counts(self):
        """merge() should add processed and failed counts"""
        results1 = BatchResults(processed=3, failed=1)
        results2 = BatchResults(processed=2, failed=4)

        results1.merge(results2)

        assert results1.processed == 5
        assert results1.failed == 5

    def test_merge_combines_algolia_updates(self):
        """merge() should extend algolia_updates list"""
        results1 = BatchResults(algolia_updates=[{"id": 1}, {"id": 2}])
        results2 = BatchResults(algolia_updates=[{"id": 3}])

        results1.merge(results2)

        assert len(results1.algolia_updates) == 3
        assert results1.algolia_updates == [{"id": 1}, {"id": 2}, {"id": 3}]

    def test_merge_empty_into_populated(self):
        """Merging empty results should not change original"""
        results1 = BatchResults(processed=5, failed=2, algolia_updates=[{"id": 1}])
        results2 = BatchResults()

        results1.merge(results2)

        assert results1.processed == 5
        assert results1.failed == 2
        assert len(results1.algolia_updates) == 1


# =============================================================================
# _is_batch_422_failure Tests
# =============================================================================

class TestIsBatch422Failure:
    """Tests for _is_batch_422_failure detection"""

    def test_empty_results_returns_false(self):
        """Empty results should not be considered 422 failure"""
        results = []
        assert ExpertService._is_batch_422_failure(results) is False

    def test_single_result_returns_false(self):
        """Single result should not trigger batch fallback (even if 422)"""
        results = [
            AvailabilityResult(
                expert_id="exp1",
                bubble_uid="uid1",
                expert_name="Expert 1",
                success=False,
                error_reason="Invalid expert data (422)"
            )
        ]
        assert ExpertService._is_batch_422_failure(results) is False

    def test_all_422_errors_returns_true(self):
        """All results with 422 errors should return True"""
        results = [
            AvailabilityResult(
                expert_id="exp1",
                bubble_uid="uid1",
                expert_name="Expert 1",
                success=False,
                error_reason="Invalid expert data (422)"
            ),
            AvailabilityResult(
                expert_id="exp2",
                bubble_uid="uid2",
                expert_name="Expert 2",
                success=False,
                error_reason="Invalid expert data (422)"
            ),
        ]
        assert ExpertService._is_batch_422_failure(results) is True

    def test_mixed_success_and_422_returns_false(self):
        """Mix of success and 422 errors should return False"""
        results = [
            AvailabilityResult(
                expert_id="exp1",
                bubble_uid="uid1",
                expert_name="Expert 1",
                success=True,
                availability_data=AvailabilityData(
                    expert_id="exp1",
                    earliest_available_unix=1234567890,
                    last_updated="2025-01-01T00:00:00Z"
                )
            ),
            AvailabilityResult(
                expert_id="exp2",
                bubble_uid="uid2",
                expert_name="Expert 2",
                success=False,
                error_reason="Invalid expert data (422)"
            ),
        ]
        assert ExpertService._is_batch_422_failure(results) is False

    def test_all_different_errors_returns_false(self):
        """All failures but not 422 should return False"""
        results = [
            AvailabilityResult(
                expert_id="exp1",
                bubble_uid="uid1",
                expert_name="Expert 1",
                success=False,
                error_reason="Empty availability"
            ),
            AvailabilityResult(
                expert_id="exp2",
                bubble_uid="uid2",
                expert_name="Expert 2",
                success=False,
                error_reason="Authentication error (401)"
            ),
        ]
        assert ExpertService._is_batch_422_failure(results) is False

    def test_none_error_reason_handled(self):
        """None error_reason should not cause exception"""
        results = [
            AvailabilityResult(
                expert_id="exp1",
                bubble_uid="uid1",
                expert_name="Expert 1",
                success=False,
                error_reason=None
            ),
            AvailabilityResult(
                expert_id="exp2",
                bubble_uid="uid2",
                expert_name="Expert 2",
                success=False,
                error_reason=None
            ),
        ]
        assert ExpertService._is_batch_422_failure(results) is False


# =============================================================================
# _build_algolia_record Tests
# =============================================================================

class TestBuildAlgoliaRecord:
    """Tests for _build_algolia_record utility"""

    def test_builds_correct_structure(self):
        """Should build Algolia record with correct fields"""
        # Create mock expert
        expert = MagicMock()
        expert.bubble_uid = "bubble_123"
        expert.expert_name = "John Doe"
        expert.cronofy_id = "cronofy_456"

        availability = AvailabilityData(
            expert_id="cronofy_456",
            earliest_available_unix=1736935200,
            last_updated="2025-01-15T10:00:00Z"
        )

        record = ExpertService._build_algolia_record(expert, availability)

        assert record["objectID"] == "bubble_123"
        assert record["expert_name"] == "John Doe"
        assert record["cronofy_id"] == "cronofy_456"
        assert record["earliest_available_unix"] == 1736935200
        assert record["availability_last_updated"] == "2025-01-15T10:00:00Z"

    def test_handles_none_timestamp(self):
        """Should handle None earliest_available_unix"""
        expert = MagicMock()
        expert.bubble_uid = "bubble_123"
        expert.expert_name = "Jane Doe"
        expert.cronofy_id = "cronofy_789"

        availability = AvailabilityData(
            expert_id="cronofy_789",
            earliest_available_unix=None,
            last_updated="2025-01-15T10:00:00Z"
        )

        record = ExpertService._build_algolia_record(expert, availability)

        assert record["earliest_available_unix"] is None


# =============================================================================
# _handle_expert_success Tests
# =============================================================================

class TestHandleExpertSuccess:
    """Tests for _handle_expert_success"""

    @pytest.mark.asyncio
    async def test_clears_error_and_updates_availability(self):
        """Should clear errors and update expert availability"""
        expert = MagicMock()
        expert.bubble_uid = "bubble_123"
        expert.expert_name = "John Doe"
        expert.cronofy_id = "cronofy_456"
        expert.earliest_available_unix = 1000000000
        expert.update_availability = AsyncMock()

        availability = AvailabilityData(
            expert_id="cronofy_456",
            earliest_available_unix=1736935200,
            last_updated="2025-01-15T10:00:00Z"
        )

        with patch('services.expert_service.AvailabilityError') as mock_error:
            mock_error.clear_error = AsyncMock()

            record = await ExpertService._handle_expert_success(
                expert, availability, batch_num=1
            )

            mock_error.clear_error.assert_called_once_with("bubble_123")
            expert.update_availability.assert_called_once_with(1736935200)
            assert record["objectID"] == "bubble_123"

    @pytest.mark.asyncio
    async def test_returns_algolia_record(self):
        """Should return properly formatted Algolia record"""
        expert = MagicMock()
        expert.bubble_uid = "bubble_123"
        expert.expert_name = "John Doe"
        expert.cronofy_id = "cronofy_456"
        expert.earliest_available_unix = None
        expert.update_availability = AsyncMock()

        availability = AvailabilityData(
            expert_id="cronofy_456",
            earliest_available_unix=1736935200,
            last_updated="2025-01-15T10:00:00Z"
        )

        with patch('services.expert_service.AvailabilityError') as mock_error:
            mock_error.clear_error = AsyncMock()

            record = await ExpertService._handle_expert_success(
                expert, availability, batch_num=1
            )

            assert "objectID" in record
            assert "expert_name" in record
            assert "cronofy_id" in record
            assert "earliest_available_unix" in record
            assert "availability_last_updated" in record


# =============================================================================
# _handle_expert_failure Tests
# =============================================================================

class TestHandleExpertFailure:
    """Tests for _handle_expert_failure"""

    @pytest.mark.asyncio
    async def test_logs_error_to_database(self):
        """Should log error to AvailabilityError table"""
        expert = MagicMock()
        expert.bubble_uid = "bubble_123"
        expert.expert_name = "John Doe"
        expert.cronofy_id = "cronofy_456"

        with patch('services.expert_service.AvailabilityError') as mock_error:
            mock_error.log_error = AsyncMock()

            await ExpertService._handle_expert_failure(
                expert,
                error_reason="Invalid expert data (422)",
                error_details="Calendar not found",
                batch_num=1
            )

            mock_error.log_error.assert_called_once_with(
                bubble_uid="bubble_123",
                expert_name="John Doe",
                cronofy_id="cronofy_456",
                error_reason="Invalid expert data (422)",
                error_details="Calendar not found"
            )


# =============================================================================
# _process_batch_results Tests
# =============================================================================

class TestProcessBatchResults:
    """Tests for _process_batch_results"""

    @pytest.mark.asyncio
    async def test_processes_all_success(self):
        """Should process all successful results"""
        experts = [MagicMock(), MagicMock()]
        for i, exp in enumerate(experts):
            exp.bubble_uid = f"uid_{i}"
            exp.expert_name = f"Expert {i}"
            exp.cronofy_id = f"cronofy_{i}"
            exp.earliest_available_unix = None
            exp.update_availability = AsyncMock()

        results = [
            AvailabilityResult(
                expert_id="cronofy_0",
                bubble_uid="uid_0",
                expert_name="Expert 0",
                success=True,
                availability_data=AvailabilityData(
                    expert_id="cronofy_0",
                    earliest_available_unix=1000,
                    last_updated="2025-01-01T00:00:00Z"
                )
            ),
            AvailabilityResult(
                expert_id="cronofy_1",
                bubble_uid="uid_1",
                expert_name="Expert 1",
                success=True,
                availability_data=AvailabilityData(
                    expert_id="cronofy_1",
                    earliest_available_unix=2000,
                    last_updated="2025-01-01T00:00:00Z"
                )
            ),
        ]

        with patch('services.expert_service.AvailabilityError') as mock_error:
            mock_error.clear_error = AsyncMock()

            batch_results = await ExpertService._process_batch_results(
                experts, results, batch_num=1
            )

            assert batch_results.processed == 2
            assert batch_results.failed == 0
            assert len(batch_results.algolia_updates) == 2

    @pytest.mark.asyncio
    async def test_processes_mixed_results(self):
        """Should handle mix of success and failure"""
        experts = [MagicMock(), MagicMock()]
        for i, exp in enumerate(experts):
            exp.bubble_uid = f"uid_{i}"
            exp.expert_name = f"Expert {i}"
            exp.cronofy_id = f"cronofy_{i}"
            exp.earliest_available_unix = None
            exp.update_availability = AsyncMock()

        results = [
            AvailabilityResult(
                expert_id="cronofy_0",
                bubble_uid="uid_0",
                expert_name="Expert 0",
                success=True,
                availability_data=AvailabilityData(
                    expert_id="cronofy_0",
                    earliest_available_unix=1000,
                    last_updated="2025-01-01T00:00:00Z"
                )
            ),
            AvailabilityResult(
                expert_id="cronofy_1",
                bubble_uid="uid_1",
                expert_name="Expert 1",
                success=False,
                error_reason="Empty availability",
                error_details="No slots found"
            ),
        ]

        with patch('services.expert_service.AvailabilityError') as mock_error:
            mock_error.clear_error = AsyncMock()
            mock_error.log_error = AsyncMock()

            batch_results = await ExpertService._process_batch_results(
                experts, results, batch_num=1
            )

            assert batch_results.processed == 1
            assert batch_results.failed == 1
            assert len(batch_results.algolia_updates) == 1


# =============================================================================
# Error Handler Tests
# =============================================================================

class TestBatchErrorHandlers:
    """Tests for batch-level error handlers"""

    @pytest.mark.asyncio
    async def test_handle_batch_api_error_422_triggers_fallback(self):
        """422 API error should trigger individual fallback"""
        from services.cronofy_service import CronofyAPIError
        import httpx

        error = CronofyAPIError(
            "422 Unprocessable Entity",
            status_code=422,
            response=MagicMock()
        )

        experts = [MagicMock()]
        experts[0].bubble_uid = "uid_1"
        experts[0].expert_name = "Expert 1"
        experts[0].cronofy_id = "cronofy_1"

        with patch.object(
            ExpertService, '_process_experts_individually',
            new_callable=AsyncMock
        ) as mock_individual:
            mock_individual.return_value = BatchResults(processed=1, failed=0)

            result = await ExpertService._handle_batch_api_error(
                error, experts, batch_num=1, fetch_params=(60, 0, 0, 30)
            )

            mock_individual.assert_called_once()
            assert result.processed == 1

    @pytest.mark.asyncio
    async def test_handle_batch_api_error_401_logs_all(self):
        """401 API error should log error for all experts"""
        from services.cronofy_service import CronofyAPIError

        error = CronofyAPIError(
            "401 Unauthorized",
            status_code=401,
            response=MagicMock()
        )

        experts = [MagicMock(), MagicMock()]
        for i, exp in enumerate(experts):
            exp.bubble_uid = f"uid_{i}"
            exp.expert_name = f"Expert {i}"
            exp.cronofy_id = f"cronofy_{i}"

        with patch('services.expert_service.AvailabilityError') as mock_error:
            mock_error.log_error = AsyncMock()

            result = await ExpertService._handle_batch_api_error(
                error, experts, batch_num=1, fetch_params=(60, 0, 0, 30)
            )

            assert mock_error.log_error.call_count == 2
            assert result.failed == 2
            assert result.processed == 0

    @pytest.mark.asyncio
    async def test_handle_batch_server_error_logs_all(self):
        """Server error should log error for all experts"""
        import httpx

        response = MagicMock()
        response.status_code = 500

        error = httpx.HTTPStatusError(
            "500 Internal Server Error",
            request=MagicMock(),
            response=response
        )

        experts = [MagicMock(), MagicMock()]
        for i, exp in enumerate(experts):
            exp.bubble_uid = f"uid_{i}"
            exp.expert_name = f"Expert {i}"
            exp.cronofy_id = f"cronofy_{i}"

        with patch('services.expert_service.AvailabilityError') as mock_error:
            mock_error.log_error = AsyncMock()

            result = await ExpertService._handle_batch_server_error(error, experts)

            assert mock_error.log_error.call_count == 2
            assert result.failed == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
