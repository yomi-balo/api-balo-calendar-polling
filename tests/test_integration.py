"""Integration tests for the API endpoints"""

import pytest
from fastapi.testclient import TestClient
from tortoise.contrib.test import finalizer, initializer
import asyncio
from unittest.mock import patch

from main import app
from models.expert import Expert
from config.settings import settings


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session", autouse=True)
async def initialize_tests():
    """Initialize test database"""
    await initializer(
        ["models.expert"],
        db_url="sqlite://:memory:",
        app_label="models",
    )
    yield
    await finalizer()


@pytest.fixture
def client():
    """Create test client"""
    with TestClient(app) as client:
        yield client


@pytest.fixture
async def sample_expert():
    """Create a sample expert for testing"""
    expert = await Expert.create(
        expert_name="Test Expert",
        cronofy_id="test_cronofy_123",
        calendar_ids=["cal_1", "cal_2"],
        bubble_uid="bubble_123",
        version=0
    )
    yield expert
    await expert.delete()


class TestHealthEndpoints:
    """Test health check endpoints"""
    
    def test_root_endpoint(self, client):
        """Test root endpoint returns app name"""
        response = client.get("/")
        assert response.status_code == 200
        assert "is running" in response.json()["message"]
    
    def test_health_check_endpoint(self, client):
        """Test health check endpoint structure"""
        response = client.get("/health")
        assert response.status_code == 200
        
        data = response.json()
        required_fields = [
            "status", "scheduler_running", "database_connected",
            "database_url_set", "cronofy_token_set", "algolia_configured",
            "cache_enabled", "cache_size", "app_version", "timestamp"
        ]
        
        for field in required_fields:
            assert field in data
        
        assert data["status"] in ["healthy", "unhealthy"]
        assert isinstance(data["cache_size"], int)
        assert data["app_version"] == settings.APP_VERSION


class TestExpertEndpoints:
    """Test expert management endpoints"""
    
    def test_create_experts_bulk(self, client):
        """Test bulk expert creation"""
        expert_data = {
            "experts": [
                {
                    "expert_name": "Integration Test Expert",
                    "cronofy_id": "integration_cronofy_123",
                    "calendar_ids": ["cal_int_1"],
                    "bubble_uid": "bubble_int_123"
                }
            ]
        }
        
        response = client.post("/experts/calendars", json=expert_data)
        assert response.status_code == 200
        
        data = response.json()
        assert data["updated_count"] == 1
        assert "Successfully processed" in data["message"]
    
    def test_get_experts_with_pagination(self, client):
        """Test paginated expert retrieval"""
        response = client.get("/experts/calendars?page=1&limit=10")
        assert response.status_code == 200
        
        data = response.json()
        required_fields = ["items", "total", "page", "limit", "total_pages", "has_next", "has_previous"]
        
        for field in required_fields:
            assert field in data
        
        assert data["page"] == 1
        assert data["limit"] == 10
        assert isinstance(data["items"], list)
    
    @pytest.mark.asyncio
    async def test_get_expert_by_bubble_uid(self, client, sample_expert):
        """Test retrieving expert by bubble UID"""
        response = client.get(f"/experts/{sample_expert.bubble_uid}")
        assert response.status_code == 200
        
        data = response.json()
        assert data["expert_name"] == sample_expert.expert_name
        assert data["bubble_uid"] == sample_expert.bubble_uid
        assert data["cronofy_id"] == sample_expert.cronofy_id
    
    @pytest.mark.asyncio
    async def test_update_expert(self, client, sample_expert):
        """Test updating expert data"""
        update_data = {
            "cronofy_id": "updated_cronofy_456",
            "calendar_ids": ["cal_updated_1", "cal_updated_2"]
        }
        
        response = client.put(f"/experts/{sample_expert.bubble_uid}", json=update_data)
        assert response.status_code == 200
        
        data = response.json()
        assert data["cronofy_id"] == update_data["cronofy_id"]
        assert set(data["calendar_ids"]) == set(update_data["calendar_ids"])
    
    @pytest.mark.asyncio
    async def test_delete_expert(self, client):
        """Test deleting expert"""
        # First create an expert to delete
        expert = await Expert.create(
            expert_name="Delete Test Expert",
            cronofy_id="delete_test_cronofy",
            calendar_ids=["delete_cal_1"],
            bubble_uid="delete_bubble_123",
            version=0
        )
        
        response = client.delete(f"/experts/{expert.bubble_uid}")
        assert response.status_code == 200
        
        data = response.json()
        assert "deleted successfully" in data["message"]
        
        # Verify expert was deleted
        deleted_expert = await Expert.get_by_bubble_uid(expert.bubble_uid)
        assert deleted_expert is None
    
    def test_get_nonexistent_expert(self, client):
        """Test retrieving non-existent expert returns 404"""
        response = client.get("/experts/nonexistent_uid")
        assert response.status_code == 404
        assert response.json()["detail"] == "Expert not found"


class TestValidation:
    """Test input validation"""
    
    def test_invalid_expert_data(self, client):
        """Test validation with invalid expert data"""
        invalid_data = {
            "experts": [
                {
                    "expert_name": "",  # Empty name should fail
                    "cronofy_id": "test",
                    "calendar_ids": [],  # Empty calendar_ids should fail
                    "bubble_uid": "test"
                }
            ]
        }
        
        response = client.post("/experts/calendars", json=invalid_data)
        assert response.status_code == 422  # Validation error
    
    def test_duplicate_bubble_uids(self, client):
        """Test validation prevents duplicate bubble UIDs"""
        duplicate_data = {
            "experts": [
                {
                    "expert_name": "Expert 1",
                    "cronofy_id": "cronofy_1",
                    "calendar_ids": ["cal_1"],
                    "bubble_uid": "duplicate_uid"
                },
                {
                    "expert_name": "Expert 2",
                    "cronofy_id": "cronofy_2",
                    "calendar_ids": ["cal_2"],
                    "bubble_uid": "duplicate_uid"  # Duplicate UID
                }
            ]
        }
        
        response = client.post("/experts/calendars", json=duplicate_data)
        assert response.status_code == 422  # Validation error
    
    def test_pagination_validation(self, client):
        """Test pagination parameter validation"""
        # Test invalid page number
        response = client.get("/experts/calendars?page=0&limit=10")
        assert response.status_code == 422
        
        # Test invalid limit
        response = client.get("/experts/calendars?page=1&limit=0")
        assert response.status_code == 422
        
        # Test limit too high
        response = client.get("/experts/calendars?page=1&limit=1000")
        assert response.status_code == 422


@pytest.mark.asyncio
class TestCacheIntegration:
    """Test cache integration"""
    
    async def test_cache_invalidation_on_updates(self, client, sample_expert):
        """Test that cache is invalidated when experts are updated"""
        # First request should cache the data
        response1 = client.get("/experts/calendars")
        assert response1.status_code == 200
        
        # Update the expert
        update_data = {
            "cronofy_id": "updated_for_cache_test",
            "calendar_ids": ["updated_cal"]
        }
        
        update_response = client.put(f"/experts/{sample_expert.bubble_uid}", json=update_data)
        assert update_response.status_code == 200
        
        # Second request should reflect the update (cache should be cleared)
        response2 = client.get("/experts/calendars")
        assert response2.status_code == 200
        
        # Find the updated expert in the response
        experts = response2.json()["items"]
        updated_expert = next(
            (expert for expert in experts if expert["bubble_uid"] == sample_expert.bubble_uid),
            None
        )
        
        assert updated_expert is not None
        assert updated_expert["cronofy_id"] == "updated_for_cache_test"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])