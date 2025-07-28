import logging
from typing import List, Optional
from algoliasearch.search_client import SearchClient
from algoliasearch.search_index import SearchIndex

from config.settings import settings

logger = logging.getLogger(__name__)


class AlgoliaService:
    """Service for handling Algolia search integration"""

    def __init__(self):
        self.client: Optional[SearchClient] = None
        self.index: Optional[SearchIndex] = None
        self._initialize()

    def _initialize(self):
        """Initialize Algolia client with proper error handling"""
        if not settings.get_algolia_configured():
            logger.warning("Algolia credentials not provided - Algolia features will be disabled")
            return

        try:
            self.client = SearchClient.create(settings.ALGOLIA_APP_ID, settings.ALGOLIA_API_KEY)
            self.index = self.client.init_index(settings.ALGOLIA_INDEX_NAME)
            logger.info("Algolia client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Algolia: {e}")
            self.client = None
            self.index = None

    def is_configured(self) -> bool:
        """Check if Algolia is properly configured"""
        return self.index is not None

    async def update_expert_records(self, records: List[dict]) -> bool:
        """Update expert records in Algolia"""
        if not self.is_configured():
            logger.warning(f"Algolia updates skipped for {len(records)} records - Algolia not configured")
            return False

        if not records:
            return True

        try:
            # Split into batches if needed
            algolia_batches = [
                records[i:i + settings.ALGOLIA_BATCH_SIZE]
                for i in range(0, len(records), settings.ALGOLIA_BATCH_SIZE)
            ]

            for batch in algolia_batches:
                self.index.partial_update_objects(batch)

            logger.info(f"Successfully updated {len(records)} expert records in Algolia")
            return True

        except Exception as e:
            logger.error(f"Failed to update Algolia: {str(e)}")
            return False


# Global Algolia service instance
algolia_service = AlgoliaService()