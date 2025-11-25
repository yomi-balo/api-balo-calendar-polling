import os
import logging
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Settings:
    """Application settings and environment variables"""

    # API Configuration
    CRONOFY_ACCESS_TOKEN: Optional[str] = os.getenv("CRONOFY_ACCESS_TOKEN")
    ALGOLIA_APP_ID: Optional[str] = os.getenv("ALGOLIA_APP_ID")
    ALGOLIA_API_KEY: Optional[str] = os.getenv("ALGOLIA_API_KEY")
    ALGOLIA_INDEX_NAME: str = os.getenv("ALGOLIA_INDEX_NAME", "experts")

    # Database Configuration
    _database_url: Optional[str] = os.getenv("DATABASE_URL")

    @property
    def DATABASE_URL(self) -> Optional[str]:
        """Convert postgresql:// to postgres:// for Tortoise ORM compatibility"""
        if self._database_url and self._database_url.startswith("postgresql://"):
            url = self._database_url.replace("postgresql://", "postgres://", 1)
            logger.info("Converted postgresql:// to postgres:// for Tortoise ORM compatibility")
            return url
        return self._database_url

    # Scheduler Configuration
    AVAILABILITY_UPDATE_INTERVAL_MINUTES: int = int(os.getenv("AVAILABILITY_UPDATE_INTERVAL_MINUTES", "5"))
    ERROR_RETRY_INTERVAL_MINUTES: int = int(os.getenv("ERROR_RETRY_INTERVAL_MINUTES", "10"))
    ERROR_RETRY_MIN_AGE_MINUTES: int = int(os.getenv("ERROR_RETRY_MIN_AGE_MINUTES", "2"))

    CRONOFY_API_BASE: str = "https://api-au.cronofy.com/v1"
    CRONOFY_MAX_EXPERTS_PER_REQUEST: int = 15  # Changed from 15 calendars to 10 experts
    CRONOFY_REQUEST_TIMEOUT: int = 25
    CRONOFY_DEFAULT_DURATION: int = int(os.getenv("CRONOFY_DEFAULT_DURATION", "60"))  # Meeting duration in minutes
    CRONOFY_DEFAULT_BUFFER_BEFORE: int = int(
        os.getenv("CRONOFY_DEFAULT_BUFFER_BEFORE", "0"))  # Buffer before in minutes
    CRONOFY_DEFAULT_BUFFER_AFTER: int = int(os.getenv("CRONOFY_DEFAULT_BUFFER_AFTER", "0"))  # Buffer after in minutes
    CRONOFY_DEFAULT_DAYS_AHEAD: int = int(os.getenv("CRONOFY_DEFAULT_DAYS_AHEAD", "30"))  # How many days to look ahead

    # Algolia Configuration
    ALGOLIA_BATCH_SIZE: int = 100

    # Performance Configuration  
    CACHE_DEFAULT_TTL: int = int(os.getenv("CACHE_DEFAULT_TTL", "300"))  # 5 minutes
    CACHE_MAX_SIZE: int = int(os.getenv("CACHE_MAX_SIZE", "1000"))  # Max cached items
    CACHE_MAX_MEMORY_MB: int = int(os.getenv("CACHE_MAX_MEMORY_MB", "50"))  # Railway optimized
    
    # Database Performance
    DB_MIN_CONNECTIONS: int = int(os.getenv("DB_MIN_CONNECTIONS", "1"))
    DB_MAX_CONNECTIONS: int = int(os.getenv("DB_MAX_CONNECTIONS", "5"))  # Railway optimized
    DB_CONNECTION_TIMEOUT: int = int(os.getenv("DB_CONNECTION_TIMEOUT", "60"))
    
    # HTTP Client Performance  
    HTTP_CONNECT_TIMEOUT: int = int(os.getenv("HTTP_CONNECT_TIMEOUT", "10"))
    HTTP_READ_TIMEOUT: int = int(os.getenv("HTTP_READ_TIMEOUT", "25"))
    HTTP_MAX_KEEPALIVE: int = int(os.getenv("HTTP_MAX_KEEPALIVE", "10"))
    HTTP_MAX_CONNECTIONS: int = int(os.getenv("HTTP_MAX_CONNECTIONS", "20"))

    # App Configuration
    APP_NAME: str = "Calendar Caching API"
    APP_VERSION: str = "1.1.0"  # Updated for performance improvements

    def validate(self) -> None:
        """Validate required settings"""
        if not self.DATABASE_URL:
            raise RuntimeError("DATABASE_URL is required")

    def get_cronofy_configured(self) -> bool:
        """Check if Cronofy is properly configured"""
        return bool(self.CRONOFY_ACCESS_TOKEN)

    def get_algolia_configured(self) -> bool:
        """Check if Algolia is properly configured"""
        return bool(self.ALGOLIA_APP_ID and self.ALGOLIA_API_KEY)


# Global settings instance
settings = Settings()