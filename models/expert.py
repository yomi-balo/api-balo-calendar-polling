from tortoise.models import Model
from tortoise import fields
from typing import List, Optional
from datetime import datetime, timezone


class Expert(Model):
    """Expert model for storing calendar information"""

    id = fields.IntField(pk=True)
    expert_name = fields.CharField(max_length=255)  # Name as JSON-safe string
    cronofy_id = fields.CharField(max_length=255, unique=True, index=True)  # Cronofy ID
    calendar_ids = fields.JSONField()  # Array of calendar IDs
    bubble_uid = fields.CharField(max_length=255, unique=True, index=True)  # Bubble UID
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)
    last_availability_check = fields.DatetimeField(null=True)
    earliest_available_unix = fields.BigIntField(null=True)
    version = fields.IntField(default=0)  # For optimistic locking

    class Meta:
        table = "experts"

    def __str__(self):
        return f"Expert({self.expert_name}, {self.cronofy_id})"

    @classmethod
    async def upsert(cls, expert_name: str, cronofy_id: str, calendar_ids: List[str], bubble_uid: str) -> 'Expert':
        """Insert or update expert record using bubble_uid as unique identifier"""
        expert, created = await cls.get_or_create(
            bubble_uid=bubble_uid,
            defaults={
                'expert_name': expert_name,
                'cronofy_id': cronofy_id,
                'calendar_ids': calendar_ids,
            }
        )

        if not created:
            # Update existing expert with version increment for optimistic locking
            expert.expert_name = expert_name
            expert.cronofy_id = cronofy_id
            expert.calendar_ids = calendar_ids
            expert.updated_at = datetime.now(timezone.utc)
            expert.version += 1
            await expert.save(update_fields=['expert_name', 'cronofy_id', 'calendar_ids', 'updated_at', 'version'])

        return expert

    @classmethod
    async def get_by_bubble_uid(cls, bubble_uid: str) -> Optional['Expert']:
        """Get expert by bubble UID"""
        try:
            return await cls.get(bubble_uid=bubble_uid)
        except cls.DoesNotExist:
            return None

    @classmethod
    async def get_by_cronofy_id(cls, cronofy_id: str) -> Optional['Expert']:
        """Get expert by Cronofy ID"""
        try:
            return await cls.get(cronofy_id=cronofy_id)
        except cls.DoesNotExist:
            return None

    @classmethod
    async def get_all_ordered(cls) -> List['Expert']:
        """Get all experts ordered by most recently updated"""
        return await cls.all().order_by('-updated_at')

    async def update_availability(self, earliest_available_unix: Optional[int]):
        """Update expert's availability data with optimistic locking"""
        current_version = self.version
        rows_updated = await Expert.filter(
            bubble_uid=self.bubble_uid,
            version=current_version
        ).update(
            last_availability_check=datetime.now(timezone.utc),
            earliest_available_unix=earliest_available_unix,
            version=current_version + 1
        )
        
        if rows_updated == 0:
            # Record was modified by another process, refresh and retry once
            await self.refresh_from_db()
            await Expert.filter(bubble_uid=self.bubble_uid).update(
                last_availability_check=datetime.now(timezone.utc),
                earliest_available_unix=earliest_available_unix,
                version=self.version + 1
            )
        else:
            # Update local version to match database
            self.version = current_version + 1
            self.last_availability_check = datetime.now(timezone.utc)
            self.earliest_available_unix = earliest_available_unix