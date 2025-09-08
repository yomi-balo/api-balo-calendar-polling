from tortoise.models import Model
from tortoise import fields
from datetime import datetime, timezone
import pytz


class AvailabilityError(Model):
    """Model for tracking availability check errors for experts"""

    bubble_uid = fields.CharField(max_length=255, pk=True)  # Primary key for uniqueness per expert
    expert_name = fields.CharField(max_length=255)
    cronofy_id = fields.CharField(max_length=255)
    error_reason = fields.CharField(max_length=500)  # "API error", "Empty availability", etc.
    error_details = fields.TextField(null=True)  # Additional error context
    unix_timestamp = fields.BigIntField()  # Unix timestamp when error occurred
    melbourne_time = fields.CharField(max_length=100)  # Human-readable Melbourne time
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "availability_errors"

    def __str__(self):
        return f"AvailabilityError({self.expert_name}, {self.error_reason})"

    @classmethod
    async def log_error(
        cls, 
        bubble_uid: str, 
        expert_name: str, 
        cronofy_id: str, 
        error_reason: str, 
        error_details: str = None
    ) -> 'AvailabilityError':
        """Log or update an availability error for an expert"""
        now = datetime.now(timezone.utc)
        unix_timestamp = int(now.timestamp())
        
        # Convert to Melbourne timezone for human readability
        melbourne_tz = pytz.timezone('Australia/Melbourne')
        melbourne_time = now.astimezone(melbourne_tz).strftime('%Y-%m-%d %H:%M:%S %Z')
        
        # Use get_or_create to handle the case where an error already exists
        error_record, created = await cls.get_or_create(
            bubble_uid=bubble_uid,
            defaults={
                'expert_name': expert_name,
                'cronofy_id': cronofy_id,
                'error_reason': error_reason,
                'error_details': error_details,
                'unix_timestamp': unix_timestamp,
                'melbourne_time': melbourne_time
            }
        )
        
        if not created:
            # Update existing record with new error details
            error_record.error_reason = error_reason
            error_record.error_details = error_details
            error_record.unix_timestamp = unix_timestamp
            error_record.melbourne_time = melbourne_time
            await error_record.save(update_fields=[
                'error_reason', 'error_details', 'unix_timestamp', 
                'melbourne_time', 'updated_at'
            ])
        
        return error_record

    @classmethod
    async def clear_error(cls, bubble_uid: str):
        """Remove error log for an expert (called when availability check succeeds)"""
        await cls.filter(bubble_uid=bubble_uid).delete()

    @classmethod
    async def get_all_errors(cls):
        """Get all current availability errors ordered by most recent"""
        return await cls.all().order_by('-updated_at')

    @classmethod
    async def get_error_by_bubble_uid(cls, bubble_uid: str):
        """Get error log for a specific expert"""
        try:
            return await cls.get(bubble_uid=bubble_uid)
        except cls.DoesNotExist:
            return None