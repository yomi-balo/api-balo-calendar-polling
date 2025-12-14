"""Unit tests for retry utilities"""

import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

from core.retry_utils import retry_with_exponential_backoff, with_retry


# =============================================================================
# retry_with_exponential_backoff Tests
# =============================================================================

class TestRetryWithExponentialBackoff:
    """Tests for retry_with_exponential_backoff function"""

    @pytest.mark.asyncio
    async def test_succeeds_on_first_try(self):
        """Should return result on first successful call"""
        call_count = 0

        async def success_func():
            nonlocal call_count
            call_count += 1
            return "success"

        result = await retry_with_exponential_backoff(
            success_func,
            max_retries=3,
            base_delay=0.01
        )

        assert result == "success"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_retries_on_failure(self):
        """Should retry when function raises exception"""
        call_count = 0

        async def fail_twice():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("temporary error")
            return "success"

        result = await retry_with_exponential_backoff(
            fail_twice,
            max_retries=3,
            base_delay=0.01,
            exceptions=(ValueError,)
        )

        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_raises_after_max_retries(self):
        """Should raise exception after max retries exhausted"""
        call_count = 0

        async def always_fail():
            nonlocal call_count
            call_count += 1
            raise ValueError("permanent error")

        with pytest.raises(ValueError) as exc_info:
            await retry_with_exponential_backoff(
                always_fail,
                max_retries=3,
                base_delay=0.01,
                exceptions=(ValueError,)
            )

        assert "permanent error" in str(exc_info.value)
        assert call_count == 4  # Initial + 3 retries

    @pytest.mark.asyncio
    async def test_only_retries_specified_exceptions(self):
        """Should only retry on specified exception types"""
        call_count = 0

        async def raise_type_error():
            nonlocal call_count
            call_count += 1
            raise TypeError("not retryable")

        with pytest.raises(TypeError):
            await retry_with_exponential_backoff(
                raise_type_error,
                max_retries=3,
                base_delay=0.01,
                exceptions=(ValueError,)  # Only retry ValueError
            )

        # Should not retry since TypeError is not in exceptions
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_exponential_delay_calculation(self):
        """Should increase delay exponentially"""
        delays = []

        async def track_delay():
            raise ValueError("fail")

        with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            mock_sleep.side_effect = lambda d: delays.append(d)

            with pytest.raises(ValueError):
                await retry_with_exponential_backoff(
                    track_delay,
                    max_retries=3,
                    base_delay=1.0,
                    max_delay=100.0,
                    exponential_factor=2.0,
                    jitter=False  # Disable jitter for predictable test
                )

        # Delays should be: 1.0, 2.0, 4.0
        assert len(delays) == 3
        assert delays[0] == 1.0
        assert delays[1] == 2.0
        assert delays[2] == 4.0

    @pytest.mark.asyncio
    async def test_max_delay_cap(self):
        """Should cap delay at max_delay"""
        delays = []

        async def track_delay():
            raise ValueError("fail")

        with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            mock_sleep.side_effect = lambda d: delays.append(d)

            with pytest.raises(ValueError):
                await retry_with_exponential_backoff(
                    track_delay,
                    max_retries=5,
                    base_delay=10.0,
                    max_delay=15.0,  # Cap at 15
                    exponential_factor=2.0,
                    jitter=False
                )

        # Delays should be capped: 10, 15, 15, 15, 15
        assert all(d <= 15.0 for d in delays)

    @pytest.mark.asyncio
    async def test_jitter_adds_randomness(self):
        """Should add jitter to delays when enabled"""
        delays_with_jitter = []
        delays_without_jitter = []

        async def fail_func():
            raise ValueError("fail")

        # With jitter
        with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            mock_sleep.side_effect = lambda d: delays_with_jitter.append(d)
            with pytest.raises(ValueError):
                await retry_with_exponential_backoff(
                    fail_func,
                    max_retries=2,
                    base_delay=1.0,
                    jitter=True
                )

        # Without jitter
        with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            mock_sleep.side_effect = lambda d: delays_without_jitter.append(d)
            with pytest.raises(ValueError):
                await retry_with_exponential_backoff(
                    fail_func,
                    max_retries=2,
                    base_delay=1.0,
                    jitter=False
                )

        # With jitter, delays should be between 0.5*base and 1.0*base
        for delay in delays_with_jitter:
            assert 0.5 <= delay <= 2.0  # Account for exponential

    @pytest.mark.asyncio
    async def test_zero_retries(self):
        """Should not retry when max_retries is 0"""
        call_count = 0

        async def fail_func():
            nonlocal call_count
            call_count += 1
            raise ValueError("fail")

        with pytest.raises(ValueError):
            await retry_with_exponential_backoff(
                fail_func,
                max_retries=0,
                base_delay=0.01
            )

        assert call_count == 1

    @pytest.mark.asyncio
    async def test_multiple_exception_types(self):
        """Should retry on multiple exception types"""
        call_count = 0

        async def alternate_errors():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("error 1")
            elif call_count == 2:
                raise TypeError("error 2")
            return "success"

        result = await retry_with_exponential_backoff(
            alternate_errors,
            max_retries=3,
            base_delay=0.01,
            exceptions=(ValueError, TypeError)
        )

        assert result == "success"
        assert call_count == 3


# =============================================================================
# with_retry Decorator Tests
# =============================================================================

class TestWithRetryDecorator:
    """Tests for with_retry decorator"""

    @pytest.mark.asyncio
    async def test_decorator_wraps_function(self):
        """Should wrap async function with retry logic"""
        call_count = 0

        @with_retry(max_retries=3, base_delay=0.01, exceptions=(ValueError,))
        async def flaky_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("temporary")
            return "success"

        result = await flaky_function()

        assert result == "success"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_decorator_preserves_function_name(self):
        """Should preserve original function name"""
        @with_retry(max_retries=1)
        async def my_named_function():
            return "result"

        assert my_named_function.__name__ == "my_named_function"

    @pytest.mark.asyncio
    async def test_decorator_passes_arguments(self):
        """Should pass args and kwargs to wrapped function"""
        @with_retry(max_retries=1, base_delay=0.01)
        async def function_with_args(a, b, c=None):
            return f"{a}-{b}-{c}"

        result = await function_with_args("x", "y", c="z")
        assert result == "x-y-z"

    @pytest.mark.asyncio
    async def test_decorator_with_default_params(self):
        """Should work with default parameters"""
        @with_retry()
        async def simple_function():
            return "ok"

        result = await simple_function()
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_decorator_raises_after_retries(self):
        """Should raise after max retries exhausted"""
        @with_retry(max_retries=2, base_delay=0.01, exceptions=(RuntimeError,))
        async def always_fails():
            raise RuntimeError("permanent failure")

        with pytest.raises(RuntimeError) as exc_info:
            await always_fails()

        assert "permanent failure" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_decorator_custom_exception_filter(self):
        """Should only retry specified exceptions"""
        call_count = 0

        @with_retry(max_retries=3, base_delay=0.01, exceptions=(ValueError,))
        async def raises_key_error():
            nonlocal call_count
            call_count += 1
            raise KeyError("not retryable")

        with pytest.raises(KeyError):
            await raises_key_error()

        # Should not retry KeyError
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_decorator_custom_delays(self):
        """Should use custom delay parameters"""
        @with_retry(
            max_retries=2,
            base_delay=0.5,
            max_delay=1.0,
            exponential_factor=3.0,
            jitter=False
        )
        async def fail_once():
            if not hasattr(fail_once, '_called'):
                fail_once._called = True
                raise ValueError("first call fails")
            return "success"

        result = await fail_once()
        assert result == "success"


# =============================================================================
# Edge Cases and Error Handling
# =============================================================================

class TestRetryEdgeCases:
    """Tests for edge cases and error handling"""

    @pytest.mark.asyncio
    async def test_handles_async_generator(self):
        """Should work with functions returning various types"""
        @with_retry(max_retries=1, base_delay=0.01)
        async def return_list():
            return [1, 2, 3]

        result = await return_list()
        assert result == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_handles_none_return(self):
        """Should handle functions returning None"""
        @with_retry(max_retries=1, base_delay=0.01)
        async def return_none():
            return None

        result = await return_none()
        assert result is None

    @pytest.mark.asyncio
    async def test_handles_exception_with_no_message(self):
        """Should handle exceptions without messages"""
        @with_retry(max_retries=1, base_delay=0.01, exceptions=(Exception,))
        async def raise_empty_exception():
            raise Exception()

        with pytest.raises(Exception):
            await raise_empty_exception()

    @pytest.mark.asyncio
    async def test_concurrent_retries(self):
        """Should handle concurrent retry operations"""
        call_counts = {"a": 0, "b": 0}

        @with_retry(max_retries=2, base_delay=0.01, exceptions=(ValueError,))
        async def flaky_a():
            call_counts["a"] += 1
            if call_counts["a"] < 2:
                raise ValueError("a fails")
            return "a_success"

        @with_retry(max_retries=2, base_delay=0.01, exceptions=(ValueError,))
        async def flaky_b():
            call_counts["b"] += 1
            if call_counts["b"] < 2:
                raise ValueError("b fails")
            return "b_success"

        results = await asyncio.gather(flaky_a(), flaky_b())

        assert results == ["a_success", "b_success"]
        assert call_counts["a"] == 2
        assert call_counts["b"] == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
