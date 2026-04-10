"""Tests for bls_stats.http_client — retry and client creation."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import httpx
import pytest

from bls_stats.http_client import (
    MAX_RETRIES,
    create_client,
    get_with_retry,
)


class TestCreateClient:
    def test_returns_httpx_client(self):
        c = create_client()
        assert isinstance(c, httpx.Client)
        c.close()

    def test_custom_headers(self):
        c = create_client(headers={'X-Test': 'value'})
        assert 'X-Test' in c.headers
        c.close()


class TestGetWithRetry:
    def test_success_on_first_try(self):
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client.get.return_value = mock_response

        result = get_with_retry(mock_client, 'https://example.com/test')
        assert result == mock_response
        assert mock_client.get.call_count == 1

    @patch('bls_stats.http_client.time.sleep')
    def test_retries_on_429(self, mock_sleep):
        mock_client = MagicMock()
        rate_limited = MagicMock()
        rate_limited.status_code = 429
        success = MagicMock()
        success.status_code = 200
        mock_client.get.side_effect = [rate_limited, success]

        result = get_with_retry(mock_client, 'https://example.com/test')
        assert result == success
        assert mock_client.get.call_count == 2
        mock_sleep.assert_called_once()

    @patch('bls_stats.http_client.time.sleep')
    def test_retries_on_500(self, mock_sleep):
        mock_client = MagicMock()
        error = MagicMock()
        error.status_code = 500
        success = MagicMock()
        success.status_code = 200
        mock_client.get.side_effect = [error, success]

        result = get_with_retry(mock_client, 'https://example.com/test')
        assert result == success

    @patch('bls_stats.http_client.time.sleep')
    def test_exponential_backoff(self, mock_sleep):
        mock_client = MagicMock()
        error = MagicMock()
        error.status_code = 503
        success = MagicMock()
        success.status_code = 200
        mock_client.get.side_effect = [error, error, error, success]

        get_with_retry(mock_client, 'https://example.com/test')
        waits = [call.args[0] for call in mock_sleep.call_args_list]
        assert waits == [1, 2, 4]

    def test_does_not_retry_4xx(self):
        mock_client = MagicMock()
        not_found = MagicMock()
        not_found.status_code = 404
        not_found.raise_for_status.side_effect = httpx.HTTPStatusError(
            '404', request=MagicMock(), response=not_found,
        )
        mock_client.get.return_value = not_found

        with pytest.raises(httpx.HTTPStatusError):
            get_with_retry(mock_client, 'https://example.com/test')
        assert mock_client.get.call_count == 1

    @patch.dict(os.environ, {'BLS_API_KEY': 'test-key-123'})
    def test_appends_api_key(self):
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client.get.return_value = mock_response

        get_with_retry(mock_client, 'https://data.bls.gov/test')
        _, kwargs = mock_client.get.call_args
        assert kwargs['params']['registrationkey'] == 'test-key-123'

    @patch.dict(os.environ, {'BLS_API_KEY': 'test-key-123'})
    def test_no_api_key_for_non_bls(self):
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client.get.return_value = mock_response

        get_with_retry(mock_client, 'https://example.com/test')
        _, kwargs = mock_client.get.call_args
        assert 'registrationkey' not in kwargs['params']

    def test_max_retries_constant(self):
        assert MAX_RETRIES == 8
