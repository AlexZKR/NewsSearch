import urllib3

from newssearch.config.settings import RetryBackoffSettings


def get_retry(rs: RetryBackoffSettings) -> urllib3.Retry:  # pragma: no cover
    return urllib3.Retry(
        total=rs.max_retries,
        allowed_methods=rs.allowed_methods,
        status_forcelist=rs.status_forcelist,
        backoff_factor=rs.backoff_factor,
        backoff_max=rs.max_backoff,
        raise_on_redirect=True,
        raise_on_status=True,
    )
