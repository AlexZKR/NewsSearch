from pytest import skip

skip(
    reason="Tests for deprecated requests transport. See requests_transport.py",
    allow_module_level=True,
)
