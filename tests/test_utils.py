import pytest

from tests.utils import retry


def test_retry():
    assert retry(lambda: "foo") == "foo"
    assert retry(lambda x: f"foo: {x}", kwargs={"x": "bar"}) == "foo: bar"
    assert retry(lambda x: f"foo: {x}", args=("bar",)) == "foo: bar"


def test_retry_error():
    def _raise_error():
        raise ValueError("oh noes")

    with pytest.raises(TimeoutError) as e:
        retry(_raise_error, retries=2, interval=0.1)

    assert e.match("oh noes")
