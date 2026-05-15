from types import SimpleNamespace

from starlette.responses import Response

from beam import ownership


def test_get_or_create_owner_key_uses_existing_cookie():
    request = SimpleNamespace(cookies={ownership.OWNER_COOKIE_NAME: "client-123"})

    owner_key, created = ownership.get_or_create_owner_key(request)

    assert owner_key == "client-123"
    assert created is False


def test_get_or_create_owner_key_creates_missing_cookie():
    request = SimpleNamespace(cookies={})

    owner_key, created = ownership.get_or_create_owner_key(request)

    assert owner_key
    assert created is True


def test_set_owner_cookie_marks_cookie_http_only():
    response = Response()

    ownership.set_owner_cookie(response, "client-123")

    cookie = response.headers["set-cookie"]
    assert f"{ownership.OWNER_COOKIE_NAME}=client-123" in cookie
    assert "HttpOnly" in cookie
    assert "SameSite=lax" in cookie
