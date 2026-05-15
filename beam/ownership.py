import secrets


OWNER_COOKIE_NAME = "beam_owner_key"
OWNER_COOKIE_MAX_AGE = 60 * 60 * 24 * 365


def normalize_owner_key(value):
    value = str(value or "").strip()
    return value or None


def new_owner_key():
    return secrets.token_urlsafe(32)


def get_request_owner_key(request):
    cookies = getattr(request, "cookies", {}) or {}
    return normalize_owner_key(cookies.get(OWNER_COOKIE_NAME))


def get_or_create_owner_key(request):
    owner_key = get_request_owner_key(request)
    if owner_key:
        return owner_key, False
    return new_owner_key(), True


def set_owner_cookie(response, owner_key):
    owner_key = normalize_owner_key(owner_key)
    if not owner_key:
        return
    response.set_cookie(
        OWNER_COOKIE_NAME,
        owner_key,
        max_age=OWNER_COOKIE_MAX_AGE,
        httponly=True,
        samesite="lax",
    )
