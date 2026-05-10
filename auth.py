# auth.py — Password hashing and session token helpers.
# All password storage uses bcrypt with a per-password salt.
# Session tokens are UUID4 strings stored in the DB.

import uuid
from typing import Optional

SESSION_EXPIRY_HOURS = 8


def hash_password(password: str) -> str:
    import bcrypt
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def check_password(password: str, password_hash: str) -> bool:
    import bcrypt
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except Exception:
        return False


def generate_session_token() -> str:
    return str(uuid.uuid4())


def validate_password_strength(password: str) -> Optional[str]:
    if len(password) < 8:
        return "Password must be at least 8 characters"
    return None
