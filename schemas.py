from typing import Any, Dict
from pydantic import BaseModel


# Simple, permissive input schema for incoming applications.
# This allows any JSON payload fields through and returns them via `.dict()`
class ApplicationIn(BaseModel):
    class Config:
        extra = "allow"


# Optionally, you can add more strict schemas later (e.g. ApplicationOut, Decision, etc.)

