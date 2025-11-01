from typing import Dict
import os, httpx

class CognismProvider:
    def __init__(self, base_url: str, batch_size: int = 50, api_key: str | None = None):
        self.base_url = base_url.rstrip('/')
        self.batch_size = batch_size
        self.api_key = api_key or os.getenv("COGNISM_API_KEY")

    def enrich(self, rec: Dict) -> Dict:
        # Stub for offline demo. Replace with real API call + mapping.
        return rec
