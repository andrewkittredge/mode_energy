import os
import requests
import pandas as pd
from typing import Optional, Dict, Any


class ModoEnergyAPIClient:
    """
    Python client for the Modo Energy API.
    https://developers.modoenergy.com/docs/getting-started
    """

    BASE_URL = "https://api.modoenergy.com/pub/v1"

    def __init__(self, api_token: Optional[str] = None):
        self.api_token = api_token or os.getenv("MODO_API_TOKEN")
        if not self.api_token:
            raise ValueError(
                "Modo Energy API token must be provided or set in the MODO_API_TOKEN environment variable."
            )
        self.headers = {"X-Token": self.api_token}

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict:
        url = f"{self.BASE_URL}/{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def get_paginated(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        Fetches all paginated results from an endpoint and returns as a pandas DataFrame.
        """
        url = f"{self.BASE_URL}/{endpoint}"
        df = pd.DataFrame()
        while url:
            response = requests.get(
                url,
                # headers=self.headers,
                params=params,
            )
            response.raise_for_status()
            data = response.json()
            if "results" in data:
                df = pd.concat([df, pd.DataFrame(data["results"])], ignore_index=True)
            url = data.get("next")
            params = None  # Only use params on first request
        return df


# Example usage:
# client = ModoEnergyAPIClient(api_token="your_x_token")
# df = client.get_paginated("gb/modo/markets/system-price-live", params={"date_from": "2024-01-25", "date_to": "2024-01-25"})
# print(df.head())
