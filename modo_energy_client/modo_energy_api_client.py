import os
import requests
import pandas as pd
from typing import Optional, Dict, Any
from datetime import date
import time
import logging

from tqdm.auto import tqdm


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
        Shows a progress bar if tqdm is installed.
        If a 403 error is encountered, sleeps for 65 seconds and retries the request.
        """

        url = f"{self.BASE_URL}/{endpoint}"
        df = pd.DataFrame()
        page = 0
        with tqdm(total=None, desc="Fetching pages ", unit="page") as pbar:
            while url:
                while True:
                    try:
                        response = requests.get(
                            url,
                            headers={"accept": "application/json"},
                            params=params,
                        )
                        response.raise_for_status()
                        break
                    except requests.exceptions.HTTPError as e:
                        if response.status_code == 403:
                            logging.warning(
                                "Received 403 Forbidden. Sleeping for 65 seconds before retrying..."
                            )
                            time.sleep(65)
                            continue
                        else:
                            raise
                data = response.json()
                if "results" in data:
                    df = pd.concat(
                        [df, pd.DataFrame(data["results"])], ignore_index=True
                    )
                url = data.get("next")
                params = None  # Only use params on first request
                page += 1

                pbar.update(1)

        return df

    def get_ercot_generation_fuel_mix(
        self, date_from: date, date_to: date
    ) -> pd.DataFrame:
        """
        Fetch ERCOT generation fuel mix data.
        Example endpoint: 'us/ercot/nodal/generation-fuel-mix'
        Accepts date_from and date_to as arguments (YYYY-MM-DD or YYYY-MM format).
        """
        endpoint = "us/ercot/system/fuel-mix"
        params = {
            "date_from": date_from.strftime("%Y-%m-%d"),
            "date_to": date_to.strftime("%Y-%m-%d"),
        }
        df = self.get_paginated(endpoint, params)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)
        return df


# Example usage:
# client = ModoEnergyAPIClient(api_token="your_x_token")
# df = client.get_paginated("gb/modo/markets/system-price-live", params={"date_from": "2024-01-25", "date_to": "2024-01-25"})
# print(df.head())
