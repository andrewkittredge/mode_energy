import os
import requests
import requests_cache
import pandas as pd
from typing import Optional, Dict, Any
from datetime import date
import time
import logging

from tqdm.auto import tqdm
from pandera.typing import DataFrame as pandera_DataFrame
import pandera as pa

from modo_energy_client import ERCOTGenerationFuelMixSchema
from modo_energy_client import ERCOT_BESS_owners_schema

REQUESTS_CACHE = requests_cache.CachedSession(cache_name="MODO_API_CACHE")


class ModoEnergyAPIClient:
    """
    Python client for the Modo Energy API.
    https://developers.modoenergy.com/docs/getting-started
    """

    BASE_URL = "https://api.modoenergy.com/pub/v1"

    def __init__(
        self,
        api_token: Optional[str] = None,
        cache_name: str = "modo_energy_cache",
        cache_expire: int = 600,
    ):
        self.api_token = api_token or os.getenv("MODO_API_TOKEN")
        if not self.api_token:
            raise ValueError(
                "Modo Energy API token must be provided or set in the MODO_API_TOKEN environment variable."
            )
        self.headers = {"X-Token": self.api_token}
        # Set up requests-cache for all requests

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
                        response = REQUESTS_CACHE.get(
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

    @pa.check_types
    def get_ercot_generation_fuel_mix(
        self, date_from: date, date_to: date
    ) -> pandera_DataFrame[ERCOTGenerationFuelMixSchema.ERCOTGenerationFuelMixSchema]:
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

    @pa.check_types
    def get_ercot_modo_owners(
        self, date_from: str = None, date_to: str = None, **kwargs
    ) -> pandera_DataFrame[ERCOT_BESS_owners_schema.ERCOT_BESS_owners_schema]:
        """
        Fetch ERCOT BESS owners data from the 'us/ercot/modo/owners' endpoint.
        Optionally accepts date_from and date_to as YYYY-MM or YYYY-MM-DD strings.
        Additional query params can be passed as kwargs.

        https://developers.modoenergy.com/reference/bess-owners-ercot
        """
        endpoint = "us/ercot/modo/owners"
        params = {}

        params["date_from"] = date_from

        params["date_to"] = date_to
        params.update(kwargs)
        df = self.get_paginated(endpoint, params)
        df["date"] = pd.to_datetime(df["date"]).dt.normalize()
        return df
