# -*- coding: utf-8 -*-
"""
FAO data extractor for milk production (2014–2023), corrected filter logic.
"""
from datetime import datetime
import pandas as pd
import requests
from typing import Literal, Any, Dict, List


# FAOSTAT API wrapper
class Request:
    settings: Dict[str, Any] = {"timeout": 120.0}
    expected_settings = {"timeout"}

    def get(self, url: str, **kwargs) -> requests.Response:
        with requests.get(url, **kwargs, **self.settings) as resp:
            resp.raise_for_status()
            return resp

    @staticmethod
    def get_data(response: requests.Response) -> List[Dict[str, Any]]:
        return response.json()["data"]


def fetch_data(url: str, **kwargs) -> List[Dict[str, Any]]:
    resp = Request().get(url, **kwargs)
    return Request.get_data(resp)


class FAOSTAT:
    baseurl = "https://faostatservices.fao.org/api/v1"
    lang = "en"

    def get_codelist(self, code_id: str, domain: str) -> pd.DataFrame:
        url = f"{self.baseurl}/{self.lang}/codes/{code_id}/{domain}"
        data = fetch_data(url)
        return pd.DataFrame(data)

    def get_data(self,
                 domain: str,
                 filters: Dict[str, Any],
                 show_codes=True,
                 show_flags=True,
                 show_notes=True,
                 null_values=True,
                 limit=-1,
                 output_type="objects") -> pd.DataFrame:
        url = f"{self.baseurl}/{self.lang}/data/{domain}"
        params = []
        for k, v in filters.items():
            params.append((k, v))
        params += [
            ("show_codes", show_codes),
            ("show_flags", show_flags),
            ("show_notes", show_notes),
            ("null_values", null_values),
            ("limit", limit),
            ("output_type", output_type),
        ]
        data = fetch_data(url, params=params)
        return pd.DataFrame(data)


faostat_api = FAOSTAT()


# ------------------------------------------------------
# Corrected functions for fixed years 2014–2023
# ------------------------------------------------------

def get_data(
    domain_code: str = "QCL",
    filters: Dict[str, Any] = None,
) -> pd.DataFrame:
    """
    Retrieve FAOSTAT data for milk item codes, years 2014–2023.
    """
    if filters is None:
        filters = {
            'item': [882, 886, 887, 888, 889, 894, 895, 896, 897, 898, 899,
                     901, 904, 951, 952, 953, 955, 982, 983, 984,
                     1020, 1021, 1022, 1130],
            'year': [2014, 2015, 2016, 2017, 2018,
                     2019, 2020, 2021, 2022, 2023]
        }
    print(f"Retrieving {domain_code} for items: {filters['item']} and years: {filters['year']}")
    df = faostat_api.get_data(domain_code, filters)
    return df


def load_production_data_of_crops_and_livestock_products(
    domain_code: str = "QCL",
    crops: List[str] = [
        "Raw milk of cattle", "Butter of cow milk", "Ghee from cow milk",
        "Skim milk of cows", "Whole milk, condensed", "Whole milk, evaporated",
        "Skim milk, evaporated", "Skim milk, condensed", "Whole milk powder",
        "Skim milk and whey powder", "Buttermilk, dry", "Cheese from whole cow milk",
        "Cheese from skimmed cow milk", "Raw milk of buffalo", "Butter of buffalo milk",
        "Ghee from buffalo milk", "Cheese from milk of buffalo, fresh or processed",
        "Raw milk of sheep", "Butter and ghee of sheep milk",
        "Cheese from milk of sheep, fresh or processed",
        "Raw milk of goats", "Cheese from milk of goats, fresh or processed",
        "Butter of goat milk", "Raw milk of camel"
    ],
    years: List[int] = [2014, 2015, 2016, 2017, 2018,
                       2019, 2020, 2021, 2022, 2023],
    save_as_csv: Literal[False, str] = "faostat_data_{dataset}_{describe}_at_{timestamp}.csv"
) -> pd.DataFrame:
    """
    Load FAOSTAT milk production data (2014–2023) by label selection.
    """
    all_items = faostat_api.get_codelist("items", domain_code)
    selection = all_items[all_items['label'].isin(crops)]
    codes = selection['code'].astype(int).tolist()

    df = faostat_api.get_data(
        domain_code,
        filters={'item': codes, 'year': years}
    )

    if save_as_csv:
        fname = save_as_csv.format(
            dataset=domain_code,
            describe=f"{len(crops)}items_{len(years)}years",
            timestamp=datetime.now().strftime("%Y-%m-%dT%H%M%S"),
        )
        df.to_csv(fname, index=False)
        print(f"Saved output to {fname}")

    return df


if __name__ == "__main__":
    df = load_production_data_of_crops_and_livestock_products()
    print(df.head())
