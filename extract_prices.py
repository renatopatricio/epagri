#!/usr/bin/env python
# -*- coding: utf-8 -*-
#=========================
#==================================OBSERVAÇÕES=========================================

# Este codigo é para testar 

#==================================FIM OBSERVAÇÕES=====================================


#==================================INICIO DO CODIGO=====================================

 
import pandas as pd
import requests
import os
from datetime import datetime
from typing import Literal

# --------------------------------------------------
# WRAPPER DE REQUISIÇÕES
# --------------------------------------------------
class Request:
    settings: dict = {"timeout": 120.}
    expected_settings: set = {"timeout"}

    def configure(self, **kwargs):
        assert set(kwargs.keys()).issubset(self.expected_settings), "Argumentos inválidos"
        self.settings.update(kwargs)

    def get(self, url: str, **kwargs) -> requests.Response:
        with requests.get(url, **kwargs, **self.settings) as response:
            self._raise_for_status(response, context={"url": url})
            return response

    def _raise_for_status(self, response: requests.Response, context: dict):
        if response.status_code == 500 and response.text == "Index: 0, Size: 0":
            resource = context["url"].split("/")[-1].split("?")[0]
            raise Exception(f"{resource} não encontrado no servidor FAOSTAT")
        if response.status_code == 524:
            raise TimeoutError("Tempo de requisição excedido")
        response.raise_for_status()

    @staticmethod
    def get_data(response: requests.Response) -> list[dict]:
        return response.json().get("data", [])


__requests__ = Request()


def fetch_data(url: str, **kwargs) -> list[dict]:
    resp = __requests__.get(url, **kwargs)
    return Request.get_data(resp)


# --------------------------------------------------
# CLASSES DE SUPORTE
# --------------------------------------------------
class Records(list):
    _expected_order: list[str]

    def __init__(self, data: list[dict], columns: list[str] = []):
        super().__init__(data)
        self._expected_order = columns

    @property
    def df(self) -> pd.DataFrame:
        cols = list(self._expected_order)
        for rec in self:
            for k in rec:
                if k not in cols:
                    cols.append(k)
        return pd.DataFrame.from_records(self, columns=cols)


class FAOSTAT:
    baseurl: str = "https://faostatservices.fao.org/api/v1"
    lang: Literal["en", "fr", "es"] = "en"

    def get_codelist(self, code_id: str, domain_code: str) -> pd.DataFrame:
        url = f"{self.baseurl}/{self.lang}/codes/{code_id}/{domain_code}"
        data = fetch_data(url)
        return Records(data).df

    def get_data(
        self,
        domain_code: str,
        filters: dict,
        show_codes: bool = True,
        show_flags: bool = True,
        show_notes: bool = False,
        null_values: bool = False,
        limit: int = -1,
        output_type: str = "objects"
    ) -> pd.DataFrame:
        url = f"{self.baseurl}/{self.lang}/data/{domain_code}"
        params = []
        
        # Formata os parâmetros corretamente
        for k, v in filters.items():
            if isinstance(v, list):
                params.append((k, ",".join(map(str, v))))
            else:
                params.append((k, v))
                
        params += [
            ("show_codes", str(show_codes).lower()),
            ("show_flags", str(show_flags).lower()),
            ("show_notes", str(show_notes).lower()),
            ("null_values", str(null_values).lower()),
            ("limit", str(limit)),
            ("output_type", output_type),
        ]
        
        try:
            data = fetch_data(url, params=params)
            return Records(data).df
        except Exception as e:
            print(f"Erro ao acessar o domínio {domain_code}. Verifique se o domínio existe e os parâmetros estão corretos.")
            raise


# instância global
faostat_api = FAOSTAT()

# --------------------------------------------------
# FUNÇÃO PRINCIPAL DE EXTRAÇÃO DE PREÇOS
# --------------------------------------------------

def load_price_data_of_grains(
    domain_code: str = "PP",
    grains: list[str] = [
        "Raw milk of cattle",
        "Raw milk of buffalo",
        "Raw milk of camel",
        "Raw milk of goats",
        "Raw milk of sheep",
    ],
    years: list[int] = [2015,2016,2017,2018,2019,2020,2021,2022,2023,2024],
    output_csv: str = "faostat_prices_{n_grains}grains_{n_years}years_{ts}.csv"
) -> str:
    """
    Extrai os preços de produção (domínio 'PP') para os grãos e anos especificados,
    salva sempre em CSV e retorna o caminho do arquivo gerado.
    """
    try:
        # 1) obtém lista completa de itens (grãos)
        all_items = faostat_api.get_codelist("items", domain_code)
        
        print("Available columns in all_items:", all_items.columns.tolist())
        
        # 2) filtra apenas os itens cujo label está em grains
        label_column = 'label' if 'label' in all_items.columns else 'description'
        selection = all_items[all_items[label_column].isin(grains)]
        
        if selection.empty:
            raise ValueError(f"No items found matching the specified grains: {grains}")
            
        print("Selected items:")
        print(selection[[label_column, 'code']])
            
        # 3) extrai os dados de preços
        # Primeiro verifica se há dados disponíveis para os itens selecionados
        print("Verificando disponibilidade de dados...")
        
        # Tenta obter dados para um único ano/item primeiro para testar
        test_item = selection.iloc[0]['code']
        test_year = years[0]
        
        try:
            test_data = faostat_api.get_data(
                domain_code=domain_code,
                filters={
                    "item": test_item,
                    "year": test_year
                },
                show_codes=True,
                show_flags=True,
                limit=1
            )
            print(f"Teste bem-sucedido para item {test_item}, ano {test_year}")
        except Exception as test_error:
            raise Exception(f"Falha ao acessar dados do domínio {domain_code}. Verifique se o domínio existe e contém dados para os itens especificados. Erro: {str(test_error)}")
        
        # Se o teste passou, obtém todos os dados
        df_prices = faostat_api.get_data(
            domain_code=domain_code,
            filters={
                "item": list(selection['code'].astype(int)),
                "year": years
            },
            show_codes=True,
            show_flags=True,
            show_notes=False,
            null_values=False,
            limit=-1,
            output_type="objects"
        )

        # 4) gera o nome do arquivo e salva em CSV
        filename = output_csv.format(
            n_grains=len(grains),
            n_years=len(years),
            ts=datetime.now().strftime("%Y%m%dT%H%M%S")
        )
        df_prices.to_csv(filename, index=False)
        print(f"[OK] Dados de preços salvos em: {os.path.abspath(filename)}")

        return filename
        
    except Exception as e:
        print(f"[ERROR] Falha ao carregar dados de preço: {str(e)}")
        raise


if __name__ == "__main__":
    try:
        csv_path = load_price_data_of_grains()
        print(f"Arquivo gerado: {csv_path}")
    except Exception as e:
        print(f"Erro durante a execução: {str(e)}")
        print("Sugestões:")
        print("1. Verifique se o domínio 'PP' existe na API FAOSTAT")
        print("2. Confira se os itens selecionados possuem dados disponíveis")
        print("3. Tente reduzir o escopo (menos itens ou anos)")