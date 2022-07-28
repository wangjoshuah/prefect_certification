from typing import List

import pandas as pd
import httpx
from prefect import flow,  task
from prefect_dask import DaskTaskRunner
from prefect import get_run_logger

base_api = "https://pokeapi.co/api/v2/"


@task
def get_all_pokemon():
    """
    Get all pokemon data from the pokeapi.co API
    """
    raw_json = httpx.get(base_api + "pokemon"
                     , params={
                            "limit": 151
                            , "offset": 0
        }).json()
    names = [pokemon["name"] for pokemon in raw_json["results"]]
    return names

@task
def get_pokemon_data(pokemon):
    """
    Get pokemon data from the pokeapi.co API
    """
    print(f"Pokemon id: {pokemon}")
    return httpx.get(base_api + "pokemon/" + pokemon).json()

@task
def get_pokemon_type(pokemon_data):
    """
    Get pokemon type from the pokeapi.co API
    """
    name = pokemon_data["name"]
    primary_type = pokemon_data["types"][0]["type"]["name"]
    return {"name": name, "primary_type": primary_type}


@flow(name="Download Pokemon Data", task_runner=DaskTaskRunner())
def download_pokemon_data(pokemon_names: List[str]):
    """
    Download pokemon data from the pokeapi.co API
    """
    pokemon_data = [get_pokemon_data(pokemon) for pokemon in pokemon_names]
    pokemon_types = [get_pokemon_type(data) for data in pokemon_data]
    type_df = pd.DataFrame(pokemon_types)
    type_df.to_csv("pokemon_data/pokemon_types.csv", index=False)

@flow
def ingest_pokemon_data():
    """
    Ingest all Pokemon data from the pokeapi.co API
    """
    pokemon = get_all_pokemon()
    download_pokemon_data(pokemon)



if __name__ == "__main__":
    ingest_pokemon_data()
