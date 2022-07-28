from typing import List

import pandas as pd
import httpx
from prefect import flow, task
from prefect_dask import DaskTaskRunner
from prefect.tasks import task_input_hash
import json

base_api = "https://pokeapi.co/api/v2/"
_storage_base_dir = "./pokemon_data/"


@task
def get_all_pokemon():
    """
    Get all pokemon data from the pokeapi.co API
    """
    raw_json = httpx.get(
        base_api + "pokemon", params={"limit": 151, "offset": 0}
    ).json()
    names = [pokemon["name"] for pokemon in raw_json["results"]]
    # names = ["ditto"]
    return names


@task(cache_key_fn=task_input_hash, retries=3)
def get_pokemon_data(pokemon):
    """
    Get pokemon data from the pokeapi.co API
    """
    print(f"Pokemon id: {pokemon}")
    data = httpx.get(base_api + "pokemon/" + pokemon).json()
    f = open(_storage_base_dir + pokemon + ".json", "w")
    f.write(json.dumps(data))
    f.close()
    return data


@task(cache_key_fn=task_input_hash, retries=3)
def get_pokemon_type(pokemon_data):
    """
    Get pokemon type from the pokeapi.co API
    """
    name = pokemon_data["name"]
    primary_type = pokemon_data["types"][0]["type"]["name"]
    return {"name": name, "primary_type": primary_type}


@task()
def output_pokemon_types(pokemon_types):
    """
    Output pokemon types to a CSV file
    """
    df = pd.DataFrame(pokemon_types)
    df.to_csv(_storage_base_dir + "pokemon_types.csv", index=False)


@flow(name="Download Pokemon Data", task_runner=DaskTaskRunner())
def download_pokemon_data(pokemon_names: List[str]):
    """
    Download pokemon data from the pokeapi.co API
    """
    pokemon_data = [get_pokemon_data.submit(pokemon) for pokemon in pokemon_names]
    pokemon_types = [get_pokemon_type.submit(data) for data in pokemon_data]
    return pokemon_types


@flow
def ingest_pokemon_data():
    """
    Ingest all Pokemon data from the pokeapi.co API
    """
    pokemon = get_all_pokemon.submit()
    pokemon_types = download_pokemon_data(pokemon)
    output_pokemon_types.submit(pokemon_types)


if __name__ == "__main__":
    ingest_pokemon_data()
