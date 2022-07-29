import json
import sys
from typing import List

import httpx
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_dask import DaskTaskRunner

print(sys.path)

from src.blocks.pokemon_api_block import PokemonApiBlock, pokemon_api_block_key
from src.blocks.storage_block import STORAGE_BLOCK, StorageBlock


@task
def get_all_pokemon():
    """
    Get all pokemon data from the pokeapi.co API
    """
    pokemon_api_block = PokemonApiBlock.load(pokemon_api_block_key)
    base_api = pokemon_api_block.url
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
    pokemon_api_block = PokemonApiBlock.load(pokemon_api_block_key)
    base_api = pokemon_api_block.url
    storage_block = StorageBlock.load(STORAGE_BLOCK)
    data = httpx.get(base_api + "pokemon/" + pokemon).json()
    f = open(storage_block.base_dir + pokemon + ".json", "w")
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
    storage_block = StorageBlock.load(STORAGE_BLOCK)
    df = pd.DataFrame(pokemon_types)
    df.to_csv(storage_block.base_dir + "pokemon_types.csv", index=False)


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
