import httpx
from prefect import flow,  task
import pokebase as pb

base_api = "https://pokeapi.co/api/v2/"


@task
def get_all_pokemon():
    """
    Get all pokemon data from the pokeapi.co API
    """
    return ["ditto"]

@task
def get_pokemon_data(pokemon):
    """
    Get pokemon data from the pokeapi.co API
    """
    return httpx.get(base_api + "pokemon/" + pokemon).json()

@task
def get_pokemon_type(pokemon_data):
    """
    Get pokemon type from the pokeapi.co API
    """
    return pokemon_data["types"]

@flow
def ingest_pokemon_data():
    """
    Ingest all Pokemon data from the pokeapi.co API
    """
    pokemon = get_all_pokemon()
    pokemon_data = [get_pokemon_data(pokemon) for pokemon in pokemon]
    pokemon_types = [get_pokemon_type(data) for data in pokemon_data]
    print(pokemon_types)


if __name__ == "__main__":
    ingest_pokemon_data()
