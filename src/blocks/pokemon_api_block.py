from prefect.blocks.core import Block

pokemon_api_block_key = "pokemon-api-block"


class PokemonApiBlock(Block):
    url: str = "https://pokeapi.co/api/v2/"


if __name__ == "__main__":
    pokemon_api_block = PokemonApiBlock()
    pokemon_api_block.save(name=pokemon_api_block_key)
