from prefect.blocks.core import Block

STORAGE_BLOCK = "storage-block"


class StorageBlock(Block):
    """
    A block that represents a storage block
    """
    base_dir = "./pokemon_data/"


if __name__ == "__main__":
    print("saving")
    storage_block = StorageBlock()
    storage_block.save(name=STORAGE_BLOCK)
    print("done saving")
