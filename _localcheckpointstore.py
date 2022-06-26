import logging
from pathlib import Path
from json import dumps, loads
from json import JSONDecodeError
from azure.eventhub.aio import CheckpointStore
from typing import Iterable, Dict, Any, Union, Optional

# Raised in BlobCheckpointStore. If raised, ignored by Azure SDK.
# Note sure when/where/how to utilize here
from azure.eventhub.exceptions import OwnershipLostError

logger = logging.getLogger(__name__)

class LocalCheckpointError(Exception):
    pass

class LocalCheckpointStore(CheckpointStore):
    
    def __init__(self, dir_path: str, *args, **kwargs):
        self.dir_path: Path = Path(dir_path)
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, *args):
        return
    
    async def list_ownership(
        self,
        fully_qualified_namespace: str,
        eventhub_name: str,
        consumer_group: str
    ) -> Iterable[Dict[str, Any]]:
        ownership_dirs = self.dir_path.joinpath(
            fully_qualified_namespace,
            eventhub_name,
            consumer_group,
            "ownership",
        )
        result = []
        ownership_dirs = Path(str(ownership_dirs).lower())
        if not ownership_dirs.exists():
            return result
        
        for partition_dir in ownership_dirs.iterdir():
            try:
                with open(partition_dir / "metadata.json", 'r') as metafile:
                    metadata = loads(metafile.read())
                ownership = {
                    "fully_qualified_namespace": fully_qualified_namespace,
                    "eventhub_name": eventhub_name,
                    "consumer_group": consumer_group,
                    "partition_id": partition_dir.name,
                    "owner_id": metadata["ownerid"],
                    "last_modified_time": partition_dir.stat().st_mtime
                }
                result.append(ownership)
            except JSONDecodeError:
                logger.error(f"Unable to decode ownership object at {str(partition_dir)}")
                raise LocalCheckpointError("Could not decode ownership object. File data possibly corrupted.")
            except FileNotFoundError as e:
                logger.error(f"Could not find ownership object at {str(e.filename)}")
                raise LocalCheckpointError("Could not find ownership object. Please verify.")
            except PermissionError as e:
                logger.error(f"CCould not store ownership data. Permission denied for directory at {str(e.filename)}")
                raise LocalCheckpointError("Unable to read ownership data. Permission denied.")
            except Exception as e:
                logger.exception(e)
                raise e
        return result

async def claim_ownership(
    self,
    ownership_list: Iterable[Dict[str, Any]],
    **kwargs: Any
) -> Iterable[Dict[str, Any]]:
    result = []
    for ownership in ownership_list:
        owner_id = ownership["owner_id"]
        metadata = {"ownerid": owner_id}
        ownership_dirs = self.dir_path.joinpath(
            ownership["fully_qualified_namespace"],
            ownership["eventhub_name"],
            ownership["consumer_group"],
            "ownership",
            ownership["partition_id"],
        )
        ownership_dirs = Path(str(ownership_dirs).lower())
        ownership_dirs.mkdir(parents=True, exist_ok=True)
        with open(ownership_dirs / "metadata.json", 'w+') as metafile:
            metafile.write(dumps(metadata))
        ownership["last_modified_time"] = (ownership_dirs / "metadata.json").stat().st_mtime
        ownership.update(metadata)
        result.append(ownership)
    return result

