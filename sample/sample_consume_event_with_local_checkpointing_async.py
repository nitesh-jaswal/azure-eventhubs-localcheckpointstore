import asyncio

from os import environ
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.aio import PartitionContext
from azure.eventhub import EventData

from .._localcheckpointstore import LocalCheckpointStore

CONN_STR = environ.get("EVENTHUB_CONN_STR")
EVENTHUB_NAME = environ.get("EVENTHUB_NAME")
CONSUMER_GROUP = environ.get("EVENTHUB_CONSUMER_GROUP", "$Default")
# Path to root directory where checkpoint objects will be stored
CHECKPOINT_DIR = environ.get("EVENTHUB_CHECKPOINT_DIR") # Eg: /data/my_username/eventhub_checkpint/

async def _execute_on_event(
    partition_context: PartitionContext, 
    event_data: EventData
):
        print(f"Received the event: \"{event_data.body_as_str(encoding='UTF-8')}\" from the partition with ID: \"{partition_context.partition_id}\"")
        await partition_context.update_checkpoint(event_data)

async def main():
    checkpoint_store = LocalCheckpointStore(dir_path=CHECKPOINT_DIR)
    consumer_client = EventHubConsumerClient.from_connection_string(
        conn_str=CONN_STR,
        eventhub_name=EVENTHUB_NAME,
        consumer_group=CONSUMER_GROUP,  
        checkpoint_store=checkpoint_store
    )
    async with consumer_client:
        await consumer_client.receive(
            on_event=_execute_on_event,
            starting_position="-1"
        )

if __name__ == "__main__":
    asyncio.run(main())
    print("Exiting...")