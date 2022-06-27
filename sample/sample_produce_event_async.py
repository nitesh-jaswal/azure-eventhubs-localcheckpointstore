import asyncio

from os import environ
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

CONN_STR = environ.get("EVENTHUB_CONN_STR")
EVENTHUB_NAME = environ.get("EVENTHUB_NAME")

async def main():
    producer_client = EventHubProducerClient.from_connection_string(
        conn_str=CONN_STR,
        eventhub_name=EVENTHUB_NAME,
    )
    async with producer_client:
        event_data_batch = await producer_client.create_batch()

        event_data_batch.add(EventData('First event '))
        event_data_batch.add(EventData('Second event'))
        event_data_batch.add(EventData('Third event'))

        await producer_client.send_batch(event_data_batch)

if __name__ == "__main__":
    asyncio.run(main())
    print("Produced events succesfully. Exiting...")