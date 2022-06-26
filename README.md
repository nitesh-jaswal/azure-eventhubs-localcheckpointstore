# azure-eventhubs-localcheckpointstore
The default Azure SDK for python comes with the ability to checkpoint Azure EventHub events to the Azure Blob Storage only. However, the SDK does provide an Abstract Base Class  for custom implementation to a different storage device. This project provides a custom implementation of the checkpoint store for a local drive/file system. 
