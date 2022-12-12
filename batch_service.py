from pyarrow.flight import (
    FlightServerBase,
    FlightInfo,
    RecordBatchStream,
    FlightDescriptor,
    FlightEndpoint,
)
from pyarrow import parquet as pq
import pyarrow.dataset as ds

import logging
from pathlib import Path


class Server(FlightServerBase):
    """
    Simple RPC service that allows you to:
        1. Write data to parquet datasets (existent or created on the spot)
        2. Read data from parquet datasets
    This works locally, but the obvious idea is that this service sits up on top of a cloud storage
    """

    def __init__(self, location="grpc://0.0.0.0:8815", storage="./storage", **kwargs):
        super(Server, self).__init__(location)
        self._location = location
        self._storage = storage

    def _make_flight_info(self, dataset):
        dataset_path = f"{self._storage}/{dataset}"
        dataset_object = ds.dataset(dataset_path, format="parquet")
        schema = dataset_object.schema
        size = 0
        for f in dataset_object.files:
            logging.info(f"Reading metadata from {f}")
            size += pq.read_metadata(f).serialized_size

        descriptor = FlightDescriptor.for_path(dataset_path.encode("utf-8"))
        endpoints = [FlightEndpoint(dataset_path, [self._location])]
        logging.info(f"Retrieving flight info for dataset {dataset}...")
        return FlightInfo(
            schema, descriptor, endpoints, dataset_object.count_rows(), size
        )

    def get_flight_info(self, context, descriptor):
        return self._make_flight_info(descriptor.path[0].decode("utf-8"))

    def list_flights(self, context, criteria):
        for dataset in Path(self._storage).iterdir():
            yield self._make_flight_info(dataset.name)

    def do_put(self, context, descriptor, reader, writer):
        dataset = descriptor.path[0].decode("utf-8")
        logging.info(f"Descriptor {dataset}")
        dataset_path = f"{self._storage}/{dataset}"
        data_table = reader.read_all()
        logging.info(f"Writing to dataset {dataset}...")
        pq.write_to_dataset(data_table, dataset_path)

    def do_get(self, context, ticket):
        dataset_path = ticket.ticket.decode("utf-8")
        logging.info(f"Retrieving dataset {dataset_path}...")
        return RecordBatchStream(ds.dataset(dataset_path).to_table())


if __name__ == "__main__":
    LOG_FORMAT = "%(asctime)s %(levelname)s: %(message)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        datefmt=DATE_FORMAT,
    )
    server = Server()
    logging.info("Started gRPC server!")
    server.serve()
