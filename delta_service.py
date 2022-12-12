from pyarrow.flight import (
    FlightServerBase,
    FlightInfo,
    RecordBatchStream,
    FlightDescriptor,
    FlightEndpoint,
)
from pyarrow import parquet as pq
import pyarrow.dataset as ds
from deltalake import DeltaTable

import logging
from pathlib import Path


class DeltaServer(FlightServerBase):
    """
    Simple RPC service that allows you to read data from DeltaTables.
    This opens up the opportunity to build a read application on top of your Delta Lake.
    This works locally, but the obvious idea is that this service sits up on top of a cloud storage.
    """

    def __init__(self, location="grpc://0.0.0.0:8815", storage="./delta_storage", **kwargs):
        super(DeltaServer, self).__init__(location)
        self._location = location
        self._storage = storage

    def _make_flight_info(self, table_name):
        dataset_path = f"{self._storage}/{table_name}"
        delta_table = DeltaTable(dataset_path)
        dataset_object = delta_table.to_pyarrow_dataset()
        schema = dataset_object.schema
        size = 0
        for f in dataset_object.files:
            logging.info(f"Reading metadata from {dataset_path}/{f}")
            size += pq.read_metadata(f"{dataset_path}/{f}").serialized_size

        descriptor = FlightDescriptor.for_path(dataset_path.encode("utf-8"))
        endpoints = [FlightEndpoint(dataset_path, [self._location])]
        logging.info(f"Retrieving flight info for delta table {table_name}...")
        return FlightInfo(
            schema, descriptor, endpoints, dataset_object.count_rows(), size
        )

    def get_flight_info(self, context, descriptor):
        return self._make_flight_info(descriptor.path[0].decode("utf-8"))

    def list_flights(self, context, criteria):
        for dataset in Path(self._storage).iterdir():
            yield self._make_flight_info(dataset.name)

    def do_get(self, context, ticket):
        delta_path = ticket.ticket.decode("utf-8")
        logging.info(f"Retrieving table data from {delta_path}...")
        return RecordBatchStream(DeltaTable(delta_path).to_pyarrow_table())

if __name__ == "__main__":
    LOG_FORMAT = "%(asctime)s %(levelname)s: %(message)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        datefmt=DATE_FORMAT,
    )
    server = DeltaServer()
    logging.info("Started gRPC server!")
    server.serve()
