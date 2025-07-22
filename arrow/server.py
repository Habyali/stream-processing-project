import pyarrow
import pyarrow.flight
import ast
import pyarrow
import pyarrow.flight
class FlightServer(pyarrow.flight.FlightServerBase):
    def __init__(self, host="localhost", location=None,
                 tls_certificates=None, verify_client=False,
                 root_certificates=None, auth_handler=None):
        super(FlightServer, self).__init__(
            location, auth_handler, tls_certificates, verify_client,
            root_certificates)
        self.flights = {}
        self.host = host
        self.tls_certificates = tls_certificates

    @classmethod
    def descriptor_to_key(cls, descriptor):
        return (descriptor.descriptor_type.value, descriptor.command,
                tuple(descriptor.path or tuple()))

    def _make_flight_info(self, key, descriptor, table):
        if self.tls_certificates:
            location = pyarrow.flight.Location.for_grpc_tls(
                self.host, self.port)
        else:
            location = pyarrow.flight.Location.for_grpc_tcp(
                self.host, self.port)
        endpoints = [pyarrow.flight.FlightEndpoint(
            ticket=pyarrow.flight.Ticket(str(key).encode('utf-8')),
            locations=[location])]

        mock_sink = pyarrow.MockOutputStream()
        stream_writer = pyarrow.RecordBatchStreamWriter(
            mock_sink, table.schema)
        stream_writer.write_table(table)
        stream_writer.close()
        data_size = mock_sink.size()

        return pyarrow.flight.FlightInfo(table.schema,
                                         descriptor, endpoints,
                                         table.num_rows, data_size)

    def list_flights(self, context, criteria):
        for key, table in self.flights.items():
            if key[1] is not None:
                descriptor = \
                    pyarrow.flight.FlightDescriptor.for_command(key[1])
            else:
                descriptor = pyarrow.flight.FlightDescriptor.for_path(*key[2])

            yield self._make_flight_info(key, descriptor, table)

    def get_flight_info(self, context, descriptor):
        key = FlightServer.descriptor_to_key(descriptor)
        if key in self.flights:
            table = self.flights[key]
            return self._make_flight_info(key, descriptor, table)
        raise pyarrow.flight.FlightServerError("Flight not found")

    def do_put(self, context, descriptor, reader, writer):
        key = FlightServer.descriptor_to_key(descriptor)
        table = reader.read_all()
        self.flights[key] = table

    def do_get(self, context, ticket):
        key = ast.literal_eval(ticket.ticket.decode('utf-8'))
        if key not in self.flights:
            return pyarrow.flight.FlightDataStream(pyarrow.table([]))
        return pyarrow.flight.FlightDataStream(self.flights[key])

if __name__ == "__main__":
    location = "grpc://0.0.0.0:8815"
    server = FlightServer(location=location)
    print(f"Starting Arrow Flight Server on {location}")
    server.serve()