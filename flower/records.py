import datetime

from influxdb import InfluxDBClient
from .utils import tasks


class Recorder:
    """ Continuously write data into db for alert

        db_client:
        data_handler:
    """

    def __init__(self, event, **kwargs):
        self.event = event
        self.db_client = InfluxDB(**kwargs)
        self.data_handler = DataHandler(self.event, self.db_client)

    def record(self):
        self.data_handler.handle()
        self.db_client.send()


class InfluxDB:
    """InfluxDB client for Recorder"""

    def __init__(self, **kwargs):
        self.client = InfluxDBClient(**kwargs)
        self.body = []

    def send(self):
        self.client.write_points(self.body)

    def put_row(self, measurement, tags, fields):
        row = {
            "measurement": measurement,
            "tags": tags,
            "fields": fields,
            "time": datetime.datetime.utcnow().isoformat("T") + "Z"
        }
        self.body.append(row)


class DataHandler:
    """ Get data for Recorder """

    def __init__(self, event, client):
        self.event = event
        self.client = client

    def get(self):
        result = []
        for task_id, _ in tasks.iter_tasks(self.event, state="FAILURE"):
            result.append(task_id)
        return result

    def handle(self):
        result = self.get()
        meas = 'task_failure'
        tags = {}
        fields = {"cnt": len(result)}
        self.client.put_row(meas, tags, fields)
