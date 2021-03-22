from unittest import IsolatedAsyncioTestCase, mock
from unittest.mock import PropertyMock

from httpx import Response, Request

from service_logic import init, app, Buffer
from service_logic.restaurant_inspections import fetch


class Base(IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.api_response_payload = [{"restaurant_name": "101 by Tea Haus LLC", "zip_code": "78752",
                                      "inspection_date": "2019-04-30T00:00:00.000", "score": "93",
                                      "address": {"latitude": "30.336497", "longitude": "-97.718225",
                                                  "human_address": "{\"address\": \"6929 AIRPORT BLVD\", \"city\": \"AUSTIN\", \"state\": \"TX\", \"zip\": \"78752\"}"},
                                      "facility_id": "11785910", "process_description": "Routine Inspection",
                                      ":@computed_region_8spj_utxs": "4", ":@computed_region_jcrc_4uuy": "40",
                                      ":@computed_region_q9nd_rr82": "9", ":@computed_region_e9j2_6w3z": "77",
                                      ":@computed_region_m2th_e4b7": "217", ":@computed_region_rxpj_nzrk": "3",
                                      ":@computed_region_a3it_2a2z": "3641"}]

    async def asyncSetUp(self):
        await init()
        self.app = app


class TestRestaurantInspections(Base):

    @mock.patch('service_logic.restaurant_inspections.app.api_session.get', autospec=True)
    @mock.patch('service_logic.restaurant_inspections.app.buffer.to_data_queue', autospec=True)
    async def test_fetch_successful(self, mock_to_data_queue, mock_api_get):
        mock_api_get.return_value = Response(json=self.api_response_payload, status_code=200)
        mock_to_data_queue.return_value = True
        await fetch()

    @mock.patch('service_logic.restaurant_inspections.app.api_session.get', autospec=True)
    async def test_fetch_exception(self, mock_api_get):
        response = Response(json=self.api_response_payload, status_code=500,
                            request=Request(method='get', url='test.com'))
        mock_api_get.return_value = response
        with self.assertRaises(ConnectionError) as error:
            await fetch()


class TestBuffer(Base):

    def test_is_empty(self):
        buffer = Buffer()
        self.assertEqual(buffer.is_empty, True)

    def test_is_full(self):
        buffer = Buffer(mb_max_size=1)
        for x in range(40000):
            buffer.add_item({'offset': 1000, 'payload': ['a', 'b']})
        self.assertEqual(buffer.is_full, True)
        self.assertEqual(buffer.full_buffer, True)

    def test_buffer_overflow(self):
        buffer = Buffer(mb_max_size=1)
        for x in range(40000):
            buffer.add_item({'offset': 1000, 'payload': ['a', 'b']})
        self.assertEqual(len(buffer.buffer_overflow), 13114)

    def test_has_items(self):
        buffer = Buffer(mb_max_size=1)
        for x in range(1):
            buffer.add_item({'offset': 1000, 'payload': ['a', 'b']})
        self.assertEqual(buffer.has_items, True)

    def test_add_items(self):
        buffer = Buffer(mb_max_size=1)
        for x in range(5):
            buffer.add_item({'offset': 1000, 'payload': ['a', 'b']})
        self.assertEqual(buffer.buffer_size, 195)
        self.assertEqual(len(buffer.buffer), 5)
        self.assertEqual(buffer.buffer_overflow, [])
        self.assertEqual(buffer.full_buffer, False)

    def test_flush(self):
        buffer = Buffer(mb_max_size=1)
        for x in range(40000):
            buffer.add_item({'offset': 1000, 'payload': ['a', 'b']})
        buffer.flush()
        self.assertEqual(buffer.buffer_size, 511446)
        self.assertEqual(len(buffer.buffer), 13114)
        self.assertEqual(buffer.buffer_overflow, [])
        self.assertEqual(buffer.full_buffer, False)
        self.assertEqual(buffer.has_items, True)
        self.assertEqual(buffer.is_empty, False)

    @mock.patch('service_logic.app.data_queue', autospec=True)
    async def test_to_data_queue_finished(self, mock_data_queue):
        buffer = Buffer(mb_max_size=1)
        buffer.full_buffer = True
        result = await buffer.to_data_queue(offset=1,
                                            response=Response(json=self.api_response_payload, status_code=200))
        self.assertEqual(result, True)

    @mock.patch('service_logic.app.data_queue', autospec=True)
    async def test_to_data_queue_continue(self, mock_data_queue):
        original_value = app.api_pagination_limit
        app.api_pagination_limit = 0
        buffer = Buffer(mb_max_size=1)
        buffer.full_buffer = True
        result = await buffer.to_data_queue(offset=1,
                                            response=Response(json=self.api_response_payload, status_code=200))
        self.assertEqual(result, False)
        app.api_pagination_limit = original_value


### For Record Test
record_list = [
    {'offset': 1000, 'payload': ['a', 'b']},
    {'offset': 2000, 'payload': ['c', 'd']},
    {'offset': 3000, 'payload': ['e', 'f']},
    {'offset': 4000, 'payload': ['g', 'h']}
]
