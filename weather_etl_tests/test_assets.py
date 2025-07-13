import pytest
from dagster import build_op_context
from unittest.mock import patch, MagicMock
from weather_etl.assets import (
    test_db_connection,
    create_tables,
    fetch_and_store_all_cities,
)


class DummyCursor:
    def execute(self, *args, **kwargs):
        pass

    def fetchone(self):
        return None

    def close(self):
        pass


class DummyConn:
    def cursor(self):
        return DummyCursor()

    def commit(self):
        pass

    def close(self):
        pass


dummy_context = build_op_context(resources={"pg_conn": DummyConn()})


def test_test_db_connection():
    assert test_db_connection(dummy_context) is True


def test_create_tables_runs():
    assert create_tables(dummy_context) is True


@patch("weather_etl.assets.requests.get")
def test_fetch_and_store_mock(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "current": {
            "last_updated_epoch": 1720771200,
            "temp_c": 22.5,
            "temp_f": 72.5,
            "is_day": 1,
            "condition": {
                "text": "Sunny",
                "icon": "//cdn.weatherapi.com/weather/64x64/day/113.png",
                "code": 1000,
            },
            "wind_mph": 5.6,
            "wind_kph": 9.0,
            "wind_degree": 180,
            "wind_dir": "S",
            "pressure_mb": 1013,
            "pressure_in": 29.91,
            "precip_mm": 0.0,
            "precip_in": 0.0,
            "humidity": 45,
            "cloud": 0,
            "feelslike_c": 22.0,
            "feelslike_f": 71.6,
            "windchill_c": 22.0,
            "windchill_f": 71.6,
            "heatindex_c": 22.0,
            "heatindex_f": 71.6,
            "dewpoint_c": 10.0,
            "dewpoint_f": 50.0,
            "vis_km": 10,
            "vis_miles": 6,
            "gust_mph": 7.2,
            "gust_kph": 11.5,
            "uv": 5,
        },
        "forecast": {
            "forecastday": [
                {
                    "hour": [
                        {
                            "time_epoch": 1720771200,
                            "temp_c": 23.0,
                            "temp_f": 73.4,
                            "is_day": 1,
                            "condition": {
                                "text": "Partly cloudy",
                                "icon": "",
                                "code": 1003,
                            },
                            "wind_mph": 6.0,
                            "wind_kph": 9.5,
                            "wind_degree": 170,
                            "wind_dir": "S",
                            "pressure_mb": 1012,
                            "pressure_in": 29.88,
                            "precip_mm": 0.0,
                            "precip_in": 0.0,
                            "humidity": 50,
                            "cloud": 25,
                            "feelslike_c": 23.0,
                            "feelslike_f": 73.4,
                            "windchill_c": 23.0,
                            "windchill_f": 73.4,
                            "heatindex_c": 23.0,
                            "heatindex_f": 73.4,
                            "dewpoint_c": 11.0,
                            "dewpoint_f": 51.8,
                            "will_it_rain": 0,
                            "chance_of_rain": 0,
                            "vis_km": 10,
                            "vis_miles": 6,
                            "gust_mph": 8.0,
                            "gust_kph": 13.0,
                            "uv": 6,
                        }
                    ]
                }
            ]
        },
    }
    mock_response.raise_for_status = lambda: None
    mock_get.return_value = mock_response

    try:
        result = fetch_and_store_all_cities(dummy_context)
        assert result is True
    except Exception as e:
        pytest.fail(f"Asset failed with exception: {e}")
