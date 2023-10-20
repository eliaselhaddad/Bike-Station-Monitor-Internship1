import requests


class HttpClient:
    def __init__(self):
        pass

    @staticmethod
    def get_data(url: str) -> dict | None:
        response = requests.get(url)
        json = response.json()
        return json


def test_http_client():
    print("")
