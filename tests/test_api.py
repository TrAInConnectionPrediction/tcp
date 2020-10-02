import pytest
from flask import jsonify


def test_connect(client):
    resp = client.post(
        '/api/connect', data={'screen': '1920x1080', 'ip': '0.0.0.0'})
    data = resp.get_data()
    assert resp.status_code == 200
    assert "bhf" in str(data)


@pytest.mark.parametrize(('startbhf', 'zielbhf', 'date'), (
    ('Tübingen Hbf', 'Köln Hbf', '01.10.2020 12:00'),
    ('Berlin Hbf', 'Hamburg Hbf', '01.10.2020 12:00'),
))
def test_trip(client, startbhf, zielbhf, date):
    resp = client.post(
        '/api/trip', data={'startbhf': startbhf, 'zielbhf': zielbhf, 'date': date})
    resp.data
    data = resp.get_data()
    assert resp.status_code == 200

def test_gitid(client):
    resp = client.post(
        '/api/gitid', data={'key': 'dev'})
    data = resp.get_data()
    assert resp.status_code == 200
