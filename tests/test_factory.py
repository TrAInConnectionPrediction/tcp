from server import create_app

def test_config():
    assert not create_app().testing
    assert create_app({'TESTING': True}).testing

def test_website(client):
    assert client.get('/').status_code == 200
    assert client.get('/thisdoesnotexist').status_code == 200