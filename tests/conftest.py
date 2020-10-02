import pytest
from server import create_app


@pytest.fixture
def app():

    app = create_app({
        'TESTING': True,
        'DEPLOY_KEY': 'dev'
    })

    yield app


@pytest.fixture
def client(app):
    return app.test_client()
