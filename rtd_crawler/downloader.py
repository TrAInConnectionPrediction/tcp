import requests
from stem import Signal
from stem.control import Controller
from config import tor_password

class Tor:
    def __init__(self):
        self.renew_connection()
        self.session = self.new_session()

    def new_session(self):
        session = requests.session()
        # Tor uses the 9050 port as the default socks port
        session.proxies = {'http':  'socks5://127.0.0.1:9050',
                           'https': 'socks5://127.0.0.1:9050'}
        return session

    # signal TOR for a new connection 
    def renew_connection(self):
        with Controller.from_port(port = 9051) as controller:
            controller.authenticate(password=tor_password)
            controller.signal(Signal.NEWNYM)
    
    def new_ip(self):
        self.renew_connection()
        self.session = self.new_session()

class download_dave(Tor):
    def  __init__(self):
        super().__init__()
        self.PLAN_BASE_URL = 'http://iris.noncd.db.de/iris-tts/timetable/plan/' # + '8010097/191218/10'
        self.REAL_BASE_URL = 'http://iris.noncd.db.de/iris-tts/timetable/fchg/' # + '8010097'

    def get_request(self, url):
        try:
            resp = self.session.get(url)
            if (resp.status_code != 200 or resp.text == '[]'):
                raise ValueError('Something went wrong while doing session.get(' + url + ') status code: ' + str(resp.status_code))
            return resp.text
        except requests.exceptions.ChunkedEncodingError:
            return None
        else:
            raise ValueError('Something went wrong while doing session.get(' + url + ') status code: ' + str(resp.status_code))

    def get_data(self, url):
        for _i in range(3):
            try:
                return self.get_request(url=url)
            except ValueError:
                return None
            except requests.exceptions.ConnectionError:
                pass
        else:
            raise requests.exceptions.ConnectionError
    
    def get_plan(self, station_id, date, hour):
        return self.get_data(url=self.PLAN_BASE_URL + str(station_id) + '/' + date + '/' + str(hour))

    def get_real(self, station_id):
        return self.get_data(url=self.REAL_BASE_URL + str(station_id))