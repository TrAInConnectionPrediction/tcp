import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import requests
from stem import Signal
from stem.control import Controller
from config import tor_password


class Tor:
    def __init__(self):
        self.new_session()

    def new_session(self):
        self.session = requests.session()
        # Tor uses the 9050 port as the default socks port
        self.session.proxies = {'http': 'socks5://127.0.0.1:9050',
                                'https': 'socks5://127.0.0.1:9050'}

    @staticmethod
    def renew_connection():
        with Controller.from_port(port=9051) as controller:
            controller.authenticate(password=tor_password)
            controller.signal(Signal.NEWNYM)

    def new_ip(self):
        self.renew_connection()
        self.new_session()


class DownloadDave(Tor):
    def __init__(self):
        super().__init__()
        # + '8010097/191218/10'
        self.PLAN_BASE_URL = 'http://iris.noncd.db.de/iris-tts/timetable/plan/'
        self.REAL_BASE_URL = 'http://iris.noncd.db.de/iris-tts/timetable/fchg/'  # + '8010097'
        self.RECENT_CHANGE_URL = 'https://iris.noncd.db.de/iris-tts/timetable/rchg/'  # + '8010097'

    def get_request(self, url):
        resp = self.session.get(url, timeout=50)
        if resp.status_code != 200 or resp.text == '[]':
            raise ValueError('Something went wrong while doing session.get(' +
                             url + ') status code: ' + str(resp.status_code))
        return resp.text.replace('\'', '"')

    def get_data(self, url):
        # try 3 times to get the data. It is unlikely that the
        # same connection problem occurred 3 times in a row
        for _i in range(3):
            try:
                return self.get_request(url=url)
            except ValueError:
                return None
            except requests.exceptions.ChunkedEncodingError:
                pass
            except requests.exceptions.ConnectionError:
                pass
            except requests.exceptions.Timeout:
                pass
        else:
            print('connection error on', url)
            return None

    def get_plan(self, station_id, date, hour):
        return self.get_data(url=self.PLAN_BASE_URL + str(station_id) + '/' + date + '/' + '{:02}'.format(hour))

    def get_real(self, station_id):
        return self.get_data(url=self.REAL_BASE_URL + str(station_id))

    def get_recent_change(self, eva):
        return self.get_data(url=self.RECENT_CHANGE_URL + str(eva))
