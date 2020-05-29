import datetime
import pandas as pd
import numpy as np
import os
import random
import lxml.etree as etree
import sqlalchemy
import collections
import re
from config import db_database, db_password, db_server, db_username

class NoLocationError(Exception):
    pass

class BetriebsstellenBill:
    def __init__(self):
        self.engine = sqlalchemy.create_engine('postgresql://'+ db_username +':' + db_password + '@' + db_server + '/' + db_database + '?sslmode=require')
        self.betriebsstellen = pd.read_sql('SELECT * FROM betriebstellen', con=self.engine)
        self.engine.dispose()

        self.name_index_betriebsstellen = self.betriebsstellen.set_index('name')
        self.ds100_index_betriebsstellen = self.betriebsstellen.set_index('ds100')
        self.NoLocationError = NoLocationError

    def __len__(self):
        return len(self.betriebsstellen)

    def get_name(self, ds100):
        return self.ds100_index_betriebsstellen.at[ds100, 'name']
    
    def get_ds100(self, name):
        return self.name_index_betriebsstellen.at[name, 'ds100']

    def get_location(self, name=None, ds100=None):
        if name:
            return self.get_location(ds100=self.get_ds100(name=name))
        else:
            lon = self.ds100_index_betriebsstellen.at[ds100, 'lon']
            lat = self.ds100_index_betriebsstellen.at[ds100, 'lat']
            # print(type(lon), type(lat))
            if type(lon) == np.ndarray:
                lon = lon[0]
            if type(lat) == np.ndarray:
                lat = lat[0]
            # print(lon, lat)
            if not lon or not lat:
                raise self.NoLocationError
            else:
                return (lon,lat)

class StationPhillip:
    def __init__(self):
        try:
            self.engine = sqlalchemy.create_engine('postgresql://'+ db_username +':' + db_password + '@' + db_server + '/' + db_database + '?sslmode=require')
            self.station_df = pd.read_sql('SELECT * FROM stations', con=self.engine)
            self.station_df.to_pickle('data_buffer/station_offline_buffer')
            self.engine.dispose()
        except:
            try:
                self.station_df = pd.read_pickle('data_buffer/station_offline_buffer')
                print('Using local station buffer')
            except FileNotFoundError:
                raise FileNotFoundError('There is no connection to the database and no local buffer')

        self.station_df['eva'] = self.station_df['eva'].astype(int)
        self.name_index_stations = self.station_df.set_index('name')
        self.eva_index_stations = self.station_df.set_index('eva')
        self.ds100_index_stations = self.station_df.set_index('ds100')
        self.sta_list = self.station_df['name'].tolist()
        self.random_sta_list = self.station_df['name'].tolist()

    def __len__(self):
        return len(self.station_df)

    def __iter__(self):
        self.n = 0
        return self
    
    def __next__(self):
        if self.n < self.__len__():
            self.n += 1
            return self.sta_list[self.n -1]
        else:
            raise StopIteration

    def get_eva(self, name=None, ds100=None):
        """get the eva from name or ds100

        Keyword Arguments:
            name {string} -- official station name (default: {None})
            ds100 {string} -- ds100 of station (different from ds100 of Betriebsstalle) (default: {None})

        Returns:
            int -- eva of station
        """
        if name:
            return self.name_index_stations.at[name, 'eva']
        elif ds100:
            return self.ds100_index_stations.at[ds100, 'eva']
        else:
            return None

    def get_name(self, eva=None, ds100=None):
        """get the name from eva or ds100

        Keyword Arguments:
            eva {int} -- eva of station (default: {None})
            ds100 {string} -- ds100 of station (different from ds100 of Betriebsstalle) (default: {None})

        Returns:
            string -- official station name
        """
        if eva:
            return self.eva_index_stations.at[eva, 'name']
        elif ds100:
            return self.ds100_index_stations.at[ds100, 'name']
        else:
            return None
    
    def get_ds100(self, name=None, eva=None):
        """get the ds100 from eva or station name

        Keyword Arguments:
            name {string} -- official station name (default: {None})
            eva {int} -- eva of station (default: {None})

        Returns:
            string -- ds100 of station (different from ds100 of Betriebsstalle)
        """
        if name:
            return self.name_index_stations.at[name, 'ds100']
        elif eva:
            return self.eva_index_stations.at[eva, 'ds100']
        else:
            return None

    def get_location(self, name=None, eva=None, ds100=None):
        """get the location of a station

        Keyword Arguments:
            name {string} -- official station name (default: {None})
            eva {int} -- eva of station (default: {None})
            ds100 {string} -- ds100 of station (different from ds100 of Betriebsstalle) (default: {None})

        Returns:
            tuple -- longitude and latitide
        """
        if name or ds100:
            return self.get_location(eva=self.get_eva(name=name, ds100=ds100))
        else:
            return (self.eva_index_stations.at[eva, 'lon'],
                    self.eva_index_stations.at[eva, 'lat'])

    def random_iter(self):
        """random iterator over station names

        Yields:
            string -- station names in random order
        """
        random.shuffle(self.random_sta_list)
        for sta in self.random_sta_list:
            yield sta


class FileLisa:
    BASEPATH = 'rtd/'
    XML_CHILD_RE = re.compile(r'(<timetable station="[^"]*"[ eva="\d*"]*>)(.*)(<\/timetable>)', flags=re.RegexFlag.S)


    def clean_station_name(self, station):
        return station.strip().replace('/', 'slash')


    def concat_xmls(self, xml1, xml2):
        # iter the elements to concat
        for xml_child in xml2:
            xml1.append(xml_child)
        return xml1


    def concat_xmls_as_str(self, xml1: str, xml2: str) -> str:
        content2 = re.search(self.XML_CHILD_RE, xml2)[2]
        concatted = re.sub(self.XML_CHILD_RE, '\\1\\2' + content2 + '\\3', xml1)
        return concatted


    def save_xml_as_str(self, xml: str, directory: str, file_name: str):
        if xml and xml != 'None' and xml != '<timetable/>\n':
            #create dir if not present
            if not os.path.exists(directory):
                os.makedirs(directory)
            try:
                old_xml = self.open_xml_as_str(directory + file_name)
                xml = self.concat_xmls_as_str(old_xml, xml)
            except FileNotFoundError:
                pass
            with open(directory + file_name, 'w', encoding="utf-8") as f:
                f.write(xml)
                # print(xml, file=f)


    def save_xml(self, xml, directory, file_name):
        if xml and xml != 'None' and xml != '<timetable/>\n':
            #create dir if not present
            if not os.path.exists(directory):
                os.makedirs(directory)

            old_xml = self.open_xml(directory + file_name)
            if old_xml is not None:
                try:
                    tree = etree.ElementTree()
                    root = etree.fromstring(xml.encode('utf-8'))
                    xml = self.concat_xmls(old_xml, root)
                    tree._setroot(xml)
                    tree.write(directory + file_name)
                except Exception as ex:
                    print('Save xml exeption:', ex)
            else:
                with open(directory + file_name, 'w', encoding="utf-8") as f:
                    f.write(xml)


    def open_xml_as_str(self, dir_name) -> str:
        with open(dir_name, 'r') as f:
            xml = f.read()
            return xml


    def open_xml(self, dir_name):
        try:
            tree = etree.parse(dir_name)
            xroot = tree.getroot()
            return xroot
        except OSError: # FileNotFound in lxml
            return None
        except etree.XMLSyntaxError: # File is empty
            return None


    def save_station_xml(self, xml: str, station: str, date: int, data_type: str):
        directory = self.BASEPATH + self.clean_station_name(station) + '/'
        file_name = str(date) + '_' + data_type + '.xml'
        self.save_xml(xml, directory, file_name)


    def open_station_xml(self, station: str, date: int, data_type: str):
        directory = self.BASEPATH + self.clean_station_name(station) + '/'
        file_name = str(date) + '_' + data_type + '.xml'
        return self.open_xml(directory + file_name)


    def delete_xml(self, station: str, date: int, data_type: str):
        directory = self.BASEPATH + self.clean_station_name(station) + '/'
        file_name = str(date) + '_' + data_type + '.xml'
        self.delete(directory + file_name)


    def delete(self, path: str):
        if os.path.isfile(path):
            os.remove(path)


if __name__ == '__main__':
    fl = FileLisa()
    # xml = fl.concat_xmls_as_str(xml1, xml2)
    plan = '<timetable station="Hilden S&#252;d"><s id="-4549549882670356501-2005271456-37"><tl f="S" t="p" o="800337" c="S" n="34886"/><ar pt="2005271640" pp="1" l="1" ppth="Dortmund Hbf|Dortmund-Dorstfeld|Dortmund-Dorstfeld S&#252;d|Dortmund Universit&#228;t|Dortmund-Oespel|Dortmund-Kley|Bochum-Langendreer|Bochum-Langendreer West|Bochum Hbf|Bochum-Ehrenfeld|Wattenscheid-H&#246;ntrop|Essen-Eiberg|Essen-Steele Ost|Essen-Steele|Essen Hbf|Essen West|Essen-Frohnhausen|M&#252;lheim(Ruhr)Hbf|M&#252;lheim(Ruhr)Styrum|Duisburg Hbf|Duisburg-Schlenk|Duisburg-Buchholz|Duisburg-Gro&#223;enbaum|Duisburg-Rahm|Angermund|D&#252;sseldorf Flughafen|D&#252;sseldorf-Unterrath|D&#252;sseldorf-Derendorf|D&#252;sseldorf-Zoo|D&#252;sseldorf Wehrhahn|D&#252;sseldorf Hbf|D&#252;sseldorf Volksgarten|D&#252;sseldorf-Oberbilk|D&#252;sseldorf-Eller Mitte|D&#252;sseldorf-Eller|Hilden"/><dp pt="2005271640" pp="1" l="1" ppth="Solingen Vogelpark|Solingen Hbf"/></s></timetable>'
    fl.concat_xmls_as_str(plan, plan)
    print('lol')
    