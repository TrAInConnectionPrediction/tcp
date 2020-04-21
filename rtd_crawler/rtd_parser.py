import pandas as pd 
import numpy as np 
import io
import xml.etree.ElementTree as ET
import os
import datetime
import sqlalchemy
from progress.bar import Bar
import concurrent.futures

from helpers import file_lisa, station_phillip
from speed import to_unix, parse_plan, fill_unknown_data, xml_parser, concat_changes, parse_realtime, unix_date

from config import db_database, db_password, db_server, db_username

engine = sqlalchemy.create_engine('postgresql://'+ db_username +':' + db_password + '@' + db_server + '/' + db_database + '?sslmode=require') 

# import cProfile, pstats, io

# def profile(fnc):
    
#     """A decorator that uses cProfile to profile a function"""
    
#     def inner(*args, **kwargs):
        
#         pr = cProfile.Profile()
#         pr.enable()
#         retval = fnc(*args, **kwargs)
#         pr.disable()
#         s = io.StringIO()
#         sortby = 'cumulative'
#         ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
#         ps.print_stats()
#         print(s.getvalue())
#         return retval

#     return inner

MESSAGE = {
    'id':None,
    'c':None,
    'ts':None
}

EVENT = {
    'cpth':None,
    'cp':None,
    'ct':None,
    'cs':None,
    'clt':None,
    'l':None,
    'm':[MESSAGE]
}

pind = {'platform': 0, 'arr': 1, 'dep': 2, 'stay_time': 3, 'pla_arr_path': 4,
        'pla_dep_path': 5, 'train_type': 6, 'train_number': 7, 'product_class': 8,
        'trip_type': 9, 'owner': 10, 'first_id': 11, 'middle_id': 12, 'last_id': 13,
        'arr_changed_path': 14, 'arr_changed_platform': 15, # 'message': 14, 
        'arr_changed_time': 16, 'arr_changed_status': 17, 'arr_cancellation_time': 18,
        'arr_line': 19, 'arr_message': 20, 'dep_changed_path': 21, 
        'dep_changed_platform': 22, 'dep_changed_time': 23, 'dep_changed_status': 24,
        'dep_cancellation_time': 25, 'dep_line': 26, 'dep_message': 27}

rind = {'first_id': 0, 'middle_id': 1, 'last_id': 2, 'arr_changed_path': 3, # 'message': 3, 
        'arr_changed_platform': 4, 'arr_changed_time': 5, 'arr_changed_status': 6,
        'arr_cancellation_time': 7, 'arr_line': 8, 'arr_message': 9,
        'dep_changed_path': 10, 'dep_changed_platform': 11, 'dep_changed_time': 12,
        'dep_changed_status': 13, 'dep_cancellation_time': 14, 'dep_line': 15,
        'dep_message': 16}

mind = {'id': 0, 'code': 1, 'time': 2}
iind = {'first_id': 0, 'middle_id': 1, 'last_id': 2}
tind = {'train_type': 0, 'train_number': 1, 'product_class': 2, 'trip_type': 3, 'owner': 4}
aind = {'platform': 0, 'arr': 1, 'dep': 2, 'stay_time': 3, 'pla_arr_path': 4, 'pla_dep_path': 5}
eind = { 'changed_path': 0, 'changed_platform': 1, 'changed_time': 2, 'changed_status': 3,
        'cancellation_time': 4, 'line': 5, 'message': 6}

def parse_plan_xml(xroot):
    """
    This function parses and concats plan xmls

    Args:
        xroot (ET.Element): ET Element of plan xml
    
    Returns:
        np.ndarray: parsed plan
        None: if there is no plan
    """
    # xml to list-dict mix
    plan = list(xml_parser(part) for part in list(xroot))
    plan = list(fill_unknown_data(part, real=False) for part in plan)
    plan = list(parse_plan(part) for part in plan)

    # create numpy array from the parsed data
    parsed_array = np.array(plan)
    if not parsed_array.size:
        return None
    # add later needed cols now, to not have to change the size of the parsed_array again.
    cols_to_append = len(pind) - parsed_array.shape[1]
    cols_to_append = np.full((parsed_array.shape[0], cols_to_append), -1)
    parsed_array = np.c_[parsed_array, cols_to_append]
    return parsed_array


# @profile
def parse_realtime_xml(xroot):
    """
    This function parses and concats realtime xmls

    Args:
        xroot (ET.Element): ET Element of real xml
    
    Returns:
        np.ndarray: parsed and concatted changes
        None: if there is no realtime info
    """
    if xroot is None:
        return None
    # xml to list-dict mix
    realtime = list(xml_parser(part) for part in list(xroot))

    # realtime is a mix of stops and sometimes messages. The messages are unwanted and have to be removed.
    # The eva is not part of a message, so we can use that as filter to remove the messages
    realtime = list(part for part in realtime if 'eva' in part)

    realtime = list(fill_unknown_data(part, real=True) for part in realtime)
    realtime = list(parse_realtime(part) for part in realtime)

    array = np.array(realtime)
    if not array.size:
        return None

    array = concat_all_changes(array)
    return array

def add_realtime_to_plan(plan, real):
    """
    This function adds the fitting realtime info to the plan

    Args:
        plan(np.ndarray): parsed plan
        real(np.ndarray): parsed and concatted changes
    
    Returns:
        np.ndarray: plan with realtime info
    """
    if not real is None:
        for i in range(plan.shape[0]):
            plan_id = plan[i, pind['first_id']]
            data = real[real[:, rind['first_id']] == plan_id, :]

            if data.size != 0:
                plan[i, pind['first_id']:] = data[0, :]
    return plan

def concat_all_changes(real):
    """
    This function concats changes as its common to have one message twice or multiple delay infos where only the newest is important

    Args:
        real(np.ndarray): parsed changes (Cannot handle empty real)
    
    Returns:
        np.ndarray: concatted changes with one message per train
    """
    # find unique message ids
    unique_ids = np.unique(real[:, rind['first_id']])
    concatted = np.full((unique_ids.shape[0], real.shape[1]), -1, dtype=object)
    for i, f_id in enumerate(unique_ids):
        # concat all the messages with the same id
        concatted[i, :] = concat_changes(real[np.in1d(real[:, rind['first_id']], [f_id]), :])
    return concatted

def upload_data(df):
    """
    This function uploads the data to our database

    Args:
        df(pd.DataFrame): fully parsed and prepared data
    """
    # print('uploading date of lenght: ', len(df))
    df.to_sql('rtd', con=engine, if_exists='append', method='multi')
    #### TODO: Add some retrying if it does not work ####

def parse_station(plan, real):
    """
    This function parses plan and real and adds real to plan

    Args:
        plan(ET.Element): plan xml or None
        real(ET.Element): real xml or None
    
    Returns:
        np.ndarray: plan + real
    """
    plan = parse_plan_xml(plan)
    real = parse_realtime_xml(real)
    if plan is None:
        return None
    else:
        return add_realtime_to_plan(plan, real)

def prepare_plan_for_upload(plan):
    """
    This function prepares data for upload by changing remaining np.ndarrays to list and replacing -1s with np.nan

    Args:
        plan(np.ndarray): parsed plan + real
    
    Returns:
        np.ndarray: plan + real with np.nans and list suitable for upload to PostgreSQL database
    """
    for i in range(plan.shape[0]):
        plan[i, pind['pla_arr_path']] = plan[i, pind['pla_arr_path']].tolist() if type(plan[i, pind['pla_arr_path']]) != int else -1
        plan[i, pind['pla_dep_path']] = plan[i, pind['pla_dep_path']].tolist() if type(plan[i, pind['pla_dep_path']]) != int else -1
        plan[i, pind['arr_changed_path']] = plan[i, pind['arr_changed_path']].tolist() if type(plan[i, pind['arr_changed_path']]) != int else -1
        plan[i, pind['dep_changed_path']] = plan[i, pind['dep_changed_path']].tolist() if type(plan[i, pind['dep_changed_path']]) != int else -1
        plan[i, pind['arr_message']] = plan[i, pind['arr_message']].tolist() if type(plan[i, pind['arr_message']]) != int else -1
        plan[i, pind['dep_message']] = plan[i, pind['dep_message']].tolist() if type(plan[i, pind['dep_message']]) != int else -1

    buf = np.full((plan.shape[0]), [-1], dtype=object)
    for b in range(buf.shape[0]): buf[b] = [-1]
    np.place(plan, plan == -1, [np.nan])
    np.place(plan[:, pind['pla_arr_path']], plan[:, pind['pla_arr_path']] == buf, [np.nan])
    np.place(plan[:, pind['pla_dep_path']], plan[:, pind['pla_dep_path']] == buf, [np.nan])
    np.place(plan[:, pind['arr_changed_path']], plan[:, pind['arr_changed_path']] == buf, [np.nan])
    np.place(plan[:, pind['dep_changed_path']], plan[:, pind['dep_changed_path']] == buf, [np.nan])
    for b in range(buf.shape[0]): buf[b] = [[-1, -1, -1]]
    np.place(plan[:, pind['arr_message']], plan[:, pind['arr_message']] == buf, [np.nan])
    np.place(plan[:, pind['dep_message']], plan[:, pind['dep_message']] == buf, [np.nan])
    return plan

# @profile
def parse_full_day(date):
    global engine
    engine = sqlalchemy.create_engine('postgresql://'+ db_username +':' + db_password + '@' + db_server + '/' + db_database + '?sslmode=require') 
    fl = file_lisa()
    stations = station_phillip()

    date1 = unix_date(to_unix(date))
    date2 = unix_date(to_unix(date - datetime.timedelta(days = 1)))

    # multithreading variables
    max_threads = 5
    uploaders = {}
    running_threads = []
    buffer = pd.DataFrame()

    bar = Bar('parsing ' + str(date), max = len(stations))
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for station in stations:
            bar.next()
            # print(' eta: ', bar.eta_td, end='\r')
            # open xmls that need to be parsed
            # real1: real of the same day as plan
            # real2: real of the day before plan, as a train rolling at 0:10 probably has changes from the day before
            plan = fl.open_plan_xml(station, date1)
            real1 = fl.open_real_xml(station, date1)
            real2 = fl.open_real_xml(station, date2)

            # check wether there is plan and or real data and parse it accordingly
            if plan is None:
                # print('plan is None1')
                continue
            elif real1 == None and real2 == None:
                real = None
            elif real1 == None:
                real = real2
            elif real2 == None:
                real = real1
            else:
                real = fl.concat_xmls(real1, real2)
            plan = parse_station(plan, real)
            if plan is None:
                # print('plan is None2')
                continue

            plan = prepare_plan_for_upload(plan)
            con_df = pd.DataFrame(data = plan,
                                columns = [*pind])
            # add station column to dataframe
            con_df['station'] = station
            buffer = pd.concat([buffer, con_df], ignore_index=True)

            # upload the data as soon as it is longer than 1000 lines. This is more efficient than uploading each stations data individually
            if len(buffer) > 1000:
                # start new thread
                uploaders[station] = executor.submit(upload_data, buffer)
                running_threads.append(station)

                buffer = pd.DataFrame()

                # collect runnung threads if there are to many running ones
                while len(running_threads) >= max_threads:
                    # running_threads[0] contains the station name
                    uploaders[running_threads[0]].result()
                    del uploaders[running_threads[0]]
                    del running_threads[0]

        # upload the data that did not make it over the 1000 line limit
        uploaders[station] = executor.submit(upload_data, buffer)
        running_threads.append(station)
        # collect all remaining running threads
        while running_threads:
            # running_threads[0] contains the station name
            uploaders[running_threads[0]].result()
            del uploaders[running_threads[0]]
            del running_threads[0]

        for station in stations:
            # delete the files that are no longer used (the parsed plan and real from two day ago)
            fl.delete_plan(station, date1)
            fl.delete_real(station, date2)
    engine.dispose()
    bar.finish()

station = 'Aachen Hbf'
if __name__ == '__main__':
    parse_full_day(datetime.datetime.today())
    # prfl()