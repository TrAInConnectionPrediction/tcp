# c#yt#ho#n: p#rofi#le=#Tr#ue
# python3 setup.py build_ext --inplace

cimport cython
import datetime
import numpy as np
cimport numpy as np



cdef dict MESSAGE = {
    'id':-1,
    'c':-1,
    'ts':-1
}

cdef dict EVENT = {
    'cpth':-1,
    'cp':-1,
    'ct':-1,
    'cs':-1,
    'clt':-1,
    'l':-1,
    'm':[MESSAGE]
}
cdef dict pind = {'platform': 0, 'arr': 1, 'dep': 2, 'stay_time': 3, 'pla_arr_path': 4,
        'pla_dep_path': 5, 'train_type': 6, 'train_number': 7, 'product_class': 8,
        'trip_type': 9, 'owner': 10, 'first_id': 11, 'middle_id': 12, 'last_id': 13,
        'arr_changed_path': 14, 'arr_changed_platform': 15, # 'message': 14, 
        'arr_changed_time': 16, 'arr_changed_status': 17, 'arr_cancellation_time': 18,
        'arr_line': 19, 'arr_message': 20, 'dep_changed_path': 21, 
        'dep_changed_platform': 22, 'dep_changed_time': 23, 'dep_changed_status': 24,
        'dep_cancellation_time': 25, 'dep_line': 26, 'dep_message': 27}

cdef dict rind = {'first_id': 0, 'middle_id': 1, 'last_id': 2, 'arr_changed_path': 3, # 'message': 3, 
        'arr_changed_platform': 4, 'arr_changed_time': 5, 'arr_changed_status': 6,
        'arr_cancellation_time': 7, 'arr_line': 8, 'arr_message': 9,
        'dep_changed_path': 10, 'dep_changed_platform': 11, 'dep_changed_time': 12,
        'dep_changed_status': 13, 'dep_cancellation_time': 14, 'dep_line': 15,
        'dep_message': 16}

cpdef int to_unix(dt, pattern=-1):
    """
    A function to convert to unix time
    
    Args:
        dt (datetime-object or string if pattern): datetime
        patter (string): pattern to convert string to datetime-object

    Return:
        int: unix timestamp
    """
    
    if dt is -1:
        return -1 # np.nan # 0 # -1
    # in this application '%y%m%d%H%M' is the standard pattern. To convert it,
    # it is the fastest to directly contruct a datetime object from it
    elif (pattern is '%y%m%d%H%M'): 
        try:
            return datetime.datetime(int('20' + dt[0:2]), int(dt[2:4]), int(dt[4:6]), int(dt[6:8]), int(dt[8:10])).replace(tzinfo=datetime.timezone.utc).timestamp()
        except OverflowError:
            print('OverflowError in speed.to_unix: \r\ndt, pattern')
            print(dt + ', %y%m%d%H%M')
            return datetime.datetime(int('20' + dt[0:2]), int(dt[2:4]), int(dt[4:6]), int(dt[6:8]), int(dt[8:10])).replace(tzinfo=datetime.timezone.utc).timestamp()
    elif pattern is -1: # Convert to unix
        return dt.replace(tzinfo=datetime.timezone.utc).timestamp()
    else:# convert to datetime and then to unix
        return datetime.datetime.strptime(dt, pattern).replace(tzinfo=datetime.timezone.utc).timestamp()

cpdef int unix_date(int unix):
    """
    A function to generate the unix date (unix timestamp from 00:00 of the day)\n
    
    Args:
        unix (int): unix timestamp
    Return:
        int: unix timestamp / unix date
    """
    # get datetime of today
    dt = datetime.datetime.fromtimestamp(unix).date()
    dt = datetime.datetime.combine(dt, datetime.datetime.min.time())

    # convert todays time to unix
    return int(to_unix(dt))

cpdef int unix_now():
    """
    Gets current unix timestamp

    Args:
        -
    Return:
        int: unix timestamp
    """
    return to_unix(datetime.datetime.now())

cpdef dict xml_parser(xml):
    '''
    a recursive function to convert a xml root object into
    a list dict mix
    '''
    cdef dict parsed = xml.attrib
    for xml_child in list(xml):
        if xml_child.tag in parsed:
            parsed[xml_child.tag].append(xml_parser(xml_child))
        else:
            parsed[xml_child.tag] = [xml_parser(xml_child)]
    return parsed

cpdef dict fill_emptys(dict data, dict pattern):
    '''
    a recursive function to fill a parsed xml with -1s to
    have the same structure each and every time
    '''
    # cdef key, value
    for key, value in pattern.items():
        if not key in data:
            data[key] = value
        if isinstance(data[key], list):
            for element in data[key]:
                if isinstance(element, dict):
                    element = fill_emptys(element, value[0])
    return data

cdef normalize_station(station):
    return station.replace('(', ' (').replace(')',') ')

@cython.wraparound(False)   # Deactivate negative indexing.
cdef np.ndarray split_path(path):
    """
    This function converts a string of a path to a patharray

    Args:
        path(str): path that contains '|' seperated station names
    
    Returns:
        np.ndarray: array of stations or np.array([-1]) if there is no path
    """
    # as there may not be any path
    if (path is -1):
        return np.array([-1])
    cdef list path_list = path.split('|') # map(normalize_station, path.split('|'))
    return np.array(path_list)

cpdef dict fill_unknown_data(dict parsed_xml, bint real=False):
    cdef dict pattern
    # check for api type
    if real == True:
        # real time data and changes
        pattern = {
            'id':-1,
            'eva':-1,
            # 'm':[MESSAGE],
            'ar':[EVENT],
            'dp':[EVENT]
        }
    else:
        # static data 
        pattern = {
            'id':-1, 
            'tl':[{'f':-1, 
                't':-1, 
                'o':-1, 
                'c':-1, 
                'n':-1}], 
            'ar':[{'pt':-1, 
                'pp':-1, 
                'l':-1, 
                'ppth':-1, 
                'cpth':-1}], 
            'dp':[{'pt':-1, 
                'pp':-1, 
                'l':-1, 
                'ppth':-1, 
                'cpth':-1}]
        }
    # fill
    return fill_emptys(parsed_xml, pattern)

cdef int get_stay_time(int arr, int dep):
    """
    This function calculates the timedelta in minutes between arr and dep

    Args:
        arr(int): unix timestamp of arr or -1
        dep(int): unix timestamp of dep or -1
    
    Returns:
        int: timedalta in minutes between arr and dep
    """
    # if it is the first or last stop (arr or dep == -1), then there is no way to calculate a staytime
    if arr is -1 or dep is -1:
        return -1
    else:
        return int((dep - arr) / 60)

@cython.wraparound(False)   # Deactivate negative indexing.
cdef np.ndarray parse_ar_dp(dict ar_dp):
    cdef int dep = to_unix(ar_dp['dp'][0]['pt'], pattern='%y%m%d%H%M')
    cdef int arr = to_unix(ar_dp['ar'][0]['pt'], pattern='%y%m%d%H%M')

    cdef pla_arr_path = split_path(ar_dp['ar'][0]['ppth'])
    cdef pla_dep_path = split_path(ar_dp['dp'][0]['ppth'])

    cdef platform = ar_dp['ar'][0]['pp'] if ar_dp['ar'][0]['pp'] != -1 else ar_dp['dp'][0]['pp']
    return np.array([platform, arr, dep, get_stay_time(arr, dep), pla_arr_path, pla_dep_path], dtype=object)
    
    # return {'platform': platform,
    #         'arr': arr,
    #         'dep': dep,
    #         'stay_time': get_stay_time(arr, dep),
    #         'pla_arr_path': pla_arr_path,
    #         'pla_dep_path': pla_dep_path}

@cython.wraparound(False)   # Deactivate negative indexing.
cdef np.ndarray parse_tl(dict tl): # parse Trip Label
    return np.array([tl['c'], tl['n'], tl['f'], tl['t'], tl['o']])

    # return {'train_type': tl['c'],
    #     'train_number': tl['n'],
    #     'product_class': tl['f'],
    #     'trip_type': tl['t'],
    #     'owner': tl['o']}

@cython.wraparound(False)   # Deactivate negative indexing.
cdef np.ndarray[np.longlong_t, ndim=1] parse_id(full_id):
    # if there is no id
    if full_id is -1:
        return np.array([-1, -1, -1])
    cdef list id_list = full_id.split('-')
    # if the first id starts with a '-'
    if id_list[0] is '':
        return np.array([-int(id_list[1]), int(id_list[2]), int(id_list[3])])
    # if the first id starts with a number
    else:
        return np.array([int(id_list[0]), int(id_list[1]), int(id_list[2])])

@cython.wraparound(False)   # Deactivate negative indexing.
cdef np.ndarray[np.int_t, ndim=1] parse_message(dict message):
    cdef int time = to_unix(message['ts'], pattern='%y%m%d%H%M')
    cdef int msg_id = -1 if message['id'] == -1 else int(message['id'][1:])
    cdef int code = int(message['c'])
    return np.array([msg_id, code, time])

@cython.wraparound(False)   # Deactivate negative indexing.
cdef np.ndarray[np.int_t, ndim=2] parse_messages(list messages):
    # cdef dict single_message
    # cdef list pml = []
    cdef Py_ssize_t i
    cdef np.ndarray[np.int_t, ndim=2] parsed_messages = np.empty((len(messages), 3), dtype=np.int)
    for i in range(parsed_messages.shape[0]):
        parsed_messages[i, :] = parse_message(messages[i])
    return parsed_messages

@cython.wraparound(False)   # Deactivate negative indexing.
cdef np.ndarray parse_event(dict event, arr_dep): # an event is an arrival or a departure
    cdef np.ndarray path = split_path(event['cpth'])
    cdef int changed_time = to_unix(event['ct'], pattern='%y%m%d%H%M')
    cdef int cancelation_time = to_unix(event['clt'], pattern='%y%m%d%H%M')
    cdef np.ndarray[np.int_t, ndim=2] messages = parse_messages(event['m'])

    return np.array([path, event['cp'], changed_time,
        event['cs'], cancelation_time, event['l'], messages])

@cython.wraparound(False)   # Deactivate negative indexing.
cpdef np.ndarray parse_plan(dict plan):
    cdef arr_dep = parse_ar_dp(plan)
    cdef tl = parse_tl(plan['tl'][0])
    cdef np.ndarray[np.longlong_t, ndim=1] train_ids = parse_id(plan['id'])

    return np.concatenate([arr_dep, tl, train_ids])

@cython.wraparound(False)   # Deactivate negative indexing.
cpdef np.ndarray parse_realtime(dict realtime):
    #parse ids
    cdef np.ndarray[np.longlong_t, ndim=1] train_ids = parse_id(realtime['id'])

    return np.concatenate([train_ids, parse_event(realtime['ar'][0], 'arr'), parse_event(realtime['dp'][0], 'dep')])

@cython.wraparound(False)   # Deactivate negative indexing.
cdef np.ndarray[np.int_t, ndim=2] concat_messages(np.ndarray[np.int_t, ndim=2] msg_list):
    # array that represents an empty message ([-1, -1, -1])
    cdef np.ndarray[np.int_t, ndim=1] empty_msg = np.array([-1, -1, -1])
    cdef list messages = []
    cdef np.ndarray[np.int_t, ndim=1] test

    # cdef msgs
    cdef np.ndarray[np.int_t, ndim=1] msg
    cdef Py_ssize_t i, j

    for i in range(msg_list.shape[0]):
        msg = msg_list[i, :]
        # continue if msg has no content ≈ msg has no msg_id
        if msg[0] == -1: # np.array_equal(msg, empty_msg): # MESSAGE:
            continue
        # check if this msg is already in messages:
        for j in range(len(messages)):
            if msg[0] == messages[j][0] and msg[2] == messages[j][2]:
                break
        # for test in messages:
        #     if np.array_equal(msg, test):
        #         break
        else:
            messages.append(msg)

    # if there where no messages, retrun an empty message
    if not messages:
        return np.array([empty_msg])
    else:
        return np.array(messages)

@cython.wraparound(False)   # Deactivate negative indexing.
cpdef np.ndarray concat_changes(np.ndarray changes):
    cdef np.ndarray concatted = np.full(changes.shape[1], -1, dtype=(object))
    concatted[[rind['first_id'], rind['middle_id'], rind['last_id']]] = changes[0, [rind['first_id'], rind['middle_id'], rind['last_id']]]
    
    # concat messages
    cdef np.ndarray arr_messages = np.concatenate(changes[:, rind['arr_message']])
    if arr_messages.ndim == 1:
        arr_messages = np.array([arr_messages])
    concatted[rind['arr_message']] = concat_messages(arr_messages)

    cdef np.ndarray dep_messages = np.concatenate(changes[:, rind['dep_message']])
    if dep_messages.ndim == 1:
        dep_messages = np.array([dep_messages])
    concatted[rind['dep_message']] = concat_messages(dep_messages)

    # concat delays / platform changes / cancelations / ...
    cdef list cols_to_concat = ['arr_changed_path', 'arr_changed_platform', 'arr_changed_time', 'arr_changed_status', 'arr_cancellation_time', 'arr_line', 
                      'dep_changed_path', 'dep_changed_platform', 'dep_changed_time', 'dep_changed_status', 'dep_cancellation_time', 'dep_line']
    cdef np.ndarray[np.uint8_t, ndim=1] index
    cdef int i
    cdef np.ndarray buf
    cdef str col
    for col in cols_to_concat:
        buf = changes[:, rind[col]]
        
        index = np.full((buf.shape[0]), False)
        for i in range(buf.shape[0]):
            if not buf[i] is -1: # and not buf[i] is np.array([-1]):
                if type(buf[i]) == np.ndarray:
                    if buf[i][0] != -1:
                        index[i] = True
                else:
                    index[i] = True
        buf = buf[index]
        if not buf.size is 0:
            # get the last info, as it is probably always the newest info
            concatted[rind[col]] = buf[buf.shape[0]-1] # buf[-1]
    return concatted