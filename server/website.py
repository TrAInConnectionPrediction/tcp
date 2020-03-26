from flask import Flask, render_template, request, redirect, jsonify
from datetime import datetime, timedelta
from pytz import timezone
import json
import sys
import pandas as pd
import warnings
from flask_cors import CORS
import logging
import logging.handlers as handlers

# self-writen stuff
from predict_data import prediction_data
from connection import get_connection, clean_data
from random_forest import predictor

logger = logging.getLogger('my_app')
logger.setLevel(logging.INFO)

# Here we define our formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

logHandler = handlers.TimedRotatingFileHandler('website.log', when='M', interval=1, backupCount=10)
logHandler.setLevel(logging.INFO)
# Here we set our logHandler's formatter
logHandler.setFormatter(formatter)


logger.addHandler(logHandler)

#we need to change the paths
app = Flask(__name__, template_folder='website',static_folder='website/')
# TODO add config.py data
app.config['SECRET_KEY'] = ''
app.config['DEBUG'] = True



CORS(app)

# make a new random-forest-predictor instance
rfp = predictor()
pred_d = prediction_data()


def fromUnix(unix):
    """
    Convert from unix timestamp to GMT+1
    Removes millisecons (/1000) add one hour (+3600) and set timezone

    Args:
        unix (int): UNIX timestamp with milliseconds

    Returns:
        datetime: a datetime object with GMT+1 Timezone set
    """

    return datetime.utcfromtimestamp(float(unix)/1000 + 3600).replace(tzinfo=timezone('Europe/Berlin'))

def analysis(connection):
    """
    Analyses/evaluates/rates a given connection using machine learning
    Args:
        connection (dict/json): The connection 

    Returns:
        dict: the connection with the evaluation/rating
    """

    logger.info('Mache Analyse...')
    total_score = 0
    #change between scheduled and predicted time
    time = 'scheduledTime' # 'time'
    connection[-1]['totaltime'] = 0
    connection[-1]['transfers'] = len(connection)-2
    
    #sometimes the first segment is a Fußweg. We cannot predict delays for that
    index1 = 1 if (connection[0]['train']['name'] == 'Fußweg') else 0
    index2 = 3 if (connection[-2]['train']['name'] == 'Fußweg') else 2

    #add adelay5 to first and last station
    fs_data = pred_d.get_pred_data(connection[index1]['segmentStart']['title'], connection[index1]['train']['name'], fromUnix(connection[index1]['departure'][time]))
    ls_data = pred_d.get_pred_data(connection[-index2]['segmentDestination']['title'], connection[-index2]['train']['name'], fromUnix(connection[-index2]['departure'][time]))
    if (fs_data == False):
        connection[index1]['ddelay5'] = 1 - 0.95
    else:
        fs_data = pd.DataFrame(fs_data,index=[0])
        _x, _x, _x, _x, _x, connection[index1]['ddelay5'], _x, _x = rfp.predict(fs_data)
    
    if (ls_data == False):
        connection[-index2]['adelay5'] = 1 - 0.95
    else:
        ls_data = pd.DataFrame(ls_data,index=[0])
        _x, connection[-index2]['adelay5'], _x, _x, _x, _x, _x, _x = rfp.predict(ls_data)
    
    ##############
    for i in range(len(connection)-1):
        if ('ICE' in connection[i]['train']['name']):
            connection[i]['train']['d_type'] = 'ice'
        elif ('IC' in connection[i]['train']['name'] or 'EC' in connection[i]['train']['name']):
            connection[i]['train']['d_type'] = 'ic'
        elif ('RE' in connection[i]['train']['name'] or 'RB' in connection[i]['train']['name']):
            connection[i]['train']['d_type'] = 're'
        elif ('S' in connection[i]['train']['name']):
            connection[i]['train']['d_type'] = 's'
        else:
            connection[i]['train']['d_type'] = 'unknown'

    #there are two segments more than connections (= overall info at the end - 1 bc it is like that)
    for i in range(index1, len(connection)-index2):
        if (connection[i]['train']['name'] == 'Fußweg'):
            connection[-1]['transfers'] -= 1
            continue
        if (connection[i+1]['train']['name'] != 'Fußweg'):
            data1 = pred_d.get_pred_data(connection[i]['segmentDestination']['title'], connection[i]['train']['name'], fromUnix(connection[i]['arrival'][time]))
            data2 = pred_d.get_pred_data(connection[i+1]['segmentStart']['title'], connection[i+1]['train']['name'], fromUnix(connection[i+1]['departure'][time]))
            transtime = ((fromUnix(connection[i+1]['departure'][time]) - fromUnix(connection[i]['arrival'][time])).seconds//60)%60 #we just want minutes
            logger.debug(str(fromUnix(connection[i+1]['departure'][time])) + " - " + str(fromUnix(connection[i]['arrival'][time])) + " = " + str(transtime))
            connection[i]['con_score'], connection[i]['adelay5'], connection[i+1]['ddelay5'] = rfp.predict_con(data1, data2, transtime)
        else:
            data1 = pred_d.get_pred_data(connection[i]['segmentDestination']['title'], connection[i]['train']['name'], fromUnix(connection[i]['arrival'][time]))
            data2 = pred_d.get_pred_data(connection[i+2]['segmentStart']['title'], connection[i+1]['train']['name'], fromUnix(connection[i+1]['departure'][time]))
            transtime = ((fromUnix(connection[i+2]['departure'][time]) - fromUnix(connection[i]['arrival'][time])).seconds//60)%60 #we just want minutes
            logger.debug(str(fromUnix(connection[i+2]['departure'][time])) + " - " + str(fromUnix(connection[i]['arrival'][time])) + " = " + str(transtime))
            connection[i]['con_score'], connection[i]['adelay5'], connection[i+2]['ddelay5'] = rfp.predict_con(data1, data2, transtime)

        logger.debug(data1)
        logger.debug(data2)
        

        logger.info("Score for connection[" + connection[i]['train']['name'] +  ' to ' + connection[i+1]['train']['name'] + ' in ' + connection[i]['segmentDestination']['title'] + "] = " + str(connection[i]['con_score']))
        if (total_score == 0):
            total_score = connection[i]['con_score']
        else:
            total_score *= connection[i]['con_score']

    #Calculate total time from start to end
    totaltime = (fromUnix(connection[-2]['arrival'][time]) - fromUnix(connection[0]['departure'][time]))
    #we strip the seconds off the back
    connection[-1]['totaltime'] = str(totaltime)[:-3] 
    
    #When there ist no connection we always give 100% score
    if(len(connection) == 2): 
        connection[-1]['total_score'] = 100
    else:
        connection[-1]['total_score'] =  int(total_score * 100)

    logger.info('Verbindungsscore:' + str(total_score))
    return connection


def calc_con(startbhf, zielbhf, date):
    """
    Gets a connection from ```startbhf``` to ```zielbhf``` at a given date ```date``` using marudors HAFAS api.
    And rates the connection

    Args:
        startbhf (str): Start train station
        zielbhf (str): Destination train station
        date (str): Datetime in format : ```%d.%m.%Y %H:%M```

    Returns:
        dict: a dict with different connections
    """
    logger.info("Getting connection(s) for " + startbhf + ", " + zielbhf + ", " + date + "\n")
    connections = get_connection(startbhf, zielbhf, datetime.strptime(date, '%d.%m.%Y %H:%M'))
    connections = json.loads(connections)["routes"]
    connections = clean_data(connections)

    for i in range(len(connections)):
        connections[i] = analysis(connections[i])
    return connections

@app.route("/")
@app.route("/home/")
def home(output = []):
    """
    Gets called when somebody requests the website
    If we want we can redirect to kepiserver.de to the main server

    Args:
        -

    Returns:
        html page: the html homepage
    """
    #return redirect("http://kepiserver.de/tcp", code=302)
    return render_template('index.html')


@app.route('/connect', methods=['POST'])
def connect():
    """
    Gets called when the website is loaded

    Args:
        You can get something from the post request, but at the moment nothing

    Returns:
        list: a list of strings with all the known train stations
    """
    logger.debug("Somebody connected")
    data = request.form['keyword']
    bhfs = pd.read_csv("data/wetterdaten/bahnhof_to_weather_location.csv", sep=",", index_col=False)
    data = {"bhf": bhfs["bhf"].tolist()}
    resp = jsonify(data) 
    #resp.headers.add('Access-Control-Allow-Origin', '*')
    return resp

@app.route('/api', methods=['POST'])
def api():
    """
    Interface using POST request
    Gets a connection from ```startbhf``` to ```zielbhf``` at a given date ```date``` using marudors HAFAS api.
    And rates the connection

    Args:
        In the POST request

    Returns:
        json: All the possible connections
    """


    #add check for right datetime here

    data = calc_con(request.form['startbhf'], request.form['zielbhf'], request.form['date'])
    resp = jsonify(data) 
    return resp

@app.errorhandler(404)
def not_found(e): 
    # inbuilt function which takes error as parameter 
    # defining function 
    return render_template("404.html") 

if __name__ == '__main__':
    #ssl_context='adhoc'
    print( "\n\x1b[1;32m████████╗ ██████╗██████╗  \n\
╚══██╔══╝██╔════╝██╔══██╗ \n\
   ██║   ██║     ██████╔╝ \n\
   ██║   ██║     ██╔═══╝  \n\
   ██║   ╚██████╗██║      \n\
   ╚═╝    ╚═════╝╚═╝ \x1b[0m\n")
    logger.info( "\n\x1b[1;32m████████╗ ██████╗██████╗  \n\
╚══██╔══╝██╔════╝██╔══██╗ \n\
   ██║   ██║     ██████╔╝ \n\
   ██║   ██║     ██╔═══╝  \n\
   ██║   ╚██████╗██║      \n\
   ╚═╝    ╚═════╝╚═╝ \x1b[0m\n")
    app.run(host= '127.0.0.1', port=5000)
