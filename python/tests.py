import pandas as pd
import re
from progress.bar import Bar
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from sklearn import linear_model
import sys
import numpy as np

sys.path.insert(1, 'C:/Users/McToel/Documents/GitHub/train_connection_prediction/server')
sys.path.insert(1, 'C:/Users/Serverkonto/Documents/Visual Studio Code/Python/Train_Conection_Prediction/train_connection_prediction/server')

from missing_stations import remove_false_stations

from random_forest import predictor
from predict_data import prediction_data



import cProfile, pstats, io

def profile(fnc):
    
    """A decorator that uses cProfile to profile a function"""
    
    def inner(*args, **kwargs):
        
        pr = cProfile.Profile()
        pr.enable()
        retval = fnc(*args, **kwargs)
        pr.disable()
        s = io.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print(s.getvalue())
        return retval

    return inner


@profile
def test_con_pred():
    rfp = predictor()
    pred_d = prediction_data()
    path = 'data/connecting_trains.csv'

    connecting_trains = pd.read_csv(path, index_col=False, compression='zip')
    connecting_trains = connecting_trains.dropna()
    connecting_trains = connecting_trains.drop_duplicates()
    connecting_trains = connecting_trains.reset_index(drop=True)

    columns = ['con_score','act_tf_time']
    index = range(len(connecting_trains))
    test_results = pd.DataFrame(index=index, columns= columns) #pd.DataFrame(data={'con_score':[''], 'act_tf_time':['']})
    bar = Bar('processing', max=len(connecting_trains)/10)

    for index, train in connecting_trains.iterrows():
        features1 = pred_d.get_pred_data(train['bhf'], train['antrain'],pd.to_datetime(train['date']+' '+train['arr']))
        features2 = pred_d.get_pred_data(train['bhf'], train['abtrain'],pd.to_datetime(train['date']+' '+train['dep']))
        transfertime = int((datetime.strptime(train['dep'], '%H:%M') - datetime.strptime(train['arr'], '%H:%M')).seconds / 60)

        test_results.at[index, 'con_score'], _x, _y =  rfp.predict_con(features1, features2, transfertime)
        test_results.at[index, 'act_tf_time'] = transfertime - train['adelay'] + train['ddelay']
        test_results.at[index, 'pla_tf_time'] = transfertime
        if (index % 10 == 0):
            bar.next()
        # if(index > 1000):
        #     break
    
    bar.finish()
    
    test_results = test_results.dropna()
    test_results['con_score'] = pd.to_numeric(test_results['con_score'])
    test_results['act_tf_time'] = pd.to_numeric(test_results['act_tf_time'])
    test_results['pla_tf_time'] = pd.to_numeric(test_results['pla_tf_time'])
    test_results = test_results.sort_values(by=['act_tf_time'])
    # test_results[['con_score']].plot()
    # test_results.plot.scatter(x='con_score', y='act_tf_time')
    # plt.show()

    test_results.to_csv('data/testResults/test_results.csv', index=False)


def pred_first_station():
    from rf_chain import predictor
    rfp = predictor()
    pred_d = prediction_data()
    path = 'data/combinedData/na_droped_trains2.csv'

    df = pd.read_csv(path, index_col=False, compression='zip', engine='c')

    bar = Bar('processing', max=len(df)/1000)

    for i in range(len(df)):
        if (df.at[i, 'station_number'] == 0):
            fs_data = pred_d.get_pred_data(df.at[i, 'bhf'], df.at[i, 'trainno'],pd.to_datetime(df.at[i, 'date']+' '+df.at[i, 'arr']))
            fs_data = pd.DataFrame(fs_data,index=[0])
            delays = rfp.predict(fs_data)

            df.at[i, 'adelay0'] = delays['adelay0']
            df.at[i, 'adelay5'] = delays['adelay5']
            df.at[i, 'adelay10'] = delays['adelay10']
            df.at[i, 'adelay15'] = delays['adelay15']
            df.at[i, 'ddelay0'] = delays['ddelay0']
            df.at[i, 'ddelay5'] = delays['ddelay5']
            df.at[i, 'ddelay10'] = delays['ddelay10']
            df.at[i, 'ddelay15'] = delays['ddelay15']
        if (i % 1000 == 0):
            bar.next()
    
    bar.finish()

    df.to_csv(path, index=False, compression='zip')
    df.to_csv(path+'a', index=False)


def test_analisis():
    test_results = pd.read_csv('data/testResults/test_results.csv', index_col=False)

    fontdict_x = {'fontsize': 17,
                'fontweight' : 'normal',
                'verticalalignment': 'top',
                'horizontalalignment': 'center'}

    fontdict_y = {'fontsize': 17,
                'fontweight' : 'normal',
                'verticalalignment': 'baseline',
                'horizontalalignment': 'center'}

    test_results = test_results.sample(frac = 0.05)
    test_results = test_results[test_results['pla_tf_time'] < 19]
    test_results = test_results[test_results['pla_tf_time'] == 5]
    # total = len(test_results['act_tf_time'])
    # test_results = test_results[test_results['act_tf_time'] > 1]
    # print(len(test_results['act_tf_time'])/total)

    test_results = test_results.sort_values(by=['act_tf_time']).reset_index(drop=True)
    test_results = test_results[100:(len(test_results)-100)]

    # np.random.seed(19680801)
    # N = len(test_results)
    # scatter_transfer = np.random.rand(N) % 1
    # test_results['act_tf_time'] += scatter_transfer
    # test_results['pla_tf_time'] += scatter_transfer

    fig, pla = plt.subplots()
    fig, act = plt.subplots()

    act.scatter(x=test_results['con_score'], y=test_results['act_tf_time'], marker='x')#c=test_results['pla_tf_time'], label=test_results['pla_tf_time'], 
    pla.scatter(x=test_results['con_score'], y=test_results['pla_tf_time'], marker='x')#c=test_results['pla_tf_time'], label=test_results['pla_tf_time'],
    pla.set_xlabel('Verbindungs-Score', fontdict = fontdict_x)
    pla.set_ylabel('geplamte Umsteigezeit in Min.', fontdict = fontdict_y)
    #test_results[['con_score']].plot()

    z = np.polyfit(test_results['con_score'], test_results['act_tf_time'], 1)
    p = np.poly1d(z)
    act.scatter(x=test_results['con_score'],y=p(test_results['con_score']),marker='o')
    

    act.set_xlabel('Verbindungs-Score', fontdict = fontdict_x)
    act.set_ylabel('tatsächliche Umsteigezeit in Min.', fontdict = fontdict_y)
    # the line equation:
    print ('y=%.6fx+(%.6f)'%(z[0],z[1]))
    plt.show()
    


#test_con_pred()
# test_analisis()
pred_first_station()
