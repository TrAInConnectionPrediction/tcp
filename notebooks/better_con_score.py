import pandas as pd
from datetime import datetime

# self-writen stuff
from predict_data import prediction_data
from random_forest import predictor

path = 'data/connecting_trains_full.csv' # 'data/connecting_trains_full.csv'

# make a new random-forest-predictor instance
rfp = predictor()
pred_d = prediction_data()

combined = pd.read_csv(path, engine='c', compression='zip')

combined['adelay0'] = 0.0
combined['adelay5'] = 0.0
combined['adelay10'] = 0.0
combined['adelay15'] = 0.0

combined['ddelay0'] = 0.0
combined['ddelay5'] = 0.0
combined['ddelay10'] = 0.0
combined['ddelay15'] = 0.0

from progress.bar import Bar

# andata = pd.DataFrame() # pred_d.get_pred_data(combined.at[0, 'bhf'], combined.at[0, 'antrain'], datetime.strptime(combined.at[0, 'date'], '%Y-%m-%d'))
# abdata = pd.DataFrame() # pred_d.get_pred_data(combined.at[0, 'bhf'], combined.at[0, 'abtrain'], datetime.strptime(combined.at[0, 'date'], '%Y-%m-%d'))


bar = Bar('Predicting', max=len(combined))
for i, connection in combined.iterrows():
    try:
        data =  pred_d.get_pred_data(connection['bhf'], connection['antrain'], datetime.strptime(connection['date'], '%Y-%m-%d'))
        # andata = pd.concat([andata, pd.DataFrame(data, index=[0])])
        pred = rfp.predict(pd.DataFrame(data, index=[0]))
        combined.at[i, 'adelay0'] =  pred['adelay0'] 
        combined.at[i, 'adelay5'] =  pred['adelay5'] 
        combined.at[i, 'adelay10'] = pred['adelay10']
        combined.at[i, 'adelay15'] = pred['adelay15']

        data =  pred_d.get_pred_data(connection['bhf'], connection['abtrain'], datetime.strptime(connection['date'], '%Y-%m-%d'))
        # abdata = pd.concat([abdata, pd.DataFrame(data, index=[0])])
        # abdata = abdata.append(pd.DataFrame(data, index=[0]))
        pred = rfp.predict(pd.DataFrame(data, index=[0]))
        combined.at[i, 'ddelay0'] =  pred['ddelay0'] 
        combined.at[i, 'ddelay5'] =  pred['ddelay5'] 
        combined.at[i, 'ddelay10'] = pred['ddelay10']
        combined.at[i, 'ddelay15'] = pred['ddelay15']
    except:
        print('Error at i = ', i, ' and connection = ', connection)
    if i % 15 == 0:
        bar.next(15)
        print(' eta: ', bar.eta_td, end='\r')
    # if i > 200000:
    #     break

combined.to_csv('better_con_score.csv', index=False, compression='zip')