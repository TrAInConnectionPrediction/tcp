from sklearn.ensemble import ExtraTreesClassifier
from sklearn.metrics import multilabel_confusion_matrix
import numpy as np
import pandas as pd
from joblib import dump, load
import sys
import concurrent.futures

def pred_batch(batch, minimum, maximum, rfp, pred_d):
    for i in range(len(batch)):
        if (batch.at[i, 'station_number'] >= minimum and batch.at[i, 'station_number'] < maximum):
            fs_data = pred_d.get_pred_data(batch.at[i, 'bhf'], batch.at[i, 'trainno'],pd.to_datetime(batch.at[i, 'date']+' '+batch.at[i, 'arr']))
            fs_data = pd.DataFrame(fs_data,index=[0])
            delays = rfp.predict(fs_data)

            batch.at[i, 'adelay0'] = delays['adelay0']
            batch.at[i, 'adelay5'] = delays['adelay5']
            batch.at[i, 'adelay10'] = delays['adelay10']
            batch.at[i, 'adelay15'] = delays['adelay15']
            batch.at[i, 'ddelay0'] = delays['ddelay0']
            batch.at[i, 'ddelay5'] = delays['ddelay5']
            batch.at[i, 'ddelay10'] = delays['ddelay10']
            batch.at[i, 'ddelay15'] = delays['ddelay15']
    return batch, index

def pred_stations(minimum, maximum):
    rfp = predictor()
    pred_d = prediction_data()
    # dodo: preload every fahrplan as we need them anyway
    path = 'data/combinedData/na_droped_trains2.csv'

    df = pd.read_csv(path, index_col=False, compression='zip', engine='c')
    batch_size = 10000
    pred_bat = {}
    running_threads = []
    max_threads = 8
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for i in range(0, len(df), batch_size):
            pred_bat[str(i)] = executor.submit(pred_batch,
                                            df[i:min(i+batch_size, len(df))].reset_index(drop=True),
                                            minimum, 
                                            maximum, 
                                            rfp, 
                                            pred_d)
            running_threads.append(str(i))
            if (len(running_threads) >= max_threads):
                df[i:min(i+batch_size, len(df))] = pred_bat[running_threads[0]].result()
                del running_threads[0]

        for i in running_threads:
            pass

    bar = Bar('processing', max=len(df)/1000)

    for i in range(len(df)):
        if (df.at[i, 'station_number'] >= minimum and df.at[i, 'station_number'] < maximum):
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


def train_and_test(X, y, X_test, y_test, name):
    clf = ExtraTreesClassifier(n_estimators=70, max_depth=12,
                                 random_state=0, n_jobs = None, criterion = 'entropy')
    clf.fit(X, y)
    
    #save the trained model
    dump(clf, 'server/' + name + '_extra.joblib')

    #make and print some acc statistics
    test_pred = clf.predict(X_test)
    acc = test_pred[test_pred == y_test]
    print('Feature importances', clf.feature_importances_)
    print('acc_' + name + ':', len(acc)/ len(test_pred))

    #print confusion matrix
    c_m = multilabel_confusion_matrix(y_test, test_pred).ravel()
    dadx = [' ad0', ' ad5', 'ad10', 'ad15', ' dd0', ' dd5', 'dd10', 'dd15']
    print('\ttn, \tfp, \tfn, \ttp, \tacc1, \t\t\tacc2')
    for i in range(8):
        print(dadx[i], c_m[4*i+0], c_m[4*i+1], c_m[4*i+2], c_m[4*i+3], c_m[4*i+3]/(c_m[4*i+3]+c_m[4*i+1]), c_m[4*i+0]/(c_m[4*i+0]+c_m[4*i+2]), sep='\t')


def first_np_sets(dataset):
    # make the balance of delayed trains in the dataset better
    set1 = dataset[dataset['adelay10'] == True]
    buffer1 = dataset[dataset['adelay10'] == False]
    buffer1 = buffer1.sample(n=len(set1)*3, random_state=0)
    set1 = pd.concat([set1, buffer1], ignore_index=True, sort=False)
    set1 = set1.sample(n=len(set1), random_state=0).reset_index(drop=True)
    dataset = set1
    del set1

    #only train on these features
    dataset = dataset[['adelay0', 'adelay5', 'adelay10', 'adelay15', 'ddelay0', 'ddelay5', 'ddelay10', 'ddelay15',
                    'month',
                    'dayofweek',
                    'hour',
                    'lat',
                    'lon',
                    'destination_lat',
                    'destination_lon',
                    'total_lenth',
                    'total_time',
                    'delta_lon',
                    'delta_lat']]
                    # 'relative_humidity', 'dew_point_c', 'air_pressure_hpa', 'temperature_c', 'trainno', 'weather_condition', 'type', 'bhf', 'wind_speed_kmh',

    #classes
    columns = ['adelay0', 'adelay5', 'adelay10', 'adelay15', 'ddelay0', 'ddelay5', 'ddelay10', 'ddelay15']

    #split into train and test set / labels
    train_set1 = dataset.sample(frac=0.8,random_state=0)
    test_set1 = dataset.drop(train_set1.index)

    train_labels = train_set1[columns].copy()
    train_set1.drop(columns, axis=1, inplace=True)

    test_labels = test_set1[columns].copy()
    test_set1.drop(columns, axis=1, inplace=True)

    train_set1 = train_set1.to_numpy()
    train_labels = train_labels.to_numpy()

    test_set1 = test_set1.to_numpy()
    test_labels = test_labels.to_numpy()

    return train_set1, train_labels, test_set1, test_labels

def np_sets(dataset, balance):
    # make the balance of delayed trains in the dataset better
    set1 = dataset[dataset['adelay10'] == True]
    buffer1 = dataset[dataset['adelay10'] == False]
    buffer1 = buffer1.sample(n=len(set1)*balance, random_state=0)
    set1 = pd.concat([set1, buffer1], ignore_index=True, sort=False)
    set1 = set1.sample(n=len(set1), random_state=0).reset_index(drop=True)
    dataset = set1
    del set1

    #only train on these features
    dataset = dataset[['adelay0', 'adelay5', 'adelay10', 'adelay15', 'ddelay0', 'ddelay5', 'ddelay10', 'ddelay15',
                    'month',
                    'dayofweek',
                    'hour',
                    'track_length_since_start',
                    'time_since_first_station',
                    'station_number',
                    'lat',
                    'lon',
                    'track_length',
                    'stay_time',
                    'time_since_last_station',
                    'start_lat',
                    'start_lon',
                    'destination_lat',
                    'destination_lon',
                    'total_lenth',
                    'total_time',
                    'delta_lon',
                    'delta_lat']]
                    # 'relative_humidity', 'dew_point_c', 'air_pressure_hpa', 'temperature_c', 'trainno', 'weather_condition', 'type', 'bhf', 'wind_speed_kmh',

    #classes
    columns = ['adelay0', 'adelay5', 'adelay10', 'adelay15', 'ddelay0', 'ddelay5', 'ddelay10', 'ddelay15']

    #split into train and test set / labels
    train_set1 = dataset.sample(frac=0.8,random_state=0)
    test_set1 = dataset.drop(train_set1.index)

    train_labels = train_set1[columns].copy()
    train_set1.drop(columns, axis=1, inplace=True)

    test_labels = test_set1[columns].copy()
    test_set1.drop(columns, axis=1, inplace=True)

    train_set1 = train_set1.to_numpy()
    train_labels = train_labels.to_numpy()

    test_set1 = test_set1.to_numpy()
    test_labels = test_labels.to_numpy()

    return train_set1, train_labels, test_set1, test_labels

def first_train():
    print('creating models...', end=' ')

    path =  'data/combinedData/na_droped_trains2.csv'
    
    dataset = pd.read_csv(path, index_col=False, compression='zip', engine='c') #C:/Users/McToel/Desktop/regression.csv combinedData/na_droped_trains.csv C:/Users/Serverkonto/Desktop/regression.csv
    dataset = dataset.sample(frac=1.0,random_state=0)

    dataset = dataset[dataset['station_number'] == 0].reset_index(drop=True)
    print(len(dataset))

    date = dataset['date'].astype('datetime64[D]')
    dataset['month'] = date.dt.month
    dataset['dayofweek'] = date.dt.dayofweek
    dataset['hour'] = dataset['zeit']
    del date
    dataset = dataset[['adelay',
                    'ddelay',
                    'month',
                    'dayofweek',
                    'hour',
                    'lat',
                    'lon',
                    'destination_lat',
                    'destination_lon',
                    'total_lenth',
                    'total_time',
                    'delta_lon',
                    'delta_lat']]

    dataset['adelay0'] = dataset['adelay'] <= 5
    dataset['adelay5'] = (dataset['adelay'] > 5) #& (dataset['adelay'] <= 10)
    dataset['adelay10'] = (dataset['adelay'] > 10) #& (dataset['adelay'] <= 15)
    dataset['adelay15'] = dataset['adelay'] > 15

    dataset['ddelay0'] = dataset['ddelay'] <= 5
    dataset['ddelay5'] = (dataset['ddelay'] > 5) #& (dataset['ddelay'] <= 10)
    dataset['ddelay10'] = (dataset['ddelay'] > 10) #& (dataset['ddelay'] <= 15)
    dataset['ddelay15'] = dataset['ddelay'] > 15

    d_set, d_lab, test_set, test_lab = first_np_sets(dataset)
    del dataset
    train_and_test(d_set, d_lab, test_set, test_lab, 'first_station')


def train(minimum, maximum):
    print('creating models...', end=' ')

    path =  'data/combinedData/na_droped_trains2.csv'
    
    dataset = pd.read_csv(path, index_col=False, compression='zip', engine='c') #C:/Users/McToel/Desktop/regression.csv combinedData/na_droped_trains.csv C:/Users/Serverkonto/Desktop/regression.csv
    dataset = dataset.sample(frac=1.0,random_state=0)

    dataset = dataset[(dataset['station_number'] >= minimum)
                      & (dataset['station_number'] < maximum)]
    dataset = dataset.reset_index(drop=True)
    print('length of unbalanced dataset: ' + len(dataset))

    date = dataset['date'].astype('datetime64[D]')
    dataset['month'] = date.dt.month
    dataset['dayofweek'] = date.dt.dayofweek
    dataset['hour'] = dataset['zeit']
    del date
    dataset = dataset[['adelay',
                    'ddelay',
                    'month',
                    'dayofweek',
                    'hour',
                    'track_length_since_start',
                    'time_since_first_station',
                    'station_number',
                    'lat',
                    'lon',
                    'track_length',
                    'stay_time',
                    'time_since_last_station',
                    'start_lat',
                    'start_lon',
                    'destination_lat',
                    'destination_lon',
                    'total_lenth',
                    'total_time',
                    'delta_lon',
                    'delta_lat']]

    dataset['adelay0'] = dataset['adelay'] <= 5
    dataset['adelay5'] = (dataset['adelay'] > 5) #& (dataset['adelay'] <= 10)
    dataset['adelay10'] = (dataset['adelay'] > 10) #& (dataset['adelay'] <= 15)
    dataset['adelay15'] = dataset['adelay'] > 15

    dataset['ddelay0'] = dataset['ddelay'] <= 5
    dataset['ddelay5'] = (dataset['ddelay'] > 5) #& (dataset['ddelay'] <= 10)
    dataset['ddelay10'] = (dataset['ddelay'] > 10) #& (dataset['ddelay'] <= 15)
    dataset['ddelay15'] = dataset['ddelay'] > 15

    d_set, d_lab, test_set, test_lab = np_sets(dataset, minimum, maximum)
    del dataset
    train_and_test(d_set, d_lab, test_set, test_lab, 'first_station')

    def train_all():
        train_first()




class predictor:
    def __init__(self):
        try:
            self.multiclass_rf = load('server/first_station_extra.joblib')
        except FileNotFoundError:
            if input("Models not present! Create Models? y/[n]") == "y":
                train()
                self.__init__()
            else:
                sys.exit('Please create Models')

    def predict(self, features_df):
        #this is to make sure that we olny pass the right features in the right order into the predictor.
        features_df = features_df[[ 'month',
                                    'dayofweek',
                                    'hour',
                                    'track_length_since_start',
                                    'time_since_first_station',
                                    'station_number',
                                    'lat',
                                    'lon',
                                    'track_length',
                                    'stay_time',
                                    'time_since_last_station',
                                    'start_lat',
                                    'start_lon',
                                    'destination_lat',
                                    'destination_lon',
                                    'total_lenth',
                                    'total_time',
                                    'delta_lon',
                                    'delta_lat']]
        
        #convert df to 2d numpy array
        features = features_df.to_numpy()
        features = features.reshape(1, -1)
        del features_df

        #make a probability prediction for one train
        delays = np.array(self.multiclass_rf.predict_proba(features))[:, 0,1]

        #return delay probabilitys in a dict
        return {'adelay0': delays[0],
                'adelay5': delays[1],
                'adelay10': delays[2],
                'adelay15': delays[3],
                'ddelay0': delays[4],
                'ddelay5': delays[5],
                'ddelay10': delays[6],
                'ddelay15': delays[7] }

first_train()