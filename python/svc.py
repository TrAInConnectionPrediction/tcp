from sklearn.ensemble import ExtraTreesClassifier
from sklearn.metrics import multilabel_confusion_matrix
import numpy as np
import pandas as pd
from joblib import dump, load
from sklearn.svm import SVC


def train_and_test(X, y, X_test, y_test, name):
    clf = SVC(gamma='auto')
    clf.fit(X, y.ravel())
    
    #save the trained model
    dump(clf, 'server/' + name + '_rf.joblib')

    #make and print some acc statistics
    test_pred = clf.predict(X_test)
    #acc = test_pred[test_pred == y_test]
    #print('Feature importances', clf.feature_importances_)
    #print('acc_' + name + ':', len(acc)/ len(test_pred))
    print('Confusion Matrix:', multilabel_confusion_matrix(y_test, test_pred))


def np_sets(dataset):
    # make the balance of delayed trains in the dataset better
    set1 = dataset[dataset['adelay10'] == True]
    buffer1 = dataset[dataset['adelay10'] == False]
    buffer1 = buffer1.sample(n=len(set1))
    set1 = pd.concat([set1, buffer1], ignore_index=True, sort=False)
    set1 = set1.sample(n=len(set1)).reset_index(drop=True)
    dataset = set1
    del set1

    #only train on these features
    dataset = dataset[['adelay10',
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
                    'time_since_last_station']]
                    # 'relative_humidity', 'dew_point_c', 'air_pressure_hpa', 'temperature_c', 'trainno', 'weather_condition', 'type', 'bhf', 'wind_speed_kmh',

    #classes
    columns = ['adelay10']

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

def train_svc():
    print('creating models...', end=' ')

    path =  'data/combinedData/na_droped_trains.csv'
    
    dataset = pd.read_csv(path, index_col=False, compression='zip') #C:/Users/McToel/Desktop/regression.csv combinedData/na_droped_trains.csv C:/Users/Serverkonto/Desktop/regression.csv
    dataset = dataset.sample(frac=0.1,random_state=0)

    date = dataset['date'].astype('datetime64[D]')
    dataset['month'] = date.dt.month
    dataset['dayofweek'] = date.dt.dayofweek
    dataset['hour'] = dataset['zeit']
    del date
    dataset = dataset.drop(['date', 'zeit', 'isadelay5', 'isadelay10', 'isadelay15', 'isddelay', 'dayname', 'daytype', ],axis = 1)

    #dataset[['bhf', 'weather_condition', 'trainno', 'type']] = handle_non_numerical_data(dataset[['bhf', 'weather_condition', 'trainno', 'type']])

    # one_hot = pd.get_dummies(dataset['type'])
    # dataset = dataset.drop('type',axis = 1)
    # dataset = dataset.join(one_hot)
    dataset['adelay0'] = dataset['adelay'] <= 5
    dataset['adelay5'] = (dataset['adelay'] > 5) #& (dataset['adelay'] <= 10)
    dataset['adelay10'] = (dataset['adelay'] > 10) #& (dataset['adelay'] <= 15)
    dataset['adelay15'] = dataset['adelay'] > 15

    dataset['ddelay0'] = dataset['ddelay'] <= 5
    dataset['ddelay5'] = (dataset['ddelay'] > 5) #& (dataset['ddelay'] <= 10)
    dataset['ddelay10'] = (dataset['ddelay'] > 10) #& (dataset['ddelay'] <= 15)
    dataset['ddelay15'] = dataset['ddelay'] > 15

    d_set, d_lab, test_set, test_lab = np_sets(dataset)
    del dataset
    train_and_test(d_set, d_lab, test_set, test_lab, 'svc')

train_svc()