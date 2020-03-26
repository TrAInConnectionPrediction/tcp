from sklearn.metrics import multilabel_confusion_matrix
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import StratifiedKFold
import pandas as pd
import random
import json
import sys

sys.stdout = open('output3.txt','wt')


path =  'data/combinedData/na_droped_trains2.csv'
dataset = pd.read_csv(path, index_col=False, compression='zip')
dataset.columns

date = dataset['date'].astype('datetime64[D]')
dataset['month'] = date.dt.month
dataset['dayofweek'] = date.dt.dayofweek
dataset['hour'] = dataset['zeit']

def balance(dset, label, random_state):
    # make the balance of delayed trains in the dataset better
    #split dataset
    minor = dset[dataset[label] == True]
    major = dset[dataset[label] == False]
    #set major dataset to lenght of minor dataset by randomly seletcting datapoints
    major = major.sample(n=len(minor),random_state=random_state)
    #combine datsets
    balancedset = pd.concat([minor, major], ignore_index=True, sort=False)
    #I think this shuffels? and ensure length
    balancedset = balancedset.sample(n=len(balancedset),random_state=random_state).reset_index(drop=True)
    #print(len(balancedset))
    return balancedset

def held_out_label(df, feat_labels, label, held_out,held_out_var):
    X = df[feat_labels]
    y = df[label]
    X_train = df[df[held_out_var] != held_out][feat_labels]
    X_test = df[df[held_out_var] == held_out][feat_labels]
    y_train = df[df[held_out_var] != held_out][label]
    y_test = df[df[held_out_var] == held_out][label]
    return X_train, X_test, y_train, y_test


random_state = random.randint(1, 1000)
label = 'isadelay5'
feat_labels = [ 'month',
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
                'delta_lat']
                # 'relative_humidity', 'dew_point_c', 'air_pressure_hpa', 'temperature_c', 'trainno', 'weather_condition', 'type', 'bhf', 'wind_speed_kmh',
df = dataset.sample(frac=1,random_state=random_state)
X = df[feat_labels]
y = df[label]
del df

print('SplitDataset')
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=random_state)

#and now with balanced
dataset_bal = balance(dataset, label, random_state)
X_bal = dataset_bal[feat_labels]
y_bal = dataset_bal[label]
X_train_bal, X_test_bal, y_train_bal, y_test_bal = train_test_split(X_bal, y_bal, test_size=0.1, random_state=random_state)


from sklearn.dummy import DummyClassifier
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import ExtraTreesClassifier
 
#'RandomForest' : RandomForestClassifier(n_estimators=len(feat_labels)-2, max_depth=12, n_jobs=-1,random_state=random_state),
models = {
    'DecisionTree' : DecisionTreeClassifier(random_state=random_state),
    'RandomForest' : RandomForestClassifier(n_estimators=70, n_jobs=-1,random_state=random_state),
    'ExtraTrees' : ExtraTreesClassifier(n_estimators=70,n_jobs=-1,random_state=random_state),
    'OurTrees' : ExtraTreesClassifier(n_estimators=70, max_depth=12, n_jobs = -1, criterion = 'entropy',random_state=random_state),
    'OurTreeswithout' : ExtraTreesClassifier(n_estimators=70, max_depth=12, n_jobs = -1,random_state=random_state)
}

# models = {
#     'OurTrees' : ExtraTreesClassifier(n_estimators=70, max_depth=12, n_jobs = -1, criterion = 'entropy'),
# }
zeroR = DummyClassifier(strategy="most_frequent").fit( X_train,y_train).predict(X_test)
zeroR_bal = DummyClassifier(strategy="most_frequent").fit( X_train_bal,y_train_bal).predict(X_test_bal)
infos = {'random_state': random_state,
         'ZeroR': (1 - ((y_test != zeroR).sum() / X_test.shape[0])),
         'ZeroR_bal': (1 - ((y_test_bal != zeroR_bal).sum() / X_test_bal.shape[0]))
        }
        
        
for model in models:
    print(model)

    if False:
        scores = cross_val_score(models[model], X, y, cv=StratifiedKFold(n_splits=5))
        infos[model + "_cross_val_strat"] = "Accuracy: %0.2f (+/- %0.2f)" % (scores.mean(), scores.std() * 2)

    scores = cross_val_score(models[model], X, y, cv=5)
    infos[model + "_cross_val"] = "Accuracy: %0.2f (+/- %0.2f)" % (scores.mean(), scores.std() * 2)

    models[model].fit(X_train ,y_train)
    model_classified = models[model].predict(X_test)
    infos[model] = 1 - ((y_test != model_classified).sum() / X_test.shape[0])

    infos[model + '_matrix'] = confusion_matrix(y_test, model_classified, labels=[0,1])

    #BALANCED


    if False:
        scores = cross_val_score(models[model], X_bal, y_bal, cv=StratifiedKFold(n_splits=5))
        infos[model + "_cross_val_strat_bal"] = "Accuracy: %0.2f (+/- %0.2f)" % (scores.mean(), scores.std() * 2)

    scores = cross_val_score(models[model], X_bal, y_bal, cv=5)
    infos[model + "_cross_val_bal"] = "Accuracy: %0.2f (+/- %0.2f)" % (scores.mean(), scores.std() * 2)

    models[model].fit(X_train_bal ,y_train_bal)
    model_classified = models[model].predict(X_test_bal)
    infos[model + "_bal"] = 1 - ((y_test_bal != model_classified).sum() / X_test_bal.shape[0])

    infos[model + '_matrix_bal'] = confusion_matrix(y_test_bal, model_classified, labels=[0,1])
    print(infos)

    #for feature in zip(feat_labels, models[model].feature_importances_):
    #   print(feature)
#
#print("Writeing to file #1")
#print(infos)
##with open('info.json', 'w') as fp:
# #   json.dump(infos, fp)
#print("Done")
#infos['train_acc']={}
#for model in models:
#    print("\n",)
#    print(model)
#    infos['train_acc'][model]={}
#    for train in dataset['trainno'].unique():
#        print(train,end=', ')
#        X_train, X_test, y_train, y_test = held_out_label(dataset, feat_labels, label, train, 'trainno')
#        models[model].fit(X_train ,y_train)
#        model_classified = models[model].predict(X_test)
#        infos['train_acc'][model][train] = 1 - ((y_test != model_classified).sum() / X_test.shape[0])
#    print(str(infos))
#
print("\n\n\n\n INFOS:")
print(str(infos))
print("Done\n\n\n\n\n\n")

infos['bhf_acc']={}
for model in models:
    print("\n")
    print(model)
    infos['bhf_acc'][model]={}
    for train in dataset['bhf'].unique():
        print(train,end=', ')
        X_train, X_test, y_train, y_test = held_out_label(dataset, feat_labels, label, train, 'bhf')
        models[model].fit(X_train ,y_train)
        model_classified = models[model].predict(X_test)
        infos['bhf_acc'][model][train] = 1 - ((y_test != model_classified).sum() / X_test.shape[0])
    print(str(infos))


print("\n\n\n\n INFOS:")
print(str(infos))
print("Done\n\n\n\n\n\n")
