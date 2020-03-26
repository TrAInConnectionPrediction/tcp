import pandas as pd
import glob
from os.path import basename
from sklearn.model_selection import train_test_split

datei = 'data/combinedData/na_droped_trains.csv'

infos = {}
print(datei)
df = pd.read_csv(datei, index_col=False, compression='zip')
dataset = df.sample(frac=1.0,random_state=0)

date = dataset['date'].astype('datetime64[D]')
dataset['month'] = date.dt.month
dataset['dayofweek'] = date.dt.dayofweek
dataset['hour'] = dataset['zeit']
del date
X = dataset[['month',
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
y = df['isadelay5']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

datei_info = {}


from sklearn.metrics import confusion_matrix
from sklearn.dummy import DummyClassifier

dummy_classifier = DummyClassifier(strategy="most_frequent")
dummy_classifier.fit( X_train,y_train)
y_dummy_classifier = dummy_classifier.predict(X_test)
datei_info['zeror'] = 1 - ((y_test != y_dummy_classifier).sum() / X_test.shape[0])
datei_info['zeror_matrix'] = confusion_matrix(y_test, y_dummy_classifier, labels=[0,1])

from sklearn.neighbors import KNeighborsClassifier
knn = KNeighborsClassifier(n_neighbors=5,n_jobs=-1)
model_knn = knn.fit(X_train, y_train)
y_pred_knn = model_knn.predict(X_test)
datei_info['knn'] = 1 - ((y_test != y_pred_knn).sum() / X_test.shape[0])
datei_info['knn_matrix'] = confusion_matrix(y_test, y_pred_knn, labels=[0,1])

from sklearn.naive_bayes import GaussianNB
gnb = GaussianNB(n_jobs=-1)
model_nb = gnb.fit(X_train, y_train)
y_pred_nb = model_nb.predict(X_test)
datei_info['gnb'] = 1 - ((y_test != y_pred_nb).sum() / X_test.shape[0])
datei_info['gnb_matrix'] = confusion_matrix(y_test, y_pred_nb, labels=[0,1])

from sklearn import svm
model_svm = svm.SVC(gamma='scale',n_jobs=-1)
model_svm.fit(X_train, y_train)
y_pred_svm = model_svm.predict(X_test)
datei_info['svm'] = 1 - ((y_test != y_pred_svm).sum() / X_test.shape[0])
datei_info['svm_matrix'] = confusion_matrix(y_test, y_pred_svm, labels=[0,1])

from sklearn import tree
model_dt = tree.DecisionTreeClassifier(n_jobs=-1)
model_dt = model_dt.fit(X_train, y_train)
y_pred_dt = model_dt.predict(X_test)
datei_info['dt'] = 1 - ((y_test != y_pred_dt).sum() / X_test.shape[0])
datei_info['dt_matrix'] = confusion_matrix(y_test, y_pred_dt, labels=[0,1])

from sklearn.linear_model import LogisticRegression
model_lr = LogisticRegression(random_state=0, solver='liblinear', n_jobs=-1).fit(X_train, y_train)
y_pred_lr = model_lr.predict(X_test)
datei_info['lr'] = 1 - ((y_test != y_pred_lr).sum() / X_test.shape[0])
datei_info['lr_matrix'] = confusion_matrix(y_test, y_pred_lr, labels=[0,1])

infos[datei] = datei_info
print(infos)

avg = {'zeror': 0.0,
        'gnb': 0.0,
        'svm': 0.0, 
        'dt': 0.0, 
        'lr': 0.0, 
       }
maxname = {'zeror': '',
        'gnb': '',
        'svm': '', 
        'dt': '', 
        'lr': '', 
       }

maxi = {'zeror': 0.0,
        'gnb': 0.0,
        'svm': 0.0, 
        'dt': 0.0, 
        'lr': 0.0, 
       }
i = 1
for data in infos:
    avg['zeror'] +=  infos[data]['zeror']
    avg['gnb'] +=  infos[data]['gnb']
    if (infos[data]['gnb'] - infos[data]['zeror']) > maxi['gnb']:
        maxi['gnb'] = (infos[data]['gnb'] - infos[data]['zeror'])
        maxname['gnb'] = data
    avg['svm'] +=  infos[data]['svm']
    if (infos[data]['svm'] - infos[data]['zeror']) > maxi['svm']:
        maxi['svm'] = (infos[data]['svm'] - infos[data]['zeror'])
        maxname['svm'] = data
    avg['dt'] +=  infos[data]['dt']
    if (infos[data]['dt'] - infos[data]['zeror']) > maxi['dt']:
        maxi['dt'] = (infos[data]['dt'] - infos[data]['zeror'])
        maxname['dt'] = data
    avg['lr'] +=  infos[data]['lr']
    if (infos[data]['lr'] - infos[data]['zeror']) > maxi['lr']:
        maxi['lr'] = (infos[data]['lr'] - infos[data]['zeror'])
        maxname['lr'] = data
    i+=1

avg['zeror'] /=  i

avg['gnb'] /=  i

avg['svm'] /=  i

avg['dt'] /=  i

avg['lr'] /=  i

print(avg)
print(maxi)
print(maxname)
