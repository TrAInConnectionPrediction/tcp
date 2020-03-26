import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt
from progress.bar import Bar

def remove_false_stations(zugname):
    path = 'data/trainData/' + zugname + '.csv'
    path2 = 'data/streckendaten/' + zugname + '.csv'

    Daten = pd.read_csv(path, sep=",", index_col=False)
    Haltestellen = pd.read_csv(path2, sep=",", index_col=False)
    NewDaten = pd.DataFrame(columns = list(Daten))

    NewDaten = Daten.loc[Daten['bhf'].isin(Haltestellen['bhf'])]
    NewDaten.to_csv(path, index=False)

def add_missing_stations(Zugname):
    path = 'data/trainData/' + zugname + '.csv'
    path2 = 'data/streckendaten/' + zugname + '.csv'

    Daten = pd.read_csv(path, sep=",", index_col=False)
    Haltestellen = pd.read_csv(path2, sep=",", index_col=False)
    NewDaten = pd.DataFrame(columns = list(Daten))

    NewDaten = Daten.loc[Daten['bhf'].isin(Haltestellen['bhf'])]
    NewDaten.to_csv(Path, index=False)

    ####-This loop will take some time-####
    bar = Bar('Second Stepp', max=int(len(NewDaten) / 100))
    a = 0
    FinalDaten = pd.DataFrame(columns = list(Daten))
    i = 0
    b = 0
    while (i < len(NewDaten)):
        if (NewDaten.loc[i, 'bhf'] == Haltestellen.loc[(i + b) % len(Haltestellen['bhf']), 'bhf']):
            FinalDaten = FinalDaten.append(NewDaten.loc[i], ignore_index=True)
            i += 1
        else:
            FinalDaten.loc[a] = np.nan
            FinalDaten.loc[a, 'bhf'] = Haltestellen.loc[(i + b) % len(Haltestellen['bhf']), 'bhf']
            b += 1
        a += 1
        if(i % 100 == 0):
            bar.next()
    bar.finish()
    ####-------------------------------####

    FinalDaten = FinalDaten.replace({'':np.nan, '99:99':np.nan}, regex=True)

    FinalDaten.to_csv(path, index=False)

# Zugname = 'ICE_704' #input("Zugname:")
# add_missing_stations(Zugname)
