import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.engine import DB_CONNECT_STRING
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_sql('SELECT * FROM hyperparametertuning', DB_CONNECT_STRING).iloc[8:-1, :]
df['label'] = df['index'].str.replace('_ar', '').astype('int')
df = df.set_index('label', drop=True)
df = df.drop(columns=['trials'], axis=0)
df.plot()
plt.show()