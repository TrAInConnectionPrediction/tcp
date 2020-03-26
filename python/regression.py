import matplotlib.pyplot as plt
import pandas as pd
import tensorflow as tf
import numpy as np

from tensorflow import keras
from tensorflow.keras import layers

plt.ion()

path = 'data/combinedData/regression.csv'

dataset = pd.read_csv(path, index_col=False, compression='zip')
dataset = dataset.drop(['ddelay'], axis=1)

dataset1 = dataset[['adelay', 'track_length_since_start', 'time_since_first_station', 'station_number', 'station_number', 'lat', 'lon', 'track_length', 'zeit']]
dataset = dataset1.join(dataset[['relative_humidity', 'stay_time', 'EC', 'dew_point_c', 'air_pressure_hpa', 'time_since_last_station', 'temperature_c']])

dataset[dataset['adelay'] > 30] = 30

train_dataset = dataset.sample(frac=0.8,random_state=0)
test_dataset = dataset.drop(train_dataset.index)

train_labels = train_dataset.pop('adelay')
test_labels = test_dataset.pop('adelay')

train_data = train_dataset.to_numpy()
train_labels = train_labels.to_numpy()

test_data = test_dataset.to_numpy()
test_labels = test_labels.to_numpy()

def plot_history(history):
  hist = pd.DataFrame(history.history)
  hist['epoch'] = history.epoch
  hist.to_clipboard()

# Display training progress by printing a single dot for each completed epoch
class PrintDot(keras.callbacks.Callback):
  def on_epoch_end(self, epoch, logs):
    if epoch % 100 == 0: print('')
    print('.', end='')

EPOCHS = 200

# The patience parameter is the amount of epochs to check for improvement
early_stop = keras.callbacks.EarlyStopping(monitor='val_loss', patience=10)

def build_model():
  model = keras.Sequential([
    layers.Dense(32, activation='relu', input_shape=[len(train_dataset.keys())]),
    layers.Dense(32, activation='relu'),
    layers.Dense(1)
  ])

  optimizer = tf.keras.optimizers.RMSprop(0.01)

  model.compile(loss='mse',
                optimizer=optimizer,
                metrics=['mae', 'mse'])
  return model
model = build_model()



history = model.fit(train_data, train_labels, epochs=EPOCHS,
                    validation_split = 0.2, verbose=0, callbacks=[early_stop, PrintDot()])
plot_history(history)
tf.saved_model.save(model, 'saved_model/')


test_predictions = model.predict(test_data).flatten()

plt.scatter(test_labels, test_predictions)
plt.xlabel('True delay')
plt.ylabel('Predicted delay')
plt.axis('equal')
plt.axis('square')
#plt.xlim([0,plt.xlim()[1]])
#plt.ylim([0,plt.ylim()[1]])
plt.show()
print('lul')