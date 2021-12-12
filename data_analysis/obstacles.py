import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import RtdRay
import datetime
import matplotlib.pyplot as plt
from config import CACHE_PATH
import pandas as pd
import numpy as np
import seaborn as sn
import matplotlib.pyplot as plt

# SELECT summary, count(summary) as count, avg(category) as category FROM simpler_obstacles WHERE category = 0 GROUP BY summary ORDER BY summary DESC;
#                                 summary                                 | count |        category
# ------------------------------------------------------------------------+-------+------------------------
#  Störung: Witterungsbedingte Einflüsse  - Sturmschäden                  |    14 | 0.00000000000000000000
#  Störung: Witterungsbedingte Einflüsse  - Extreme Witterung             |     3 | 0.00000000000000000000
#  Störung: Unregelmäßigkeiten Bau                                        |    23 | 0.00000000000000000000
#  Störung: Störung auf Nachbarinfrastruktur                              |     4 | 0.00000000000000000000
#  Störung: Störung an Signalanlagen  - Stellwerksstörung                 |   129 | 0.00000000000000000000
#  Störung: Störung an Signalanlagen  - Sonstige Störung an Signalanlagen |    49 | 0.00000000000000000000
#  Störung: Störung an Signalanlagen  - Signalstörung                     |    50 | 0.00000000000000000000
#  Störung: Störung an Signalanlagen  - Rotausleuchtung                   |   104 | 0.00000000000000000000
#  Störung: Störung an Signalanlagen  - BÜ-Störung                        |   429 | 0.00000000000000000000
#  Störung: Störung an Oberleitungsanlagen                                |    94 | 0.00000000000000000000
#  Störung: Störung am Fahrweg  - Weichenstörung                          |   448 | 0.00000000000000000000
#  Störung: Störung am Fahrweg  - Sonstige Störung am Fahrweg             |   696 | 0.00000000000000000000
#  Störung: Störung am Fahrweg  - Schienenbruch                           |     7 | 0.00000000000000000000
#  Störung: Störung am Fahrweg  - Hindernis im/am Gleis                   |    31 | 0.00000000000000000000
#  Störung: Störung am Fahrweg  - Gleislagefehler                         |    47 | 0.00000000000000000000
#  Störung: Störung am Fahrweg  - Dammrutsch                              |     4 | 0.00000000000000000000
#  Störung: Störung am Fahrweg  - Brückenbeschädigung                     |    39 | 0.00000000000000000000
#  Störung: Sonstige Unregelmäßigkeit                                     |     8 | 0.00000000000000000000
#  Störung: Nicht-/Unterbesetzung von Stellwerken                         |     2 | 0.00000000000000000000
#  Störung: Gefährliches Ereignis  - Zugkollision                         |    61 | 0.00000000000000000000
#  Störung: Gefährliches Ereignis  - Vorbeifahrt am Haltbegriff           |     1 | 0.00000000000000000000
#  Störung: Gefährliches Ereignis  - Unfall am Bahnübergang               |    28 | 0.00000000000000000000
#  Störung: Gefährliches Ereignis  - Sonstige Kollision                   |     1 | 0.00000000000000000000
#  Störung: Gefährliches Ereignis  - Personenunfall                       |   364 | 0.00000000000000000000
#  Störung: Gefährliches Ereignis  - Fahrzeugbrand                        |     5 | 0.00000000000000000000
#  Störung: Fahrzeugstörung  - Wagenschaden                               |    26 | 0.00000000000000000000
#  Störung: Fahrzeugstörung  - Triebfahrzeugschaden                       |   155 | 0.00000000000000000000
#  Störung: Fahrzeugstörung  - Sonstige Fahrzeugstörung                   |    10 | 0.00000000000000000000
#  Störung: Fahrzeugstörung  - Bremsstörung                               |    68 | 0.00000000000000000000
#  Störung: Böschungsbrand                                                |     1 | 0.00000000000000000000
#  Störung: Behördliche Maßnahme  - Sonstige Behördliche Maßnahmen        |   109 | 0.00000000000000000000
#  Störung: Behördliche Maßnahme  - Polizeieinsatz                        |   183 | 0.00000000000000000000
#  Störung: Behördliche Maßnahme  - Personen im oder am Gleis             |   227 | 0.00000000000000000000
#  Störung: Behördliche Maßnahme  - Feuerwehr- oder Notarzteinsatz        |    22 | 0.00000000000000000000

priorities_text = {
    'obstacles_priority_24' : 'Totalsperrung',
    'obstacles_priority_37' : 'Fahren auf dem Gegengleis mit Zs 8 oder Befehl',
    'obstacles_priority_63' : 'Fahrzeitverlängerung auf Regellaufweg',
    'obstacles_priority_65' : 'Fahren auf dem Gegengleis mit Zs 6',
    'obstacles_priority_70' : 'Sonstiges',
    'obstacles_priority_80' : 'Abweichung vom Fahrplan für Zugmeldestellen',
}

def analyze_single_priority(priority_col: str, obstacle_rtd, non_obstacle_rtd):
    obstacle_rtd = obstacle_rtd.loc[obstacle_rtd[priority_col] > 0]
    non_obstacle_rtd = non_obstacle_rtd.sample(n=len(obstacle_rtd), random_state=0)

    ar_mean_with = obstacle_rtd['ar_delay'].mean()
    dp_mean_with = obstacle_rtd['dp_delay'].mean()
    ar_cancelations_with = (obstacle_rtd['ar_cs'] == 'c').mean()
    dp_cancelations_with = (obstacle_rtd['dp_cs'] == 'c').mean()
    ar_mean_without = non_obstacle_rtd['ar_delay'].mean()
    dp_mean_without = non_obstacle_rtd['dp_delay'].mean()
    ar_cancelations_without = (non_obstacle_rtd['ar_cs'] == 'c').mean()
    dp_cancelations_without = (non_obstacle_rtd['dp_cs'] == 'c').mean()

    print(priority_col)
    print('=====================')
    print('Number of datapoints:', len(obstacle_rtd))
    print('\t\twith\twithout\tdelta')
    print('ar_delay', round(ar_mean_with, 2), round(ar_mean_without, 2), round(ar_mean_with - ar_mean_without, 2), sep='\t')
    print('dp_delay', round(dp_mean_with, 2), round(dp_mean_without, 2), round(dp_mean_with - dp_mean_without, 2), sep='\t')
    print('ar_cancel', round(ar_cancelations_with, 4), round(ar_cancelations_without, 4), round(ar_cancelations_with - ar_cancelations_without, 4), sep='\t')
    print('dp_cancel', round(dp_cancelations_with, 4), round(dp_cancelations_without, 4), round(dp_cancelations_with - dp_cancelations_without, 4), sep='\t')
    print()

    non_obstacle_rtd.groupby('ar_delay')['ar_pt'].count().rename('No obstacles').plot(logy=True, legend=True, title=priorities_text[priority_col])
    obstacle_rtd.groupby('ar_delay')['ar_pt'].count().rename('obstacles').plot(logy=True, legend=True)
    plt.savefig(f'data/{priority_col}_ar.png')
    plt.close()
    # plt.show()
    non_obstacle_rtd.groupby('dp_delay')['dp_pt'].count().rename('No obstacles').plot(logy=True, legend=True, title=priorities_text[priority_col])
    obstacle_rtd.groupby('dp_delay')['dp_pt'].count().rename('obstacles').plot(logy=True, legend=True)
    plt.savefig(f'data/{priority_col}_dp.png')
    plt.close()
    # plt.show()

if __name__ == '__main__':
    import helpers.fancy_print_tcp

    # obstacle_rtd = RtdRay.load_data(min_date=datetime.datetime(2021, 3, 14)).compute()
    # non_obstacle_rtd = obstacle_rtd.loc[
    #     ~(obstacle_rtd['obstacles_priority_24'] > 0) &
    #     ~(obstacle_rtd['obstacles_priority_37'] > 0) &
    #     ~(obstacle_rtd['obstacles_priority_63'] > 0) &
    #     ~(obstacle_rtd['obstacles_priority_65'] > 0) &
    #     ~(obstacle_rtd['obstacles_priority_70'] > 0) &
    #     ~(obstacle_rtd['obstacles_priority_80'] > 0)
    # ]
    # non_obstacle_rtd.to_pickle(CACHE_PATH + '/non_obstacle_rtd.pkl')
    # obstacle_rtd = obstacle_rtd.loc[
    #     (obstacle_rtd['obstacles_priority_24'] > 0) |
    #     (obstacle_rtd['obstacles_priority_37'] > 0) |
    #     (obstacle_rtd['obstacles_priority_63'] > 0) |
    #     (obstacle_rtd['obstacles_priority_65'] > 0) |
    #     (obstacle_rtd['obstacles_priority_70'] > 0) |
    #     (obstacle_rtd['obstacles_priority_80'] > 0)
    # ]
    # obstacle_rtd.to_pickle(CACHE_PATH + '/obstacle_rtd.pkl')

    non_obstacle_rtd = pd.read_pickle(CACHE_PATH + '/non_obstacle_rtd.pkl')
    obstacle_rtd = pd.read_pickle(CACHE_PATH + '/obstacle_rtd.pkl')

    # Filter data errors and extremes
    obstacle_rtd = obstacle_rtd.loc[
        (obstacle_rtd['ar_delay'] >= -5) &
        (obstacle_rtd['dp_delay'] >= -1) &
        (obstacle_rtd['ar_delay'] <= 100) &
        (obstacle_rtd['dp_delay'] <= 100)
    ]
    non_obstacle_rtd = non_obstacle_rtd.loc[
        (non_obstacle_rtd['ar_delay'] >= -5) &
        (non_obstacle_rtd['dp_delay'] >= -1) &
        (non_obstacle_rtd['ar_delay'] <= 100) &
        (non_obstacle_rtd['dp_delay'] <= 100)
    ]

    # analyze_single_priority('obstacles_priority_24', obstacle_rtd, non_obstacle_rtd)
    # analyze_single_priority('obstacles_priority_37', obstacle_rtd, non_obstacle_rtd)
    # analyze_single_priority('obstacles_priority_63', obstacle_rtd, non_obstacle_rtd)
    # analyze_single_priority('obstacles_priority_65', obstacle_rtd, non_obstacle_rtd)
    # analyze_single_priority('obstacles_priority_70', obstacle_rtd, non_obstacle_rtd)
    # analyze_single_priority('obstacles_priority_80', obstacle_rtd, non_obstacle_rtd)

    non_obstacle_rtd = non_obstacle_rtd.sample(n=len(obstacle_rtd), random_state=0)

    ar_mean_with = obstacle_rtd['ar_delay'].mean()
    dp_mean_with = obstacle_rtd['dp_delay'].mean()
    ar_cancelations_with = (obstacle_rtd['ar_cs'] == 'c').mean()
    dp_cancelations_with = (obstacle_rtd['dp_cs'] == 'c').mean()
    ar_mean_without = non_obstacle_rtd['ar_delay'].mean()
    dp_mean_without = non_obstacle_rtd['dp_delay'].mean()
    ar_cancelations_without = (non_obstacle_rtd['ar_cs'] == 'c').mean()
    dp_cancelations_without = (non_obstacle_rtd['dp_cs'] == 'c').mean()

    print('all')
    print('===')
    print('Number of datapoints:', len(obstacle_rtd))
    print('\t\twith\twithout\tdelta')
    print('ar_delay', round(ar_mean_with, 2), round(ar_mean_without, 2), round(ar_mean_with - ar_mean_without, 2), sep='\t')
    print('dp_delay', round(dp_mean_with, 2), round(dp_mean_without, 2), round(dp_mean_with - dp_mean_without, 2), sep='\t')
    print('ar_cancel', round(ar_cancelations_with, 4), round(ar_cancelations_without, 4), round(ar_cancelations_with - ar_cancelations_without, 4), sep='\t')
    print('dp_cancel', round(dp_cancelations_with, 4), round(dp_cancelations_without, 4), round(dp_cancelations_with - dp_cancelations_without, 4), sep='\t')
    print()

    ml_columns = [
        'obstacles_priority_24',
        'obstacles_priority_37',
        'obstacles_priority_63',
        'obstacles_priority_65',
        'obstacles_priority_70',
        'obstacles_priority_80',
        'ar_delay',
        'dp_delay',
    ]

    corr = obstacle_rtd[ml_columns].corr()
    sn.heatmap(corr, annot=False, cmap='coolwarm', vmin=-1, vmax=1)
    plt.show()

    non_obstacle_rtd.groupby('ar_delay')['ar_pt'].count().rename('No obstacles').plot(logy=True, legend=True, title='Alle Bauarbeiten')
    obstacle_rtd.groupby('ar_delay')['ar_pt'].count().rename('obstacles').plot(logy=True, legend=True)
    plt.savefig(f'data/all_ar.png')
    plt.close()
    # plt.show()
    non_obstacle_rtd.groupby('dp_delay')['dp_pt'].count().rename('No obstacles').plot(logy=True, legend=True, title='Alle Bauarbeiten')
    obstacle_rtd.groupby('dp_delay')['dp_pt'].count().rename('obstacles').plot(logy=True, legend=True)
    plt.savefig(f'data/all_dp.png')
    plt.close()
    # plt.show()
    # plt.scatter(x=obstacle_rtd['length_sum'], y=obstacle_rtd['ar_delay'], alpha=0.01)
    # plt.show()
