# Analyse
Diagramme zur Datenanalyse in unserer Projektdokumentation für Jugend Forscht 2021

## Datenfehler
![alt](overhour_rtd.png)
## Verspätungen während einer Stunde
![alt](overhour_recent_changes.png)
## Verspätungen während eines Tags
![alt](overday.png)
## Verspätungen während einer Woche
![alt](overweek.png)
## Datenverfügbarkeit übers Jahr
![alt](overyear.png)
## Verspätungsverteilung
![alt](delay_distrib.png)
## Verspätungskarte
![alt](mapplot.png)
## Verspätungen
![alt](delay_per_train_type.png)
## Ausfälle
![alt](cancellations_per_train_type.png)
## Packed-Bubbles
Um solche Packed-Bubble Charts zu erstellen, mussten wir unseren eingenen Algorithmus schreiben, da dieser Diagramtyp nicht von Matplotlib unterstützt wird. (Es gibt zwar einen auf Stackoverflow dazu, der ist aber unglaublich langsam und daher nicht für unsere Menge an Bubbles geeignet.) Hier ein GIF dazu, wie unser Algorithmus arbeitet:
![alt](packed_bubble_chart.gif)
## Verspätung in Abhängigkeit der bereits gefahrenen Strecke
![alt](delayoverstrecke.png)