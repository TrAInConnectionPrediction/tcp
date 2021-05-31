import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
import geopy.distance
import pandas as pd
import requests
import json
from helpers import StationPhillip

def add_station(eva):

    name_data = requests.get('https://marudor.de/api/hafas/v1/station/{station}'.format(station=eva)).json()
    name = ''
    for row in name_data:
        if int(row['id']) == eva:
            name = row['title']
            stations.name_index_stations.loc[name, 'lat'] = row['coordinates']['lat']
            stations.name_index_stations.at[name, 'lon'] = row['coordinates']['lng']
            # stations.name_index_stations.at[name, 'ds100'] = row['ds100']
            stations.name_index_stations.at[name, 'eva'] = eva
            break
    else:
        print('not found:', eva)
        return

    # data = requests.get('https://marudor.de/api/businessHub/experimental/station/{name}'.format(name=name)).json()
    # for row in data:
    #     if name == row['title']:
    #         stations.name_index_stations.loc[name, 'lat'] = row['location']['latitude']
    #         stations.name_index_stations.at[name, 'lon'] = row['location']['longitude']
    #         stations.name_index_stations.at[name, 'ds100'] = row['ds100']
    #         stations.name_index_stations.at[name, 'eva'] = eva
    #         break
    # else:
    #     print('not found:', name)

    df = stations.name_index_stations.reset_index()
    df = df.drop('index', axis=1)
    from database import DB_CONNECT_STRING
    df.to_sql('stations', con=DB_CONNECT_STRING, if_exists='replace')


if __name__ == '__main__':
    # Add missing locations to stations
    stations = StationPhillip()
    add_station(570973)
    # add_station(555310)
    # for station in stations:
    #     coords = stations.get_location(name=station)
    #     if pd.isna(coords[0]) or pd.isna(coords[1]):
    #         print('getting coordinates for', station)
    #         data = requests.get('https://marudor.de/api/hafas/v1/station/{station}'.format(station=station.replace('/', ' '))).json()
    #         for row in data:
    #             if station == row['title']:
    #                 stations.name_index_stations.at[station, 'lat'] = row['coordinates']['lat']
    #                 stations.name_index_stations.at[station, 'lon'] = row['coordinates']['lng']
    #                 break
    #         else:
    #             print('no location for', station)
    #
    # # Manual changes
    # stations.name_index_stations = stations.name_index_stations.drop('Radolfzell Fähre', axis=0)
    # stations.name_index_stations = stations.name_index_stations.drop('Romanshorn (See)', axis=0)
    #
    # stations.name_index_stations.at['Mosbach (Baden)', 'lat'] = 49.35237
    # stations.name_index_stations.at['Mosbach (Baden)', 'lon'] = 9.143585
    #
    # stations.name_index_stations.at['Bahnhofsvorplatz, Aue', 'lat'] = 50.590814
    # stations.name_index_stations.at['Bahnhofsvorplatz, Aue', 'lon'] = 12.698261
    #
    # stations.name_index_stations.at['Postplatz, Aue', 'lat'] = 50.587982
    # stations.name_index_stations.at['Postplatz, Aue', 'lon'] = 12.700374
    # df = stations.name_index_stations.reset_index()
    # df = df.drop('index', axis=1)
    # from database import DB_CONNECT_STRING
    # df.to_sql('stations', con=DB_CONNECT_STRING, if_exists='replace')

    import helpers.fancy_print_tcp
    stations = StationPhillip()

    old_stations = pd.read_pickle('data/old_stations_2021_03_25.pkl')
    missing_stations = ['Linkenheim Friedrichstraße, Linkenheim-Hochstetten','Linkenheim Schulzentrum, Linkenheim-Hochstetten','Haus Bethlehem, Karlsruhe','Kurt-Schumacher-Straße, Karlsruhe','August-Bebel-Straße, Karlsruhe','Marktplatz, Karlsruhe','Bösensell','Galgenschanze','OberurselWeißkirchen/Steinbach','Riegelsberg Süd, Riegelsberg','Rathaus, Riegelsberg','Güchenbach, Riegelsberg','Wolfskaulstr., Riegelsberg','Heinrichshaus Malstatt, Saarbrücken','Post, Riegelsberg','Riegelsberghalle, Riegelsberg','In der Hommersbach, Heusweiler','Kirschhof, Heusweiler','Lebach Süd','Walpershofen Mitte, Riegelsberg','Markt, Heusweiler','Eiweiler, Heusweiler','Landsweiler Süd, Lebach','Eiweiler Nord, Heusweiler','Landsweiler Nord, Lebach','Gisorsstr., Riegelsberg','Mühlenstr. Walpershofen, Riegelsberg','Walpershofen/Etzenhofen, Riegelsberg','Schulzentrum, Heusweiler','Amersfoort','Brackwede','Waggonfabrik','Langensteinbach Schießhüttenäcker, Karlsbad','Vils Stadt','Ostendstraße, Karlsruhe','Wolfartsweierer Straße, Karlsruhe','Pfeddersheim','Schloss Gottesaue, Karlsruhe','Philipp-Reis-Straße, Karlsruhe','Europapl./PostGalerie (Karlstr.), Karlsruhe','Wusterhausen(Dosse) NE','Tägerwilen Dorf','Herisau','Mörsch Narzissenstraße, Rheinstetten','Mörsch Merkurstraße, Rheinstetten','Mörsch Römerstraße, Rheinstetten','Mörsch Rösselsbrünnle, Rheinstetten','Forchheim Oberfeldstraße, Rheinstetten','Forchheim Hauptstraße, Rheinstetten','Mörsch Bach West, Rheinstetten','Forchheim Hallenbad, Rheinstetten','Daxlanden Nussbaumweg, Karlsruhe','Forchheim Messe/Leichtsandstraße, Rheinstetten','Mörsch Am Hang, Rheinstetten','Mörsch Rheinaustraße, Rheinstetten','Rheinhafenstraße, Karlsruhe','Eugendorf','Eckenerstraße, Karlsruhe','Wallersee','Daxlanden Dornröschenweg, Karlsruhe','Weng b.Neumarkt','Daxlanden Karl-Delisle-Straße, Karlsruhe','Daxlanden Thomas-Mann-Straße, Karlsruhe','Kostrzyn','Neumarkt-Köstendorf','Zell/Pram','Kumpfmühl','Kimpling','Riedau','Salzburg Kasern','St. Margrethen','Klingnau','Döttingen','Siggenthal-Würenlingen','Obertrattnach-Markt Hofkirchen','Haiding','Schlüßlberg','Hazlov','Vojtanov obec','Munderfing','Schalchen-Mattighofen','Zelena Lhota','Friedburg','Lengau','Straßwalchen West','Nörvenich-Rommelsheim','Bezdekov u Klatov','Hojsova Straz','Hojsova Straz-Brcalnik','St.Georgen/Mattig','Uttendorf-Helpfau','Nyrsko','Mattighofen','Mauerkirchen','Furth b.Mattighofen','Rietheim(CH)','Jelenia Gora Zabobrze','Kwieciszowice','Jerzmanki','Rybnica','Rebiszow','Mlynsko','Gryfow Sl.','Stara Kamienica','Olszyna Lubanska','Batowice Lubanskie','Zareba','Ubocze','Mikulowa','Zweidlen','Mellikon','Kaiserstuhl AG','Rümikon AG','Koblenz Dorf','Altach','Pepinster-Cite','Spa-Géronstère','Theux','Franchimont','Juslenville','Klaus in Vorarlberg','Nüziders','Schlins-Beschling','Sulz-Röthis','Hatlerdorf(Dornbirn)','Feldkirch Amberg','Daxlanden Hammäcker, Karlsruhe','Schwarzach i Vorarl.','Haselstauden (Dornbirn)','KIT-Campus Nord Bahnhof, Eggenstein-Leopoldshafen','Brno hl.n.','Horsens st','Vejle st','Aarhus','Chlumcany u Dobran','Karez','Plzen zast.','Forchheim Leichtsandstr./Messe Karlsruhe, Rheinste','Dobrany','Prestice','Holysov','Petrovice nad Uhlavo','Svihov u Klatov','Lancut','Belfort-Montbéliard TGV','Besançon Franche-Comté TGV','Lorüns','Kaltenbrunnen im Montafon','Burgbrohl','Wierzbowa Slaska','Bludenz Brunnenfeld','Bludenz-Moos','Kolpingplatz, Karlsruhe','Ebertstraße, Karlsruhe','Cervene Porici','Borovy','Prestice-Zastavka','Rheinhafen, Karlsruhe','Lubliniec','Olawa','Raciborz','Janowice Wielkie','Brzesko Okocim','Brzeg','Bochnia','Debica','Ciechanowice','Gliwice','Przeworsk','Zabrze','Boguszow Gorce Zach.','Glogow','Tarnow','Jaroslaw','Przemysl Zasanie','Skanderborg st','Zdice','Leipzig Roßplatz','Bamberg Bahnhof','Leipzig Augustusplatz','Hamburg Hbf (Spitalerstraße)','Hamburg Hbf (Kirchenallee)','Lubin Gorniczy','Wojanow','Przemysl Gl.','Boguszow Gorce Wsch.','Rzeszow Gl.','Sedzislaw','Walbrzych Glowny','Chalupki','Zagajnik','Chojnow','Lasow','Jedrzychowice','Milano Lambrate','Zebrzydowa','Berlin Hbf (Washingtonplatz)','Bebra Busbahnhof','Ziltendorf Kreuzung','Wiesenau Dorfstraße','Cottbus Busbahnhof','Bietigheim Bahnhof/ZOB, Bietigheim-Bissingen','Berlin Südkreuz (Bus)','Rostock Albrecht-Kossel-Platz','München Hauptbahnhof Nord','Halle(Saale)Hbf Ernst-Kamieth-Str. Hst.9','Dresden Hbf Strehlener Str.','Bad Schandau-Postelwitz','Pirna Busbf','Nürnberg Hbf (Nelson-Mandela-Platz/Südausgang)','Königstein, Reißiger Platz','Köln Hbf (Breslauer Platz)','Kiel Hbf Kaistraße','Bochum Hbf (Buddenbergplatz)','Magdeburg Hbf ZOB','Leipzig K-Eisner-/A-Hoffmann-Straße','Ollenhauerstr./Lindauer Allee, Berlin','Bernau Bahnhofsplatz (ggü alte Post)','Erkner ZOB','Sanitz ZOB','Groß Lüsewitz Abzweig','Plauen(Vogtl) ob Bf (Busbahnhof)','An der Bahnbrücke, Chemnitz','Leipzig Hbf Ostseite','Chemnitz-Schönau, CVAG-Wendeanlage','Chemnitz Mitte Zwickauer/Reichsstraße','Falkensee ZOB','Praha Masarykovo n.','Hohenthurm Mölbitzer Weg','Luckaitztal Buchwäldchen Ortsmitte','Neubrandenburg ZOB','Sternfeld Ort','Gnevkow Wendeschleife','Altentreptow ZOB','Rakow Alte Schule','Beetz-Sommerfeld Bahnhofstraße','Hollerich','Rostock-Kassebohm Weißes Kreuz','Mönchhagen B105/Oberdorf/Unterdorf','Friedland Bahnhof Westseite','Rathaus, Kolkwitz','Norden KOF','Sponholz Ort','Wien Hbf (Bahnsteige 1-2)','Raddusch Ortsmitte','Stolpen Grüne Aue','Mehltheuer Leubnitzer Straße','Dossow Dorfstraße','Walsleben Schule','Netzeband Kirche','Steinefrenz Ort','Kreuzlingen, Bahnhof','Porschdorf (Kr. Pirna)  Straße zum Bahnhof','Eschwege-Niederhone Am Steg','Ulbersdorf Almenhof','Hainewalde Gemeindeamt','Meroux','Berlin-Schönefeld Flughafen','Berlin Gehrenseestr.','Europapl./PostGalerie (Kaiserstr), Karlsruhe','Tullastraße/Verkehrsbetriebe, Karlsruhe','Berlin Betriebsbf Schöneweide','Mühlburger Tor (Kaiserallee), Karlsruhe','Kronenplatz (Fritz-Erler-Str.), Karlsruhe','Werderstraße, Karlsruhe','Tivoli, Karlsruhe','Ludwigstr. Malstatt, Saarbrücken','Römerkastell, Saarbrücken','Kieselhumes, Saarbrücken','Cottbuser Platz Malstatt, Saarbrücken','Pariser Platz/St.Paulus Malstatt, Saarbrücken','Siedlerheim Malstatt, Saarbrücken','Uhlandstr., Saarbrücken','Rastpfuhl Malstatt, Saarbrücken','Kaiserstr., Saarbrücken','Hauptbahnhof, Saarbrücken','Trierer Str., Saarbrücken','Hellwigstr., Saarbrücken','Landwehrplatz, Saarbrücken','Johanneskirche, Saarbrücken','Knielinger Allee, Karlsruhe','Neureut Adolf-Ehrmann-Bad, Karlsruhe','Hochstetten Altenheim, Linkenheim-Hochstetten','Neureut Welschneureuter Straße, Karlsruhe','Rüppurrer Tor, Karlsruhe','Eggenstein Spöcker Weg, Eggenstein-Leopoldshafen','Neubeckum','Linkenheim Süd, Linkenheim-Hochstetten','Eggenstein Süd, Eggenstein-Leopoldshafen','Städt. Klinikum/Moltkestraße, Karlsruhe','Eggenstein Schweriner Straße, Eggenstein-Leopoldsh','Leopoldshafen Frankfurter Straße, Eggenstein-Leopo','Fischweier, Karlsbad','Durlacher Tor/KIT-Campus Süd, Karlsruhe','Windelsbleiche','Friedrichstal Nord, Stutensee','Friedrichstal Saint-Riquier-Platz, Stutensee','Blankenloch Kirche, Stutensee','Büchig, Stutensee','Hagsfeld Süd, Karlsruhe','Friedrichstal Mitte, Stutensee','Hauptfriedhof, Karlsruhe','Hagsfeld Geroldsäcker, Karlsruhe','Blankenloch Nord, Stutensee','Hirtenweg/Technologiepark, Karlsruhe','Rintheim Sinsheimer Straße, Karlsruhe','Blankenloch Süd, Stutensee','Halle(W) Gerry-Weber-Stadion','Lindau Hbf','Sarnau','Hagsfeld Reitschulschlag (Schleife), Karlsruhe','Puch b.Hallein Urstein','Kuchl Garnei','Paracelsus-Bad (U), Berlin','Elz(Limburg/Lahn) Mitte','Brohl(Brohltalbahn)','Sylbach','Neuenbürg(Enz) Eyachbrücke','Blankenloch Mühlenweg, Stutensee','Hagsfeld Jenaer Straße, Karlsruhe','Spöck Hochhaus, Stutensee','Hagsfeld Reitschulschlag, Karlsruhe','Blankenloch Tolna-Platz, Stutensee','Krumbach(Schw)Schule','Kyritz Am Bürgerpark','Hallein Burgfried','Leopoldshafen Viermorgen, Eggenstein-Leopoldshafen','Blumenkamp','Sennelager','Sennestadt','Dingden','Wroclaw Muchobor','Milkowice','Tocnik','Luzany','Trzcinsko','Marciszow','Boguszow Gorce','Boleslawiec','Tomaszow Boleslawiecki','Osetnica','Okmiany','München Ost Friedenstraße','Cottbus Spreewaldbahnhof','Schmilka Grenzübergang','Flughafen Terminal 1, Frankfurt a.M.','Leipzig Deutsche Nationalbibliothek','Rastatt Bahnhof Ost','Bahnhof, Bad Krozingen','Leipzig Naunhofer Straße','Stralsund Bahnhofstraße','Staffel Ost','Bockhorst, Güstrow','Engelsdorf Riesaer Straße','Graal-Müritz Ostseering','Dreikirchen Hauptstraße','Mücka Schule','Neumark(Sachs) Ladestraße','Lohsa Ziegelteich','Pirna-Copitz Schulstraße','Fretzdorf Dorf','Karlstor, Karlsruhe','Plzen-Doudlevce','Dehtin','Kedzierzyn Kozle','Witkow Sl.','Rokitki','Steinbach Rebland, Baden-Baden','Eberswalde Busbahnhof','Fa. Wolman, Sinzheim','Haueneberstein Industriegebiet, Baden-Baden','Steinpleis Post','Königsplatz/Mauerstraße, Kassel','Sroda Slaska','Modla','Finkenheerd Lindenstraße','Bühl (Baden)','Leipzig G-Schumann-/Lindenthaler Straße','Leipzig Ostfriedhof','Klitten Jahmen','Domazlice mesto','Opatija Matulji','Leipzig-Lützschena Hallesche Straße','Pforzheim Hbf ZOB','Kartung Abzw. Winden, Sinzheim','Pirna-Copitz Nord Heinrich-Heine-Str.','Lalendorf Am Waldrand','Uebigau Markt','Girod Bus','Mathystraße, Karlsruhe','Turnhalle, Sinzheim','Husby West','Malomice','Jurdani','Leipzig S-Bahnhof Möckern','Entenfang, Karlsruhe','Kurort Rathen, Parkplatz','Am Glücksberg, Chemnitz','Lindau Hbf (Bahnhofplatz)','Uhyst Gaststätte','Leszno Gorne','Postojna','Hoyerswerda-Neustadt Ziolkowskistraße','Kassel Hauptbahnhof Nord','Erlangen Busbahnhof (Parkplatzstraße)','Lübeck Alexander-Fleming-Straße','Permani','Rijeka','Pillgram Schulstraße','Krumpa Addinol','Krnjevo','Ilirska Bistrica','Ainring-Mitterfelden Abzw. B20','Bregenz Bahnhof (Busterminal)','Rengershausen Kirche, Baunatal','Bayerisch Gmain Alpgarten','Lochau-Hörbranz Bahnhof (Vorplatz)','Fürstenwalde ZOB','Heidelberg Hbf Bahnstadt','Hangelsberg Berliner Damm','Sapjane','Leipzig Georg-Schumann-/Lützowstraße','Tägerwilen Dorf, Bahnhof','Chorin Dorf','Rüdnitz Dorf','Lehrte Neues Zentrum','Rukavac','Schwerin Marienplatz','Borovnica','Greifswald ZOB','Ruhland Busbahnhof','Hauptbahnhof Bahnhofstr., Chemnitz','Beutersitz Bahnhof/Wildgruber Straße','Heckershausen Mitte, Ahnatal','Jusici','Schwante An der Kirche','Bitterfeld Busbahnhof','Wustrau-Radensleben L164','Birkengrund Am Birkengrund','Stenn Wendestelle','Pivka','Göttingen Busbf']
    old_stations = old_stations.loc[old_stations['name'].isin(missing_stations), ['name', 'eva', 'ds100', 'lat', 'lon']]
    no_ds100 = []
    for i, row in old_stations.iterrows():
        if row['ds100'] is None or len(row['ds100']) > 5:
            search_results = stations.search_iris(row['name'])
            for search_result in search_results:
                if search_result['name'] == row['name']:
                    old_stations.loc[i, 'ds100'] = search_result['ds100']
                    print('found:', row['name'])
                    break
            else:
                no_ds100.append(row['name'])
                print(row['name'])
    old_stations = old_stations.loc[~old_stations['name'].isin(no_ds100)]
    stations.stations_df.append(old_stations)
    print('len:', len(stations))
