import os
import csv
import json

from influxInterface import Influxdb
import timeStamp


if __name__ == '__main__':

    with open('/home/florian/enedis/config.json', 'r') as config_file:
        config = json.load(config_file)

    # Spécifiez le chemin du répertoire contenant les fichiers CSV
    repertoire = '/media/Flo-Pictures/home/Dev/ECS'
    influx_db = Influxdb(config, "database")

    # Boucle sur tous les fichiers du répertoire
    for fichier in os.listdir(repertoire):
        # Vérifie si le fichier est un fichier CSV
        if fichier.endswith(".csv"):
            chemin_fichier = os.path.join(repertoire, fichier)
            print(fichier)

            # Ouvrez le fichier CSV et effectuez les opérations nécessaires
            with open(chemin_fichier, 'r') as fichier_csv:
                lecteur_csv = csv.reader(fichier_csv)

                points=[]

                # Boucle sur les lignes du fichier CSV
                for ligne in lecteur_csv:
                    utc_timestamp = timeStamp.getTimestampFromStr(ligne[0])

                    point = {
                        "measurement": "ECS",
                        "tags": {},
                        "time": timeStamp.getTimestampFromStr(ligne[0]),
                        "fields": {
                            "temperature": float(ligne[1]),
                            "volume": float(ligne[2])
                        }
                    }

                    print(point)

                    points.append(point)
                
                influx_db.writePoints(points)
