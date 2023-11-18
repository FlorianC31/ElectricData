import requests
#import os
#import sys
import pandas as pd
from bs4 import BeautifulSoup


if __name__ == "__main__":

    # source of the form: http://192.168.1.33/index.php/hidden/export_file
    url = 'http://192.168.1.33/index.php/hidden/exec_export_file'

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    start_time = "2023-11-15 00:00:00"
    end_time = "2023-11-15 23:59:59"

    data = {
        'start_time': start_time,
        'end_time': end_time
    }

    response = requests.post(url, data=data, headers=headers)

    if response.status_code != 200:
        print(f"La requête a échoué avec le code d'état : {response.status_code}")
        exit()


    # Utiliser la date de début comme nom de fichier Excel
    excel_file_name = start_time.replace(":", "").replace(" ", "_") + ".xls"


    # empty list
    data = []

    # for getting the header from
    # the HTML file
    list_header = []
    soup = BeautifulSoup(response.content,'html.parser')
    header = soup.find_all("table")[0].find("tr")

    for items in header:
        try:
            list_header.append(items.get_text())
        except:
            continue
    
    #print(list_header)


    # for getting the data
    HTML_data = soup.find_all("table")[0].find_all("tr")[1:]

    for element in HTML_data:
        sub_data = []
        for sub_element in element:
            try:
                sub_data.append(sub_element.get_text())
            except:
                continue
        data.append(sub_data)

    points = {}

    for row in data:
        timestamp = row[9]
        if timestamp not in points:
            points[timestamp] = {}

        panelId = (row[0], row[1])
        points[timestamp][panelId] = {}
        for i in range(2,9):
            points[timestamp][panelId][list_header[i]] = row[i]
        #print(timestamp, panelId, points[timestamp][panelId])

    totalEnergy = 0
    for timestamp in points:
        nbPanels = len(points[timestamp])
        first = True
        for panelId in points[timestamp]:
            if first:
                total = {}
                for measure in points[timestamp][panelId]:
                    total[measure] = float(points[timestamp][panelId][measure])
                first = False
            else:
                for measure in points[timestamp][panelId]:
                    total[measure] += float(points[timestamp][panelId][measure])

        total['DC Voltage(V)'] /= nbPanels
        total['Grid Frequency(Hz)'] /= nbPanels
        total['Temperature(oC)'] /= nbPanels
        total['Grid Voltage(V)'] /= nbPanels

        totalEnergy += total['Energy(kWh)'] 

        points[timestamp]['total'] = total

        print(timestamp, points[timestamp]['total'])

    print("Total Energy of the day (kWh):", totalEnergy)
    #print(data)

    # Storing the data into Pandas
    # DataFrame
    dataFrame = pd.DataFrame(data = data, columns = list_header)

    # Converting Pandas DataFrame
    # into CSV file
    dataFrame.to_csv('ecuRecord.csv')

