import csv
import pandas as pd
import re
import sqlite3
import json
from lxml import etree


def remove_checked(string):
    pos = string.find("[")
    final_string = string[:pos]
    return final_string


def extract_integer(string):
    final_string = ""
    for i in range(len(string)):
        if string[i].isdigit():
            final_string += string[i]
        else:
            pass
    if final_string == string:
        return final_string, False
    elif final_string != string and bool(re.search(r'\d', string)):
        return final_string, True
    else:
        return final_string, False


def check_data(csv_file):
    list1 = []
    with open(csv_file, "r") as file:
        file_reader = csv.reader(file, delimiter=",")
        for line in file_reader:
            list1.append(line)
        file.close()
    count = 0
    for element in list1:
        for i in range(len(element)):
            element[i], cell_corrected = extract_integer(element[i])
            if cell_corrected:
                count += 1
    with open(f"{csv_file[:-4]}[CHECKED].csv", "w", encoding="utf-8") as w_file:
        file_writer = csv.writer(w_file, delimiter=",", lineterminator="\n")
        for i in range(1, len(list1)):
            file_writer.writerow(list1[i])
        return w_file, count


def sql_to_json(db_file):
    connection = sqlite3.connect(db_file)
    tmp_cur = connection.cursor()
    table = tmp_cur.execute("""SELECT * FROM convoy;""")
    connection.commit()
    rows = table.fetchall()

    temp_list = []
    count = 0
    for el in rows:
        temp_dict = {"vehicle_id": el[0], "engine_capacity": el[1], "fuel_consumption": el[2],
                          "maximum_load": el[3]}
        temp_list.append(temp_dict)
        count += 1
    connection.close()


def sql_to_xml(db_file):
    connection = sqlite3.connect(db_file)
    cur = connection.cursor()
    xml_string = "<convoy>"
    table = cur.execute("""SELECT * FROM convoy;""")
    connection.commit()
    tmp_rows = table.fetchall()
    tmp_string = ""
    count = 0
    for elem in tmp_rows:
        tmp_string += "<vehicle>"
        tmp_string += f"<vehicle_id>{elem[0]}</vehicle_id>"
        tmp_string += f"<engine_capacity>{elem[1]}</engine_capacity>"
        tmp_string += f"<fuel_consumption>{elem[2]}</fuel_consumption>"
        tmp_string += f"<maximum_load>{elem[3]}</maximum_load>"
        tmp_string += "</vehicle>"
        count += 1
    xml_string += tmp_string + "</convoy>"
    tmp_root = etree.fromstring(xml_string)
    tmp_tree = etree.ElementTree(tmp_root)
    tmp_tree.write(f"{db_file[:-5]}.xml")
    if count == 1:
        print(f"1 vehicle was saved into {db_file[:-5]}.xml")
    else:
        print(f"{count} vehicles were saved into {db_file[:-5]}.xml")


def pitstop_count(engine_capacity, fuel_consumption):
    liters_used_in_1_km = fuel_consumption / 100
    liters_needed_in_450_km = liters_used_in_1_km * 450
    tmp_pitstop = 0
    liters_used_in_450_km = liters_needed_in_450_km
    while liters_used_in_450_km - engine_capacity > 0:
        liters_used_in_450_km -= engine_capacity
        tmp_pitstop += 1
    return tmp_pitstop, liters_needed_in_450_km


def score_get(engine_capacity, fuel_consumption, maximum_load):
    score_s = 0
    pitstop_score, liters_consumed = pitstop_count(engine_capacity, fuel_consumption)
    if pitstop_score >= 2:
        pass
    elif pitstop_score == 1:
        score_s += 1
    else:
        score_s += 2

    if liters_consumed <= 230:
        score_s += 2
    else:
        score_s += 1

    if maximum_load >= 20:
        score_s += 2
    else:
        pass

    return score_s


# MAIN PROGRAM
print("Input file name")
file_name = input()
if file_name[-5:] == ".xlsx":
    my_df = pd.read_excel(r'{}'.format(file_name), sheet_name='Vehicles', dtype=str)
    file_name2 = file_name[:-5]
    my_df.to_csv(f'{file_name2}.csv', index=None)
    shape = my_df.shape
    rows = shape[0]
    if rows == 1:
        print(f"1 line was added to {file_name2}.csv")
    else:
        print(f"{rows} lines were added to {file_name2}.csv")
    correct_file, cell_count = check_data(f"{file_name2}.csv")
    if cell_count == 1:
        print(f"1 cell was corrected in {file_name2}[CHECKED].csv")
    else:
        print(f"{cell_count} cells were corrected in {file_name2}[CHECKED].csv")
    file_name = file_name2
elif file_name.find("[CHECKED].csv") != (-1):
    file_name = remove_checked(file_name)
elif file_name.find(".s3db") != (-1):
    pass
else:
    correct_file, cell_count = check_data(file_name)
    if cell_count == 1:
        print(f"1 cell was corrected in {file_name[:-4]}[CHECKED].csv")
    else:
        print(f"{cell_count} cells were corrected in {file_name[:-4]}[CHECKED].csv")
    file_name = file_name[:-4]

if file_name.find(".s3db") != (-1):
    conn = sqlite3.connect(f'{file_name}')
    cursor = conn.cursor()
    rows = cursor.fetchall()
    for row in rows:
        cursor.execute(f"UPDATE convoy COLUMN score UPDATE {score_get(row[1, row[2], row[3]])}")
    conn.commit()
    conn.close()
else:
    conn = sqlite3.connect(f'{file_name}.s3db')
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS convoy (vehicle_id INT PRIMARY KEY,
                        engine_capacity INT NOT NULL,
                        fuel_consumption INT NOT NULL,
                        maximum_load INT NOT NULL,
                        score INT NOT NULL);""")
    with open(f"{file_name}[CHECKED].csv", "r") as c_file:
        no_records = 0
        f_reader = csv.reader(c_file, delimiter=",")
        for line in f_reader:
            start_execute = True
            for item in line:
                if not item.isdigit():
                    start_execute = False
                    break
            if start_execute:
                line = list(line)
                score = score_get(int(line[1]), int(line[2]), int(line[3]))
                line.append(score)

                cursor.execute(f"""INSERT or IGNORE INTO convoy VALUES {tuple(line)}""")
                no_records += 1
                conn.commit()
            else:
                pass

    if no_records == 1:
        print(f"1 record was inserted was inserted into {file_name}.s3db")
    else:
        print(f"{no_records} records were inserted into {file_name}.s3db")
    file_name = f"{file_name}.s3db"
    conn.close()

conn = sqlite3.connect(f'{file_name}')
cursor = conn.cursor()
rows = cursor.execute("select * from convoy")

vehicle_dict = {"convoy": []}
JSON_count = 0
vehicle_xml_string = "<convoy>"
XML_count = 0
temporary_string = ""
temporary_list = []

for row in rows:
    if row[4] > 3:

        temporary_dict = {"vehicle_id": row[0], "engine_capacity": row[1], "fuel_consumption": row[2],
                          "maximum_load": row[3]}
        temporary_list.append(temporary_dict)
        JSON_count += 1

    else:
        temporary_string += "<vehicle>"
        temporary_string += f"<vehicle_id>{row[0]}</vehicle_id>"
        temporary_string += f"<engine_capacity>{row[1]}</engine_capacity>"
        temporary_string += f"<fuel_consumption>{row[2]}</fuel_consumption>"
        temporary_string += f"<maximum_load>{row[3]}</maximum_load>"
        temporary_string += "</vehicle>"
        XML_count += 1
resulting_dict = vehicle_dict.update({"convoy": temporary_list})

with open(f"{file_name[:-5]}.json", "w") as j_file:
    json.dump(vehicle_dict, j_file)

if JSON_count == 1:
    print(f"1 vehicle was saved into {file_name[:-5]}.json")
else:
    print(f"{JSON_count} vehicles were saved into {file_name[:-5]}.json")

if XML_count == 1:
    print(f"1 vehicle was saved into {file_name[:-5]}.xml")
else:
    print(f"{XML_count} vehicles were saved into {file_name[:-5]}.xml")
vehicle_xml_string += temporary_string + "</convoy>"

root = etree.fromstring(vehicle_xml_string)
tree = etree.ElementTree(root)
tree.write(f"{file_name[:-5]}.xml", method='html')

conn.commit()
conn.close()
