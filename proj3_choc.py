import sqlite3
import csv
import json
from prettytable import PrettyTable
# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'


def init_db(db_name):
    conn = sqlite3.connect('choc.db')
    cur = conn.cursor()

    # Drop tables
    statement = '''
        DROP TABLE IF EXISTS 'Bars';
    '''
    cur.execute(statement)
    conn.commit()

    statement = '''
        CREATE TABLE 'Bars' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Company' TEXT NOT NULL,
            'SpecificBeanBarName' TEXT NOT NULL,
            'REF' TEXT NOT NULL,
            'ReviewDate' TEXT NOT NULL,
            'CocoaPercent' REAL,
            'CompanyLocation' TEXT NOT NULL,
            'CompanyLocationId' INTEGER,
            'Rating' REAL,
            'BeanType' TEXT,
            'BroadBeanOrigin' TEXT,
            'BroadBeanOriginId' TEXT,
            FOREIGN KEY (CompanyLocationId) REFERENCES Countries(Id)
            FOREIGN KEY (BroadBeanOriginId) REFERENCES Countries(Id)
        );
    '''
    cur.execute(statement)
    conn.commit()

    statement = '''
        DROP TABLE IF EXISTS 'Countries';
    '''
    cur.execute(statement)

    conn.commit()
    dict_countries = {}
    statement = '''
        CREATE TABLE 'Countries' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Alpha2' TEXT,
            'Alpha3' TEXT,
            'EnglishName' TEXT,
            'Region' TEXT,
            'Subregion' TEXT,
            'Population' INTEGER,
            'Area' REAL
        );
    '''
    cur.execute(statement) #executing

    conn.commit()
    conn.close()


def insert_csv_data(csv_file):
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    for row in csv_file:
        newpercent = float(row[4][:-1])
        insertion = (row[0], row[1], row[2], row[3], newpercent, row[5], row[6], row[7], row[8])
        statement = 'INSERT INTO "Bars" '
        statement += 'VALUES (NULL, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, NULL) '
        cur.execute(statement, insertion)
    conn.commit()

def insert_json_file(json_file):
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    my_dict= {}
    id_ = 1
    for items in json_file:
        insertion = (items["alpha2Code"], items["alpha3Code"], items["name"], items["region"], items["subregion"], items["population"], items["area"])
        statement = 'INSERT INTO Countries '
        statement += 'VALUES (NULL,?, ?, ?, ?, ?, ?, ?)'
        my_dict[items["name"]] = id_
        id_ +=1
        # print(my_dict)
        cur.execute(statement, insertion)
    conn.commit()

    for keys in my_dict:
        try:
            cur.execute('UPDATE Bars SET CompanyLocationId =' + str(my_dict[keys]) + ' WHERE CompanyLocation =' + '"' + keys + '"')
            cur.execute('UPDATE Bars SET BroadBeanOriginId =' + str(my_dict[keys]) + ' WHERE BroadBeanOrigin =' + '"' + keys + '"')

        except:
            pass
    conn.commit()



init_db(DBNAME)

bars_csv= open(BARSCSV)
csvReader = csv.reader(bars_csv)
csv_list = list(csvReader)
del(csv_list[0])
insert_csv_data(csv_list)


countries_json = open(COUNTRIESJSON, "r")
json_file = countries_json.read()
json_diction = json.loads(json_file)
insert_json_file(json_diction)
#
#
# Part 2: Implement logic to process user commands
def bars_command(command):
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    statement1 = 'SELECT SpecificBeanBarName, Company, CompanyLocation, Rating, CocoaPercent, BroadBeanOrigin '
    statement2 = 'FROM BARS '
    join_statement1 = ''
    join_statement2 = ''
    sell_source = ''
    sell_region = ''
    source_region = ''
    rating_cocoa = 'ORDER BY Bars.Rating '
    top_bottom = 'DESC ' #-- changed this from desc
    limit = 'LIMIT 10'


    response = command.split()
    for words in response:
        if "sellcountry" in words:
            split = words.split("=")
            country_abbr = split[1].upper()
            join_statement1 = 'JOIN Countries as co ON Bars.CompanyLocationId = co.Id '
            sell_source = 'WHERE co.Alpha2 = "' + country_abbr + '" '
            # print(country_abbr)

        if "sourcecountry" in words:
            split = words.split("=")
            country_abbr = split[1].upper()
            join_statement1 = 'JOIN Countries as co ON Bars.CompanyLocationId = co.Id '
            join_statement2 = 'JOIN Countries as cn ON Bars.BroadBeanOriginId = cn.Id '
            sell_source = 'WHERE co.Alpha2 = "' + country_abbr + '" '


        if "sellregion" in words:
            split = words.split("=")
            sellregion_name = split[1]
            print(sellregion_name)
            join_statement1 = 'JOIN Countries as co ON Bars.CompanyLocationId = co.Id '
            join_statement2 = 'JOIN Countries as cn ON Bars.BroadBeanOriginId = cn.Id '
            sell_region = 'WHERE co.Region = "' + sellregion_name + '" '

        if "sourceregion" in words:
            split = words.split("=")
            sourceregion_name = split[1]
            print(sourceregion_name)
            join_statement1 = 'JOIN Countries as co ON Bars.CompanyLocationId = co.Id '
            source_region = 'WHERE co.Region = "' + sourceregion_name + '" '

        if "cocoa" in words:
            rating_cocoa = 'ORDER BY Bars.CocoaPercent '

        if "ratings" in words:
            rating_cocoa = 'ORDER BY Bars.Rating '

        if "top" in words:
            split = words.split("=")
            limit_no = split[1]
            top_bottom = 'DESC '
            limit = 'LIMIT "' + limit_no + '" '

        if "bottom" in words:
            split = words.split("=")
            limit_no = split[1]
            top_bottom = ''
            limit = 'LIMIT "' + limit_no + '" '

    cur.execute(statement1 + statement2 + join_statement1 + join_statement2 + sell_source + sell_region + source_region + rating_cocoa + top_bottom + limit)
    # print(statement1 + statement2 + join_statement1 + join_statement2 + sell_source + sell_region + source_region + rating_cocoa + top_bottom + limit)

    beautiful_table = PrettyTable()
    beautiful_table.field_names = ["SpecificBeanBarName", "Company", "CompanyLocation", "Rating", "CocoaPercent", "BroadBeanOrigin"]
    for row in cur:
        beautiful_table.add_row(row)
    print(beautiful_table)
    result = cur.execute(statement1 + statement2 + join_statement1 + join_statement2 + sell_source + sell_region + source_region + rating_cocoa + top_bottom + limit)
    return result.fetchall()

def companies_command(command):
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    statement1 = 'SELECT DISTINCT Company, CompanyLocation, ROUND(AVG(Bars.Rating), 1) '
    statement2 = 'FROM BARS '
    join_statement1 = ''
    join_statement2 = ''
    country = ''
    region = ''
    group_by = 'GROUP BY Company '
    having = 'HAVING COUNT (*) > 4 '
    rating_cocoa = 'ORDER BY AVG(Rating) '
    top_bottom = 'DESC '
    limit = 'LIMIT 10'

    response = command.split()
    for words in response:
        if "region" in words:
            split = words.split("=")
            region_name = split[1]
            join_statement1 = 'JOIN Countries as co ON Bars.CompanyLocationId = co.Id '
            region = 'WHERE co.Region = "' + region_name + '" '
            # print(country_abbr)


        if "country" in words:
            split = words.split("=")
            country_abbr = split[1].upper()
            join_statement1 = 'JOIN Countries as co ON Bars.CompanyLocationId = co.Id '
            country = 'WHERE co.Alpha2 = "' + country_abbr + '" '


        if "bars_sold" in words:
            statement1 = 'SELECT Company, CompanyLocation, COUNT(SpecificBeanBarName) '
            rating_cocoa = 'ORDER BY COUNT(SpecificBeanBarName)'

        if "cocoa" in words:
            rating_cocoa = 'ORDER BY AVG(Bars.CocoaPercent) '

        if "ratings" in words:
            rating_cocoa = 'ORDER BY AVG(Rating) '

        if "top" in words:
            split = words.split("=")
            limit_no = split[1]
            top_bottom = 'DESC '
            limit = 'LIMIT "' + limit_no + '" '

        if "bottom" in words:
            split = words.split("=")
            limit_no = split[1]
            top_bottom = ''
            limit = 'LIMIT "' + limit_no + '" '

    cur.execute(statement1 + statement2 + join_statement1 + join_statement2 + country + region + group_by + having + rating_cocoa + top_bottom + limit)


    beautiful_table = PrettyTable()
    beautiful_table.field_names = ["Company", "CompanyLocation", "Average_Ratings"]
    for row in cur:
        beautiful_table.add_row(row)
    print(beautiful_table)
    print(statement1 + statement2 + join_statement1 + join_statement2 + country + region + group_by + having + rating_cocoa + top_bottom + limit)
    result = cur.execute(statement1 + statement2 + join_statement1 + join_statement2 + country + region + group_by + having + rating_cocoa + top_bottom + limit)
    return result.fetchall()


def countries_command(command):
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    statement1 = 'SELECT Countries.EnglishName, Region, ROUND(AVG(ba.Rating), 1) '
    statement2 = 'FROM Countries '
    join_statement1 = 'JOIN Bars as ba ON Countries.Id = ba.CompanyLocationId '
    join_statement2 = ''
    region = ''
    group_by = 'GROUP BY EnglishName '
    having = 'HAVING COUNT (*) > 4 '
    rating_cocoa = 'ORDER BY AVG(Rating) '
    top_bottom = 'DESC '
    limit = 'LIMIT 10'

    response = command.split()
    for words in response:
        if "region" in words:
            split = words.split("=")
            region_name = split[1]
            region = 'WHERE Countries.Region = "' + region_name + '" '

        if "bars_sold" in words:
            statement1 = 'SELECT Countries.EnglishName, Region, COUNT(ba.SpecificBeanBarName) '
            rating_cocoa = 'ORDER BY COUNT(ba.SpecificBeanBarName)'

        if "sources" in words:
            # statement1 = 'SELECT Countries.EnglishName, Region, COUNT(SpecificBeanBarName)
            join_statement1 = 'JOIN Bars as ba ON Countries.Id = ba.BroadBeanOriginId ' #---- chenged this from location id blah, also changed from bn to ba

        if "sellers" in words:
            join_statement1 = 'JOIN Bars as ba ON Countries.Id = ba.CompanyLocationId '

        if "ratings" in words:
            rating_cocoa = 'ORDER BY AVG(Rating) '

        if "cocoa" in words:
            statement1 = 'SELECT Countries.EnglishName, Region, ROUND(AVG(ba.CocoaPercent), 1)'
            rating_cocoa = 'ORDER BY AVG(ba.CocoaPercent) '

        if "top" in words:
            split = words.split("=")
            limit_no = split[1]
            top_bottom = 'DESC '
            limit = 'LIMIT "' + limit_no + '" '

        if "bottom" in words:
            split = words.split("=")
            limit_no = split[1]
            top_bottom = ''
            limit = 'LIMIT "' + limit_no + '" '


    cur.execute(statement1 + statement2 + join_statement1 + join_statement2 + region + group_by + having + rating_cocoa + top_bottom + limit)

    beautiful_table = PrettyTable()
    beautiful_table.field_names = ["Countries", "Region", "Average_rating"]
    for row in cur:
        beautiful_table.add_row(row)

    print(beautiful_table)
    result = cur.execute(statement1 + statement2 + join_statement1 + join_statement2 + region + group_by + having + rating_cocoa + top_bottom + limit)
    return result.fetchall() #--- added Dima

def regions_command(command):
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    print("i'm in regions")
    statement1 = 'SELECT Region, ROUND(AVG(ba.Rating), 1) '
    statement2 = 'FROM Countries '
    join_statement1 = 'JOIN Bars as ba ON Countries.Id = ba.CompanyLocationId '
    join_statement2 = ''
    region = ''
    group_by = 'GROUP BY Region '
    having = 'HAVING COUNT (*) > 4 '
    rating_cocoa = 'ORDER BY AVG(Rating) '
    top_bottom = 'DESC '
    limit = 'LIMIT 10'


    response = command.split()
    print(response)
    for words in response:
        if "bars_sold" in words:
            statement1 = 'SELECT Countries.Region, COUNT(ba.SpecificBeanBarName) '
            rating_cocoa = 'ORDER BY COUNT(ba.SpecificBeanBarName)'

        if "sources" in words:
            # statement1 = 'SELECT Countries.EnglishName, Region, COUNT(SpecificBeanBarName)
            join_statement1 = 'JOIN Bars as ba ON Countries.Id = ba.BroadBeanOriginId ' #---- chenged this from location id blah, also changed from bn to ba

        if "sellers" in words:
            join_statement1 = 'JOIN Bars as ba ON Countries.Id = ba.CompanyLocationId '

        if "ratings" in words:
            rating_cocoa = 'ORDER BY AVG(Rating) '

        if "cocoa" in words:
            statement1 = 'SELECT Countries.Region, ROUND(AVG(ba.CocoaPercent), 1)'
            rating_cocoa = 'ORDER BY AVG(ba.CocoaPercent) '

        if "top" in words:
            split = words.split("=")
            limit_no = split[1]
            top_bottom = 'DESC '
            limit = 'LIMIT "' + limit_no + '" '

        if "bottom" in words:
            split = words.split("=")
            limit_no = split[1]
            top_bottom = ''
            limit = 'LIMIT "' + limit_no + '" '


    cur.execute(statement1 + statement2 + join_statement1 + join_statement2 + region + group_by + having + rating_cocoa + top_bottom + limit)
    beautiful_table = PrettyTable()
    beautiful_table.field_names = ["Region", "Avg-rating/Count_bars"]
    for row in cur:
        beautiful_table.add_row(row)
    print(beautiful_table)
    result = cur.execute(statement1 + statement2 + join_statement1 + join_statement2 + region + group_by + having + rating_cocoa + top_bottom + limit)
    return result.fetchall()

# bars_command("bars sellcountry=US cocoa bottom=5") #-- only working with alpha2code as US
# companies_command("companies ratings  top=8") #---- only working with US no other alph2 codes ae working
# countries_command("countries region=Asia ratings")
# regions_command("regions bars_sold")
#

def process_command(command):
    # commands_list = ["bars", "countries", "regions","companies"]
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    if command.split()[0] == "countries":
        return countries_command(command)

    if command.split()[0] == "bars":
        return bars_command(command)

    if command.split()[0] == "companies":
        return companies_command(command)

    if command.split()[0] == "regions":
        return regions_command(command)

    # if command.split()[0] not in commands_list:
    #     print("Bad command")
    #     break


def load_help_text():
    with open('help.txt') as f:
        return f.read()

commands_list = ["bars", "countries", "regions", "companies"]
commands_list2 = ["sellcountry", "sourcecountry", "sellregion", "sourceregion", "ratings", "top", "bottom", "country", "region", "cocoa", "bars_sold", "sellers"]

#Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')
        if response == "exit":
            break
        if response == 'help':
            print(help_text)
            continue
        if response == '':
            continue
        split_response = response.split()
        if split_response[0] not in commands_list:
            print("Command not found: " + response)
            continue

        if len(split_response) >= 2:
            bad_command = False
            for i in range(1, len(split_response)):
                if split_response[i] not in commands_list2:
                    print("Command not found: " + response)
                    bad_command = True
                    break
            if bad_command:
                continue
        process_command(response)

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    init_db(DBNAME)
    insert_csv_data(csv_list)
    insert_json_file(json_diction)
    interactive_prompt()
