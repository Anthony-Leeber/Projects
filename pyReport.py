'''
*******************************************************************************
* This script gets data from MariaDB and produces a report of what schemas are 
* using excessive data in terms of test results. The MariaDB hostname, 
* username, password, and database name are read from credentials.properties 
* file. The 'percent' can be changed to select the desired ratio of test result 
* data to total data. The results will be located in file named 
* 'Results_Size_Analysis.txt' and will be grouped by tower and sorted by their 
* percentage of test result data to total data.
*
*******************************************************************************
'''
#!/usr/bin/python
import configparser
config = configparser.RawConfigParser()
config.read('credentials.properties')

import mysql.connector
import datetime
from itertools import groupby
from decimal import getcontext, Decimal
print('mysql.connector imported.')
getcontext().prec = 2 # sets decimal precision to 2 places

#set up for file to write report to, with unique timestamp
ts = datetime.datetime.now()
timestamp = str(ts).split(".")
timestamp[0] = timestamp[0].replace(" ","_")
timestamp[0] = timestamp[0].replace(":","-")
filename = "Results_Size_Analysis_" + timestamp[0] + ".txt"

#DB Connection: information may need to be changed in credentials.properties
mydb = mysql.connector.connect(
    host = config.get('DatabaseSection', 'hostname'),
    user = config.get('DatabaseSection', 'user'),
    password = config.get('DatabaseSection', 'password'),
    database = config.get('DatabaseSection', 'database')
)

#opens/creates file to be read:
#file will be created in same folder as pyReport.py unless new path specified
File = open(filename,"w+")

# Query to select:
sql =   "Select * from \
        ( \
                SELECT 'Universe' as Table1, table_schema as DBName, \
                           ROUND(SUM(data_length + index_length) / 1024 / 1024, 1) 'Universe DB Size in MB' \
                       FROM information_schema.tables \
                       GROUP BY table_schema) U \
                left join     \
                (              \
                SELECT 'Results' as Table1, table_schema as DBNamex, \
                             ROUND(SUM(data_length + index_length) / 1024 / 1024, 1) \
                    'Results DB Size in MB' \
                    FROM information_schema.tables where table_name like '%result%' \
                    GROUP BY table_schema) R \
        on uCASE(U.DBName) = ucase(R.DBNamex) \
        left join \
        (SELECT 'Owner' as Table1, DatabaseName, Owner as Ownero, Status as Statuso, Comments, Tower \
         FROM ACOE_DEV.DatabaseManagement) d \
         on uCASE(U.DBName) = ucase(d.DatabaseName)"
         
#Executes query:         
mycursor = mydb.cursor()
mycursor.execute(sql)
myresult = mycursor.fetchall()

percent = .80 # critical threshold percentage of (result size) / (total size) to be considered "large"

File.write("The following Table Results occupy " + str(percent*100) + "% or more of total size:\n")

def printResult(r):
    File.write("\nTower: " + str(r[11]))
    File.write("\n\tDB: "+ str(r[1]))
    File.write("\n\t\tUniverse: " + str(r[0]))
    File.write("\n\t\tUniverse Size (MB): " + str(r[2]))
    File.write("\n\t\tResults: " + str(r[3]))
    File.write("\n\t\tResults Size (MB): " + str(r[5]))
    File.write("\n\t\tRatio: " + str(Decimal(r[5] / r[2])))
    File.write("\n\t\tOwner: " + str(r[8]))
def calculateRatio(r):
    return (r[5] / r[2])
def sortByTower(r):
    return str(r[11])
    
#creates the resultList of each row 
resultList = []
for r in myresult:
    #print(type(r))
    if str(type(r[5])) == "<class 'decimal.Decimal'>": #selects rows with measurable results size
        ratio = Decimal(r[5] / r[2])
        calcRatio = calculateRatio(r)
        if calcRatio > percent:
            #reports = Report(str(r[11]),str(r[1]),str(r[0]),str(r[2]),str(r[3]),str(r[5]),str(Decimal(r[5] / r[2])),str(r[8]))
            resultList.append(r)
            
resultList.sort(key= calculateRatio, reverse=True)

#sorts the resultList by Tower:
groups = []
uniquekeys = []
resultList = sorted(resultList, key= sortByTower)
for k, g in groupby(resultList, sortByTower):
    groups.append(list(g))
    uniquekeys.append(k)

#prints the sorted resultList:
for l in resultList:
    print(str(calculateRatio(l)))
    printResult(l)
print('program end')
File.close()
