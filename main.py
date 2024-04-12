import pymongo
import re
import pyodbc

client = pymongo.MongoClient('mongodb://admin:PR2G1Vz6M7VO7ZEHKzL0JXKg4v8ryJ1b@192.168.51.14:27017/admin')
year = 12
month = 11

connection = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                        "Server=192.168.51.11;"
                        "Database=PaySystemAlpha;"
                        "uid=UserAlpha;pwd=ALSUtCF%utnWJk6Oa~RI?#q0v!")

buffer = dict()
def clean(dbName):
    db = client[dbName]
    accounts = db["Account"]
    accountCount = accounts.count_documents({})
    accounts = accounts.find()
    print("Количество абонентов в платежной системе " + dbName + " " + str(accountCount))

    i = 0
    for account in accounts:
        i += 1
        if (i % 1000 == 0):
            print("Обработано " + str(i) + " из " + str(accountCount))

        for dp in account["DestParams"]:
            year = re.search("12=\\d{4}", dp["Value"])

            if(year != None):
                year = year.group(0)
            else:
                continue

            if year != "12=2024" and account["_id"]["Account"] != "001":
                DPId = dp["Id"]

                LoggingWithPDName(DPId, account, dbName, dp)
                Deletion(DPId, account, db, dbName)


def Deletion(DPId, account, db, dbName):
    acc = account["_id"]["Account"]
    eps = dbName.replace("eps_", "")
    filterQuery = {"_id.Account": acc, "_id.EPSId": int(eps)}
    updateQuery = {"$pull": {"DestParams": {"Id": int(DPId)}}}
    db.Account.update_one(filterQuery, updateQuery)


def LoggingWithPDName(DPId, account, dbName, dp):
    if buffer.get(DPId) == None:
        query = f"SELECT PaymentDestinationName from PaymentDestinations where PaymentDestinationID = {DPId}"
        pdName = str(connection.execute(query).fetchone()[0])
        buffer[DPId] = pdName
    pdName = buffer[DPId]
    acc = account["_id"]["Account"]
    params = dp["Value"][:13]
    with open("E:\\DeletedRows.txt", "a") as file:
        input = "Account: " + acc + " " + dbName + " PD: " + str(DPId) + " " + "PDName: " + pdName + " " + params + "\n"
        file.write(input)


dbs = client.list_database_names()[2:]
for dbName in dbs:
    clean(dbName)