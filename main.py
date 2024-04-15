import pymongo
import re
import pyodbc

client = pymongo.MongoClient('mongodb://admin:PR2G1Vz6M7VO7ZEHKzL0JXKg4v8ryJ1b@192.168.51.14:27017/admin')

connection = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                        "Server=192.168.51.11;"
                        "Database=PaySystemAlpha;"
                        "uid=UserAlpha;pwd=ALSUtCF%utnWJk6Oa~RI?#q0v!")

buffer = dict()
def clean(eps):
    db = client[eps]
    accounts = db["Account"]
    account_count = accounts.count_documents({})
    accounts = accounts.find()
    print(f"Количество абонентов в платежной системе {eps}: {account_count}")

    for i, account in enumerate(accounts):
        account_id = account["_id"]["Account"]
        if account_id == "001":
            continue

        for service in account["DestParams"]:
            year = re.search("12=\\d{4}", service["Value"])

            if year is None or year.group(0) == "12=2024":
                continue
            service_id = service["Id"]

            log_with_service_name(service_id, account, eps, service)
            delete_services(service_id, account, db, eps)

        if len(account["DestParams"]) == 0:
            eps_id = account["_id"]["EPSId"]
            delete_account(db, account_id, eps_id)

        if i % 1000 == 0:
            print(f"Обработано {i} из {account_count}")


def delete_services(service_id, account, db, service_name):
    acc = account["_id"]["Account"]
    eps = service_name.replace("eps_", "")
    filter_query = {"_id.Account": acc, "_id.EPSId": int(eps)}
    update_query = {"$pull": {"DestParams": {"Id": int(service_id)}}}
    db.Account.update_one(filter_query, update_query)


def delete_account(db, account, eps):
    db.Account.delete_one({"_id.Account": account, "_id.EPSId": int(eps)})


def log_with_service_name(service_id, account, eps, service_params):
    get_service_name(service_id)

    service_name = buffer[service_id]
    acc = account["_id"]["Account"]
    params = service_params["Value"][:13]
    with open("E:\\DeletedRows.txt", "a") as file:
        line = f"Account: {acc} {eps} PD: {service_id} PDName: {service_name} {params} \n"
        file.write(line)


def get_service_name(service_id):
    service_name = buffer.get(service_id)
    if service_name is None:
        query = f"SELECT PaymentDestinationName from PaymentDestinations where PaymentDestinationID = {service_id}"
        service_name = str(connection.execute(query).fetchone()[0])
        buffer[service_id] = service_name


dbs = client.list_database_names()[2:]
for db_name in dbs:
    clean(db_name)
