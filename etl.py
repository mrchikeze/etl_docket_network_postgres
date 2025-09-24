from extract import extract
from transform import transform
from load import load, load_medicals, load_person, db_conn
from log import log



def run_etl():


    #Start ETL
    log("ETL STARTED")

    log("Extract phase Started")
    datas=extract()
    log("Extract phase Ended")


    log("Transform phase Started")
    datasets = transform(datas)
    load(datasets,["persons.csv", "medicals.csv"])
    log("Transform phase Ended")


    log("Load phase Started")
    csv_person="persons.csv"
    conn=db_conn()
    load_person(csv_person,conn)

    #csv_contacts="contacts.csv"
    #load_contacts(csv_contacts)

    csv_medicals="medicals.csv"
    load_medicals(csv_medicals,conn)

    #csv_payments="payments.csv"
    #load_payments(csv_payments)
    log("Load phase Ended")

    log("ETL Job Ended")

if __name__ == "__main__":
    run_etl()