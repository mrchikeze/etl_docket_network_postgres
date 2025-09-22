import psycopg2
import pandas as pd
import csv
import numpy as np
from datetime import date
from datetime import datetime
import os
from dotenv import load_dotenv

logfile ="logfile.txt"
all_dim=[]


#This is extract function
def extract():
    url="FakeNamesUK1.csv"
    data=pd.read_csv(url, low_memory=False)
    print(f"{len(data)} will be extracted and loaded and transformed")
    return data



#This is Transform Function
def transform(datas):

    

    df=datas[['Number','Gender','Title','Name','Name.1', 'Address','ZipCode','EmailAddress','Username','Password',
                'CCType','CCNumber','CVV2','CCExpires','BloodType','Kilograms','Centimeters']]
    df = df.copy()
    df = df.rename({"Number":"ID", "Name.1":"LastName", "Name":"FirstName"}, axis="columns")

    #selecting valid records only
    df = df[pd.to_numeric(df["Kilograms"], errors="coerce").notna()]
    df = df[pd.to_numeric(df["Centimeters"], errors="coerce").notna()]

    #dropping empty ID and convertin to integer
    df = df.dropna(subset=["ID"])
    df["ID"] = df["ID"].astype(int)


    

    df.loc[:, "BMI"] = round(pd.to_numeric(df["Kilograms"], errors="coerce") /
                         (pd.to_numeric(df["Centimeters"], errors="coerce")**2), 2)



    df['BMI'] = round(pd.to_numeric(df['Kilograms'], errors='coerce') / 
                    pd.to_numeric(df['Centimeters'], errors='coerce') / 
                    pd.to_numeric(df['Centimeters'], errors='coerce') * 10000, 2)




    conditions = [
    df['BMI'].lt(18.5),
    df['BMI'].le(24.9),
    df['BMI'].le(29.9)
    ]

    choices = ['Underweight', 'Healthy', 'Overweight']


    df['BodyType'] = np.select(conditions, choices, default='Obese')

    #creating dimensions for analysis
    persons=df[['ID','FirstName','LastName','Gender']]

    medicals = df[['BloodType','Kilograms','Centimeters','BMI','BodyType','ID']]

    all_dim=[persons, medicals]

    print(f"{len(df)} records normalised into {len(all_dim)} tables and will be loaded into the data warehouse")

    return persons,medicals


#connecting to database and creating tables

# Connect to postgres system DB

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    dbname="postgres"
)

conn.autocommit = True
cur = conn.cursor()

# Terminate other connections to target DB
cur.execute("""
    SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
    WHERE datname = 'my_etl_db'
    AND pid <> pg_backend_pid();
""")

# Drop and recreate DB
cur.execute("DROP DATABASE IF EXISTS my_etl_db;")
cur.execute("CREATE DATABASE my_etl_db;")
cur.close()
conn.close()

# Reconnect to the new DB
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    dbname=os.getenv("DB_NAME")
)
cur = conn.cursor()

# Persons table
cur.execute("""
CREATE TABLE IF NOT EXISTS persons (
    PID INT PRIMARY KEY,
    FirstName VARCHAR(100),
    LastName VARCHAR(100),
    Gender VARCHAR(10)    
);
""")

# Medicals table
cur.execute("""
CREATE TABLE IF NOT EXISTS medicals (
    ID SERIAL PRIMARY KEY,
    BloodType VARCHAR(5),
    Kilograms DOUBLE PRECISION,  -- better than VARCHAR
    Centimeters DOUBLE PRECISION,
    BMI DOUBLE PRECISION,
    BodyType VARCHAR(50),
    PID INT,
    FOREIGN KEY(PID) REFERENCES persons(PID)
);
""")

conn.commit()


#Loading to the sql tables
def load_person(csv_person):
    cursor=conn.cursor()
    with open (csv_person,'r') as file:
        reader=csv.reader(file)
        next(reader)
        for row in reader:
            cursor.execute("INSERT INTO persons VALUES (%s, %s, %s, %s)", row)
        conn.commit()
        print (f"{(csv_person)} was loaded succesfully into the Data warehouse")
        cursor.close()
        



def load_medicals(csv_medicals):
    cursor = conn.cursor()
    with open(csv_medicals, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # skip header
        for row in reader:
            cursor.execute("INSERT INTO medicals (BloodType, Kilograms, Centimeters, BMI, BodyType, PID) VALUES (%s, %s, %s, %s, %s, %s)", row)
    conn.commit()
    cursor.close()
    print (f"{(csv_medicals)} was loaded succesfully into the Data warehouse")
    print ("Done....")
    conn.close()

#staging the data before loading to the database
def load(datasets, filenames):
    for df, filename in zip(datasets,filenames):
        df.to_csv(filename, index=False)
        
        print(f"{(filename)} table was staged ready to be loaded into the data warehouse")




#Log function to write to a text file the processes
def log(message):
 timestamp_format = '%Y-%h-%d-%H:%M:%S' #Year-Monthname-Day-Hour-Minute-Second
 now = datetime.now()
 timestamp = now.strftime(timestamp_format)
 with open("logfile.txt","a") as f:
     f.write(timestamp + ',' + message + '\n')




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
load_person(csv_person)

#csv_contacts="contacts.csv"
#load_contacts(csv_contacts)

csv_medicals="medicals.csv"
load_medicals(csv_medicals)

#csv_payments="payments.csv"
#load_payments(csv_payments)
log("Load phase Ended")

log("ETL Job Ended")