import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import csv
import numpy as np
from datetime import date
from datetime import datetime
import os
from dotenv import load_dotenv


#connecting to database and creating tables

# Connect to postgres system DB
def db_conn():

    load_dotenv()

    conn1 = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        dbname="postgres"
    )    # Terminate other connections to target DB
    
    # Drop and recreate DB
    conn1.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with conn1.cursor() as cur:

        cur=conn1.cursor()
        cur.execute("DROP DATABASE IF EXISTS my_etl_db;")
        cur.execute("CREATE DATABASE my_etl_db;")
    
        conn1.close()

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
    return conn

 



#Loading to the sql tables
def load_person(csv_person,conn):
    cursor=conn.cursor()
    with open (csv_person,'r') as file:
        reader=csv.reader(file)
        next(reader)
        for row in reader:
            cursor.execute("INSERT INTO persons VALUES (%s, %s, %s, %s)", row)
        conn.commit()
        print (f"{(csv_person)} was loaded succesfully into the Data warehouse")
        cursor.close()
        



def load_medicals(csv_medicals,conn):
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


