import pandas as pd
import csv
import numpy as np



all_dim=[]


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