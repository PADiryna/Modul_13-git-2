import sqlalchemy as db
import csv
from sqlalchemy import create_engine, Table, Column, Integer, MetaData
# from sqlalchemy.orm import create_engine, Table, Column, Integer, MetaData


engine = db.create_engine('sqlite:///database_2.db')

connection = engine.connect()

metadata = db.MetaData()

stations = db.Table('stations', metadata,
    # db.Column('id', db.Integer, primary_key=True),
    db.Column('station', db.String), 
    db.Column('latitude', db.String), 
    db.Column('longitude', db.String), 
    db.Column('elevation', db.String), 
    db.Column('name', db.String), 
    db.Column('country', db.String),
    db.Column('state', db.String) 
)

metadata.create_all(engine)

insert_query = stations.insert()

with open('C:\Kodilla_kurs\Python\Modul_13-git_2\clean_stations.csv', 'r', encoding="utf-8") as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',')
    engine.execute(
        insert_query,
        [{"station": row[0], "latitude": row[1], "longitude": row[2], "elevation": row[3],
          "name": row[4], "country": row[5], "state": row[6]} 
            for row in csv_reader]
    )

measure = db.Table('measure', metadata,
    db.Column('station', db.String), 
    db.Column('date', db.String), 
    db.Column('precip', db.String), 
    db.Column('tobs', db.Integer)
)    

metadata.create_all(engine)

insert_query = measure.insert()

with open('C:\Kodilla_kurs\Python\Modul_13-git_2\clean_measure.csv', 'r', encoding="utf-8") as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',')
    engine.execute(
        insert_query,
        [{"station": row[0], "date": row[1], "precip": row[2], "tobs": row[3]} 
            for row in csv_reader]
    )  

connection.execute("SELECT * FROM stations LIMIT 5").fetchall()
results = engine.execute("SELECT * FROM stations LIMIT 5").fetchall()

for r in results:
   print(r)