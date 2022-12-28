import pandas as pd
from sqlalchemy import create_engine
import pymysql
import random

pymysql.install_as_MySQLdb()

connect_string = 'mysql+pymysql://root:Asd1707!@localhost/django_db'

sql_engine = create_engine(connect_string)


names = ('Hoang', 'Nam', 'Huy', 'Ngoc', 'Vu')

genders = ('Male', 'Female')


dict = {'id': [],
        'name': [],
        'age': [],
        'gender': []}

for i in range(10000):
    dict['id'].append(i)
    dict['name'].append(random.choice(names))
    dict['age'].append(random.choice(range(1, 100)))
    dict['gender'].append(random.choice(genders))


df = pd.DataFrame(dict)


df.to_sql('models', sql_engine, if_exists='replace', index=False)

print(df.tail(5).to_string())
