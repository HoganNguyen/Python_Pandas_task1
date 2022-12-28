import pandas as pd
from sqlalchemy import create_engine
import random

connect_string = 'mysql+pymysql://root:Asd1707!@localhost/django_db'

sql_engine = create_engine(connect_string)

dict = {'model_id1': [],
        'model_id2': [],
        'distance': []}

for i in range(10000):
    dict['model_id1'].append(random.choice(range(1, 10000)))
    dict['model_id2'].append(random.choice(range(1, 10000)))
    dict['distance'].append(random.choice(range(0, 2)))

df = pd.DataFrame(dict)

# print(df.head(5).to_string())

df.to_sql(name='duplicate', con=sql_engine, if_exists='replace', index=False)
