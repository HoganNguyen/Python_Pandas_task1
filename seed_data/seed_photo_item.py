import pandas as pd
from sqlalchemy import create_engine
import random
import string

connect_string = 'mysql+pymysql://root:Asd1707!@localhost/django_db'

sql_engine = create_engine(connect_string)

urls = ['https://fb.com/huyhoangg1707', 'https://youtube.com/@PewDiePie',
        'https://pynative.com/python-generate-random-string/']

dict = {'id': [],
        'url': [],
        'price': [],
        'model_id': [],
        'description': [],
        }


for i in range(10000):
    dict['id'].append(i)
    dict['url'].append(random.choice(urls))
    dict['price'].append(random.choice(range(1, 1000)))
    dict['model_id'].append(random.choice(range(1, 10000)))
    dict['description'].append(
        ''.join(random.choice(string.ascii_letters) for i in range(10)))

df = pd.DataFrame(dict)


# query = ("""SELECT * FROM `django_db`.`photo_items`;
# """)

df.to_sql('photo_items', sql_engine, index=False, if_exists='replace')

# df = pd.read_sql(query,
# con=sql_engine)

print(df.tail(5).to_string())
