import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

connect_string = 'mysql+pymysql://root:Asd1707!@localhost/django_db'

sql_engine = create_engine(connect_string)

duplicate = pd.read_sql(sql='''
    SELECT * FROM duplicate;
''', con=sql_engine)

random_duplicate = duplicate[:5000]

print(len(random_duplicate))
file_path = Path(
    '/home/nguyenhuyhoang/HoganNguyen/Python/test_prj/learn_python/_csv/random_duplicate.csv')
random_duplicate.to_csv(file_path)

group_id1 = random_duplicate.groupby(by='model_id1').agg(
    {'model_id2': list}).reset_index()
group_id2 = random_duplicate.groupby(by='model_id2').agg(
    {'model_id1': list}).reset_index()


id1_join = pd.merge(left=group_id1.set_index('model_id1'), right=group_id2.set_index(
    'model_id2'), how='outer', left_index=True, right_index=True)


print(group_id1.head())
print(group_id2.head())

# print(id1_join.iloc[5777])

id1_join['model_id1'].loc[id1_join['model_id1'].isnull(
)] = id1_join['model_id1'].loc[id1_join['model_id1'].isnull()].apply(lambda x: [])

id1_join['model_id2'].loc[id1_join['model_id2'].isnull(
)] = id1_join['model_id2'].loc[id1_join['model_id2'].isnull()].apply(lambda x: [])

id1_join['models'] = id1_join['model_id2'] + id1_join['model_id1']

models = pd.read_sql('''
SELECT * FROM models''', con=sql_engine)

for model_id_list in id1_join.itertuples():
    min_model = min(model_id_list[0], min(model_id_list[3]))
    models['id'] = models['id'].replace([model_id_list[0]], min_model)
file_path = Path(
    '/home/nguyenhuyhoang/HoganNguyen/Python/test_prj/learn_python/_csv/models_deduplicated.csv')
models.to_csv(
    file_path)
print(models.iloc[1293])


models.to_sql('models', if_exists= 'replace', con=sql_engine, index=False)