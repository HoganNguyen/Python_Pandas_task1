import pandas as pd
from sqlalchemy import create_engine

connect_string = 'mysql+pymysql://root:Asd1707!@localhost/django_db'

sql_engine = create_engine(connect_string)

photo_items = pd.read_sql(sql='SELECT * FROM photo_items',
                          con=sql_engine, index_col='id')

models = pd.read_sql(sql='SELECT * FROM models',
                     con=sql_engine, index_col='id')

photo_items.to_csv(
    '/home/nguyenhuyhoang/HoganNguyen/Python/test_prj/learn_python/_csv/photo_item.csv')
print(photo_items.head(5).to_string())

print('############################################')

models.to_csv(
    '/home/nguyenhuyhoang/HoganNguyen/Python/test_prj/learn_python/_csv/models.csv')
print(models.head(5).to_string())


tables = [photo_items, models]

result = pd.merge(left=photo_items, right=models,
                  left_on='model_id', right_on='id')

result.to_csv(
    '/home/nguyenhuyhoang/HoganNguyen/Python/test_prj/learn_python/_csv/merge_photo_model.csv')
print(result.head(10).to_string())

print(photo_items.shape)

photo_items = photo_items.loc[photo_items['model_id'] != 550]

print(photo_items.shape)

print(photo_items.head(5).to_string())

result = pd.merge(left=photo_items, right=models,
                  left_on='model_id', right_on='id', how='right')
result.to_csv(
    '/home/nguyenhuyhoang/HoganNguyen/Python/test_prj/learn_python/_csv/merge_photo_model_right.csv')
print(result.head(5).to_string())

result = pd.merge(left=photo_items, right=models,
                  left_on='model_id', right_on='id', how='left')

result.to_csv(
    '/home/nguyenhuyhoang/HoganNguyen/Python/test_prj/learn_python/_csv/merge_photo_model_left.csv')
print(result.head(5).to_string())


print('\n\n####################Condition: age>20, gender=Male####################')
models_query = models.loc[(models['age'] > 20) & (models['gender'] == 'Male')]
print(models_query.head(10).to_string())
models_query.to_csv(
    '/home/nguyenhuyhoang/HoganNguyen/Python/test_prj/learn_python/_csv/models_on_condition.csv')

print('\n\n####################Sort by model_id####################')
photo_items = photo_items.sort_values(by='model_id')
print(photo_items.head(10).to_string())
photo_items.to_csv('/home/nguyenhuyhoang/HoganNguyen/Python/test_prj/learn_python/_csv/sort_by_model_id.csv')

print('\n\n####################Sort by gender####################')
models_query = models_query.sort_values(by='gender')
print(models_query.head(10).to_string())
models_query.to_csv(
    '/home/nguyenhuyhoang/HoganNguyen/Python/test_prj/learn_python/_csv/sort_by_gender.csv')

print('\n\n####################Sort by price####################')
photo_items = photo_items.sort_values(by='price')
print(photo_items.head(10).to_string())
photo_items.to_csv(
    '/home/nguyenhuyhoang/HoganNguyen/Python/test_prj/learn_python/_csv/sort_by_price.csv')

print('\n\n####################Update Gender to Female####################')
models.loc[models['gender'] == "Male", 'gender'] = 'Female'
print(models.head().to_string())
models.to_csv(
    '/home/nguyenhuyhoang/HoganNguyen/Python/test_prj/learn_python/_csv/update_gender.csv')

print('\n\n####################Apply lambda price*2####################')
photo_items['price'] = photo_items.apply(lambda x: x['price'] * 2, axis=1)
print(photo_items.head(10).to_string())
photo_items.to_csv(
    '/home/nguyenhuyhoang/HoganNguyen/Python/test_prj/learn_python/_csv/lambda_pricex2.csv')

print('\n\n####################Group by Model####################')
group_by_model = photo_items.groupby(by='model_id')
print(group_by_model.head(1))

print('\n\n####################Group by Model first####################')
group_by_model = photo_items.groupby(by='model_id').head(5)
print(group_by_model.sort_values(by='model_id').head(10))
group_by_model.to_csv(
    '/home/nguyenhuyhoang/HoganNguyen/Python/test_prj/learn_python/_csv/group_by_model_first.csv')

print('\n\n####################Group by Model/Average Price####################')
group_by_model = photo_items.groupby(by='model_id').agg(
    {"price": 'median'})
print(group_by_model)
group_by_model.to_csv(
    '/home/nguyenhuyhoang/HoganNguyen/Python/test_prj/learn_python/_csv/group_by_model_avgprice.csv')

print('\n\n####################Group by Model/Max Price####################')
group_by_model = photo_items.groupby(by='model_id').agg(
    {"price": 'max'})
print(group_by_model)
group_by_model.to_csv(
    '/home/nguyenhuyhoang/HoganNguyen/Python/test_prj/learn_python/_csv/group_by_model_max.csv')

print('\n\n####################Group by Model/photo_id####################')
group_by_model = photo_items.reset_index().groupby(by='model_id').agg(
    {"id": list})

group_by_model.to_csv(
    '/home/nguyenhuyhoang/HoganNguyen/Python/test_prj/learn_python/_csv/group_by_model_photoid.csv')
max_photo_id_len = group_by_model['id'].apply(len).max()

dict = {'model_id': [],
}

for i in range(max_photo_id_len):
    dict[f'column{i + 1}'] = []

for i in group_by_model.reset_index()['model_id']:
    dict['model_id'].append(i)

for id_list in group_by_model['id']:
    id_list += [''] * (5 - len(id_list))
    for i in range(max_photo_id_len):
        dict[f'column{i + 1}'].append(id_list[i])
    
new_df = pd.DataFrame(dict)

print(type(group_by_model['id'].apply(pd.Series)))

print(new_df)

new_df.to_csv(
    '/home/nguyenhuyhoang/HoganNguyen/Python/test_prj/learn_python/_csv/split_photoid.csv')
