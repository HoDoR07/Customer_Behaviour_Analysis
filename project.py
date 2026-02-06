import pandas as pd
from sqlalchemy import create_engine

df = pd.read_csv("customer_shopping_behavior.csv")

# print(df.head())  ## Check top 5 data from first

# df.info() ## get info of data

# print(df.describe(include='all'))     ##CHECK NULL VALUE


## Filling Null Values
df['Review Rating'] = df.groupby('Category')['Review Rating'].transform(lambda x: x.fillna(x.median()))

# print(df.isnull().sum())

## Changing the All str into lower case 
df.columns = df.columns.str.lower()

## replaceng white spaces by _(underscore)
df.columns = df.columns.str.replace(' ','_')

## rename the column name
df = df.rename(columns={'purchase_amount_(usd)':'purchase_amount'})
# print(df.columns)

## Create a new column
labels = ['Young Adult', 'Adult', 'Middle Age', 'Senior']
df['age_group'] = pd.qcut(df['age'], q=4, labels=labels)
# print(df[['age','age_group']].head(10))


## create a column purchase_frequency_date
frequency_mapping = {
    'Fortnightly' : 14,
    'Weekly' : 7,
    'Monthly' : 10,
    'Quarterly' : 90,
    'Bi-Weekly' : 14,
    'Annually' : 365,
    'Every 3 Months' : 90
}

df['purchase_frequency_days'] = df['frequency_of_purchases'].map(frequency_mapping)
# print(df[['purchase_frequency_days','frequency_of_purchases']].head(10))


# print((df['discount_applied'] == df['promo_code_used']).all())

df = df.drop('promo_code_used', axis=1)
# print(df.columns)




## Database connection
# Step 1: PostgreSQL connection details
username = "postgres"          # default user
password = "Hodor18"          # your PostgreSQL password
host = "localhost"             # running locally
port = "5432"                  # default PostgreSQL port
database = "customer_behavior" # database name

# Create connection engine
engine = create_engine(
    f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
)

# Step 2: Load DataFrame into PostgreSQL
table_name = "customer"

df.to_sql(table_name, engine, if_exists="replace", index=False)

print(f"Data successfully loaded into table '{table_name}' in database '{database}'.")
