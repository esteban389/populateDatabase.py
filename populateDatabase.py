from mysql.connector import connect, Error
from faker import Faker
import decimal
import random
import pandas as pd
import argparse
import sys
## CLI ARGUMENTS

parser = argparse.ArgumentParser()
parser.add_argument('-j', '--json', action="store_true",required=False, help='Export to json')
parser.add_argument('-ni', '--no-insert', action="store_true",required=False, help="Don't insert to database")
parser.add_argument('-n', '--number',type=int, required=False, help='Number of records per table')
args = parser.parse_args()
## CLI ARGUMENTS END

## DB CONNECTION SETUP
db_config = {
    "host":"localhost",
    "port":3308,
    "user":"root",
    "password":"euler123",
    "database":"euler_human_resource"
}

connection = connect(**db_config)

cursor = connection.cursor()
## END DB CONNECTION SETUP

## CONSTANTS
TABLE_NAME = 0
COLUMN_NAME = 0
COLUMN_TYPE = 1

## END CONSTANTS

def execute_query(query):
    '''
    Executes an SQL query using the provided query string and returns the result.

    :param query: str
        A string representing an SQL query to be executed.

    :return: list of tuples
        The result of executing the SQL query.

    :raises: mysql.connector.Error
        If an error occurs during query execution.

    This function takes an SQL query as input, executes it using the active database connection, and returns the result as a list of tuples. If an error occurs during the query execution, it raises a `mysql.connector.Error` exception.

    Example usage:
    >>> result = execute_query("SELECT * FROM employees")
    >>> print(result)
    [(1, 'John Doe', 'john@example.com'), (2, 'Jane Smith', 'jane@example.com')]
    '''
    cursor.execute(query)
    return cursor.fetchall()

def get_database_tables():
    return execute_query("SHOW TABLES")

def get_table_schema(table_name):
    return execute_query("DESCRIBE {}".format(table_name))
## GET DATABASE SCHEMA

db_tables_names=get_database_tables()
for table_name in db_tables_names:
    execute_query("".format())
db_schema={}
for table in db_tables_names:
    table_name = table[TABLE_NAME]
    table_columns= get_table_schema(table[TABLE_NAME])
    column_dict = {}

    for column in table_columns:
        column_name, data_type = column[COLUMN_NAME], column[COLUMN_TYPE]
        column_dict[column_name] = data_type

    db_schema[table_name] = column_dict

## DATABASE SCHEMA ENDS HERE

## POPULATE GOES HERE
### GENERATING THE DATA
MAX_ROWS = 50
if args.number:
    MAX_ROWS=args.number
fake = Faker()
data_type_generators = {
    "int": lambda: fake.random_int(min=1, max=1000),
    "varchar(255)": lambda: fake.text(max_nb_chars=20),
    "datetime(6)": lambda: fake.iso8601(),
    "decimal(19,2)": lambda: decimal.Decimal(random.randrange(1, 100) + random.random()),
    # Add more data types as needed
}
dummy_data={}
tables_with_foreign_keys= set()
for table,columns in db_schema.items():
    dummy_data[table]=[]
    id_counter = 0
    for row in range(MAX_ROWS):
        record={}
        id_counter+=1
        for column, data_type in columns.items():
            if "{}_id".format(table) == column:
                record[column]= id_counter
            elif "status"==column:
                record[column] = "ENABLED"
            elif "id" in column:
                record[column]= random.randrange(1,MAX_ROWS)
                tables_with_foreign_keys.add(table)
            elif "total_hours"==column:
                record[column] = fake.time()
            else:
                record[column]= data_type_generators[data_type]()
        dummy_data[table].append(record)

### GENERATING THE DATA ENDS HERE

### CLOSING CONNECTION TO DATABASE WITH MYSQL-CONNECTOR
connection.commit()
connection.close()
###

### EXPORT TO JSON
if args.json:
    import json
    from decimal import Decimal
    class DecimalEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, Decimal):
                return float(o)
            return super(DecimalEncoder, self).default(o)

    json_content = json.dumps(dummy_data, cls=DecimalEncoder,indent=2)
    try:
        with open('dummy_data.json','w') as file:
            file.write(json_content)
            print("dummy_data.json created successfully")
    except Exception as e:
        print(e)

### EXPORT TO JSON END

### STOP EXECUTING IF USER DOESN'T WANT TO INSERT TO DB
if args.no_insert:
    sys.exit()

### STOP EXECUTING IF USER DOESN'T WANT TO INSERT TO DB

### STARTING CONNECTION WITH DATABASE USING SQLALCHEMY
from sqlalchemy import create_engine, MetaData
engine = create_engine(f'mysql+mysqlconnector://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}')
populated_tables=set()
populated_tables.add("user")
populated_tables.add("transaction")
def insert_data_into_table(table_name):
    with engine.connect() as conn:
        try:
            df = pd.DataFrame(dummy_data[table_name])
            df.to_sql(table_name,con=conn,if_exists='append',index=False)
            populated_tables.add(table_name)
            print(table_name," has been populated")
        except Exception as e:
            print(e)

def get_referenced_tables(table_name):
    referenced_tables=set()
    for column in db_schema[table_name]:
        table_name_from_column=column[0:len(column)-3]
        if "id" not in column or table_name_from_column==table_name:
            continue
        referenced_tables.add(table_name_from_column)
    return referenced_tables


def recursive_insert(table_name):
    if table_name in populated_tables:
        return

    if table_name in tables_with_foreign_keys:
        referenced_tables = get_referenced_tables(table_name)

        for referenced_table in referenced_tables:
            if referenced_table not in populated_tables:
                recursive_insert(referenced_table)

    insert_data_into_table(table_name)

for table in dummy_data:
    if table in tables_with_foreign_keys:
        continue
    insert_data_into_table(table)

for table_name in tables_with_foreign_keys:
    recursive_insert(table_name)

populated_tables.remove("user")
populated_tables.remove("transaction")
print(populated_tables)



## POPULATE ENDS HERE

