from faker import Faker
import decimal
import random
import pandas as pd
import argparse
import sys
from sqlalchemy import create_engine, MetaData
import json
from decimal import Decimal

# TODO  use column.type.python_type == python_types/class to decide the fake data
# TODO add functionality to add conditions to add specific data into specific columns 

class DataGenerator:
    def __init__(self, db_config):
        self.engine = create_engine(f'mysql+mysqlconnector://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}')
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
        self.fake = Faker()
        self.MAX_ROWS_PER_TABLE = args.number if args.number else 10

    data_type_generators = {
        "int": lambda: fake.random_int(min=1, max=1000),
        "VARCHAR(255)": lambda: fake.text(max_nb_chars=20),
        "datetime(6)": lambda: fake.iso8601(),
        "decimal(19,2)": lambda: decimal.Decimal(random.randrange(1, 100) + random.random()),}

    def type_test(self, data_type):
        if data_type.python_type == int:
            return lambda: fake.random_int(min=1,max=1000)
        elif data_type.python_type == str:
            return lambda: fake.text(max_nb_chars=20)
        else:
            print("Not supported")
            #raise TypeError("Data type not supported")

    def generate_data(self):
        dict_to_json = {}
        tables_with_foreign_keys = set()

        for table in self.metadata.sorted_tables:
            table_name = table.name
            dict_to_json[table_name] = []
            if len(table.foreign_keys) > 0:
                tables_with_foreign_keys.add(table_name)
            i = 0
            for row in range(self.MAX_ROWS_PER_TABLE):
                record = {}
                i += 1
                for column in table.c:
                    data_type = column.type
                    if table.primary_key.contains_column(column):
                        value = i
                        record[column.name] = value
                        continue
                    if str(column.name) == "status":
                        value = "ENABLED"
                        record[column.name] = value
                        continue
                    print(data_type.python_type, " - ",self.type_test(data_type))
                    if data_type in self.data_type_generators.keys():
                        value = self.data_type_generators[data_type]()
                        record[column.name] = value
                dict_to_json[table_name].append(record)
        return dict_to_json

 
class JSONExporter:
    @staticmethod
    def export_to_json(data, filename='dummy_data.json'):
        class DecimalEncoder(json.JSONEncoder):
            def default(self, o):
                if isinstance(o, Decimal):
                    return float(o)
                return super(DecimalEncoder, self).default(o)

        json_content = json.dumps(data, cls=DecimalEncoder, indent=2)
        try:
            with open(filename, 'w') as file:
                file.write(json_content)
                print(f"{filename} created successfully")
        except Exception as e:
            print(e)

class DatabasePopulator:
    def __init__(self, engine, metadata):
        self.engine = engine
        self.metadata = metadata
        self.populated_tables = set()
        self.tables_with_foreign_keys = set()
        for table in self.metadata.sorted_tables:
            if len(table.foreign_keys) > 0:
                self.tables_with_foreign_keys.add(table.name)

    def insert_data_into_table(self, table_name, data):
        with self.engine.connect() as conn:
            try:
                df = pd.DataFrame(data[table_name])
                df.to_sql(table_name, con=conn, if_exists='append', index=False)
                self.populated_tables.add(table_name)
                print(table_name, " has been populated")
            except Exception as e:
                print(e)

    def get_referenced_tables(self, table_name):
        referenced_tables = set()
        table = self.metadata.tables[table_name]
        for foreign_key in table.foreign_keys:
            referenced_tables.add(foreign_key.column.table.name)
        return referenced_tables

    def recursive_insert(self, table_name, data):
        if table_name in self.populated_tables:
            return

        if table_name in self.tables_with_foreign_keys:
            referenced_tables = self.get_referenced_tables(table_name)
            for referenced_table in referenced_tables:
                if referenced_table not in self.populated_tables:
                    self.recursive_insert(referenced_table, data)

        self.insert_data_into_table(table_name, data)

    def populate_database(self, data):
        for table in data:
            if table in self.tables_with_foreign_keys:
                continue
            self.insert_data_into_table(table, data)

        for table_name in self.tables_with_foreign_keys:
            self.recursive_insert(table_name, data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-j', '--json', action="store_true", required=False, help='Export to json')
    parser.add_argument('-ni', '--no-insert', action="store_true", required=False, help="Don't insert to database")
    parser.add_argument('-n', '--number', type=int, required=False, help='Number of records per table')
    args = parser.parse_args()

    if args.no_insert and not args.json:
        print("You need to choose to either insert data or export to json")
        sys.exit()

    db_config = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "",
        "database": "javaweb"
    }
    try:
        data_generator = DataGenerator(db_config)
    except Exception as e:
        print("Couldn't establish a connection with the database")
        sys.exit()
    data = data_generator.generate_data()

    if args.json:
        JSONExporter.export_to_json(data)

    if args.no_insert:
        sys.exit()

    db_populator = DatabasePopulator(data_generator.engine, data_generator.metadata)
    db_populator.populate_database(data)
    print("Populated tables:\n", db_populator.populated_tables)

