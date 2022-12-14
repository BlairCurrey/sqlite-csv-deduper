import sqlite3
import os

class Database:
    table_name = 'Deduped'
    data_path = './'
    filename = 'dedupe.db'
    os.makedirs(data_path, exist_ok=True)

    def __init__(self, unique: list[str], keep: list[str]):
        if len(unique) == 0: # empty list
            raise Exception('Must provide a column name to dedupe on')

        self.con = sqlite3.connect(Database.data_path + Database.filename)
        self.cur = self.con.cursor()
        self.unique = unique
        # TODO: get columns from csv when reading and ALTER TABLE ADD COLUMN IF NOT EXIST?
        # or require user to provide files to select? Or have both options?
        self.columns = keep + unique
        self.up()

    def up(self):
        # make all columns TEXT type
        columns_sql = ', '.join([f'{column} TEXT' for column in self.columns])
        unique_sql = f"UNIQUE({', '.join([u for u in self.unique])})"
        create_sql = f'CREATE TABLE {Database.table_name} ({columns_sql}, {unique_sql})'

        # create table
        self.con.execute(f'DROP TABLE IF EXISTS {Database.table_name}')
        self.con.execute(create_sql)

        # ALTERNATIVE - add unique constraints seperately
        # self.con.execute(f"CREATE UNIQUE INDEX compound_unique_idx ON {Database.table_name}({','.join(self.unique)})")

    def insert(self, row: dict):
        insert_data = tuple(row[col] for col in self.columns)
        columns_sql = ', '.join(self.columns) # ex: "col1, col2"
        values_sql = ', '.join(['?'] * len(self.columns)) # ex: "?, ?"
        insert_sql = f'INSERT INTO {Database.table_name} ({columns_sql}) VALUES ({values_sql})'
        # ex: "INSERT INTO SomeTable (col1, col2) VALUES (?, ?)"
        self.cur.execute(insert_sql, insert_data)
        self.con.commit()

    def print_info(self):
        row_count = self.con.execute(f'SELECT COUNT(*) FROM {Database.table_name};').fetchone()[0]
        row_sample = self.con.execute(f'SELECT * FROM {Database.table_name} LIMIT 1').fetchone()
        print(f'Dedupe table row count: {row_count}')
        print(f'Dedupe table row sample:\n\t{row_sample}')