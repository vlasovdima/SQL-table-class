# pip install mysql.connector pandas

import mysql.connector
import pandas as pd

class SQLTable:
    def __init__(self, db_config, table_name):
        # Подключение к базе данных
        self.connection = mysql.connector.connect(**db_config)
        self.cursor = self.connection.cursor(dictionary=True)
        self.table_name = table_name

    # 1. Сортировка конкретного столбца
    def get_column_sorted(self, column, asc=True):
        order = "ASC" if asc else "DESC"
        query = f"SELECT {column} FROM {self.table_name} ORDER BY {column} {order}"
        self.cursor.execute(query)
        return pd.DataFrame(self.cursor.fetchall())

    # 2. Вывод диапазона строк по ID
    def get_rows_by_id(self, start, end):
        query = f"SELECT * FROM {self.table_name} WHERE id BETWEEN %s AND %s"
        self.cursor.execute(query, (start, end))
        return pd.DataFrame(self.cursor.fetchall())

    # 3. Удаление диапазона строк по ID
    def delete_rows_by_id(self, start, end):
        query = f"DELETE FROM {self.table_name} WHERE id BETWEEN %s AND %s"
        self.cursor.execute(query, (start, end))
        self.connection.commit()

    # 4. Вывод структуры таблицы
    def get_structure(self):
        self.cursor.execute(f"DESCRIBE {self.table_name}")
        return pd.DataFrame(self.cursor.fetchall())

    # 5. Вывод строки по значению
    def find_by_value(self, column, value):
        query = f"SELECT * FROM {self.table_name} WHERE {column} = %s"
        self.cursor.execute(query, (value,))
        return pd.DataFrame(self.cursor.fetchall())

    # 6. Удаление таблицы
    def drop_table(self):
        self.cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
        self.connection.commit()

    # 7. Добавление и удаление столбца
    def add_column(self, name, col_type="VARCHAR(255)"):
        self.cursor.execute(f"ALTER TABLE {self.table_name} ADD COLUMN {name} {col_type}")
        self.connection.commit()

    def drop_column(self, name):
        self.cursor.execute(f"ALTER TABLE {self.table_name} DROP COLUMN {name}")
        self.connection.commit()

    # 8. Экспорт и импорт CSV
    def export_csv(self, path):
        self.cursor.execute(f"SELECT * FROM {self.table_name}")
        df = pd.DataFrame(self.cursor.fetchall())
        df.to_csv(path, index=False)

    def import_csv(self, path):
        df = pd.read_csv(path)
        cols = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        query = f"INSERT INTO {self.table_name} ({cols}) VALUES ({placeholders})"
        for row in df.values:
            self.cursor.execute(query, tuple(row))
        self.connection.commit()

    def __del__(self):
        if self.connection.is_connected():
            self.cursor.close()
            self.connection.close()
