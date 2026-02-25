#pip install mysql-connector-python pandas openpyxl matplotlib

import mysql.connector
import pandas as pd
import os
import datetime


class SQLTable:
    def __init__(self, db_config, table_name):
        self.db_config = db_config
        self.table_name = table_name
        self.connection = mysql.connector.connect(**db_config)
        self.cursor = self.connection.cursor(buffered=True)

    def _execute_query(self, query, params=None, commit=False):
        """Вспомогательный метод для выполнения запросов."""
        try:
            self.cursor.execute(query, params or ())
            if commit:
                self.connection.commit()

            if self.cursor.description:  # Если запрос возвращает данные
                rows = self.cursor.fetchall()
                column_names = [col[0] for col in self.cursor.description]
                return pd.DataFrame(rows, columns=column_names)
            return None
        except mysql.connector.Error as err:
            print(f"Ошибка SQL: {err}")
            return None

    # 1. Вывод конкретного столбца в порядке убывания или возрастания
    def fetch_column_ordered(self, column_name, ascending=True):
        direction = "ASC" if ascending else "DESC"
        query = f"SELECT `{column_name}` FROM {self.table_name} ORDER BY `{column_name}` {direction}"
        return self._execute_query(query)

    # 2. Вывод диапазона строк по айди
    def select_rows_by_id_range(self, start_id, end_id):
        query = f"SELECT * FROM {self.table_name} WHERE `id` BETWEEN %s AND %s"
        return self._execute_query(query, (start_id, end_id))

    # 3. Удаление диапазона строк по айди
    def delete_rows_by_id_range(self, start_id, end_id):
        query = f"DELETE FROM {self.table_name} WHERE `id` BETWEEN %s AND %s"
        self._execute_query(query, (start_id, end_id), commit=True)
        print(f"Строки с {start_id} по {end_id} удалены.")

    # 4. Вывод структуры таблицы
    def print_table_structure(self):
        print(f"\nСтруктура таблицы '{self.table_name}':")
        df = self._execute_query(f"DESCRIBE {self.table_name}")
        if df is not None:
            print(df[['Field', 'Type', 'Null', 'Key']])

    # 5. Вывод строки, содержащей конкретное значение в конкретном столбце
    def select_row_by_value(self, column_name, value):
        query = f"SELECT * FROM {self.table_name} WHERE `{column_name}` = %s"
        return self._execute_query(query, (value,))

    # 6. Удаление таблицы
    def drop_table(self):
        confirm = input(f"Вы уверены, что хотите удалить таблицу {self.table_name}? (y/n): ")
        if confirm.lower() == 'y':
            self._execute_query(f"DROP TABLE IF EXISTS {self.table_name}", commit=True)
            print(f"Таблица {self.table_name} стерта с лица земли.")

    # 7. Добавление и удаление нового столбца
    def add_column(self, column_name, data_type="VARCHAR(255)"):
        query = f"ALTER TABLE {self.table_name} ADD COLUMN `{column_name}` {data_type}"
        self._execute_query(query, commit=True)
        print(f"Столбец {column_name} добавлен.")

    def delete_column(self, column_name):
        query = f"ALTER TABLE {self.table_name} DROP COLUMN `{column_name}`"
        self._execute_query(query, commit=True)
        print(f"Столбец {column_name} удален.")

    # 8. Экспорт и импорт таблицы в CSV
    def export_to_csv(self, file_path=None):
        df = self._execute_query(f"SELECT * FROM {self.table_name}")
        if df is not None:
            if not file_path:
                timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                file_path = f"{self.table_name}_{timestamp}.csv"
            df.to_csv(file_path, index=False)
            print(f"Данные успешно экспортированы в {file_path}")

    def import_from_csv(self, file_path):
        df = pd.read_csv(file_path)
        # Формируем SQL для вставки
        cols = ", ".join([f"`{c}`" for c in df.columns])
        placeholders = ", ".join(["%s"] * len(df.columns))
        query = f"INSERT INTO {self.table_name} ({cols}) VALUES ({placeholders})"

        for row in df.values:
            self.cursor.execute(query, tuple(row))
        self.connection.commit()
        print(f"Импортировано {len(df)} строк из {file_path}.")

    def __del__(self):
        if hasattr(self, 'connection') and self.connection.is_connected():
            self.cursor.close()
            self.connection.close()

