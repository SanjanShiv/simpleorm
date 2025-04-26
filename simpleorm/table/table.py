import pandas as pd

class Table:
    def __init__(self, name, connection):
        self.name = name
        self.connection = connection
        self.query = ""

    def create(self, **values):
        columns = ', '.join(values.keys())
        placeholders = ', '.join(['%s'] * len(values))
        sql = f"INSERT INTO {self.name} ({columns}) VALUES ({placeholders})"
        self.connection.execute(sql, tuple(values.values()))
        return self

    def get(self, id):
        sql = f"SELECT * FROM {self.name} WHERE id = %s"
        result = self.connection.execute(sql, (id,))
        return result.fetchone()

    def get_many(self):
        sql = f"SELECT * FROM {self.name}"
        result = self.connection.execute(sql)
        return result.fetchall()

    def where(self, **conditions):
        condition_strings = [f"{key} = %s" for key in conditions.keys()]
        condition_clause = ' AND '.join(condition_strings)
        self.query = f"SELECT * FROM {self.name} WHERE {condition_clause}"
        self.query_params = tuple(conditions.values())
        return self

    def execute(self):
        result = self.connection.execute(self.query, self.query_params)
        return result.fetchall()

    def to_df(self):
        result = self.get_many()
        columns = [desc[0] for desc in self.connection.description()]
        return pd.DataFrame(result, columns=columns)

    def update(self, id, **values):
        set_clause = ', '.join([f"{key} = %s" for key in values.keys()])
        sql = f"UPDATE {self.name} SET {set_clause} WHERE id = %s"
        self.connection.execute(sql, tuple(values.values()) + (id,))
        return self

    def delete(self, id):
        sql = f"DELETE FROM {self.name} WHERE id = %s"
        self.connection.execute(sql, (id,))
        return self

    def count(self):
        sql = f"SELECT COUNT(*) FROM {self.name}"
        result = self.connection.execute(sql)
        return result.fetchone()[0]

    def order_by(self, *columns):
        order_clause = ', '.join(columns)
        self.query += f" ORDER BY {order_clause}"
        return self

    def limit(self, count):
        self.query += f" LIMIT {count}"
        return self



