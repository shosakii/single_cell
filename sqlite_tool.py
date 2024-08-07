import sys
import os
import sqlite3
import pandas as pd

class SqliteTool():
    def __init__(self, dbName="sqlite3Test.db"):
        self.dbName = dbName
        self._conn = sqlite3.connect(dbName)
        self._cur = self._conn.cursor()
    

    def print_row_by_ids(self, tissue_id, cell_type_id, gene_id):
        query = '''
        SELECT *
        FROM target_table
        WHERE tissue_id = ? AND cell_type_id = ? AND gene_id = ?
        '''
        self._cur.execute(query, (tissue_id, cell_type_id, gene_id))
        rows = self._cur.fetchall()
        
        return rows
    
    
    def drop_table_if_exists(self, table_name):
        self._cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
        result = self._cur.fetchone()
        if result:

            self._cur.execute(f"DROP TABLE {table_name};")
            self._conn.commit()
            print(f"Table '{table_name}' dropped successfully.")
        else:
            print(f"Table '{table_name}' does not exist.")

    def get_table_dimensions(self, table_name):

        self._cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        num_rows = self._cur.fetchone()[0]  #基因个数
        #self._cur.execute(f"SELECT * FROM {table_name} LIMIT 1")
        #first_row = self._cur.fetchone()
        self._cur.execute(f"PRAGMA table_info({table_name})")
        num_cols = len(self._cur.fetchall())-1   

        result = [num_rows, num_cols]

        return result

    def get_index_column_values(self, table_name, index_column):
        query = f"SELECT {index_column} FROM {table_name}"
        self._cur.execute(query)
        values = self._cur.fetchall()
        values_list = [value[0] for value in values]
        return values_list

    def get_table_column_names(self, table_name):
        query = f"PRAGMA table_info({table_name})"
        self._cur.execute(query)
        columns = self._cur.fetchall()
        column_names = [column[1] for column in columns]
        return column_names

    def get_unique_column_values(self, table_name, column_name):
        query = f"SELECT DISTINCT {column_name} FROM {table_name}"
        self._cur.execute(query)
        result = self._cur.fetchall()
        unique_values = [row[0] for row in result]
        return unique_values

    def reorder_table_by_column(self, table_name, column_name):
        query = f"SELECT * FROM {table_name} ORDER BY {column_name}"
        self._cur.execute(query)
        sorted_rows = self._cur.fetchall()

        self._cur.execute(f"DELETE FROM {table_name}")

        for row in sorted_rows:
            self._cur.execute(f"INSERT INTO {table_name} VALUES ({','.join(['?']*len(row))})", row)

        self._conn.commit()


    def fetch_row_as_list(self, table_name, index_column, index_value):

        query = f"SELECT * FROM {table_name} WHERE {index_column} = ?"
        self._cur.execute(query, (index_value,))
        result = self._cur.fetchone()

        result_list = list(result) if result else None

        return result_list
    def add_column(self, table_name, column_name, column_type):

        sql = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
        try:
            self._cur.execute(sql)
            self._conn.commit()
            print(f"Column '{column_name}' added to table '{table_name}' successfully.")
        except sqlite3.Error as e:
            print(f"Failed to add column to table: {e}")
            self._conn.rollback()

    def remove_duplicates(self, table_name, unique_columns):
        # 构建用于去重的列名字符串
        column_names_str = ', '.join(unique_columns)

        # 创建一个新的表来存储去重后的记录
        new_table_name = f"unique_{table_name}"

        # 检查新表是否已经存在，如果存在则删除它
        self._cur.execute(f"DROP TABLE IF EXISTS {new_table_name}")

        # 创建新的表来存储去重后的记录
        self._cur.execute(f"""
            CREATE TABLE {new_table_name} AS
            SELECT * FROM {table_name} WHERE (rowid) IN (
                SELECT MIN(rowid) as min_rowid
                FROM {table_name}
                GROUP BY {column_names_str}
            )
        """)

        # 提交更改
        self._conn.commit()

        return new_table_name

    def update_sum_column(self, table_name, new_column_name, group_by_column, sum_column):
        """
        更新表中的现有列，使其值为基于 group_by_column 的 sum_column 的总和。

        :param table_name: 表名
        :param new_column_name: 要更新的列的名称
        :param group_by_column: 分组的列名
        :param sum_column: 需要求和的列名
        """
        update_sql = f"""
        UPDATE {table_name}
        SET {new_column_name} = (
            SELECT SUM({sum_column})
            FROM {table_name} AS sub
            WHERE sub.{group_by_column} = {table_name}.{group_by_column}
        )
        """
        try:
            self._cur.execute(update_sql)
            self._conn.commit()
            print(
                f"Column '{new_column_name}' has been updated with the sum of '{sum_column}' for each '{group_by_column}'.")
        except sqlite3.Error as e:
            print(f"An error occurred: {e}")
            self._conn.rollback()

    def delete_records(self, source_table, column_name, threshold):
        """
        删除 source_table 中 column_name 小于等于 threshold 的记录。

        :param source_table: 源表名
        :param column_name: 列名
        :param threshold: 阈值
        """
        delete_sql = f"""
        DELETE FROM {source_table}
        WHERE {column_name} <= ?
        """
        self._cur.execute(delete_sql, (threshold,))
        self._conn.commit()
        print(
            f"Deleted records from '{source_table}' where '{column_name}' is less than or equal to {threshold}.")


    def filter_and_insert(self, source_table, target_table, column_name, threshold):
        """
        筛选出 source_table 中 column_name 大于 threshold 的记录，
        并将这些记录插入到 target_table 新表中。

        :param source_table: 源表名
        :param target_table: 目标表名
        :param column_name: 列名
        :param threshold: 阈值
        """
        # 创建目标表，如果它不存在
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {target_table} AS
        SELECT * FROM {source_table}
        WHERE {column_name} > {threshold}
        ;
        """
        self._cur.execute(create_table_sql)
        self._conn.commit()
        print(
            f"Table '{target_table}' has been created/updated with records where '{column_name}' is greater than '{threshold}'.")

    def update_column(self, table_name):
        """
        更新指定表中的列值。

        :param table_name: 表名
        """

        update_sql = f"""
            UPDATE {table_name}
            SET cell_pct = (number_cells * 1.0 / tissue_cell_num)
            WHERE tissue_cell_num != 0;
            """
        # 执行更新操作
        try:
            self._cur.execute(update_sql)
            self._conn.commit()
            print("column updated!")
        except sqlite3.Error as e:
            print(f"Error executing SQL: {e}")
            self._conn.rollback()

    def update_table(self, table1_name, table2_name):
        """
        从 table2 更新 table1 中的 tissue_cell_num 和 cell_pct 列。
        """
        try:
            cur = self._conn.cursor()
            cur.execute(f"""
                UPDATE {table1_name}
                 SET tissue_cell_num = (SELECT tissue_cell_num FROM {table2_name} 
                                       WHERE {table2_name}.tissue_id = {table1_name}.tissue_id
                                       AND {table2_name}.cell_type_id = {table1_name}.cell_type_id),
                    cell_pct = (SELECT cell_pct FROM {table2_name} 
                                WHERE {table2_name}.tissue_id = {table1_name}.tissue_id
                                AND {table2_name}.cell_type_id = {table1_name}.cell_type_id)
                WHERE EXISTS (SELECT 1 FROM {table2_name} 
                              WHERE {table2_name}.tissue_id = {table1_name}.tissue_id
                              AND {table2_name}.cell_type_id = {table1_name}.cell_type_id)
            """)
            self._conn.commit()
            print("数据更新成功！")
        except sqlite3.Error as e:
            print(f"发生错误：{e}")
            self._conn.rollback()

    def extract_rows_below_threshold(self, source_table, target_table, column_name, threshold):
        """
        从源表中提取指定列中值小于给定阈值的所有数据行，并将这些数据行插入到目标表中。

        :param source_table: 源表名
        :param target_table: 目标表名
        :param column_name: 列名
        :param threshold: 阈值
        """
        # 创建目标表
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {target_table} AS
        SELECT * FROM {source_table}
        WHERE {column_name} < {threshold};
        """
        self._cur.execute(create_table_sql)
        self._conn.commit()
        print(
            f"Rows with '{column_name}' below '{threshold}' have been extracted to '{target_table}'.")

    def delete_rows_by_symbols(self, table_name, symbols):

        symbols_str = ', '.join([f"'{symbol}'" for symbol in symbols])

        # 删除符合符号列表的所有数据行
        delete_rows_sql = f"""
        DELETE FROM {table_name}
        WHERE symbol IN ({symbols_str});
        """
        self._cur.execute(delete_rows_sql)
        self._conn.commit()
        print(f"Rows with symbols {symbols} have been deleted from {table_name}.")

    def update_table_pro(self, table1_name, table2_name):
        try:
            cur = self._conn.cursor()
            cur.execute(f"""
                UPDATE {table1_name} AS t1
                SET tissue_cell_num = t2.tissue_cell_num,
                    cell_pct = t2.cell_pct
                FROM {table2_name} AS t2
                WHERE t1.tissue_id = t2.tissue_id
                    AND t1.cell_type_id = t2.cell_type_id
            """)
            self._conn.commit()
            print("数据更新成功！")
        except sqlite3.Error as e:
            print(f"发生错误：{e}")
            self._conn.rollback()

    def close_connection(self):
        self._cur.close()  # 先关闭游标
        self._conn.close()  # 再关闭连接

    def update_matched_index(self, cell_name, index):
        # 更新匹配索引值
        update_index_sql = f"""
        UPDATE target_table
        SET matched_index = {index}
        WHERE cell_name = '{cell_name}';
        """
        self._cur.execute(update_index_sql)
        self._conn.commit()

    def drop_column(self, table_name, column_name):
        # 构造删除列的 SQL 语句
        drop_column_sql = f"""
        ALTER TABLE {table_name}
        DROP COLUMN {column_name};
        """
        self._cur.execute(drop_column_sql)

    def create_index(self, index_name, table_name, column_name):
        """
        在数据库中创建索引。

        Parameters:
            db (SqliteTool): SqliteTool实例，用于执行SQL语句。
            index_name (str): 要创建的索引的名称。
            table_name (str): 要在其上创建索引的表的名称。
            column_name (str): 要创建索引的列的名称。

        Returns:
            None
        """
        create_index_sql = f"CREATE INDEX {index_name} ON {table_name} ({column_name});"
        self._cur.execute(create_index_sql)
        self._conn.commit()
        print(f"Index '{index_name}' has been created on column '{column_name}' of table '{table_name}'.")

    def insert_data(self, table_name, data):
        # Assuming data is a dictionary with column names as keys
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data.values()])
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        self._cur.execute(query, list(data.values()))
        self._conn.commit()

    def delete_data(self, tissue_id, cell_type_id, gene_id):
        self._cur.execute( 'DELETE FROM csv_data WHERE tissue_id = ? AND cell_type_id = ? AND gene_id = ?',
                (tissue_id, cell_type_id, gene_id))
        self._conn.commit()
        return f"Rows with Tissue ID {tissue_id}, Cell Type ID {cell_type_id}, and Gene ID {gene_id} have been deleted."



if __name__ == '__main__':
    db = SqliteTool('example.db')
    db.get_table_dimensions("pct")
    table_column_names = db.get_table_column_names("sub_actExpr")
    print(table_column_names[1:])
