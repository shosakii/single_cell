from sqlite_tool import SqliteTool

#db = SqliteTool("example.db")

def process_raw(db):
    table = 'csv_data'
    target_table = "target_table"
    new_column = 'tissue_cell_num'
    new_columns = 'cell_pct'
    db.add_column(table, new_column, "REAL")
    db.add_column(table, new_columns, "REAL")
    db.remove_duplicates('csv_data', ['tissue_id', 'cell_type_id'])
    db.update_sum_column("unique_csv_data", new_column, "tissue_id", "number_cells")
    db.update_column("unique_csv_data")
    db.update_table_pro("csv_data", "unique_csv_data")
    db.filter_and_insert("csv_data", "target_table", "number_cells", 50)
    db._cur.execute("CREATE INDEX idx_cell_name ON target_table (cell_name)")
    db._cur.execute("CREATE INDEX idx_symbol ON target_table (symbol)")
    db._cur.execute("CREATE INDEX idx_tissue_name ON target_table (tissue_name)")



if __name__ == '__main__':
   db = SqliteTool("example.db")
   process_raw(db)




