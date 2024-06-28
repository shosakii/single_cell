import gradio as gr
import tempfile
import shutil
import os
from sqlite_tool import SqliteTool
import os
import glob
import csv

def upload_csv_to_db(file_path):
    print(file_path)
    db_path = "/usr/src/app/example.db"
    db = SqliteTool(db_path)
    # with open(file_path, 'r', encoding='utf-8') as file:
    #     csv_reader = csv.reader(file, delimiter=',')
    #     next(csv_reader)  # 跳过标题行

    #     for row in csv_reader:

    #         tissue_id = row[1].strip() or 'blank'
    #         cell_type_id = row[2].strip() or 'blank'
    #         gene_id = row[3].strip() or 'blank'
    #         number_nonzero_expression_cells = int(row[4].strip() if row[4].strip().isdigit() else 0)
    #         expression_sum = float(row[5].strip().replace(',', '.')) if row[5].strip() else 0.0
    #         number_cells = int(row[6].strip() if row[6].strip().isdigit() else 0)
    #         symbol = row[7].strip() or 'blank'
    #         cell_name = row[8].strip() or 'blank'
    #         tissue_name = row[9].strip() or 'blank'
    #         expression_sum_QC = float(row[10].strip().replace(',', '.')) if row[10].strip() else 0.0
    #         expr_pct = float(row[11].strip().replace(',', '.')) if row[11].strip() else 0.0
    #         active_expr_mean = float(row[12].strip().replace(',', '.')) if row[12].strip() else 0.0
    #         expr_mean = float(row[13].strip().replace(',', '.')) if row[13].strip() else 0.0
    #         organism = row[14].strip() or 'blank'
    #         disease = row[15].strip() or 'blank'

    #         db._cur.execute(f'''INSERT INTO target_table (tissue_id, cell_type_id, gene_id, number_nonzero_expression_cells, 
    #                                     expression_sum, number_cells, symbol, cell_name, tissue_name, 
    #                                     expression_sum_QC, expr_pct, active_expr_mean, expr_mean, organism, disease)
    #                                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
    #                         (tissue_id, cell_type_id, gene_id, number_nonzero_expression_cells, expression_sum,
    #                          number_cells, symbol, cell_name, tissue_name, expression_sum_QC, expr_pct,
    #                          active_expr_mean, expr_mean, organism, disease))

    # db._conn.commit()
    # db.close_connection()
    with open(file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file, delimiter=',')

        for row in csv_reader:
            tissue_id = row['tissue_id'].strip() or 'blank'
            cell_type_id = row['cell_type_id'].strip() or 'blank'
            gene_id = row['gene_id'].strip() or 'blank'
            number_nonzero_expression_cells = int(row['number_nonzero_expression_cells'].strip() if row['number_nonzero_expression_cells'].strip().isdigit() else 0)
            expression_sum = float(row['expression_sum'].strip().replace(',', '.')) if row[
                'expression_sum'].strip() else 0.0
            number_cells = int(row['number_cells'].strip() if row['number_cells'].strip().isdigit() else 0)
            symbol = row['symbol'].strip() or 'blank'
            cell_name = row['cell_name'].strip() or 'blank'
            tissue_name = row['tissue_name'].strip() or 'blank'
            expression_sum_QC = float(row['expression_sum_QC'].strip().replace(',', '.')) if row[
                'expression_sum_QC'].strip() else 0.0
            expr_pct = float(row['expr_pct'].strip().replace(',', '.')) if row['expr_pct'].strip() else 0.0
            active_expr_mean = float(row['active_expr_mean'].strip().replace(',', '.')) if row[
                'active_expr_mean'].strip() else 0.0
            expr_mean = float(row['expr_mean'].strip().replace(',', '.')) if row['expr_mean'].strip() else 0.0
            organism = row['organism'].strip() or 'blank'
            disease = row['disease'].strip() or 'blank'

            db._cur.execute('''INSERT INTO target_table (tissue_id, cell_type_id, gene_id, number_nonzero_expression_cells,
                                         expression_sum, number_cells, symbol, cell_name, tissue_name,
                                         expression_sum_QC, expr_pct, active_expr_mean, expr_mean, organism, disease)
                                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                           (tissue_id, cell_type_id, gene_id, number_nonzero_expression_cells, expression_sum,
                            number_cells, symbol, cell_name, tissue_name, expression_sum_QC, expr_pct, active_expr_mean,
                            expr_mean, organism, disease))

    db._conn.commit()
    db.close_connection()

def generate_file(file_obj):
    global tmpdir
    print('临时文件夹地址：{}'.format(tmpdir))
    print('上传文件的地址：{}'.format(file_obj.name))
    shutil.copy(file_obj.name, tmpdir)

    file_name = os.path.basename(file_obj.name)

    uploaded_file_path = os.path.join(tmpdir, file_name)
    print(uploaded_file_path)
    upload_csv_to_db(uploaded_file_path)

    return "文件已上传并处理完成"

def main():
    global tmpdir
    with tempfile.TemporaryDirectory(dir='.') as tmpdir:
        inputs = gr.File(label="上传CSV文件")
        outputs = gr.Textbox(label="处理结果")

        app = gr.Interface(fn=generate_file, inputs=inputs, outputs=outputs,
                           title="上传CSV文件并处理到SQLite数据库",
                           description="上传CSV文件，将其内容处理并存储到SQLite数据库中")

        app.launch()

if __name__=="__main__":
    main()


