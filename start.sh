#!/bin/bash

# 定义数据库文件路径
DB_FILE="/usr/src/app/example.db"

if [ ! -f "$DB_FILE" ]; then
    echo "Initializing database..."
    python /usr/src/app/database.py
else
    echo "Database already initialized."
fi

# 运行 main.py
python /usr/src/app/main.py

