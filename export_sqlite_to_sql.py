#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Экспорт SQLite базы в SQL дамп для импорта в другие СУБД
"""

import sqlite3
import sys
from pathlib import Path


class SQLiteExporter:
    """Экспорт SQLite в SQL скрипт"""
    
    def __init__(self, db_path: str, output_file: str):
        self.db_path = db_path
        self.output_file = output_file
        self.conn = None
    
    def connect(self):
        """Подключение к SQLite"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
    
    def close(self):
        """Закрытие соединения"""
        if self.conn:
            self.conn.close()
    
    def export_to_sql(self):
        """Экспорт всей БД в SQL файл"""
        print(f"📤 Экспорт из {self.db_path}...")
        
        with open(self.output_file, 'w', encoding='utf-8') as f:
            # Заголовок
            f.write("-- ============================================\n")
            f.write("-- SQLite Export to SQL\n")
            f.write(f"-- Source: {self.db_path}\n")
            f.write("-- ============================================\n\n")
            
            # Получаем список всех таблиц
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            print(f"📋 Найдено {len(tables)} таблиц")
            
            # Экспортируем каждую таблицу
            for table_name in tables:
                print(f"   └─ Экспорт {table_name}...")
                self.export_table(f, table_name)
            
            print(f"✅ Экспорт завершён: {self.output_file}")
    
    def export_table(self, f, table_name: str):
        """Экспорт одной таблицы"""
        cursor = self.conn.cursor()
        
        # Получаем структуру таблицы
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()
        columns = [col[1] for col in columns_info]
        
        # Получаем данные
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        
        if not rows:
            f.write(f"-- Таблица {table_name} пуста\n\n")
            return
        
        f.write(f"\n-- ============================================\n")
        f.write(f"-- Таблица: {table_name} ({len(rows)} записей)\n")
        f.write(f"-- ============================================\n\n")
        
        # Генерируем INSERT запросы
        for row in rows:
            values = []
            for val in row:
                if val is None:
                    values.append("NULL")
                elif isinstance(val, (int, float)):
                    values.append(str(val))
                else:
                    # Экранируем кавычки
                    escaped = str(val).replace("'", "''")
                    values.append(f"'{escaped}'")
            
            columns_str = ", ".join(columns)
            values_str = ", ".join(values)
            
            f.write(f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str});\n")
        
        f.write("\n")


def main():
    """Главная функция"""
    db_path = "discount_rules.db"
    output_file = "discount_rules_export.sql"
    
    if not Path(db_path).exists():
        print(f"❌ Файл {db_path} не найден!")
        sys.exit(1)
    
    exporter = SQLiteExporter(db_path, output_file)
    
    try:
        exporter.connect()
        exporter.export_to_sql()
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        sys.exit(1)
    finally:
        exporter.close()


if __name__ == "__main__":
    main()