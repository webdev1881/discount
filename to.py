#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Миграция SQLite → PostgreSQL
"""

import sqlite3
import sys


class SQLiteToPostgreSQL:
    """Конвертер SQLite в PostgreSQL SQL"""
    
    def __init__(self, db_path: str, output_file: str):
        self.db_path = db_path
        self.output_file = output_file
        self.conn = None
    
    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
    
    def close(self):
        if self.conn:
            self.conn.close()
    
    def convert_type(self, sqlite_type: str) -> str:
        """Конвертация типов SQLite → PostgreSQL"""
        type_map = {
            'INTEGER': 'INTEGER',
            'TEXT': 'TEXT',
            'REAL': 'NUMERIC',
            'BLOB': 'BYTEA',
        }
        return type_map.get(sqlite_type.upper(), 'TEXT')
    
    def export(self):
        """Экспорт в PostgreSQL формат"""
        cursor = self.conn.cursor()
        
        with open(self.output_file, 'w', encoding='utf-8') as f:
            f.write("-- PostgreSQL Export\n")
            f.write("-- Source: SQLite discount_rules.db\n\n")
            f.write("BEGIN;\n\n")
            
            # Получаем таблицы
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            print(f"📋 Найдено {len(tables)} таблиц")
            
            # Порядок экспорта (сначала справочники)
            ordered_tables = self.order_tables(tables)
            
            for table in ordered_tables:
                print(f"   └─ {table}")
                self.export_table(f, table)
            
            f.write("\nCOMMIT;\n")
            print(f"✅ Готово: {self.output_file}")
    
    def order_tables(self, tables: list) -> list:
        """Упорядочивание таблиц (справочники первыми)"""
        mapping_tables = [t for t in tables if t.startswith('mapping_')]
        reference_tables = ['merchants', 'locations', 'terminals', 'sku_sets']
        main_tables = ['discount_rules', 'rule_conditions', 'order_conditions', 
                      'result_items', 'result_item_conditions']
        
        ordered = []
        ordered.extend([t for t in mapping_tables if t in tables])
        ordered.extend([t for t in reference_tables if t in tables])
        ordered.extend([t for t in main_tables if t in tables])
        
        # Добавляем оставшиеся
        for t in tables:
            if t not in ordered:
                ordered.append(t)
        
        return ordered
    
    def export_table(self, f, table_name: str):
        """Экспорт таблицы"""
        cursor = self.conn.cursor()
        
        # Структура таблицы
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        # CREATE TABLE
        f.write(f"\n-- Таблица: {table_name}\n")
        f.write(f"DROP TABLE IF EXISTS {table_name} CASCADE;\n")
        f.write(f"CREATE TABLE {table_name} (\n")
        
        col_defs = []
        for col in columns:
            col_name = col[1]
            col_type = self.convert_type(col[2])
            not_null = " NOT NULL" if col[3] else ""
            pk = " PRIMARY KEY" if col[5] else ""
            col_defs.append(f"    {col_name} {col_type}{not_null}{pk}")
        
        f.write(",\n".join(col_defs))
        f.write("\n);\n\n")
        
        # Данные
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        
        if rows:
            col_names = [col[1] for col in columns]
            col_names_str = ", ".join(col_names)
            
            for row in rows:
                values = []
                for val in row:
                    if val is None:
                        values.append("NULL")
                    elif isinstance(val, (int, float)):
                        values.append(str(val))
                    else:
                        escaped = str(val).replace("'", "''")
                        values.append(f"'{escaped}'")
                
                values_str = ", ".join(values)
                f.write(f"INSERT INTO {table_name} ({col_names_str}) VALUES ({values_str});\n")
            
            f.write("\n")


def main():
    db_path = "discount_rules.db"
    output_file = "discount_rules_postgresql.sql"
    
    converter = SQLiteToPostgreSQL(db_path, output_file)
    
    try:
        converter.connect()
        converter.export()
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        sys.exit(1)
    finally:
        converter.close()


if __name__ == "__main__":
    main()