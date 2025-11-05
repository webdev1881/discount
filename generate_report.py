#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Генератор HTML отчета для проверки загруженных данных скидок
"""

import sqlite3
from datetime import datetime
from pathlib import Path


class ReportGenerator:
    """Генератор отчетов из БД"""
    
    def __init__(self, db_path: str = "discount_rules.db"):
        self.db_path = db_path
        self.conn = None
    
    def connect(self):
        """Подключение к БД"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
    
    def close(self):
        """Закрытие соединения"""
        if self.conn:
            self.conn.close()
    
    def execute_query(self, query: str):
        """Выполнение запроса и возврат результатов"""
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    
    def generate_html_report(self, output_file: str = "discount_report.html"):
        """Генерация HTML отчета"""
        
        html_parts = []
        
        # HTML шапка
        html_parts.append("""
<!DOCTYPE html>
<html lang="uk">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Звіт по правилах знижок</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: #f5f7fa;
            padding: 20px;
            color: #333;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 2px 20px rgba(0,0,0,0.1);
            padding: 40px;
        }
        
        h1 {
            color: #2c3e50;
            border-bottom: 4px solid #3498db;
            padding-bottom: 15px;
            margin-bottom: 30px;
            font-size: 32px;
        }
        
        h2 {
            color: #34495e;
            margin: 40px 0 20px 0;
            padding: 12px 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border-radius: 8px;
            font-size: 22px;
        }
        
        .meta-info {
            background: #ecf0f1;
            padding: 15px 20px;
            border-radius: 8px;
            margin-bottom: 30px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .meta-info div {
            font-size: 14px;
            color: #7f8c8d;
        }
        
        .meta-info strong {
            color: #2c3e50;
            font-size: 16px;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
        }
        
        thead {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }
        
        th {
            padding: 15px;
            text-align: left;
            font-weight: 600;
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        td {
            padding: 12px 15px;
            border-bottom: 1px solid #ecf0f1;
            font-size: 14px;
        }
        
        tbody tr:hover {
            background: #f8f9fa;
            transition: background 0.2s;
        }
        
        tbody tr:last-child td {
            border-bottom: none;
        }
        
        .badge {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 600;
            text-transform: uppercase;
        }
        
        .badge-active {
            background: #d4edda;
            color: #155724;
        }
        
        .badge-inactive {
            background: #f8d7da;
            color: #721c24;
        }
        
        .badge-archive {
            background: #e2e3e5;
            color: #383d41;
        }
        
        .badge-test {
            background: #fff3cd;
            color: #856404;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 30px 0;
        }
        
        .stat-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 25px;
            border-radius: 12px;
            color: white;
            box-shadow: 0 4px 12px rgba(102, 126, 234, 0.3);
        }
        
        .stat-card h3 {
            font-size: 14px;
            opacity: 0.9;
            margin-bottom: 10px;
            font-weight: 500;
        }
        
        .stat-card .number {
            font-size: 36px;
            font-weight: 700;
            margin-bottom: 5px;
        }
        
        .no-data {
            padding: 40px;
            text-align: center;
            color: #95a5a6;
            font-style: italic;
        }
        
        .highlight {
            background: #fff3cd;
            padding: 2px 6px;
            border-radius: 4px;
        }
        
        .number-cell {
            text-align: right;
            font-weight: 600;
            color: #3498db;
        }
        
        .footer {
            margin-top: 50px;
            padding-top: 20px;
            border-top: 2px solid #ecf0f1;
            text-align: center;
            color: #95a5a6;
            font-size: 14px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Звіт по правилах знижок</h1>
        <div class="meta-info">
            <div><strong>База даних:</strong> discount_rules.db</div>
            <div><strong>Дата генерації:</strong> """ + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + """</div>
        </div>
""")
        
        # 1. Статистика таблиц
        html_parts.append("<h2>📈 Загальна статистика</h2>")
        stats = self.execute_query("""
            SELECT 'discount_rules' as table_name, COUNT(*) as count FROM discount_rules
            UNION ALL SELECT 'rule_conditions', COUNT(*) FROM rule_conditions
            UNION ALL SELECT 'order_conditions', COUNT(*) FROM order_conditions
            UNION ALL SELECT 'result_items', COUNT(*) FROM result_items
            UNION ALL SELECT 'result_item_conditions', COUNT(*) FROM result_item_conditions
            UNION ALL SELECT 'sku_sets', COUNT(*) FROM sku_sets
            UNION ALL SELECT 'locations', COUNT(*) FROM locations
            UNION ALL SELECT 'merchants', COUNT(*) FROM merchants
            UNION ALL SELECT 'terminals', COUNT(*) FROM terminals
        """)
        
        html_parts.append('<div class="stats-grid">')
        table_names_ua = {
            'discount_rules': 'Правила знижок',
            'rule_conditions': 'Умови правил',
            'order_conditions': 'Умови на чек',
            'result_items': 'Результати',
            'result_item_conditions': 'Умови результатів',
            'sku_sets': 'Набори товарів',
            'locations': 'Локації',
            'merchants': 'Мерчанти',
            'terminals': 'Термінали'
        }
        
        for row in stats:
            table_name = table_names_ua.get(row['table_name'], row['table_name'])
            html_parts.append(f'''
                <div class="stat-card">
                    <h3>{table_name}</h3>
                    <div class="number">{row['count']:,}</div>
                </div>
            ''')
        html_parts.append('</div>')
        
        # 2. Активные правила
        html_parts.append("<h2>✅ Активні правила знижок</h2>")
        active_rules = self.execute_query("""
            SELECT 
                dr.id,
                dr.name,
                ms.name as status,
                dr.priority,
                dr.begin_date,
                dr.end_date
            FROM discount_rules dr
            LEFT JOIN mapping_status ms ON dr.status = ms.id
            WHERE dr.status = 1
            ORDER BY dr.priority DESC
            LIMIT 20
        """)
        
        if active_rules:
            html_parts.append("""
                <table>
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Назва правила</th>
                            <th>Статус</th>
                            <th>Пріоритет</th>
                            <th>Дата початку</th>
                            <th>Дата закінчення</th>
                        </tr>
                    </thead>
                    <tbody>
            """)
            
            for row in active_rules:
                html_parts.append(f"""
                    <tr>
                        <td class="number-cell">{row['id']}</td>
                        <td><strong>{row['name']}</strong></td>
                        <td><span class="badge badge-active">{row['status']}</span></td>
                        <td class="number-cell">{row['priority']}</td>
                        <td>{row['begin_date'] or '-'}</td>
                        <td>{row['end_date'] or '-'}</td>
                    </tr>
                """)
            
            html_parts.append("</tbody></table>")
        else:
            html_parts.append('<div class="no-data">Немає активних правил</div>')
        
        # 3. Распределение по статусам
        html_parts.append("<h2>📊 Розподіл правил за статусами</h2>")
        status_dist = self.execute_query("""
            SELECT 
                ms.name as status,
                COUNT(dr.id) as count,
                ROUND(COUNT(dr.id) * 100.0 / (SELECT COUNT(*) FROM discount_rules), 2) as percentage
            FROM discount_rules dr
            LEFT JOIN mapping_status ms ON dr.status = ms.id
            GROUP BY ms.name
            ORDER BY COUNT(dr.id) DESC
        """)
        
        if status_dist:
            html_parts.append("""
                <table>
                    <thead>
                        <tr>
                            <th>Статус</th>
                            <th>Кількість правил</th>
                            <th>Відсоток</th>
                        </tr>
                    </thead>
                    <tbody>
            """)
            
            for row in status_dist:
                badge_class = 'badge-active' if 'Активно' in row['status'] else 'badge-inactive'
                html_parts.append(f"""
                    <tr>
                        <td><span class="badge {badge_class}">{row['status']}</span></td>
                        <td class="number-cell">{row['count']}</td>
                        <td class="number-cell">{row['percentage']}%</td>
                    </tr>
                """)
            
            html_parts.append("</tbody></table>")
        
        # 4. Правила с условиями
        html_parts.append("<h2>🎯 Правила з умовами застосування (rule_conditions)</h2>")
        rules_with_conditions = self.execute_query("""
            SELECT 
                dr.id,
                dr.name,
                mdv.name as condition_type,
                mo.name as operator,
                rc.value,
                rc.group_name
            FROM discount_rules dr
            INNER JOIN rule_conditions rc ON dr.id = rc.discount_rule_id
            LEFT JOIN mapping_data_values mdv ON rc.condition_type = mdv.id
            LEFT JOIN mapping_operators mo ON rc.comparison_type = mo.id
            LIMIT 30
        """)
        
        if rules_with_conditions:
            html_parts.append("""
                <table>
                    <thead>
                        <tr>
                            <th>ID правила</th>
                            <th>Назва</th>
                            <th>Тип умови</th>
                            <th>Оператор</th>
                            <th>Значення</th>
                            <th>Група</th>
                        </tr>
                    </thead>
                    <tbody>
            """)
            
            for row in rules_with_conditions:
                html_parts.append(f"""
                    <tr>
                        <td class="number-cell">{row['id']}</td>
                        <td>{row['name']}</td>
                        <td><span class="highlight">{row['condition_type']}</span></td>
                        <td>{row['operator']}</td>
                        <td>{row['value']}</td>
                        <td>{row['group_name'] or '-'}</td>
                    </tr>
                """)
            
            html_parts.append("</tbody></table>")
        else:
            html_parts.append('<div class="no-data">Немає правил з умовами</div>')
        
        # 4.5 Условия на чек (order_conditions)
        html_parts.append("<h2>🛒 Умови на чек (order_conditions)</h2>")
        order_conditions_data = self.execute_query("""
            SELECT 
                dr.id,
                dr.name,
                mpv.name as condition_type,
                mo.name as operator,
                oc.value,
                oc.group_name
            FROM discount_rules dr
            INNER JOIN order_conditions oc ON dr.id = oc.discount_rule_id
            LEFT JOIN mapping_product_values mpv ON oc.condition_type = mpv.id
            LEFT JOIN mapping_operators mo ON oc.comparison_type = mo.id
            LIMIT 30
        """)
        
        if order_conditions_data:
            html_parts.append("""
                <table>
                    <thead>
                        <tr>
                            <th>ID правила</th>
                            <th>Назва</th>
                            <th>Тип умови</th>
                            <th>Оператор</th>
                            <th>Значення</th>
                            <th>Група</th>
                        </tr>
                    </thead>
                    <tbody>
            """)
            
            for row in order_conditions_data:
                html_parts.append(f"""
                    <tr>
                        <td class="number-cell">{row['id']}</td>
                        <td>{row['name']}</td>
                        <td><span class="highlight">{row['condition_type']}</span></td>
                        <td>{row['operator']}</td>
                        <td>{row['value']}</td>
                        <td>{row['group_name'] or '-'}</td>
                    </tr>
                """)
        
            html_parts.append("</tbody></table>")
        else:
            html_parts.append('<div class="no-data">Немає умов на чек</div>')
        
        # 5. Результаты применения скидок
        html_parts.append("<h2>💰 Результати застосування знижок</h2>")
        results = self.execute_query("""
            SELECT 
                dr.id,
                dr.name,
                mrt.name as result_type,
                mvt.name as value_type,
                ri.fixed_value,
                ri.expression,
                mdtt.name as discount_time_type,
                ss.name as sku_set,
                mgam.name as group_apply_mode
            FROM discount_rules dr
            INNER JOIN result_items ri ON dr.id = ri.discount_rule_id
            LEFT JOIN mapping_result_type mrt ON ri.result_type = mrt.id
            LEFT JOIN mapping_value_type mvt ON ri.value_type = mvt.id
            LEFT JOIN mapping_discount_time_type mdtt ON ri.discount_time_type = mdtt.id
            LEFT JOIN sku_sets ss ON ri.sku_set_id = ss.id
            LEFT JOIN mapping_group_apply_mode mgam ON ri.group_apply_mode = mgam.id
            LIMIT 30
        """)
        
        if results:
            html_parts.append("""
                <table>
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Назва правила</th>
                            <th>Тип результату</th>
                            <th>Тип значення</th>
                            <th>Фікс. значення</th>
                            <th>Вираз</th>
                            <th>Тип часу знижки</th>
                            <th>Набір товарів</th>
                            <th>Режим застосування</th>
                        </tr>
                    </thead>
                    <tbody>
            """)
            
            for row in results:
                html_parts.append(f"""
                    <tr>
                        <td class="number-cell">{row['id']}</td>
                        <td>{row['name']}</td>
                        <td><span class="highlight">{row['result_type']}</span></td>
                        <td>{row['value_type'] or '-'}</td>
                        <td class="number-cell">{row['fixed_value'] if row['fixed_value'] else '-'}</td>
                        <td>{row['expression'] or '-'}</td>
                        <td>{row['discount_time_type'] or '-'}</td>
                        <td>{row['sku_set'] or '-'}</td>
                        <td>{row['group_apply_mode'] or '-'}</td>
                    </tr>
                """)
            
            html_parts.append("</tbody></table>")
        else:
            html_parts.append('<div class="no-data">Немає результатів</div>')
        
        # 6. ТОП-10 наборов товаров
        html_parts.append("<h2>🏆 ТОП-10 найбільш використовуваних наборів товарів</h2>")
        top_sku_sets = self.execute_query("""
            SELECT 
                ss.id,
                ss.name,
                ss.ext_code,
                COUNT(DISTINCT ri.id) as usage_count
            FROM sku_sets ss
            LEFT JOIN result_items ri ON ss.id = ri.sku_set_id
            GROUP BY ss.id, ss.name, ss.ext_code
            HAVING usage_count > 0
            ORDER BY usage_count DESC
            LIMIT 10
        """)
        
        if top_sku_sets:
            html_parts.append("""
                <table>
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Назва набору</th>
                            <th>Зовнішній код</th>
                            <th>Використань</th>
                        </tr>
                    </thead>
                    <tbody>
            """)
            
            for row in top_sku_sets:
                html_parts.append(f"""
                    <tr>
                        <td class="number-cell">{row['id']}</td>
                        <td><strong>{row['name']}</strong></td>
                        <td>{row['ext_code'] or '-'}</td>
                        <td class="number-cell">{row['usage_count']}</td>
                    </tr>
                """)
            
            html_parts.append("</tbody></table>")
        
        # Footer
        html_parts.append("""
        <div class="footer">
            <p>Згенеровано автоматично | ETL Pipeline для правил знижок</p>
        </div>
    </div>
</body>
</html>
        """)
        
        # Записываем в файл
        html_content = ''.join(html_parts)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✅ HTML звіт згенеровано: {output_file}")
        return output_file


def main():
    """Главная функция"""
    generator = ReportGenerator()
    
    try:
        generator.connect()
        output_file = generator.generate_html_report()
        print(f"\n📄 Відкрийте файл у браузері: {Path(output_file).absolute()}")
    except Exception as e:
        print(f"❌ Помилка: {e}")
    finally:
        generator.close()


if __name__ == "__main__":
    main()