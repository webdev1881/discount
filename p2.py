#!/usr/bin/env python3

import asyncio
import logging
import sys
import ssl
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Dict
import pytz

import aiohttp
import aiosqlite
import orjson

class Config:
    BASE_URL = "https://89.105.216.114"
    USERNAME = "Yulia"
    PASSWORD = "SY1804$@"
    
    DB_FILE = "discount_rules.db"
    BATCH_SIZE = 100
    
    USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'

def timestamp_to_datetime(timestamp_ms: int) -> str:
    if timestamp_ms:
        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=pytz.UTC)
        return dt.strftime("%Y-%m-%d-%H-%M")
    return None

def setup_logging():
    console_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler('discount_rules_etl.log', encoding='utf-8')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[console_handler, file_handler]
    )
    return logging.getLogger("DiscountRules_ETL")

class MappingLoader:
    def __init__(self):
        self.data_values = {}
        self.operators_values = {}
        self.product_values = {}
        self.data_values_2 = {}
        self.cond_values = {}
        self.status_map = {}
        self.group_apply_mode_map = {}
    
    def load_mappings(self):
        self.data_values = {
            -1: "Місце продажу",
            2: "POS-термінал",
            5: "Емітент",
            0: "Організація",
            1: "Підрозділ",
            22: "Термінальна група",
            -1: "Контрагент",
            27: "Анкетні дані",
            13: "Вік",
            9: "День народження",
            6: "Категорія контрагента",
            7: "Контрагент",
            30: "Кіл-ть днів до ДН",
            31: "Кіл-ть днів після ДН",
            16: "Кількість балів",
            26: "Сегмент / цільова група",
            21: "Соціальна група",
            12: "Стать",
            -1: "Дата/час",
            8: "День в році",
            10: "День тижня",
            11: "Час",
            -1: "Картка",
            35: "Без картки",
            3: "Категорія картки",
            28: "Можливості картки",
            15: "Стаж картки в системі, років",
            4: "Статус картки",
            -1: "Статистика",
            34: "Кіл-ть днів після останньої покупки ПММ",
            24: "Кіл-ть днів після останньої покупки товарів",
            25: "Кіл-ть днів після першої покупки",
            17: "Кількість бонусів",
            14: "Статистика покупок",
            32: "Статистика покупок ПММ",
            23: "Статистика покупок товарів",
            -1: "Чек",
            33: "Випадковий чек (ймовірність по підрозділах, %)",
            29: "Випадковий чек (ймовірність, %)",
            20: "Тип чека",
            19: "Форма оплати"
        }
        
        self.operators_values = {
            0: "=",
            1: "!=",
            2: ">",
            3: "<",
            4: ">=",
            5: "<=",
            6: "IN",
            7: "NOT IN"
        }
        
        self.product_values = {
            1: "Номенклатура",
            2: "Група номенклатури",
            3: "Набір номенклатури",
            4: "Паливо",
            5: "Кількість",
            6: "Сума"
        }
        
        self.data_values_2 = {
            0: "=",
            1: "!=",
            2: ">",
            3: "<",
            4: ">=",
            5: "<=",
            6: "IN",
            7: "NOT IN"
        }
        
        self.cond_values = {
            1: "Номенклатура",
            2: "Група номенклатури",
            3: "Набір номенклатури",
            4: "Паливо",
            5: "Кількість",
            6: "Сума",
            7: "Ціна",
            8: "Вага"
        }
        
        self.status_map = {
            0: "Не активно",
            1: "Активно",
            2: "Архів",
            3: "На затверджені",
            4: "Тестування"
        }
        
        self.group_apply_mode_map = {
            0: "До всіх відібраних позицій чека",
            1: "Окремо по номенклатурі"
        }

class SQLiteManager:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.connection = None
        
    async def initialize(self):
        self.connection = await aiosqlite.connect(self.db_file)
        
        await self.connection.execute("PRAGMA journal_mode=WAL")
        await self.connection.execute("PRAGMA synchronous=NORMAL")
        await self.connection.execute("PRAGMA cache_size=10000")
        await self.connection.execute("PRAGMA temp_store=memory")
        await self.connection.commit()
        
    async def close(self):
        if self.connection:
            await self.connection.close()
            
    @asynccontextmanager
    async def get_connection(self):
        yield self.connection
            
    async def create_tables(self):
        async with self.get_connection() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS locations (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    merchant_id INTEGER,
                    merchant_name TEXT,
                    ext_code TEXT,
                    address TEXT
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS merchants (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    ext_code TEXT
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS terminals (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    location_id INTEGER,
                    ext_code TEXT
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS sku_sets (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    ext_code TEXT,
                    pos_name TEXT,
                    removed INTEGER,
                    only_product INTEGER,
                    only_fuel INTEGER
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mapping_data_values (
                    id INTEGER PRIMARY KEY,
                    name TEXT
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mapping_operators (
                    id INTEGER PRIMARY KEY,
                    name TEXT
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mapping_product_values (
                    id INTEGER PRIMARY KEY,
                    name TEXT
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mapping_cond_values (
                    id INTEGER PRIMARY KEY,
                    name TEXT
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mapping_status (
                    id INTEGER PRIMARY KEY,
                    name TEXT
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mapping_group_apply_mode (
                    id INTEGER PRIMARY KEY,
                    name TEXT
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS discount_rules (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    comment TEXT,
                    pos_message TEXT,
                    description TEXT,
                    operator_message TEXT,
                    operator_id INTEGER,
                    operator_id_desc TEXT,
                    begin_date TEXT,
                    end_date TEXT,
                    status TEXT,
                    priority INTEGER,
                    isolation_level INTEGER,
                    apply_mode INTEGER,
                    only_message_mode INTEGER,
                    scheduling_mode INTEGER,
                    is_for_dc_gen INTEGER,
                    exclude_sku_set_id INTEGER,
                    exclude_sku_set_id_desc TEXT,
                    ext_code TEXT,
                    min_match_count INTEGER
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS rule_conditions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    discount_rule_id INTEGER,
                    condition_type TEXT,
                    comparison_type TEXT,
                    value TEXT,
                    group_name TEXT,
                    FOREIGN KEY (discount_rule_id) REFERENCES discount_rules(id)
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS order_conditions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    discount_rule_id INTEGER,
                    sku_set_id INTEGER,
                    exclude_sku_set_id INTEGER,
                    condition_type TEXT,
                    comparison_type TEXT,
                    value TEXT,
                    group_name TEXT,
                    FOREIGN KEY (discount_rule_id) REFERENCES discount_rules(id),
                    FOREIGN KEY (sku_set_id) REFERENCES sku_sets(id),
                    FOREIGN KEY (exclude_sku_set_id) REFERENCES sku_sets(id)
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS result_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    discount_rule_id INTEGER,
                    result_type INTEGER,
                    comparison_type INTEGER,
                    value_type INTEGER,
                    fixed_value REAL,
                    expression TEXT,
                    discount_value_type INTEGER,
                    discount_time_type INTEGER,
                    sku_set_id INTEGER,
                    except_sku_set_id INTEGER,
                    sort_items_mode INTEGER,
                    group_apply_mode TEXT,
                    FOREIGN KEY (discount_rule_id) REFERENCES discount_rules(id),
                    FOREIGN KEY (sku_set_id) REFERENCES sku_sets(id),
                    FOREIGN KEY (except_sku_set_id) REFERENCES sku_sets(id)
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS result_item_conditions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    result_item_id INTEGER,
                    condition_type TEXT,
                    value TEXT,
                    FOREIGN KEY (result_item_id) REFERENCES result_items(id)
                )
            """)
            
            await conn.commit()
            
    async def create_indexes(self):
        async with self.get_connection() as conn:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_locations_name ON locations (name)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_locations_merchant_id ON locations (merchant_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_merchants_name ON merchants (name)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_terminals_name ON terminals (name)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_terminals_location_id ON terminals (location_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_sku_sets_name ON sku_sets (name)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_discount_rules_name ON discount_rules (name)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_discount_rules_status ON discount_rules (status)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_discount_rules_begin_date ON discount_rules (begin_date)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_discount_rules_end_date ON discount_rules (end_date)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_rule_conditions_rule_id ON rule_conditions (discount_rule_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_order_conditions_rule_id ON order_conditions (discount_rule_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_result_items_rule_id ON result_items (discount_rule_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_result_item_conditions_item_id ON result_item_conditions (result_item_id)")
            await conn.commit()

class DiscountRulesAPI:
    def __init__(self, config: Config, logger):
        self.config = config
        self.logger = logger
        self.session = None
        self.cookies = None
        
    async def __aenter__(self):
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        self.session = aiohttp.ClientSession(connector=connector)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
            await asyncio.sleep(0.1)
    
    async def login(self):
        url = f"{self.config.BASE_URL}/api/login"
        payload = {
            "username": self.config.USERNAME,
            "password": self.config.PASSWORD
        }
        
        async with self.session.post(url, json=payload) as response:
            if response.status == 200:
                self.cookies = response.cookies
                self.logger.info("Авторизация успешна")
                return True
            else:
                text = await response.text()
                self.logger.error(f"Ошибка авторизации: {text}")
                return False
    
    async def get_list_data(self, endpoint: str, sort_field: str = "name", offset: int = 0):
        url = f"{self.config.BASE_URL}{endpoint}"
        
        payload = {
            "count": self.config.BATCH_SIZE,
            "filter": {},
            "offset": offset,
            "period": {},
            "sort": {
                "fields": [
                    {
                        "field": sort_field,
                        "asc": True
                    }
                ]
            }
        }
        
        headers = {
            'accept': '*/*',
            'content-type': 'application/json',
            'origin': self.config.BASE_URL,
            'referer': f"{self.config.BASE_URL}/",
            'user-agent': self.config.USER_AGENT
        }
        
        async with self.session.post(url, json=payload, headers=headers, cookies=self.cookies) as response:
            if response.status == 200:
                data = await response.json()
                return data.get('data', []), data.get('count', 0)
            return [], 0
    
    async def get_discount_rules(self, offset: int = 0):
        return await self.get_list_data("/discountRule/list", "priority", offset)

class DataProcessor:
    def __init__(self, logger, mapping_loader: MappingLoader):
        self.logger = logger
        self.mapping = mapping_loader
        self.sku_sets_cache = {}
    
    def parse_value_field(self, value_str: str):
        if not value_str:
            return None
        
        value_str = value_str.replace('\\"', '"').replace('\\\\', '\\')
        
        try:
            parsed = orjson.loads(value_str)
            
            if isinstance(parsed, dict):
                if 'ids' in parsed:
                    return orjson.dumps(parsed['ids']).decode()
                elif 'id' in parsed:
                    return str(parsed['id'])
            
            return orjson.dumps(parsed).decode()
        except:
            return value_str
    
    def load_cache(self, cache_dict: dict, items: List[dict], id_field: str = 'id'):
        for item in items:
            item_id = item.get(id_field)
            if item_id:
                cache_dict[item_id] = item

class ETLPipeline:
    def __init__(self, config: Config):
        self.config = config
        self.logger = setup_logging()
        self.mapping_loader = MappingLoader()
        self.mapping_loader.load_mappings()
        self.data_processor = DataProcessor(self.logger, self.mapping_loader)
        self.db_manager = None
        
    async def initialize(self):
        self.logger.info("Инициализация ETL Pipeline...")
        
        self.db_manager = SQLiteManager(self.config.DB_FILE)
        await self.db_manager.initialize()
        
        await self.db_manager.create_tables()
        await self.db_manager.create_indexes()
        
        await self.save_mappings()
        
        self.logger.info(f"База данных: {self.config.DB_FILE}")
    
    async def cleanup(self):
        if self.db_manager:
            await self.db_manager.close()
        await asyncio.sleep(1)
        self.logger.info("ETL Pipeline завершен")
    
    async def save_mappings(self):
        async with self.db_manager.get_connection() as conn:
            for table, mapping in [
                ('mapping_data_values', self.mapping_loader.data_values),
                ('mapping_operators', self.mapping_loader.operators_values),
                ('mapping_product_values', self.mapping_loader.product_values),
                ('mapping_cond_values', self.mapping_loader.cond_values),
                ('mapping_status', self.mapping_loader.status_map),
                ('mapping_group_apply_mode', self.mapping_loader.group_apply_mode_map)
            ]:
                for key, value in mapping.items():
                    await conn.execute(f"INSERT OR REPLACE INTO {table} (id, name) VALUES (?, ?)", (key, value))
            
            await conn.commit()
            self.logger.info("Сохранены таблицы сопоставлений")
    
    async def load_reference_data(self, api: DiscountRulesAPI):
        self.logger.info("Загрузка справочников...")
        
        endpoints = {
            'locations': ('/location/list', None),
            'merchants': ('/merchant/list', None),
            'terminals': ('/terminal/list', None),
            'sku_sets': ('/skuSet/list', 'sku_sets_cache')
        }
        
        for name, (endpoint, cache_attr) in endpoints.items():
            offset = 0
            all_items = []
            
            while True:
                items, total = await api.get_list_data(endpoint, offset=offset)
                
                if not items:
                    break
                
                all_items.extend(items)
                
                if len(items) < self.config.BATCH_SIZE:
                    break
                
                offset += self.config.BATCH_SIZE
                await asyncio.sleep(0.2)
            
            if cache_attr:
                cache = getattr(self.data_processor, cache_attr)
                self.data_processor.load_cache(cache, all_items)
            
            await self.save_reference_data(name, all_items)
            
            self.logger.info(f"Загружено {len(all_items)} записей из {name}")
    
    async def save_reference_data(self, table_name: str, items: List[dict]):
        async with self.db_manager.get_connection() as conn:
            if table_name == 'locations':
                for item in items:
                    await conn.execute("""
                        INSERT OR REPLACE INTO locations (id, name, merchant_id, merchant_name, ext_code, address)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (item.get('id'), item.get('name'), item.get('merchantId'), 
                          item.get('merchantName'), item.get('extCode'), item.get('address')))
            
            elif table_name == 'merchants':
                for item in items:
                    await conn.execute("""
                        INSERT OR REPLACE INTO merchants (id, name, ext_code)
                        VALUES (?, ?, ?)
                    """, (item.get('id'), item.get('name'), item.get('extCode')))
            
            elif table_name == 'terminals':
                for item in items:
                    await conn.execute("""
                        INSERT OR REPLACE INTO terminals (id, name, location_id, ext_code)
                        VALUES (?, ?, ?, ?)
                    """, (item.get('id'), item.get('name'), item.get('locationId'), item.get('extCode')))
            
            elif table_name == 'sku_sets':
                for item in items:
                    await conn.execute("""
                        INSERT OR REPLACE INTO sku_sets (id, name, ext_code, pos_name, removed, only_product, only_fuel)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (item.get('id'), item.get('name'), item.get('extCode'), item.get('posName'),
                          1 if item.get('removed') else 0, 1 if item.get('onlyProduct') else 0, 
                          1 if item.get('onlyFuel') else 0))
            
            await conn.commit()
    
    # async def save_discount_rule(self, rule: dict):
    #     async with self.db_manager.get_connection() as conn:

    #         if not rule:
    #             return
            
    #         rule_condition_group = rule.get('ruleConditionGroup') or {}
    #         order_condition_group = rule.get('orderConditionGroup') or {}


    #         await conn.execute("""
    #             INSERT OR REPLACE INTO discount_rules (
    #                 id, name, comment, pos_message, description, operator_message,
    #                 operator_id, operator_id_desc, begin_date, end_date, status,
    #                 priority, isolation_level, apply_mode, only_message_mode,
    #                 scheduling_mode, is_for_dc_gen, exclude_sku_set_id,
    #                 exclude_sku_set_id_desc, ext_code, min_match_count
    #             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    #         """, (
    #             rule.get('id'),
    #             rule.get('name'),
    #             rule.get('comment'),
    #             rule.get('posMessage'),
    #             rule.get('description'),
    #             rule.get('operatorMessage'),
    #             rule.get('operatorId'),
    #             rule.get('operatorIdDesc'),
    #             timestamp_to_datetime(rule.get('beginDate')),
    #             timestamp_to_datetime(rule.get('endDate')),
    #             self.mapping_loader.status_map.get(rule.get('status'), str(rule.get('status'))),
    #             rule.get('priority'),
    #             rule.get('isolationLevel'),
    #             rule.get('applyMode'),
    #             rule.get('onlyMessageMode'),
    #             rule.get('schedulingMode'),
    #             rule.get('isForDcGen'),
    #             rule.get('excludeSkuSetId'),
    #             rule.get('excludeSkuSetIdDesc'),
    #             rule.get('extCode'),
    #             # rule.get('ruleConditionGroup', {}).get('minMatchCount') if rule.get('ruleConditionGroup') else None,
    #             rule_condition_group.get('minMatchCount')
    #         ))
            
    #         rule_id = rule.get('id')
            
    #         if rule_condition_group and rule_condition_group.get('requiredConditions'):
    #             for condition in rule_condition_group['requiredConditions']:
    #                 await conn.execute("""
    #                     INSERT INTO rule_conditions (discount_rule_id, condition_type, comparison_type, value, group_name)
    #                     VALUES (?, ?, ?, ?, ?)
    #                 """, (
    #                     rule_id,
    #                     self.mapping_loader.data_values.get(condition.get('type'), condition.get('type')),
    #                     self.mapping_loader.operators_values.get(condition.get('comparsionType'), condition.get('comparsionType')),
    #                     self.data_processor.parse_value_field(condition.get('value')),
    #                     condition.get('group')
    #                 ))
            
    #         if order_condition_group and order_condition_group.get('requiredConditions'):
    #             for condition in order_condition_group['requiredConditions']:
    #                 await conn.execute("""
    #                     INSERT INTO order_conditions (discount_rule_id, sku_set_id, exclude_sku_set_id, 
    #                                                  condition_type, comparison_type, value, group_name)
    #                     VALUES (?, ?, ?, ?, ?, ?, ?)
    #                 """, (
    #                     rule_id,
    #                     condition.get('skuSetId'),
    #                     condition.get('excludeSkuSetId'),
    #                     self.mapping_loader.product_values.get(condition.get('type'), condition.get('type')),
    #                     self.mapping_loader.data_values_2.get(condition.get('comparsionType'), condition.get('comparsionType')),
    #                     self.data_processor.parse_value_field(condition.get('value')),
    #                     condition.get('group')
    #                 ))
            
    #         if rule.get('resultScaleItems'):
    #             for scale_item in rule['resultScaleItems']:
    #                 if scale_item.get('results'):
    #                     for result in scale_item['results']:
    #                         restriction = result.get('restriction', {})
                            
    #                         cursor = await conn.execute("""
    #                             INSERT INTO result_items (discount_rule_id, result_type, comparison_type, 
    #                                                      value_type, fixed_value, expression, discount_value_type,
    #                                                      discount_time_type, sku_set_id, except_sku_set_id, 
    #                                                      sort_items_mode, group_apply_mode)
    #                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    #                         """, (
    #                             rule_id,
    #                             scale_item.get('type'),
    #                             scale_item.get('comparsionType'),
    #                             result.get('valueType'),
    #                             result.get('fixedValue'),
    #                             result.get('expression'),
    #                             result.get('discountValueType'),
    #                             result.get('discountTimeType'),
    #                             restriction.get('skuSetId'),
    #                             restriction.get('exceptSkuSetId'),
    #                             restriction.get('sortItemsMode'),
    #                             self.mapping_loader.group_apply_mode_map.get(restriction.get('groupApplyMode'), restriction.get('groupApplyMode'))
    #                         ))
                            
    #                         result_item_id = cursor.lastrowid
                            
    #                         if restriction.get('conditions'):
    #                             for cond in restriction['conditions']:
    #                                 await conn.execute("""
    #                                     INSERT INTO result_item_conditions (result_item_id, condition_type, value)
    #                                     VALUES (?, ?, ?)
    #                                 """, (
    #                                     result_item_id,
    #                                     self.mapping_loader.cond_values.get(cond.get('type'), cond.get('type')),
    #                                     cond.get('value')
    #                                 ))
            
    #         await conn.commit()

    async def save_discount_rule(self, rule: dict):
        async with self.db_manager.get_connection() as conn:
            if not rule:
                return
            
            rule_condition_group = rule.get('ruleConditionGroup') or {}
            order_condition_group = rule.get('orderConditionGroup') or {}
            result_scale_items = rule.get('resultScaleItems') or []
            
            await conn.execute("""
                INSERT OR REPLACE INTO discount_rules (
                    id, name, comment, pos_message, description, operator_message,
                    operator_id, operator_id_desc, begin_date, end_date, status,
                    priority, isolation_level, apply_mode, only_message_mode,
                    scheduling_mode, is_for_dc_gen, exclude_sku_set_id,
                    exclude_sku_set_id_desc, ext_code, min_match_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rule.get('id'),
                rule.get('name'),
                rule.get('comment'),
                rule.get('posMessage'),
                rule.get('description'),
                rule.get('operatorMessage'),
                rule.get('operatorId'),
                rule.get('operatorIdDesc'),
                timestamp_to_datetime(rule.get('beginDate')),
                timestamp_to_datetime(rule.get('endDate')),
                self.mapping_loader.status_map.get(rule.get('status'), str(rule.get('status'))),
                rule.get('priority'),
                rule.get('isolationLevel'),
                rule.get('applyMode'),
                rule.get('onlyMessageMode'),
                rule.get('schedulingMode'),
                rule.get('isForDcGen'),
                rule.get('excludeSkuSetId'),
                rule.get('excludeSkuSetIdDesc'),
                rule.get('extCode'),
                rule_condition_group.get('minMatchCount')
            ))
            
            rule_id = rule.get('id')
            
            if rule_condition_group.get('requiredConditions'):
                for condition in rule_condition_group['requiredConditions']:
                    await conn.execute("""
                        INSERT INTO rule_conditions (discount_rule_id, condition_type, comparison_type, value, group_name)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        rule_id,
                        self.mapping_loader.data_values.get(condition.get('type'), condition.get('type')),
                        self.mapping_loader.operators_values.get(condition.get('comparsionType'), condition.get('comparsionType')),
                        self.data_processor.parse_value_field(condition.get('value')),
                        condition.get('group')
                    ))
            
            if order_condition_group.get('requiredConditions'):
                for condition in order_condition_group['requiredConditions']:
                    await conn.execute("""
                        INSERT INTO order_conditions (discount_rule_id, sku_set_id, exclude_sku_set_id, 
                                                    condition_type, comparison_type, value, group_name)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        rule_id,
                        condition.get('skuSetId'),
                        condition.get('excludeSkuSetId'),
                        self.mapping_loader.product_values.get(condition.get('type'), condition.get('type')),
                        self.mapping_loader.data_values_2.get(condition.get('comparsionType'), condition.get('comparsionType')),
                        self.data_processor.parse_value_field(condition.get('value')),
                        condition.get('group')
                    ))
            
            for scale_item in result_scale_items:
                if scale_item.get('results'):
                    for result in scale_item['results']:
                        restriction = result.get('restriction') or {}
                        
                        cursor = await conn.execute("""
                            INSERT INTO result_items (discount_rule_id, result_type, comparison_type, 
                                                    value_type, fixed_value, expression, discount_value_type,
                                                    discount_time_type, sku_set_id, except_sku_set_id, 
                                                    sort_items_mode, group_apply_mode)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            rule_id,
                            scale_item.get('type'),
                            scale_item.get('comparsionType'),
                            result.get('valueType'),
                            result.get('fixedValue'),
                            result.get('expression'),
                            result.get('discountValueType'),
                            result.get('discountTimeType'),
                            restriction.get('skuSetId'),
                            restriction.get('exceptSkuSetId'),
                            restriction.get('sortItemsMode'),
                            self.mapping_loader.group_apply_mode_map.get(restriction.get('groupApplyMode'), restriction.get('groupApplyMode'))
                        ))
                        
                        result_item_id = cursor.lastrowid
                        
                        conditions = restriction.get('conditions') or []
                        for cond in conditions:
                            await conn.execute("""
                                INSERT INTO result_item_conditions (result_item_id, condition_type, value)
                                VALUES (?, ?, ?)
                            """, (
                                result_item_id,
                                self.mapping_loader.cond_values.get(cond.get('type'), cond.get('type')),
                                cond.get('value')
                            ))
            
            await conn.commit()
    
    async def load_all_discount_rules(self, api: DiscountRulesAPI):
        self.logger.info("Загрузка правил скидок...")
        offset = 0
        total_processed = 0
        
        while True:
            rules, total_count = await api.get_discount_rules(offset)
            
            if not rules:
                break
            
            for rule in rules:
                await self.save_discount_rule(rule)
            
            total_processed += len(rules)
            self.logger.info(f"Обработано {total_processed}/{total_count} правил скидок")
            
            if len(rules) < self.config.BATCH_SIZE:
                break
            
            offset += self.config.BATCH_SIZE
            await asyncio.sleep(0.3)
        
        return total_processed
    
    async def run(self):
        try:
            await self.initialize()
            
            async with DiscountRulesAPI(self.config, self.logger) as api:
                if not await api.login():
                    self.logger.error("Не удалось авторизоваться")
                    return
                
                await self.load_reference_data(api)
                
                total_rules = await self.load_all_discount_rules(api)
                self.logger.info(f"Всего обработано {total_rules} правил скидок")
            
            self.logger.info("ETL Pipeline завершен успешно")
            
        except Exception as e:
            self.logger.error(f"Критическая ошибка: {e}")
            raise
        finally:
            await self.cleanup()

async def main():
    if sys.platform.startswith('win'):
        import os
        os.environ['PYTHONIOENCODING'] = 'utf-8'
    
    config = Config()
    pipeline = ETLPipeline(config)
    
    try:
        await pipeline.run()
    except KeyboardInterrupt:
        print("\nОстановка...")
    except Exception as e:
        print(f"Ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())