#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL Pipeline для загрузки правил скидок из API в SQLite
Версия с расширенным логированием для отладки
"""

import asyncio
import aiohttp
import aiosqlite
import logging
import ssl
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
import pytz

# Настройка логирования с UTF-8
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('discount_rules_etl.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class Config:
    """Конфигурация подключения к API и БД"""
    
    # API Configuration
    BASE_URL = "https://89.105.216.114"
    USERNAME = "Yulia"
    PASSWORD = "SY1804$@"
    
    # Database
    DB_PATH = "discount_rules.db"
    
    # API Endpoints
    ENDPOINTS = {
        'login': '/api/login',
        'discount_rules': '/discountRule/list',
        'sku_sets': '/skuSet/list',
        'locations': '/location/list',
        'merchants': '/merchant/list',
        'terminals': '/terminal/list'
    }
    
    # Pagination
    BATCH_SIZE = 100


class MappingLoader:
    """Загрузчик справочных таблиц маппинга"""
    
    @staticmethod
    def get_mappings() -> Dict[str, Dict[int, str]]:
        """Возвращает все маппинги"""
        return {
            'data_values': {
                1: "Локація",
                2: "Мерчант",
                3: "Термінал",
                4: "Категорія картки",
                5: "Категорія клієнта",
                6: "Сума чека",
                7: "Клієнт",
                8: "Час доби",
                9: "День тижня",
                10: "Дата",
                11: "Оператор"
            },
            'operators': {
                0: "=",
                1: "!=",
                2: ">",
                3: "<",
                4: ">=",
                5: "<=",
                6: "IN",
                7: "NOT IN"
            },
            'product_values': {
                1: "Номенклатура",
                2: "Група номенклатури",
                3: "Набір номенклатури",
                4: "Паливо",
                5: "Кількість",
                6: "Сума"
            },
            'cond_values': {
                1: "Номенклатура",
                2: "Група номенклатури",
                3: "Набір номенклатури",
                4: "Паливо",
                5: "Кількість",
                6: "Сума",
                7: "Ціна",
                8: "Вага"
            },
            'status': {
                0: "Не активно",
                1: "Активно",
                2: "Архів",
                3: "На затверджені",
                4: "Тестування"
            },
            'group_apply_mode': {
                0: "До всіх відібраних позицій чека",
                1: "Окремо по номенклатурі"
            },
            'isolation_level': {
                0: "Нормальний",
                1: "Ізольований",
                2: "Високий"
            },
            'apply_mode': {
                0: "Застосувати всі",
                1: "Застосувати кращу",
                2: "Комбінований"
            },
            'scheduling_mode': {
                0: "Весь час",
                1: "За розкладом"
            },
            'result_type': {
                1: "Фіксована знижка",
                2: "Відсоткова знижка",
                3: "Фіксована ціна",
                4: "Бонуси",
                5: "Безкоштовний товар",
                6: "Набір товарів",
                7: "Кешбек",
                8: "Мультиплікатор бонусів",
                9: "Знижка на набір"
            },
            'value_type': {
                0: "Фіксоване значення",
                1: "Вираз"
            },
            'discount_value_type': {
                0: "Відсоток",
                1: "Сума"
            },
            'discount_time_type': {
                0: "Разом",
                1: "За одиницю"
            },
            'sort_items_mode': {
                0: "За зростанням ціни",
                1: "За спаданням ціни"
            }
        }


class SQLiteManager:
    """Менеджер для работы с SQLite базой данных"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None
    
    async def connect(self):
        """Открытие соединения с БД"""
        self.conn = await aiosqlite.connect(self.db_path)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA foreign_keys = OFF")
        await self.conn.execute("PRAGMA journal_mode = WAL")
        await self.conn.commit()
        logger.info(f"Подключено к БД: {self.db_path}")
    
    async def enable_foreign_keys(self):
        """Включение проверки внешних ключей после загрузки"""
        await self.conn.execute("PRAGMA foreign_keys = ON")
        await self.conn.commit()
        logger.info("Внешние ключи включены")
    
    async def close(self):
        """Закрытие соединения"""
        if self.conn:
            await self.conn.close()
            logger.info("Соединение с БД закрыто")
    
    async def create_schema(self):
        """Создание схемы БД с внешними ключами"""
        
        # 1. Справочные таблицы маппинга (создаются первыми)
        await self.conn.executescript("""
            -- Справочник статусов
            CREATE TABLE IF NOT EXISTS mapping_status (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            
            -- Справочник уровней изоляции
            CREATE TABLE IF NOT EXISTS mapping_isolation_level (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            
            -- Справочник режимов применения
            CREATE TABLE IF NOT EXISTS mapping_apply_mode (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            
            -- Справочник режимов планирования
            CREATE TABLE IF NOT EXISTS mapping_scheduling_mode (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            
            -- Справочник типов данных для условий правил
            CREATE TABLE IF NOT EXISTS mapping_data_values (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            
            -- Справочник операторов сравнения
            CREATE TABLE IF NOT EXISTS mapping_operators (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            
            -- Справочник типов продуктовых условий
            CREATE TABLE IF NOT EXISTS mapping_product_values (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            
            -- Справочник типов условий
            CREATE TABLE IF NOT EXISTS mapping_cond_values (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            
            -- Справочник режимов группового применения
            CREATE TABLE IF NOT EXISTS mapping_group_apply_mode (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            
            -- Справочник типов результатов
            CREATE TABLE IF NOT EXISTS mapping_result_type (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            
            -- Справочник типов значений
            CREATE TABLE IF NOT EXISTS mapping_value_type (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            
            -- Справочник типов значений скидки
            CREATE TABLE IF NOT EXISTS mapping_discount_value_type (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            
            -- Справочник типов времени скидки
            CREATE TABLE IF NOT EXISTS mapping_discount_time_type (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            
            -- Справочник режимов сортировки товаров
            CREATE TABLE IF NOT EXISTS mapping_sort_items_mode (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
        """)
        
        # 2. Справочники торговых сетей, локаций, терминалов
        await self.conn.executescript("""
            -- Справочник торговых сетей
            CREATE TABLE IF NOT EXISTS merchants (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                ext_code TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_merchants_name ON merchants(name);
            
            -- Справочник локаций
            CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                merchant_id INTEGER,
                merchant_name TEXT,
                ext_code TEXT,
                address TEXT,
                FOREIGN KEY (merchant_id) REFERENCES merchants(id)
            );
            CREATE INDEX IF NOT EXISTS idx_locations_name ON locations(name);
            CREATE INDEX IF NOT EXISTS idx_locations_merchant ON locations(merchant_id);
            
            -- Справочник терминалов
            CREATE TABLE IF NOT EXISTS terminals (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                location_id INTEGER,
                ext_code TEXT,
                FOREIGN KEY (location_id) REFERENCES locations(id)
            );
            CREATE INDEX IF NOT EXISTS idx_terminals_name ON terminals(name);
            CREATE INDEX IF NOT EXISTS idx_terminals_location ON terminals(location_id);
            
            -- Справочник наборов товаров
            CREATE TABLE IF NOT EXISTS sku_sets (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                ext_code TEXT,
                pos_name TEXT,
                removed INTEGER DEFAULT 0,
                only_product INTEGER DEFAULT 0,
                only_fuel INTEGER DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_sku_sets_name ON sku_sets(name);
        """)
        
        # 3. Основная таблица правил скидок (с FK на справочники)
        await self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS discount_rules (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                comment TEXT,
                pos_message TEXT,
                description TEXT,
                operator_message TEXT,
                operator_id INTEGER,
                operator_id_desc TEXT,
                begin_date TEXT,
                end_date TEXT,
                status INTEGER NOT NULL DEFAULT 0,
                priority INTEGER,
                isolation_level INTEGER,
                apply_mode INTEGER,
                only_message_mode INTEGER DEFAULT 0,
                scheduling_mode INTEGER,
                is_for_dc_gen INTEGER DEFAULT 0,
                exclude_sku_set_id INTEGER,
                exclude_sku_set_id_desc TEXT,
                ext_code TEXT,
                min_match_count INTEGER,
                FOREIGN KEY (status) REFERENCES mapping_status(id),
                FOREIGN KEY (isolation_level) REFERENCES mapping_isolation_level(id),
                FOREIGN KEY (apply_mode) REFERENCES mapping_apply_mode(id),
                FOREIGN KEY (scheduling_mode) REFERENCES mapping_scheduling_mode(id),
                FOREIGN KEY (exclude_sku_set_id) REFERENCES sku_sets(id)
            );
            CREATE INDEX IF NOT EXISTS idx_discount_rules_name ON discount_rules(name);
            CREATE INDEX IF NOT EXISTS idx_discount_rules_status ON discount_rules(status);
            CREATE INDEX IF NOT EXISTS idx_discount_rules_dates ON discount_rules(begin_date, end_date);
        """)
        
        # 4. Условия применения правил
        await self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS rule_conditions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                discount_rule_id INTEGER NOT NULL,
                condition_type INTEGER NOT NULL,
                comparison_type INTEGER NOT NULL,
                value TEXT,
                group_name TEXT,
                FOREIGN KEY (discount_rule_id) REFERENCES discount_rules(id) ON DELETE CASCADE,
                FOREIGN KEY (condition_type) REFERENCES mapping_data_values(id),
                FOREIGN KEY (comparison_type) REFERENCES mapping_operators(id)
            );
            CREATE INDEX IF NOT EXISTS idx_rule_conditions_rule ON rule_conditions(discount_rule_id);
        """)
        
        # 5. Условия на чек/товары
        await self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS order_conditions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                discount_rule_id INTEGER NOT NULL,
                sku_set_id INTEGER,
                exclude_sku_set_id INTEGER,
                condition_type INTEGER NOT NULL,
                comparison_type INTEGER NOT NULL,
                value TEXT,
                group_name TEXT,
                FOREIGN KEY (discount_rule_id) REFERENCES discount_rules(id) ON DELETE CASCADE,
                FOREIGN KEY (sku_set_id) REFERENCES sku_sets(id),
                FOREIGN KEY (exclude_sku_set_id) REFERENCES sku_sets(id),
                FOREIGN KEY (condition_type) REFERENCES mapping_product_values(id),
                FOREIGN KEY (comparison_type) REFERENCES mapping_operators(id)
            );
            CREATE INDEX IF NOT EXISTS idx_order_conditions_rule ON order_conditions(discount_rule_id);
        """)
        
        # 6. Результаты применения скидок
        await self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS result_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                discount_rule_id INTEGER NOT NULL,
                result_type INTEGER NOT NULL,
                comparison_type INTEGER,
                value_type INTEGER,
                fixed_value REAL,
                expression TEXT,
                discount_value_type INTEGER,
                discount_time_type INTEGER,
                sku_set_id INTEGER,
                except_sku_set_id INTEGER,
                sort_items_mode INTEGER,
                group_apply_mode INTEGER,
                FOREIGN KEY (discount_rule_id) REFERENCES discount_rules(id) ON DELETE CASCADE,
                FOREIGN KEY (result_type) REFERENCES mapping_result_type(id),
                FOREIGN KEY (comparison_type) REFERENCES mapping_operators(id),
                FOREIGN KEY (value_type) REFERENCES mapping_value_type(id),
                FOREIGN KEY (discount_value_type) REFERENCES mapping_discount_value_type(id),
                FOREIGN KEY (discount_time_type) REFERENCES mapping_discount_time_type(id),
                FOREIGN KEY (sku_set_id) REFERENCES sku_sets(id),
                FOREIGN KEY (except_sku_set_id) REFERENCES sku_sets(id),
                FOREIGN KEY (sort_items_mode) REFERENCES mapping_sort_items_mode(id),
                FOREIGN KEY (group_apply_mode) REFERENCES mapping_group_apply_mode(id)
            );
            CREATE INDEX IF NOT EXISTS idx_result_items_rule ON result_items(discount_rule_id);
        """)
        
        # 7. Условия внутри результатов
        await self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS result_item_conditions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                result_item_id INTEGER NOT NULL,
                condition_type INTEGER NOT NULL,
                value TEXT,
                FOREIGN KEY (result_item_id) REFERENCES result_items(id) ON DELETE CASCADE,
                FOREIGN KEY (condition_type) REFERENCES mapping_cond_values(id)
            );
            CREATE INDEX IF NOT EXISTS idx_result_item_conditions_item ON result_item_conditions(result_item_id);
        """)
        
        await self.conn.commit()
        logger.info("Схема БД создана успешно")
    
    async def load_mapping_tables(self):
        """Загрузка справочных таблиц маппинга"""
        mappings = MappingLoader.get_mappings()
        
        for table_suffix, data in mappings.items():
            table_name = f"mapping_{table_suffix}"
            
            # Очистка таблицы
            await self.conn.execute(f"DELETE FROM {table_name}")
            
            # Вставка данных
            for id_val, name_val in data.items():
                await self.conn.execute(
                    f"INSERT OR REPLACE INTO {table_name} (id, name) VALUES (?, ?)",
                    (id_val, name_val)
                )
            
            logger.info(f"Загружено {len(data)} записей в {table_name}")
        
        await self.conn.commit()


class DiscountRulesAPI:
    """HTTP клиент для работы с API правил скидок"""
    
    def __init__(self):
        self.base_url = Config.BASE_URL
        self.session: Optional[aiohttp.ClientSession] = None
        self.cookies = None
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
    
    async def __aenter__(self):
        connector = aiohttp.TCPConnector(ssl=self.ssl_context)
        self.session = aiohttp.ClientSession(connector=connector)
        await self.login()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def login(self):
        """Авторизация в системе"""
        url = f"{self.base_url}{Config.ENDPOINTS['login']}"
        payload = {
            "username": Config.USERNAME,
            "password": Config.PASSWORD
        }
        
        logger.debug(f"Авторизация: POST {url}")
        logger.debug(f"Payload: {payload}")
        
        async with self.session.post(url, json=payload) as response:
            response_text = await response.text()
            logger.debug(f"Статус авторизации: {response.status}")
            logger.debug(f"Ответ: {response_text[:500]}")
            
            if response.status == 200:
                # Сохраняем cookies из ответа
                self.cookies = response.cookies
                logger.info("Успешная авторизация")
                logger.debug(f"Полученные cookies: {self.cookies}")
            else:
                raise Exception(f"Ошибка авторизации: {response.status}")
    
    async def fetch_data(self, endpoint: str, sort_field: str = "name") -> List[Dict]:
        """Получение данных с пагинацией"""
        url = f"{self.base_url}{endpoint}"
        all_data = []
        offset = 0
        
        # Заголовки как в рабочем коде
        headers = {
            'accept': '*/*',
            'content-type': 'application/json',
            'origin': self.base_url,
            'referer': f"{self.base_url}/",
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        while True:
            payload = {
                "count": Config.BATCH_SIZE,
                "offset": offset,
                "filter": {},
                "period": {},
                "sort": {
                    "fields": [{"field": sort_field, "asc": True}]  # asc: True как в старом коде
                }
            }
            
            logger.debug(f"Запрос к {endpoint}: offset={offset}")
            logger.debug(f"Payload: {json.dumps(payload, ensure_ascii=False)}")
            logger.debug(f"Headers: {headers}")
            logger.debug(f"Cookies: {self.cookies}")
            
            # Передаем cookies и headers явно
            async with self.session.post(url, json=payload, headers=headers, cookies=self.cookies) as response:
                response_text = await response.text()
                
                logger.debug(f"Статус ответа от {endpoint}: {response.status}")
                logger.debug(f"Первые 1000 символов ответа: {response_text[:1000]}")
                
                if response.status != 200:
                    logger.error(f"Ошибка запроса {endpoint}: {response.status}")
                    logger.error(f"Полный ответ: {response_text}")
                    break
                
                try:
                    data = json.loads(response_text)
                    logger.debug(f"Структура ответа: {list(data.keys()) if isinstance(data, dict) else type(data)}")
                    
                    items = data.get('data', [])
                    total_count = data.get('count', 0)
                    
                    if not items:
                        logger.warning(f"Нет данных в поле 'data' для {endpoint}")
                        logger.debug(f"Полный ответ: {json.dumps(data, ensure_ascii=False, indent=2)[:2000]}")
                        break
                    
                    all_data.extend(items)
                    logger.info(f"Получено {len(items)} записей из {endpoint} (offset: {offset}, total: {total_count})")
                    
                    if len(items) < Config.BATCH_SIZE:
                        break
                    
                    offset += Config.BATCH_SIZE
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Ошибка парсинга JSON из {endpoint}: {e}")
                    logger.error(f"Ответ: {response_text[:1000]}")
                    break
        
        logger.info(f"Всего получено {len(all_data)} записей из {endpoint}")
        return all_data


class DataProcessor:
    """Обработчик данных для трансформации и очистки"""
    
    @staticmethod
    def timestamp_to_datetime(timestamp: Optional[int]) -> Optional[str]:
        """Конвертация Unix timestamp в формат YYYY-MM-DD-HH-MM"""
        if not timestamp:
            return None
        
        try:
            dt = datetime.fromtimestamp(timestamp / 1000, tz=pytz.UTC)
            return dt.strftime("%Y-%m-%d-%H-%M")
        except Exception as e:
            logger.warning(f"Ошибка конвертации timestamp {timestamp}: {e}")
            return None
    
    @staticmethod
    def parse_value_field(value_str: Optional[str]) -> Optional[str]:
        """Парсинг поля value для извлечения ids или сохранения как JSON"""
        if not value_str:
            return None
        
        try:
            value_obj = json.loads(value_str)
            
            # Если есть ids - возвращаем их как JSON массив
            if isinstance(value_obj, dict) and 'ids' in value_obj:
                return json.dumps(value_obj['ids'])
            
            # Если есть id - возвращаем его
            if isinstance(value_obj, dict) and 'id' in value_obj:
                return str(value_obj['id'])
            
            # Если есть value - возвращаем его
            if isinstance(value_obj, dict) and 'value' in value_obj:
                return str(value_obj['value'])
            
            # Иначе возвращаем весь объект как JSON
            return json.dumps(value_obj)
            
        except Exception as e:
            logger.warning(f"Ошибка парсинга value: {value_str[:100] if value_str else 'None'}... - {e}")
            return value_str


class ETLPipeline:
    """Главный класс для управления ETL процессом"""
    
    def __init__(self):
        self.db = SQLiteManager(Config.DB_PATH)
        self.reference_cache = {
            'locations': {},
            'merchants': {},
            'terminals': {},
            'sku_sets': {}
        }
    
    async def run(self):
        """Запуск ETL процесса"""
        try:
            logger.info("=" * 80)
            logger.info("СТАРТ ETL ПРОЦЕССА")
            logger.info("=" * 80)
            
            # 1. Подключение к БД
            await self.db.connect()
            
            # 2. Создание схемы
            await self.db.create_schema()
            
            # 3. Загрузка справочников маппинга
            await self.db.load_mapping_tables()
            
            # 4. Работа с API
            async with DiscountRulesAPI() as api:
                # 5. Загрузка справочников
                await self.load_references(api)
                
                # 6. Загрузка правил скидок
                await self.load_discount_rules(api)
            
            # 7. Включаем FK после загрузки всех данных
            await self.db.enable_foreign_keys()
            
            logger.info("=" * 80)
            logger.info("ETL ПРОЦЕСС ЗАВЕРШЕН УСПЕШНО")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"Ошибка в ETL процессе: {e}", exc_info=True)
            raise
        finally:
            await self.db.close()
    
    async def load_references(self, api: DiscountRulesAPI):
        """Загрузка справочников"""
        
        # Merchants
        logger.info("Загрузка merchants...")
        merchants = await api.fetch_data(Config.ENDPOINTS['merchants'])
        await self.db.conn.execute("DELETE FROM merchants")
        
        for merchant in merchants:
            await self.db.conn.execute(
                """INSERT OR REPLACE INTO merchants (id, name, ext_code)
                   VALUES (?, ?, ?)""",
                (merchant.get('id'), merchant.get('name'), merchant.get('extCode'))
            )
            self.reference_cache['merchants'][merchant['id']] = merchant.get('name')
        
        await self.db.conn.commit()
        logger.info(f"Загружено {len(merchants)} merchants")
        
        # Locations
        logger.info("Загрузка locations...")
        locations = await api.fetch_data(Config.ENDPOINTS['locations'])
        await self.db.conn.execute("DELETE FROM locations")
        
        for location in locations:
            merchant_id = location.get('merchantId')
            await self.db.conn.execute(
                """INSERT OR REPLACE INTO locations 
                   (id, name, merchant_id, merchant_name, ext_code, address)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    location.get('id'),
                    location.get('name'),
                    merchant_id,
                    self.reference_cache['merchants'].get(merchant_id),
                    location.get('extCode'),
                    location.get('address')
                )
            )
            self.reference_cache['locations'][location['id']] = location.get('name')
        
        await self.db.conn.commit()
        logger.info(f"Загружено {len(locations)} locations")
        
        # Terminals
        logger.info("Загрузка terminals...")
        terminals = await api.fetch_data(Config.ENDPOINTS['terminals'])
        await self.db.conn.execute("DELETE FROM terminals")
        
        for terminal in terminals:
            await self.db.conn.execute(
                """INSERT OR REPLACE INTO terminals (id, name, location_id, ext_code)
                   VALUES (?, ?, ?, ?)""",
                (
                    terminal.get('id'),
                    terminal.get('name'),
                    terminal.get('locationId'),
                    terminal.get('extCode')
                )
            )
            self.reference_cache['terminals'][terminal['id']] = terminal.get('name')
        
        await self.db.conn.commit()
        logger.info(f"Загружено {len(terminals)} terminals")
        
        # SKU Sets
        logger.info("Загрузка sku_sets...")
        sku_sets = await api.fetch_data(Config.ENDPOINTS['sku_sets'])
        await self.db.conn.execute("DELETE FROM sku_sets")
        
        for sku_set in sku_sets:
            await self.db.conn.execute(
                """INSERT OR REPLACE INTO sku_sets 
                   (id, name, ext_code, pos_name, removed, only_product, only_fuel)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    sku_set.get('id'),
                    sku_set.get('name'),
                    sku_set.get('extCode'),
                    sku_set.get('posName'),
                    sku_set.get('removed', 0),
                    sku_set.get('onlyProduct', 0),
                    sku_set.get('onlyFuel', 0)
                )
            )
            self.reference_cache['sku_sets'][sku_set['id']] = sku_set.get('name')
        
        await self.db.conn.commit()
        logger.info(f"Загружено {len(sku_sets)} sku_sets")
    
    async def load_discount_rules(self, api: DiscountRulesAPI):
        """Загрузка правил скидок"""
        logger.info("Загрузка discount_rules...")
        
        rules = await api.fetch_data(Config.ENDPOINTS['discount_rules'], sort_field='priority')
        
        # Очистка таблиц
        await self.db.conn.execute("DELETE FROM result_item_conditions")
        await self.db.conn.execute("DELETE FROM result_items")
        await self.db.conn.execute("DELETE FROM order_conditions")
        await self.db.conn.execute("DELETE FROM rule_conditions")
        await self.db.conn.execute("DELETE FROM discount_rules")
        await self.db.conn.commit()
        
        for idx, rule in enumerate(rules, 1):
            try:
                await self.process_single_rule(rule)
                if idx % 10 == 0:
                    logger.info(f"Обработано {idx}/{len(rules)} правил")
            except Exception as e:
                logger.error(f"Ошибка обработки правила {rule.get('id')}: {e}", exc_info=True)
                continue
        
        await self.db.conn.commit()
        logger.info(f"Обработано {len(rules)} правил скидок")
    
    async def process_single_rule(self, rule: Dict):
        """Обработка одного правила скидки"""
        rule_id = rule.get('id')
        
        # 1. Сохранение основного правила
        await self.db.conn.execute(
            """INSERT OR REPLACE INTO discount_rules (
                id, name, comment, pos_message, description, operator_message,
                operator_id, operator_id_desc, begin_date, end_date, status, priority,
                isolation_level, apply_mode, only_message_mode, scheduling_mode,
                is_for_dc_gen, exclude_sku_set_id, exclude_sku_set_id_desc,
                ext_code, min_match_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                rule_id,
                rule.get('name'),
                rule.get('comment'),
                rule.get('posMessage'),
                rule.get('description'),
                rule.get('operatorMessage'),
                rule.get('operatorId'),
                rule.get('operatorIdDesc'),
                DataProcessor.timestamp_to_datetime(rule.get('beginDate')),
                DataProcessor.timestamp_to_datetime(rule.get('endDate')),
                rule.get('status', 0),
                rule.get('priority'),
                rule.get('isolationLevel'),
                rule.get('applyMode'),
                rule.get('onlyMessageMode', 0),
                rule.get('schedulingMode'),
                1 if rule.get('isForDcGen') else 0,
                rule.get('excludeSkuSetId'),
                rule.get('excludeSkuSetIdDesc'),
                rule.get('extCode'),
                rule.get('ruleConditionGroup', {}).get('minMatchCount') if rule.get('ruleConditionGroup') else None
            )
        )
        
        # 2. Условия применения правил (ruleConditionGroup)
        rule_condition_group = rule.get('ruleConditionGroup')
        if rule_condition_group:
            required_conditions = rule_condition_group.get('requiredConditions', []) or []
            
            for cond in required_conditions:
                await self.db.conn.execute(
                    """INSERT INTO rule_conditions 
                       (discount_rule_id, condition_type, comparison_type, value, group_name)
                       VALUES (?, ?, ?, ?, ?)""",
                    (
                        rule_id,
                        cond.get('type'),
                        cond.get('comparsionType'),
                        DataProcessor.parse_value_field(cond.get('value')),
                        cond.get('group', '0')
                    )
                )
        
        # 3. Условия на чек (orderConditionGroup)
        order_condition_group = rule.get('orderConditionGroup')
        if order_condition_group:
            required_conditions = order_condition_group.get('requiredConditions', []) or []
            
            for cond in required_conditions:
                await self.db.conn.execute(
                    """INSERT INTO order_conditions 
                       (discount_rule_id, sku_set_id, exclude_sku_set_id, 
                        condition_type, comparison_type, value, group_name)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        rule_id,
                        cond.get('skuSetId'),
                        cond.get('excludeSkuSetId'),
                        cond.get('type'),
                        cond.get('comparsionType'),
                        DataProcessor.parse_value_field(cond.get('value')),
                        cond.get('group', '0')
                    )
                )
        
        # 4. Результаты (resultScaleItems)
        result_scale_items = rule.get('resultScaleItems', []) or []
        
        for scale_item in result_scale_items:
            result_type = scale_item.get('type')
            results = scale_item.get('results', []) or []
            
            for result in results:
                restriction = result.get('restriction') or {}
                
                # Вставка результата
                cursor = await self.db.conn.execute(
                    """INSERT INTO result_items (
                        discount_rule_id, result_type, comparison_type, value_type,
                        fixed_value, expression, discount_value_type, discount_time_type,
                        sku_set_id, except_sku_set_id, sort_items_mode, group_apply_mode
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        rule_id,
                        result_type,
                        result.get('comparsionType'),
                        result.get('valueType'),
                        result.get('fixedValue'),
                        result.get('expression'),
                        result.get('discountValueType'),
                        result.get('discountTimeType'),
                        restriction.get('skuSetId'),
                        restriction.get('exceptSkuSetId'),
                        restriction.get('sortItemsMode'),
                        restriction.get('groupApplyMode')
                    )
                )
                
                result_item_id = cursor.lastrowid
                
                # 5. Условия внутри результата (restriction.conditions)
                conditions = restriction.get('conditions', []) or []
                
                for cond in conditions:
                    await self.db.conn.execute(
                        """INSERT INTO result_item_conditions 
                           (result_item_id, condition_type, value)
                           VALUES (?, ?, ?)""",
                        (
                            result_item_id,
                            cond.get('type'),
                            DataProcessor.parse_value_field(cond.get('value'))
                        )
                    )


async def main():
    """Главная функция"""
    pipeline = ETLPipeline()
    await pipeline.run()


if __name__ == "__main__":
    asyncio.run(main())