import json
import asyncio
import re
import logging
import colorlog
from telethon import TelegramClient, events
import requests
import os
from datetime import datetime, timedelta
from collections import deque

# Храним последние сообщения для каждого канала по времени (10 минут)
recent_messages_by_channel = {}

# Глобальная очередь для хранения последних 5 общих сообщений
last_messages = deque(maxlen=5)

message_ids_with_sent_contracts = set()

original_message_texts = {}

original_contracts = {}

message_processing_lock = asyncio.Lock()

# Глобальная переменная для текущего режима
current_mode = None
mode_lock = asyncio.Lock()

# Создание глобального списка для хранения ID сообщений, с которых уже был отправлен контракт
messages_with_sent_contract = set()

# Глобальная блокировка для работы с контрактами
contracts_lock = asyncio.Lock()

# Настройка логирования с цветами
log_colors_config = {
    'DEBUG': 'cyan',
    'INFO': 'green',
    'WARNING': 'yellow',
    'ERROR': 'red',
    'CONTRACT': 'bold_purple',  # Фиолетовый цвет для уровня CONTRACT
    'CRITICAL': 'bold_red',
    'CHANNEL': 'blue',
    'MESSAGE': 'white',
}

handler = colorlog.StreamHandler()
formatter = colorlog.ColoredFormatter(
    "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S',
    log_colors=log_colors_config
)
handler.setFormatter(formatter)

logger = logging.getLogger('caller_ebun')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Добавляем новый уровень логирования CONTRACT
CONTRACT_LOG_LEVEL = 25
logging.addLevelName(CONTRACT_LOG_LEVEL, "CONTRACT")

def contract(self, message, *args, **kwargs):
    if self.isEnabledFor(CONTRACT_LOG_LEVEL):
        self._log(CONTRACT_LOG_LEVEL, message, args, **kwargs)

logging.Logger.contract = contract

# Объявление функций логирования
def log_contract(message):
    logger.contract(message)

def log_channel(message):
    logger.log(35, message)

def log_message_preview(channel_name, message_preview):
    logger.warning(f"Пропускаем сообщение из канала \033[94m{channel_name}\033[0m. Сообщение (превью): \033[97m{message_preview}\033[0m")

with open('config.json', encoding='utf-8') as f:
    config = json.load(f)

api_id = config['api_id']
api_hash = config['api_hash']
bot_username = config['bot_username']
bot_token = config['bot_token']
chat_id = config['chat_id']

contracts_file = 'contracts.json'

# Функция для отправки отчета через бот в более красивом формате с гиперссылкой
async def send_report_to_telegram(message):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(url, data=payload)
        if response.status_code == 200:
            logger.info("Отчет успешно отправлен через Telegram.")
        else:
            logger.error(f"Ошибка отправки отчета через Telegram. Статус: {response.status_code}")
    except Exception as e:
        logger.error(f"Ошибка при отправке отчета: {str(e)}")

# Загрузка контрактов с датами добавления
def load_sent_contracts():
    if os.path.exists(contracts_file):
        with open(contracts_file, 'r', encoding='utf-8') as f:
            contracts_data = json.load(f)
            contracts = {item['contract'].lower(): item['added_on'] for item in contracts_data}  # Приводим контракт к нижнему регистру
            logger.info(f"CALLER EBUN загрузил {len(contracts)} контрактов из файла. Готов работать дальше!")
            return contracts
    logger.info("CALLER EBUN не нашёл файл с контрактами, стартуем с нуля!")
    return {}

# Сохранение контрактов с датами
async def save_sent_contracts(sent_contracts):
    async with contracts_lock:  # Блокировка во время записи
        contracts_data = [{'contract': contract, 'added_on': added_on} for contract, added_on in sent_contracts.items()]
        with open(contracts_file, 'w', encoding='utf-8') as f:
            json.dump(contracts_data, f, indent=4)
        logger.info(f"CALLER EBUN зафиксировал {len(sent_contracts)} контрактов в файл. Всё под контролем!")

sent_contracts = load_sent_contracts()

# Функция для логирования полного текста сообщения
def log_full_message(channel_name, full_message):
    try:
        with open("full_messages.log", "a", encoding='utf-8') as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Полный текст сообщения из канала {channel_name}:\n{full_message}\n\n")
    except Exception as e:
        logger.error(f"Ошибка при записи полного текста сообщения в файл: {str(e)}")

# Функция для парсинга контрактов из текста сообщения
def parse_contracts(text):
    pump_contracts = set()
    regular_contracts = set()
    dexscreener_contracts = set()
    dextools_contracts = set()

    # Разделяем текст на строки
    lines = text.splitlines()
    for line in lines:
        # Поиск контрактов с префиксом pump
        pump_match = re.findall(r'\b([A-Za-z0-9]{27,40}pump)\b', line)
        for contract in pump_match:
            pump_contracts.add(contract.lower())  # Для внутренних операций используем нижний регистр
            original_contracts[contract.lower()] = contract  # Сохраняем оригинальный контракт

        # Поиск обычных контрактов Solana (не 0x)
        regular_match = re.findall(r'\b[A-Za-z0-9]{32,44}\b', line)
        for token in regular_match:
            if token.startswith("0x"):
                continue
            if token.lower() not in pump_contracts:
                regular_contracts.add(token.lower())
                original_contracts[token.lower()] = token  # Сохраняем оригинальный контракт

        # Поиск контрактов со ссылками dexscreener
        dexscreener_match = re.findall(r'https://dexscreener.com/solana/([A-Za-z0-9]{32,44})', line)
        for contract in dexscreener_match:
            dexscreener_contracts.add(contract.lower())
            original_contracts[contract.lower()] = contract  # Сохраняем оригинальный контракт

        # Поиск контрактов со ссылками dextools
        dextools_match = re.findall(r'https://www.dextools.io/app/solana/pair-explorer/([A-Za-z0-9]{32,44})', line)
        for contract in dextools_match:
            dextools_contracts.add(contract.lower())
            original_contracts[contract.lower()] = contract  # Сохраняем оригинальный контракт

    return pump_contracts, regular_contracts, dexscreener_contracts, dextools_contracts

# Функция для парсинга истории сообщений только по каналам из конфигурации
async def fetch_contracts_from_history(client):
    logger.info("CALLER EBUN начал парсинг истории...")

    for channel in config['channels']:
        try:
            logger.info(f"Парсим канал: {channel}")

            async for message in client.iter_messages(channel, offset_date=datetime.now() - timedelta(days=7)):
                if not message.raw_text:
                    continue

                pump_contracts, regular_contracts, dexscreener_contracts, dextools_contracts = parse_contracts(message.raw_text)

                all_contracts = pump_contracts | regular_contracts | dexscreener_contracts | dextools_contracts

                new_contracts_count = 0
                for contract in all_contracts:
                    if contract not in sent_contracts:
                        sent_contracts[contract] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        logger.info(f"CALLER EBUN записал контракт в дубликаты: {contract} (канал: {channel})")
                        new_contracts_count += 1
                    else:
                        logger.warning(f"Контракт {contract} уже существует. Пропускаем дубликат! (канал: {channel})")

                if new_contracts_count > 0:
                    logger.info(f"CALLER EBUN добавил {new_contracts_count} новых контрактов. Общее количество контрактов: {len(sent_contracts)}")

        except Exception as e:
            logger.error(f"Ошибка при парсинге канала {channel}: {str(e)}")

    await save_sent_contracts(sent_contracts)
    logger.info("CALLER EBUN завершил парсинг истории.")

# Функция для проверки пропускаемых фраз
def contains_skip_phrase(text):
    skip_phrases = config.get('skip_phrases', [])
    for phrase in skip_phrases:
        if phrase.lower() in text.lower():
            return True
    return False

# Функция отправки контракта в бота с приоритетами
# Функция отправки контракта в бота с приоритетами
# Функция отправки контракта в бота с приоритетами
# Функция отправки контракта в бота с приоритетами
# Функция отправки контракта в бота с приоритетами
async def send_contract_to_bot(client, contract, channel_name, message_id, contract_type="regular"):
    async with mode_lock:
        mode = current_mode

    if mode != '1':
        logger.info(f"Режим {mode}: контракт {contract} не будет отправлен боту.")
        return False

    # Проверяем, был ли контракт уже обработан
    if contract in sent_contracts:
        logger.warning(f"Контракт {contract} уже существует. Пропускаем дубликат!")
        return False

    # Получаем оригинальный контракт для отправки
    original_contract = original_contracts.get(contract, contract)  # Используем оригинальный контракт, если он существует

    if contract not in sent_contracts:
        if contract_type == "pump":
            logger.contract(f"CALLER EBUN нашёл контракт с приоритетом (pump): {original_contract}. Начинаем EBUN коллера, LFG!")
        elif contract_type == "dexscreener":
            logger.contract(f"CALLER EBUN нашёл контракт dexscreener: {original_contract}. Начинаем EBUN коллера, LFG!")
        elif contract_type == "dextools":
            logger.contract(f"CALLER EBUN нашёл контракт dextools: {original_contract}. Начинаем EBUN коллера, LFG!")
        else:
            logger.contract(f"CALLER EBUN нашёл контракт: {original_contract}. Начинаем EBUN коллера, LFG!")

        max_retries = 3
        for attempt in range(max_retries):
            try:
                await client.send_message(bot_username, original_contract)  # Отправляем оригинальный контракт
                sent_contracts[contract] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # ВАЖНО: Сразу добавляем ID сообщения в список отправленных контрактов
                message_ids_with_sent_contracts.add(message_id)

                await save_sent_contracts(sent_contracts)
                telegram_message = f"*CALLER* успешно отправил контракт:`\n{original_contract}`\n\n[Photon](https://photon-sol.tinyastro.io/en/lp/{original_contract}) | [Dexscreener](https://dexscreener.com/solana/{original_contract}) | [BullX](https://bullx.io/terminal?chainId=1399811149&address={original_contract}) | [Jupiter](https://jup.ag/swap/{original_contract}-SOL)"
                await send_report_to_telegram(telegram_message)
                return True
            except Exception as e:
                logger.error(f"Ошибка при отправке контракта: {str(e)}. Попытка {attempt + 1} из {max_retries}.")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    logger.error(f"Не удалось отправить контракт после {max_retries} попыток.")
        return False
    else:
        logger.warning(f"Контракт {contract} уже существует. Пропускаем дубликат!")
        return False

# Функция для очистки старых контрактов (старше 2 недель)
def clean_old_contracts():
    logger.info("CALLER EBUN начал очистку старых контрактов...")
    two_weeks_ago = datetime.now() - timedelta(days=14)
    old_contracts = [contract for contract, added_on in sent_contracts.items() 
                     if datetime.strptime(added_on, '%Y-%m-%d %H:%M:%S') < two_weeks_ago]

    if old_contracts:
        for contract in old_contracts:
            del sent_contracts[contract]
            logger.info(f"CALLER EBUN удалил контракт {contract}, так как он старше 2 недель.")
        asyncio.run(save_sent_contracts(sent_contracts))
        logger.info(f"CALLER EBUN завершил очистку. Удалено {len(old_contracts)} контрактов.")
    else:
        logger.info("CALLER EBUN не нашёл контрактов старше 2 недель для удаления.")

# Создание глобальной переменной для хранения ID обработанных сообщений
processed_message_ids = []  # Переменная для хранения ID сообщений, которые уже были обработаны

def get_priority_contract(pump_contracts, regular_contracts, dexscreener_contracts, dextools_contracts):
    if pump_contracts:
        return next(iter(pump_contracts)), "pump"
    elif regular_contracts:
        return next(iter(regular_contracts)), "regular"
    elif dexscreener_contracts:
        return next(iter(dexscreener_contracts)), "dexscreener"
    elif dextools_contracts:
        return next(iter(dextools_contracts)), "dextools"
    return None, None
    
# Основной режим работы: обработка новых сообщений и изменённых
# Основной режим работы: обработка новых сообщений и изменённых
# Основной режим работы: обработка новых сообщений и изменённых
# Основной режим работы: обработка новых сообщений и изменённых
# Глобальный словарь для хранения оригинальных текстов сообщени

async def monitor_new_messages(client):
    @client.on(events.NewMessage(chats=config['channels']))
    async def new_message_listener(event):
        async with message_processing_lock:  # Используем блокировку для предотвращения конкурентного выполнения
            # Добавляем новое сообщение в очередь последних 5 сообщений
            last_messages.append(event.message.id)

            # Проверяем, был ли контракт с этого сообщения уже отправлен
            if event.message.id in message_ids_with_sent_contracts:
                logger.warning(f"Контракт с этого сообщения ({event.message.id}) уже был отправлен. Пропускаем повторную обработку!")
                return  # Пропускаем дальнейшую обработку сообщения

            # Сохраняем оригинальный текст сообщения
            original_message_texts[event.message.id] = event.message.message

            # Дальнейшая логика обработки нового сообщения...
            try:
                logger.debug(f"Новое сообщение получено в канале {event.chat.title if event.chat else 'Unknown Channel'}. Текст сообщения: {event.raw_text}")

                # Очистка текста перед парсингом
                cleaned_text = event.raw_text
                log_full_message(event.chat.title if event.chat else 'Unknown Channel', cleaned_text)

                if event.media:
                    logger.debug("Сообщение содержит медиафайл.")

                message_preview = cleaned_text.split('\n')[0]
                preview_for_log = message_preview[:100] + '...' if len(message_preview) > 100 else message_preview

                # Парсим контракты
                pump_contracts, regular_contracts, dexscreener_contracts, dextools_contracts = parse_contracts(cleaned_text)
                all_contracts = pump_contracts | regular_contracts | dexscreener_contracts | dextools_contracts

                # Проверка на пропускаемые фразы
                if contains_skip_phrase(cleaned_text):
                    logger.warning("Сообщение содержит пропускаемую фразу. Пропускаем его...")

                    # Добавляем контракты в дубликаты
                    for contract in all_contracts:
                        if contract not in sent_contracts:
                            sent_contracts[contract] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            logger.warning(f"CALLER EBUN записал контракт в дубликаты: {contract}")
                        else:
                            logger.warning(f"Контракт {contract} уже существует. Пропускаем дубликат!")
                    await save_sent_contracts(sent_contracts)
                    return  # Пропускаем дальнейшую обработку сообщения

                if not all_contracts:
                    log_message_preview(event.chat.title if event.chat else 'Unknown Channel', preview_for_log)
                    logger.warning("ДВЕ повторных проверки не нашли контракт... ЕБЁМ дальше, ЛЕТСАГО!")
                    return

                sent_contract = None
                found_new_contract = False

                # Получаем username канала для создания ссылки
                channel_username = event.chat.username if event.chat and event.chat.username else 'Unknown'

                # Приоритет отправки контрактов
                if pump_contracts:
                    contract = next(iter(pump_contracts))
                    success = await send_contract_to_bot(client, contract, channel_username, event.message.id, contract_type="pump")
                    if success:
                        sent_contract = contract
                        found_new_contract = True
                elif dexscreener_contracts:
                    contract = next(iter(dexscreener_contracts))
                    success = await send_contract_to_bot(client, contract, channel_username, event.message.id, contract_type="dexscreener")
                    if success:
                        sent_contract = contract
                        found_new_contract = True
                elif dextools_contracts:
                    contract = next(iter(dextools_contracts))
                    success = await send_contract_to_bot(client, contract, channel_username, event.message.id, contract_type="dextools")
                    if success:
                        sent_contract = contract
                        found_new_contract = True
                elif regular_contracts:
                    contract = next(iter(regular_contracts))
                    success = await send_contract_to_bot(client, contract, channel_username, event.message.id)
                    if success:
                        sent_contract = contract
                        found_new_contract = True

                # Записываем все контракты в дубликаты
                for contract in all_contracts:
                    if contract not in sent_contracts:
                        sent_contracts[contract] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        logger.warning(f"CALLER EBUN записал контракт: {contract}")
                    else:
                        logger.warning(f"Контракт {contract} уже существует. Пропускаем дубликат!")

                if found_new_contract:
                    # Добавляем сообщение в обработанные только если контракт был отправлен
                    message_ids_with_sent_contracts.add(event.message.id)
                    await save_sent_contracts(sent_contracts)

                if not found_new_contract:
                    return

                channel_name = event.chat.title if event.chat else 'Unknown Channel'
                log_message_preview(channel_name, preview_for_log)

            except Exception as e:
                logger.error(f"Ошибка при обработке сообщения: {str(e)}. Повторная попытка через 1 секунду.")
                await asyncio.sleep(1)

    await client.start()  # Начинаем прослушку новых сообщений
    logger.info("CALLER EBUN приступил к работе! LETS GO, пора ЕБАТЬ коллеров...")
    await client.run_until_disconnected()

# Функция для обработки изменённых сообщений
async def handle_edited_message(event, client):
    try:
        # Проверяем, было ли сообщение действительно изменено
        if event.message.edit_date is None:
            logger.info(f"Сообщение {event.message.id} не было изменено. Пропускаем.")
            return

        # Проверяем, находится ли сообщение в списке последних 5 сообщений
        if event.message.id not in last_messages:
            return  # Пропускаем сообщение, если оно не в последних 5

        logger.warning(f"Изменённое сообщение: {event.message.id} из канала \033[94m{event.chat.title if event.chat else 'Unknown Channel'}\033[0m.")

        # Парсим контракты только один раз
        pump_contracts, regular_contracts, dexscreener_contracts, dextools_contracts = parse_contracts(event.raw_text)
        all_contracts = pump_contracts | regular_contracts | dexscreener_contracts | dextools_contracts

        # Проверка, есть ли контракты
        if not all_contracts:
            logger.warning("Изменённое сообщение не содержит контрактов.")
            return

        # Проверяем, был ли контракт с этого сообщения уже отправлен
        if event.message.id in message_ids_with_sent_contracts:
            logger.warning(f"Контракты с этого сообщения ({event.message.id}) уже были отправлены боту. Новые контракты будут записаны в дубликаты.")

            # Работаем с дубликатами
            for contract in all_contracts:
                contract_lower = contract.lower()  # Приводим контракт к нижнему регистру

                # Если контракт уже есть в дубликатах, не перезаписываем, а просто уведомляем
                if contract_lower in sent_contracts:
                    logger.warning(f"Контракт {contract_lower} уже существует в дубликатах. Пропускаем.")
                else:
                    # Если контракт новый, добавляем его в дубликаты
                    sent_contracts[contract_lower] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    logger.warning(f"CALLER EBUN записал новый контракт в дубликаты: {contract_lower}")
            
            # Сохраняем контракты после обработки дубликатов
            await save_sent_contracts(sent_contracts)
            return  # Прерываем обработку, так как контракты уже были отправлены ранее

        # Если контракт с этого сообщения ещё не отправлялся, продолжаем обычную обработку
        found_new_contract = False

        # Отправляем новые контракты боту
        for contract in all_contracts:
            contract = contract.lower()

            if contract not in sent_contracts:
                # Получаем username канала для создания ссылки
                channel_username = event.chat.username if event.chat and event.chat.username else 'Unknown'

                # Отправляем контракт в зависимости от его типа
                if pump_contracts and contract in pump_contracts:
                    success = await send_contract_to_bot(client, contract, channel_username, event.message.id, contract_type="pump")
                elif dexscreener_contracts and contract in dexscreener_contracts:
                    success = await send_contract_to_bot(client, contract, channel_username, event.message.id, contract_type="dexscreener")
                elif dextools_contracts and contract in dextools_contracts:
                    success = await send_contract_to_bot(client, contract, channel_username, event.message.id)

                if success:
                    # Записываем контракт как отправленный
                    sent_contracts[contract] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    logger.warning(f"CALLER EBUN отправил контракт: {contract}")
                    found_new_contract = True

        if found_new_contract:
            # Добавляем ID сообщения в список сообщений, из которых уже был отправлен контракт
            message_ids_with_sent_contracts.add(event.message.id)

            # Сохраняем контракты после отправки боту
            await save_sent_contracts(sent_contracts)

        logger.info("Все контракты из изменённого сообщения обработаны.")

    except Exception as e:
        logger.error(f"Ошибка при обработке изменённого сообщения: {str(e)}")

          
# Функция для перепроверки сообщений в каналах
# Функция для перепроверки сообщений в каналах
async def recheck_messages(client):
    # Словарь для хранения последнего обработанного ID для каждого канала
    last_message_ids = {}

    # Инициализируем словарь с последними ID сообщений для каждого канала
    for channel in config.get('recheck_channels', []):
        try:
            # Получаем последнее сообщение из канала
            last_message = await client.get_messages(channel, limit=1)
            if last_message:
                last_message_ids[channel] = last_message[0].id  # Сохраняем ID последнего сообщения
            else:
                last_message_ids[channel] = 0  # Если нет сообщений, устанавливаем ID в 0
        except Exception as e:
            logger.error(f"Ошибка при получении последнего сообщения для канала {channel}: {str(e)}")
            last_message_ids[channel] = 0

    while True:
        for channel in config.get('recheck_channels', []):
            try:
                # Используем offset_id для получения только новых сообщений, которые поступили после последнего обработанного
                last_id = last_message_ids.get(channel, 0)
                async for message in client.iter_messages(channel, min_id=last_id):
                    if message.id <= last_id:
                        continue  # Пропускаем старые сообщения, если они все еще присутствуют

                    # Обновляем последний обработанный ID для этого канала
                    last_message_ids[channel] = message.id

                    cleaned_text = message.raw_text
                    if cleaned_text:
                        # Проверка на наличие пропускаемых фраз
                        if any(phrase in cleaned_text.lower() for phrase in config.get('skip_phrases', [])):
                            logger.warning("Сообщение содержит пропускаемую фразу. Пропускаем его...")

                            # Добавляем контракты в дубликаты
                            pump_contracts, regular_contracts, dexscreener_contracts, dextools_contracts = parse_contracts(cleaned_text)
                            all_contracts = pump_contracts | regular_contracts | dexscreener_contracts | dextools_contracts

                            for contract in all_contracts:
                                if contract not in sent_contracts:
                                    sent_contracts[contract] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    logger.warning(f"CALLER EBUN записал контракт в дубликаты: {contract}")

                            await save_sent_contracts(sent_contracts)  # Сохраняем изменения в файл
                            logger.info(f"CALLER EBUN зафиксировал {len(sent_contracts)} контрактов в файл. Всё под контролем!")
                            continue  # Пропускаем дальнейшую обработку, если сообщение содержит пропускаемую фразу

                        # Парсим контракты
                        pump_contracts, regular_contracts, dexscreener_contracts, dextools_contracts = parse_contracts(cleaned_text)

                        all_contracts = pump_contracts | regular_contracts | dexscreener_contracts | dextools_contracts

                        if not all_contracts:
                            continue  # Если контрактов нет, просто пропускаем сообщение

                        sent_contract = None
                        found_new_contract = False

                        # Логика приоритета: сначала pump, потом dexscreener, dextools, и в конце обычные контракты
                        if pump_contracts and next(iter(pump_contracts)) not in sent_contracts:
                            success = await send_contract_to_bot(client, next(iter(pump_contracts)), channel, message.id, contract_type="pump")
                            if success:
                                sent_contract = next(iter(pump_contracts))
                                found_new_contract = True
                        elif dexscreener_contracts and next(iter(dexscreener_contracts)) not in sent_contracts:
                            success = await send_contract_to_bot(client, next(iter(dexscreener_contracts)), channel, message.id, contract_type="dexscreener")
                            if success:
                                sent_contract = next(iter(dexscreener_contracts))
                                found_new_contract = True
                        elif dextools_contracts and next(iter(dextools_contracts)) not in sent_contracts:
                            success = await send_contract_to_bot(client, next(iter(dextools_contracts)), channel, message.id, contract_type="dextools")
                            if success:
                                sent_contract = next(iter(dextools_contracts))
                                found_new_contract = True
                        elif regular_contracts and next(iter(regular_contracts)) not in sent_contracts:
                            success = await send_contract_to_bot(client, next(iter(regular_contracts)), channel, message.id)
                            if success:
                                sent_contract = next(iter(regular_contracts))
                                found_new_contract = True

                        # Записываем все контракты в дубликаты
                        for contract in all_contracts:
                            if contract not in sent_contracts:
                                sent_contracts[contract] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                logger.warning(f"CALLER EBUN записал контракт в дубликаты: {contract}")
                            else:
                                logger.warning(f"Контракт {contract} уже существует. Пропускаем дубликат!")

                        if found_new_contract:
                            await save_sent_contracts(sent_contracts)

            except Exception as e:
                logger.error(f"Ошибка при перепроверке сообщений в канале {channel}: {str(e)}")

        await asyncio.sleep(2)  # Задержка между перепроверками

# Основная функция запуска клиента и выбора режима работы
async def main():
    global current_mode
    client = TelegramClient('session_solana', api_id, api_hash)
    await client.start()

    client.add_event_handler(lambda event: handle_edited_message(event, client), events.MessageEdited(chats=config['channels']))

    recheck_task = asyncio.create_task(recheck_messages(client))

    while True:
        mode = input("Выберите режим работы:\n1 - Мониторинг новых сообщений\n2 - Парсинг истории за неделю\n3 - Очистка старых контрактов\n4 - Выход\nВведите ваш выбор: ")

        async with mode_lock:
            current_mode = mode

        if mode == '1':
            logger.info("CALLER EBUN выбрал режим мониторинга новых сообщений.")
            logger.info("CALLER EBUN записывает все сообщения в ЛОГ!")
            await monitor_new_messages(client)
        elif mode == '2':
            logger.info("CALLER EBUN выбрал режим парсинга истории за неделю.")
            await fetch_contracts_from_history(client)
        elif mode == '3':
            logger.info("CALLER EBUN выбрал режим очистки контрактов старше 2 недель.")
            clean_old_contracts()
        elif mode == '4':
            logger.info("CALLER EBUN завершает работу.")
            break
        else:
            logger.error("Неверный выбор режима. Пожалуйста, выберите 1, 2, 3 или 4.")

    recheck_task.cancel()
    try:
        await recheck_task
    except asyncio.CancelledError:
        logger.info("Перепроверка сообщений остановлена.")
    await client.disconnect()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"CALLER EBUN словил критическую ошибку: {str(e)}")
