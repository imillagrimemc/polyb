# call_orchestrator.py
import json
import logging
from logging.handlers import RotatingFileHandler
import requests
from polybus_core import PolybusServer, PolybusClient, Registry
import time


# =========================
# Настройка логирования
# =========================
logger = logging.getLogger("call_orchestrator")
logger.setLevel(logging.INFO)

# --- Логирование в консоль ---
console_handler = logging.StreamHandler()
console_formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# --- Логирование в файл с ротацией ---
file_handler = RotatingFileHandler(
    "call_orchestrator.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8"
)
file_handler.setFormatter(console_formatter)
logger.addHandler(file_handler)

status = None

# =========================
# Функции
# =========================
def parse_json(data, parent_key='', result=None):
    """
    Рекурсивно преобразует вложенный JSON в плоский словарь.
    """
    if result is None:
        result = {}

    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{parent_key}.{key}" if parent_key else key
            parse_json(value, new_key, result)
    elif isinstance(data, list):
        for index, item in enumerate(data):
            new_key = f"{parent_key}[{index}]"
            parse_json(item, new_key, result)
    else:
        result[parent_key] = data

    return result


def make_call(number: str):

    
    url = "http://192.168.1.105:8801/call"
    headers = {"Content-Type": "application/json"}

    number = number[-9:]

    headers = {
        "Content-Type": "application/json"
    }
    payload = {
        "action": "call",
        "number": number
    }

    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        return response.json()      
    except requests.exceptions.RequestException as e:
        print(f"Ошибка запроса: {e}")
        return None


def start_call(params: dict):
    """
    Обрабатывает входящие данные лида и инициирует звонок.
    """
    global status
    data = params.get('result', {})
    parsed_data = parse_json(data)
    for key, value in parsed_data.items():
        logger.info(f"{key} = {value}")

    lead_id = parsed_data.get('crm_result.lead.lead.id') or parsed_data.get('crm_result.contact.id')
    number_id = parsed_data.get('crm_result.lead.lead.phoneNumber') or parsed_data.get('crm_result.contact.phoneNumber')
    created_by_id = parsed_data.get('crm_result.lead.lead.createdById') or parsed_data.get('crm_result.contact.createdById')

    if not (lead_id and number_id and created_by_id):
        logger.warning("Недостаточно данных для инициализации звонка")
        return

    logger.info(f"lead_id={lead_id}, created_by_id={created_by_id}, number_id={number_id}")

    # ---------------------------
    # Проверяем статус текущего звонка
    # ---------------------------
    try:
        status_response = requests.post(
            "http://192.168.1.105:8801/call",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"action": "get_status"})
        ).json()
    except Exception as e:
        logger.error(f"Ошибка при получении статуса звонка: {e}")
        status_response = {"status": "idle"}

    current_status = status_response.get("status", "idle")
    logger.info(f"Текущий статус звонка: {current_status}")

    if current_status in ["idle"]:
        # нет активного звонка → можно звонить
        make_call(number_id)
    else:
        # уже есть звонок → не звонить
        logger.info("Звонок уже активен, новый звонок не инициируется")
        
# =========================
# Запуск сервиса
# =========================
def run_call_orchestrator():
    Registry().register("call_orchestrator", "127.0.0.1", 9104, ["start_call"])
    server = PolybusServer("call_orchestrator", "127.0.0.1", 9104)
    server.register_method("start_call", start_call)
    logger.info("Call orchestrator server started on 127.0.0.1:9104")
    server.serve()


if __name__ == "__main__":
    run_call_orchestrator()
