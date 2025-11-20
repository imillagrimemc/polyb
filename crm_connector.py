# crm_connector.py
import datetime
import logging
import urllib.parse
from typing import Any, Dict, Optional
import requests
from polybus_core import PolybusServer, Registry
from logging.handlers import RotatingFileHandler

# =========================
# Настройка логирования
# =========================
logger = logging.getLogger("crm_connector")
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

file_handler = RotatingFileHandler("crm_connector.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8")
file_handler.setFormatter(console_formatter)
logger.addHandler(file_handler)

# =========================
# Исключения
# =========================
class EspoAPIError(Exception):
    """Custom exception for EspoCRM API errors."""


# =========================
# Утилиты
# =========================
def http_build_query(data: Dict[str, Any]) -> str:
    pairs = {}

    def _render_key(parents: list):
        out_str = ""
        for depth, x in enumerate(parents):
            s = "[%s]" if depth > 0 or isinstance(x, int) else "%s"
            out_str += s % str(x)
        return out_str

    def _encode(value, parents_list):
        if isinstance(value, (list, tuple)):
            for i, v in enumerate(value):
                parents_list.append(i)
                _encode(v, parents_list)
                parents_list.pop()
        elif isinstance(value, dict):
            for k, v in value.items():
                parents_list.append(k)
                _encode(v, parents_list)
                parents_list.pop()
        else:
            pairs[_render_key(parents_list)] = str(value)

    _encode(data, [])
    return urllib.parse.urlencode(pairs)


# =========================
# API Клиент EspoCRM
# =========================
class EspoAPI:
    url_path = "/api/v1/"

    def __init__(self, url: str, api_key: str):
        self.url = url
        self.api_key = api_key
        self.status_code: Optional[int] = None

    def request(self, method: str, action: str, params: Optional[Dict] = None) -> requests.Response:
        if params is None:
            params = {}

        headers = {"X-Api-Key": self.api_key}
        url = self.normalize_url(action)

        kwargs = {"url": url, "headers": headers}

        if method.upper() in ["POST", "PATCH", "PUT"]:
            kwargs["json"] = params
        else:
            kwargs["url"] += "?" + http_build_query(params)

        logger.info(f"Sending {method} request to {kwargs['url']} with params {params}")

        response = requests.request(method, **kwargs)
        self.status_code = response.status_code

        if self.status_code != 200:
            reason = self.parse_reason(response.headers)
            logger.error(f"Request failed: {self.status_code}, Reason: {reason}")
            raise EspoAPIError(f"Wrong request, status code {self.status_code}, reason: {reason}")

        if not response.content:
            logger.error("Empty response content")
            raise EspoAPIError("Empty response content")

        return response

    def normalize_url(self, action: str) -> str:
        return f"{self.url}{self.url_path}{action}"

    @staticmethod
    def parse_reason(headers: Dict[str, str]) -> str:
        return headers.get("X-Status-Reason", "Unknown Error")


# =========================
# Клиент для работы с CRM
# =========================
class EspoCrmClient:
    def __init__(self, url: str = "http://192.168.1.100:8080", api_key: str = "2b295072ff26b1aa69a2968863592941"):
        self.client = EspoAPI(url, api_key)

    def create_lead(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        payload['name'] = f"{payload.get('name', 'Lead')} - {datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
        try:
            response = self.client.request("POST", "Lead", payload)
            data = response.json()
            logger.info(f"Lead created: {data}")
            return {"status": "success", "lead": data}
        except Exception as e:
            logger.error(f"Failed to create lead: {e}")
            return {"status": "error", "message": str(e)}

    def create_contact(self, first_name: str, last_name: str, phone_number: str) -> Dict[str, Any]:
        payload = {
            "name": f"{first_name} {last_name}",
            "firstName": first_name,
            "lastName": last_name,
            "phoneNumber": phone_number
        }
        try:
            response = self.client.request("POST", "Contact", payload)
            data = response.json()
            logger.info(f"Contact created: {data}")
            return {"status": "success", "contact": data}
        except Exception as e:
            logger.error(f"Failed to create contact: {e}")
            return {"status": "error", "message": str(e)}

    def get_contact(self, lead_name: str, last_name: str, phone_number: str) -> Dict[str, Any]:
        query = {
            "where[0][type]": "equals",
            "where[0][attribute]": "phoneNumber",
            "where[0][value]": phone_number
        }

        try:
            response = self.client.request("GET", "Contact", query)
            data = response.json()
        except Exception as e:
            logger.error(f"Error fetching contact: {e}")
            return {"status": "error", "message": str(e)}

        total = data.get("total", 0)
        logger.info(f"Found {total} contact(s) for phone {phone_number}")

        if total == 0:
            contact_result = self.create_contact(lead_name, last_name, phone_number)
            if contact_result["status"] != "success":
                return contact_result
            lead_result = self.create_lead({"firstName": lead_name, "lastName": last_name, "phoneNumber": phone_number})
            return {"status": "created", "contact": contact_result, "lead": lead_result}
        else:
            contact = data["list"][0]
            lead_result = self.create_lead({"firstName": lead_name, "lastName": last_name, "phoneNumber": phone_number})
            return {"status": "success", "contact": contact, "lead": lead_result}


def push_to_crm(params: Dict[str, Any]) -> Dict[str, Any]:
    first_name = params.get("firstName")
    last_name = params.get("lastName")
    phone = params.get("phoneNumber")

    logger.info(f"Pushing lead to CRM: {first_name} {last_name}, {phone}")

    client = EspoCrmClient()
    try:
        result = client.get_contact(first_name, last_name, phone)
        logger.info(f"CRM push result: {result}")
        return {"status": "pushed", "crm_result": result}
    except Exception as e:
        logger.error(f"Error pushing to CRM: {e}")
        return {"status": "error", "message": str(e)}


def run_crm_connector():
    Registry().register("crm_connector", "127.0.0.1", 9103, ["push_to_crm"])
    server = PolybusServer("crm_connector", "127.0.0.1", 9103)
    server.register_method("push_to_crm", push_to_crm)
    logger.info("CRM connector server started on 127.0.0.1:9103")
    server.serve()


if __name__ == "__main__":
    run_crm_connector()
