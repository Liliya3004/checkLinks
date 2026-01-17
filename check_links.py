import argparse
import os
import sys
from typing import Dict, Iterable, List, Optional, Tuple, Set
import time
from collections import defaultdict
from urllib.parse import urlparse

import requests
import re
from urllib.parse import urljoin

API_URL_BASE = "https://api.direct.yandex.com/json/v5"

# Описания для наиболее частых кодов ошибок
HTTP_STATUS_DESCRIPTIONS: Dict[int, str] = {
    400: "неверный запрос",
    401: "требуется авторизация",
    403: "доступ запрещён (часто блокировка ботов)",
    404: "страница не найдена",
    429: "слишком много запросов",
    500: "внутренняя ошибка сервера",
    502: "ошибочный шлюз",
    503: "сервис временно недоступен",
    504: "шлюз не отвечает",
}

# Домены-заглушки партнёрских сетей
STUB_DOMAINS = {
    "bankpro.su",
    "tb.gdeslon.ru",
}

STUB_ADMITAD_HOST = "offerwall.admitad.com"
STUB_ADMITAD_PATH_PREFIX = "/wall/offers"

CLIENT_REDIRECT_PATTERNS = [
    # meta refresh: <meta http-equiv="refresh" content="3; url=https://...">
    re.compile(r'<meta[^>]+http-equiv=["\']?refresh["\']?[^>]+content=["\']?[^"\']*url=([^"\'>\s]+)', re.I),
    # location.href = "..."
    re.compile(r'location\.href\s*=\s*["\']([^"\']+)["\']', re.I),
    # window.location = "..."
    re.compile(r'window\.location\s*=\s*["\']([^"\']+)["\']', re.I),
    # location.replace("...")
    re.compile(r'location\.replace\(\s*["\']([^"\']+)["\']\s*\)', re.I),
]


class YandexDirectClient:
    def __init__(self, token: str, client_login: str, language: str = "ru", api_timeout: int = 30) -> None:
        """Сохраняет токен, логин, язык и таймаут API для дальнейших запросов.
        """
        self.token = token
        self.client_login = client_login
        self.language = language
        self.api_timeout = api_timeout

    def _request(self, service: str, method: str, params: Dict) -> Dict:
        """Делает POST-запрос к API Директа, проверяет HTTP-ошибки и ошибки API, возвращает result.
        """
        url = f"{API_URL_BASE}/{service}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Client-Login": self.client_login,
            "Accept-Language": self.language,
        }
        try:
            response = requests.post(
                url,
                json={"method": method, "params": params},
                headers=headers,
                timeout=self.api_timeout,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"Ошибка сети при запросе {service}.{method}: {exc}") from exc

        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            raise RuntimeError(
                f"API error {payload['error'].get('error_code')}: {payload['error'].get('error_detail')}"
            )
        return payload.get("result", {})

    def iter_active_campaign_ids(self) -> Iterable[Tuple[int, str]]:
        """Запрашивает активные кампании (State=ON, Status=ACCEPTED) и отдаёт пары (campaign_id, name)."""
        params = {
            "SelectionCriteria": {"States": ["ON"], "Statuses": ["ACCEPTED"]},
            "FieldNames": ["Id", "Name", "State", "Status"],
        }
        result = self._request("campaigns", "get", params)
        for campaign in result.get("Campaigns", []):
            yield int(campaign["Id"]), campaign.get("Name", "")

    def iter_ads(self, campaign_id: int) -> Iterable[Dict]:
        """
        Итерация по объявлениям кампании.
        Проверяем только объявления в состоянии ON (не трогаем ARCHIVED, OFF и т.д.).
        """
        params: Dict[str, object] = {
            "SelectionCriteria": {"CampaignIds": [campaign_id]},
            "FieldNames": ["Id", "CampaignId", "State", "Status"],
            "TextAdFieldNames": ["Href", "DisplayUrlPath"],
            # "DynamicTextAdFieldNames": ["Href"],
            "TextAdBuilderAdFieldNames": ["Href"],
            "Page": {"Limit": 10000, "Offset": 0},
        }

        while True:
            result = self._request("ads", "get", params)
            for ad in result.get("Ads", []):
                state = ad.get("State")
                if state != "ON":
                    continue
                yield ad

            limited_by = result.get("LimitedBy")
            if limited_by is None:
                break
            params["Page"]["Offset"] = limited_by

def extract_client_redirect_url(html: str, base_url: str) -> Optional[str]:
    if not html:
        return None

    for pat in CLIENT_REDIRECT_PATTERNS:
        m = pat.search(html)
        if not m:
            continue
        target = m.group(1).strip()
        if not target:
            continue
        # поддержим относительные ссылки
        return urljoin(base_url, target)

    return None


def check_url_verbose(url: str, timeout: int = 120) -> Tuple[Optional[int], Optional[str], Optional[str], Optional[str]]:
    """
    Возвращает (status_code, error_text, final_url, html_text).
    html_text будет только для HTML-ответов, иначе None.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    try:
        r = requests.get(url, headers=headers, allow_redirects=True, timeout=timeout)
        content_type = (r.headers.get("Content-Type") or "").lower()
        html = r.text if "text/html" in content_type else None
        return r.status_code, None, r.url, html
    except requests.RequestException as exc:
        return None, str(exc), None, None


def check_url(url: str, timeout: int = 120) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    status, error, final_url, html = check_url_verbose(url, timeout=timeout)

    # если получили HTML и 200 — попробуем вытащить client-side redirect
    if status == 200 and html and final_url:
        client_target = extract_client_redirect_url(html, base_url=final_url)
        if client_target:
            # возвращаем "финал", который увидит пользователь после таймера/JS
            return status, error, client_target

    return status, error, final_url
# def check_url(url: str, timeout: int = 120) -> Tuple[Optional[int], Optional[str], Optional[str]]:
#     """
#     Делает GET по ссылке с редиректами, возвращает (status_code, error_text, final_url),
#     final_url — итоговый URL после всех редиректов (нужен для проверки заглушек);
#     при ошибке — status_code=None.
#     """
#     headers = {
#         "User-Agent": (
#             "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
#             "AppleWebKit/537.36 (KHTML, like Gecko) "
#             "Chrome/124.0.0.0 Safari/537.36"
#         ),
#         "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#         "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
#     }
#     try:
#         response = requests.get(url, headers=headers, allow_redirects=True, timeout=timeout)
#         return response.status_code, None, response.url
#     except requests.RequestException as exc:
#         return None, str(exc), None

def extract_urls_from_ad(ad: Dict) -> List[str]:
    """Достаёт Href из разных типов объявлений (TextAd, DynamicTextAd, TextAdBuilderAd)."""
    urls: List[str] = []
    for key in ("TextAd", "DynamicTextAd", "TextAdBuilderAd"):
        sub = ad.get(key)
        if sub:
            href = sub.get("Href")
            if href:
                urls.append(href)
    return urls

def load_skip_campaigns(path: Optional[str]) -> Set[int]:
    """
    Загружает список ID кампаний, которые нужно пропускать.
    Один ID на строку. Пустые строки и строки, начинающиеся с #, игнорируются.
    """
    skip_ids: Set[int] = set()
    if not path:
        return skip_ids
    if not os.path.exists(path):
        return skip_ids

    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                raw = line.strip()
                if not raw or raw.startswith("#"):
                    continue
                try:
                    cid = int(raw)
                    skip_ids.add(cid)
                except ValueError:
                    continue
    except Exception as e:
        print(f"Не удалось прочитать файл списка кампаний для пропуска '{path}': {e}", file=sys.stderr)

    return skip_ids


def parse_target_campaign_ids(args: argparse.Namespace) -> Optional[Set[int]]:
    """
    Возвращает множество целевых ID кампаний, если фильтр задан, иначе None.
    Поддерживает:
      --campaign-id (можно многократно)
      --campaign-ids "1,2,3"
    """
    ids: Set[int] = set()

    if args.campaign_id:
        ids.update(int(x) for x in args.campaign_id if x is not None)

    if args.campaign_ids:
        for part in str(args.campaign_ids).split(","):
            part = part.strip()
            if not part:
                continue
            try:
                ids.add(int(part))
            except ValueError:
                continue

    return ids if ids else None


def parse_args(argv: List[str]) -> argparse.Namespace:
    """"Описывает и парсит аргументы командной строки (токен/логин/таймауты/пути/Telegram)
    """
    parser = argparse.ArgumentParser(description="Проверка ссылок в активных кампаниях Яндекс.Директа.")
    parser.add_argument(
        "--token",
        default=os.getenv("YANDEX_API_TOKEN"),
        help="OAuth токен для доступа к API (по умолчанию YANDEX_API_TOKEN).",
    )
    parser.add_argument(
        "--client-login",
        default=os.getenv("YANDEX_CLIENT_LOGIN"),
        help="Логин кабинета без @yandex.ru (по умолчанию YANDEX_CLIENT_LOGIN).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="Таймаут проверки ссылки в секундах.",
    )
    parser.add_argument(
        "--output-file",
        default="results.txt",
        help="Базовое имя файла результатов (по умолчанию results.txt).",
    )
    parser.add_argument(
        "--telegram-token",
        default=os.getenv("TELEGRAM_BOT_TOKEN"),
        help="Токен бота Telegram (по умолчанию TELEGRAM_BOT_TOKEN).",
    )
    parser.add_argument(
        "--telegram-chat-id",
        default=os.getenv("TELEGRAM_CHAT_ID"),
        help="ID чата для детального отчёта и логов (по умолчанию TELEGRAM_CHAT_ID).",
    )
    parser.add_argument(
        "--telegram-main-chat-id",
        default=os.getenv("TELEGRAM_MAIN_CHAT_ID"),
        help="ID чата для основного отчёта (для Лемуры). Если не задан, используется telegram-chat-id.",
    )
    parser.add_argument(
        "--api-timeout",
        type=int,
        default=30,
        help="Таймаут запросов к API Яндекс.Директа в секундах (по умолчанию 30).",
    )
    parser.add_argument(
        "--skip-campaigns-file",
        default="skip_campaigns.txt",
        help="Путь к файлу со списком ID кампаний, которые нужно пропускать (по умолчанию skip_campaigns.txt).",
    )
    parser.add_argument(
        "--campaign-id",
        action="append",
        type=int,
        default=None,
        help="Проверять только кампанию с указанным ID. Можно указать несколько раз: --campaign-id 1 --campaign-id 2",
    )
    parser.add_argument(
        "--campaign-ids",
        default=None,
        help="Проверять только кампании из списка (через запятую), например: --campaign-ids 123,456",
    )
    parser.add_argument(
        "--list-campaigns",
        action="store_true",
        help="Вывести список активных кампаний (Id + Name) и завершить работу.",
    )
    return parser.parse_args(argv)


def send_telegram_message(token: str, chat_id: str, text: str) -> bool:
    """Отправляет текстовое сообщение в Telegram через sendMessage, возвращает успех/ошибку.
    """
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            print(f"Telegram API error: {data}", file=sys.stderr)
            return False
        return True
    except requests.RequestException as exc:
        print(f"Не удалось отправить сообщение в Telegram: {exc}", file=sys.stderr)
        return False


def send_telegram_document(token: str, chat_id: str, file_path: str, caption: Optional[str] = None) -> bool:
    """
    Отправляет файл в Telegram через sendDocument (лог), возвращает успех/ошибку.
    """
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    data = {"chat_id": chat_id}
    if caption:
        data["caption"] = caption

    try:
        with open(file_path, "rb") as f:
            files = {"document": (os.path.basename(file_path), f)}
            r = requests.post(url, data=data, files=files, timeout=30)
        r.raise_for_status()
        payload = r.json()
        if not payload.get("ok"):
            print(f"Telegram Document API error: {payload}", file=sys.stderr)
            return False
        return True
    except Exception as exc:
        print(f"Ошибка отправки документа в Telegram: {exc}", file=sys.stderr)
        return False


def is_stub_final_url(final_url: Optional[str]) -> bool:
    """
    Проверяет, ведёт ли final_url на домены/пути «заглушек» партнёрских сетей.
    """
    if not final_url:
        return False

    parsed = urlparse(final_url)
    host = parsed.netloc.lower()
    path = parsed.path or "/"

    if host in STUB_DOMAINS:
        return True

    if host == STUB_ADMITAD_HOST and path.startswith(STUB_ADMITAD_PATH_PREFIX):
        return True

    return False


def format_campaign_with_name(camp_id: int, names: Dict[int, str]) -> str:
    """
    Формирует строку вида Кампания ID (Name) если имя известно, иначе Кампания ID.
    """
    name = names.get(camp_id)
    if name:
        return f"Кампания {camp_id} ({name})"
    return f"Кампания {camp_id}"


def main(argv: List[str]) -> int:
    """
    Основной сценарий:
    собирает кампании, проверяет ссылки объявлений, группирует проблемы, пишет лог,
    отправляет отчёты в Telegram, печатает длительность.
    """
    args = parse_args(argv)
    target_campaign_ids = parse_target_campaign_ids(args)
    start_time = time.time()

    # --- Логи в _logs рядом со скриптом ---
    now_str_file = time.strftime("%Y-%m-%d_%H-%M-%S")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(script_dir, "_logs")
    os.makedirs(log_dir, exist_ok=True)

    orig_name = os.path.basename(args.output_file)
    base_name, ext = os.path.splitext(orig_name)
    if not ext:
        ext = ".txt"

    args.output_file = os.path.join(log_dir, f"{base_name}_{now_str_file}{ext}")

    if not args.token or not args.client_login:
        print("Необходимо передать токен и Client-Login через параметры или переменные окружения.", file=sys.stderr)
        return 1

    # Кампании для пропуска
    skip_campaigns: Set[int] = load_skip_campaigns(args.skip_campaigns_file)
    if skip_campaigns:
        print(f"Кампаний для пропуска: {len(skip_campaigns)} (из файла {args.skip_campaigns_file})")
    else:
        print("Список кампаний для пропуска пуст или файл не найден.")

    client = YandexDirectClient(
        token=args.token,
        client_login=args.client_login,
        api_timeout=args.api_timeout,
    )
    if args.list_campaigns:
        print("Активные кампании (ON + ACCEPTED):")
        for campaign_id, name in client.iter_active_campaign_ids():
            print(f"{campaign_id}\t{name}")
        return 0

    any_issue = False
    lines: List[str] = []

    # HTTP-ошибки по ссылкам:
    # кампания_id -> список: (ad_id, url, status_code_or_None, description_or_error_text, is_stub)
    issues_http: Dict[int, List[Tuple[int, str, Optional[int], Optional[str], bool]]] = defaultdict(list)
    # Ошибки API по кампаниям
    issues_api: Dict[int, str] = {}
    # ID кампании -> имя
    campaign_names: Dict[int, str] = {}

    seen_campaign_ids: Set[int] = set()

    for campaign_id, name in client.iter_active_campaign_ids():
        campaign_names[campaign_id] = name
        seen_campaign_ids.add(campaign_id)

        # Если задан фильтр по кампаниям — берём только их
        if target_campaign_ids is not None and campaign_id not in target_campaign_ids:
            continue

        if campaign_id in skip_campaigns:
            skip_msg = f"Пропускаем кампанию: {name} (ID {campaign_id}) — есть в списке исключений."
            print(skip_msg)
            lines.append(skip_msg)
            continue

        header = f"Кампания: {name} (ID {campaign_id})"
        print(header)
        lines.append(header)

        try:
            for ad in client.iter_ads(campaign_id):
                ad_id = int(ad.get("Id"))
                for url in extract_urls_from_ad(ad):
                    status, error, final_url = check_url(url, timeout=args.timeout)

                    stub = is_stub_final_url(final_url)

                    if status is not None and 200 <= status < 300 and not stub:
                        msg = f"  Объявление {ad_id}: ссылка {url} отвечает {status} (OK)"
                        print(msg)
                        lines.append(msg)
                        continue

                    any_issue = True

                    if stub:
                        desc = "переход на заглушку партнёрской сети"
                        msg = f"  Объявление {ad_id}: ссылка {url} ведёт на {final_url} ({desc})"
                        issues_http[campaign_id].append((ad_id, url, status, desc, True))
                    elif status is None:
                        desc = error or "ошибка запроса, подробности отсутствуют"
                        msg = f"  Объявление {ad_id}: ссылка {url}: {desc}"
                        issues_http[campaign_id].append((ad_id, url, None, desc, False))
                    else:
                        base_desc = HTTP_STATUS_DESCRIPTIONS.get(status)
                        if base_desc:
                            desc = base_desc
                            msg = f"  Объявление {ad_id}: ссылка {url} отвечает {status} ({base_desc})"
                        else:
                            desc = None
                            msg = f"  Объявление {ad_id}: ссылка {url} отвечает {status}"
                        issues_http[campaign_id].append((ad_id, url, status, desc, False))

                    print(msg)
                    lines.append(msg)

        except RuntimeError as e:
            any_issue = True
            err_text = f"ошибка обращения к API для кампании {campaign_id}: {e}"
            msg = f"  [API ERROR] {err_text}"
            print(msg)
            lines.append(msg)
            issues_api[campaign_id] = err_text

    if target_campaign_ids is not None:
        missing = sorted(target_campaign_ids - seen_campaign_ids)
        if missing:
            msg = (
                "Внимание: указанные кампании не найдены среди активных (ON + ACCEPTED): "
                + ", ".join(map(str, missing))
            )
            print(msg)
            lines.append(msg)

    summary_line = (
        "Найдены ссылки с отличным от 2xx ответом или ведущие на заглушку."
        if any_issue
        else "Все проверенные ссылки возвращают 2xx и не ведут на заглушки."
    )
    print(summary_line)
    lines.append(summary_line)

    # --- запись полного лога в файл ---
    now_str = time.strftime("%Y-%m-%d %H:%M:%S")

    log_content: List[str] = []
    log_content.append("Отчёт проверки ссылок")
    log_content.append(f"Дата и время запуска: {now_str}")
    if skip_campaigns:
        log_content.append(
            f"Кампании, пропущенные по списку ({args.skip_campaigns_file}): "
            f"{', '.join(map(str, sorted(skip_campaigns)))}"
        )
    log_content.append("")
    log_content.extend(lines)

    try:
        with open(args.output_file, "w", encoding="utf-8") as f:
            f.write("\n".join(log_content))
        print(f"\nРезультат сохранён: {args.output_file}")
    except Exception as e:
        print(f"Ошибка записи файла: {e}", file=sys.stderr)

    # --- подготовка и отправка отчёта в Telegram ---
    main_chat_id = args.telegram_main_chat_id or args.telegram_chat_id
    detail_chat_id = args.telegram_chat_id  # сюда доп. коды и лог

    if args.telegram_token and (main_chat_id or detail_chat_id):
        now_str = time.strftime("%Y-%m-%d %H:%M:%S")
        if issues_http or issues_api:
            # Разбиваем проблемы на группы:
            group_stub: Dict[int, List[Tuple[int, str, Optional[int], Optional[str], bool]]] = defaultdict(list)
            group_404: Dict[int, List[Tuple[int, str, Optional[int], Optional[str], bool]]] = defaultdict(list)
            group_other: Dict[int, List[Tuple[int, str, Optional[int], Optional[str], bool]]] = defaultdict(list)
            group_no_code: Dict[int, List[Tuple[int, str, Optional[int], Optional[str], bool]]] = defaultdict(list)

            for camp_id, problems in issues_http.items():
                for ad_id, url, status_code, desc, stub in problems:
                    if stub:
                        group_stub[camp_id].append((ad_id, url, status_code, desc, stub))
                    elif status_code is None:
                        group_no_code[camp_id].append((ad_id, url, status_code, desc, stub))
                    elif status_code == 404:
                        group_404[camp_id].append((ad_id, url, status_code, desc, stub))
                    else:
                        group_other[camp_id].append((ad_id, url, status_code, desc, stub))

            # Критические: заглушки + 404 + "код не получен"
            critical_campaign_ids: Set[int] = (
                set(group_stub.keys()) | set(group_404.keys()) | set(group_no_code.keys())
            )
            total_critical_campaigns = len(critical_campaign_ids)
            total_critical_ads = (
                sum(len(v) for v in group_stub.values())
                + sum(len(v) for v in group_404.values())
                + sum(len(v) for v in group_no_code.values())
            )

            # Прочие коды HTTP
            other_campaign_ids: Set[int] = set(group_other.keys())
            total_other_campaigns = len(other_campaign_ids)
            total_other_ads = sum(len(v) for v in group_other.values())

            # --- 1. Основное сообщение (главный чат: только критика) ---
            if main_chat_id:
                main_lines: List[str] = []
                main_lines.append(f"✨ Отчёт проверки ссылок — {now_str}")
                main_lines.append("")
                if total_critical_ads > 0 or issues_api:
                    main_lines.append("❌ Критические ошибки найдены")
                else:
                    main_lines.append("🟢 Критических ошибок не найдено")
                main_lines.append(f"📂 Кампаний с критическими ошибками: {total_critical_campaigns}")
                main_lines.append(f"📣 Объявлений с критическими ошибками: {total_critical_ads}")
                main_lines.append("")

                if group_stub:
                    main_lines.append("🟣 Переход на заглушку партнёрской сети:")
                    for camp_id, problems in sorted(group_stub.items()):
                        camp_title = format_campaign_with_name(camp_id, campaign_names)
                        main_lines.append(f"- {camp_title}:")
                        for ad_id, url, status_code, desc, _stub in problems:
                            main_lines.append(f"  • Объявление {ad_id}: ссылка {url} — {desc}.")
                        main_lines.append("")

                if group_404:
                    main_lines.append("🔴 Ответ 404 (страница не найдена):")
                    for camp_id, problems in sorted(group_404.items()):
                        camp_title = format_campaign_with_name(camp_id, campaign_names)
                        main_lines.append(f"- {camp_title}:")
                        for ad_id, url, status_code, desc, _stub in problems:
                            main_lines.append(
                                f"  • Объявление {ad_id}: ссылка {url} отвечает 404 (страница не найдена)."
                            )
                        main_lines.append("")

                if group_no_code:
                    main_lines.append("⚪ Код не получен (проверьте вручную):")
                    for camp_id, problems in sorted(group_no_code.items()):
                        camp_title = format_campaign_with_name(camp_id, campaign_names)
                        main_lines.append(f"- {camp_title}:")
                        for ad_id, url, status_code, desc, _stub in problems:
                            text_err = desc or "код не получен, проверьте вручную"
                            main_lines.append(f"  • Объявление {ad_id}: ссылка {url} — {text_err}.")
                        main_lines.append("")

                if issues_api:
                    main_lines.append("⚠ Ошибки API Яндекс.Директа:")
                    for camp_id, err in sorted(issues_api.items()):
                        camp_title = format_campaign_with_name(camp_id, campaign_names)
                        main_lines.append(f"- {camp_title}: {err}")
                    main_lines.append("")

                main_lines.append(f"📄 Полный лог: {os.path.basename(args.output_file)}")

                main_text = "\n".join(main_lines)
                if len(main_text) > 4000:
                    main_text = main_text[:3990] + "\n…обрезано, см. полный лог в файле."

                sent_main = send_telegram_message(args.telegram_token, main_chat_id, main_text)
                print("\nОсновной отчёт:")
                print(main_text)
                if sent_main:
                    print("Основной отчёт отправлен в Telegram.")
                else:
                    print("Не удалось отправить основной отчёт в Telegram, см. сообщение об ошибке выше.")

            # --- 2. Доп. сообщение с «другими кодами» (только в твой чат) ---
            if group_other and detail_chat_id:
                extra_lines: List[str] = []
                extra_lines.append(
                    "Сообщение для Лемуры: тебе достаточно основного отчёта, это доп. детали по другим кодам."
                )
                extra_lines.append("")
                extra_lines.append("🟠 Дополнительные ошибки (другие коды HTTP):")
                extra_lines.append(f"📂 Кампаний с такими ошибками: {total_other_campaigns}")
                extra_lines.append(f"📣 Объявлений с такими ошибками: {total_other_ads}")
                extra_lines.append("")

                for camp_id, problems in sorted(group_other.items()):
                    camp_title = format_campaign_with_name(camp_id, campaign_names)
                    extra_lines.append(f"- {camp_title}:")
                    for ad_id, url, status_code, desc, _stub in problems:
                        code_str = str(status_code) if status_code is not None else "?"
                        if desc:
                            extra_lines.append(
                                f"  • Объявление {ad_id}: ссылка {url} отвечает {code_str} ({desc})."
                            )
                        else:
                            extra_lines.append(
                                f"  • Объявление {ad_id}: ссылка {url} отвечает {code_str}."
                            )
                    extra_lines.append("")

                extra_text = "\n".join(extra_lines)
                if len(extra_text) > 4000:
                    extra_text = extra_text[:3990] + "\n…обрезано, см. полный лог в файле."

                sent_extra = send_telegram_message(args.telegram_token, detail_chat_id, extra_text)
                print("\nДополнительный отчёт (другие коды):")
                print(extra_text)
                if sent_extra:
                    print("Дополнительный отчёт отправлен в Telegram.")
                else:
                    print("Не удалось отправить дополнительный отчёт в Telegram, см. сообщение об ошибке выше.")

            # --- 3. Лог файлом (только в твой чат) ---
            if detail_chat_id:
                caption = "Полный лог проверки ссылок во вложении."
                sent_doc = send_telegram_document(args.telegram_token, detail_chat_id, args.output_file, caption)
                if sent_doc:
                    print("Файл лога отправлен в Telegram.")
                else:
                    print("Не удалось отправить файл лога в Telegram, см. сообщение об ошибке выше.")

        else:
            # Ошибок нет
            ok_text = (
                f"✨ Отчёт проверки ссылок — {now_str}\n\n"
                f"🟢 Ошибок не найдено. Все проверенные ссылки отвечают 2xx и не ведут на заглушки."
            )
            if main_chat_id:
                sent_msg = send_telegram_message(args.telegram_token, main_chat_id, ok_text)
                print("\nСообщение для Telegram:")
                print(ok_text)
                if sent_msg:
                    print("Сообщение об отсутствии ошибок отправлено в Telegram.")
                else:
                    print("Не удалось отправить сообщение в Telegram, см. сообщение об ошибке выше.")

            # Лог при отсутствии ошибок — только в твой чат (если есть)
            if detail_chat_id:
                caption = "Полный лог проверки ссылок (ошибок не найдено)."
                sent_doc = send_telegram_document(args.telegram_token, detail_chat_id, args.output_file, caption)
                if sent_doc:
                    print("Файл лога отправлен в Telegram.")
                else:
                    print("Не удалось отправить файл лога в Telegram, см. сообщение об ошибке выше.")
    else:
        print("TELEGRAM_BOT_TOKEN не задан или не задан ни один чат, отчёт в Telegram не отправлен.")

    duration = time.time() - start_time
    minutes = int(duration // 60)
    seconds = int(duration % 60)
    if minutes > 0:
        duration_str = f"{minutes} мин {seconds} сек"
    else:
        duration_str = f"{seconds} сек"

    print(f"Время выполнения программы: {duration_str}")
    finish_str = time.strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(args.output_file, "a", encoding="utf-8") as f:
            f.write("\n")
            f.write(f"Дата и время окончания: {finish_str}\n")
            f.write(f"Время выполнения: {duration_str}\n")
    except Exception as e:
        print(f"Ошибка записи завершения лога: {e}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))


"""
===============================================================================
СПРАВОЧНИК ЗАПУСКА check_links.py
===============================================================================

Обязательные параметры:
  --token            OAuth-токен Яндекс.Директа
  --client-login     Логин рекламного кабинета (без @yandex.ru)

------------------------------------------------------------------------------
1. БАЗОВЫЕ РЕЖИМЫ
------------------------------------------------------------------------------

1.1. Проверка всех активных кампаний (ON + ACCEPTED), без Telegram
python3 check_links.py \
  --token "..." \
  --client-login "..."

1.2. Проверка всех активных кампаний + Telegram-отчёты
python3 check_links.py \
  --token "..." \
  --client-login "..." \
  --telegram-token "..." \
  --telegram-chat-id "..." \
  --telegram-main-chat-id "..."

1.3. Только вывести список активных кампаний и выйти
python3 check_links.py \
  --token "..." \
  --client-login "..." \
  --list-campaigns

------------------------------------------------------------------------------
2. ТЕСТИРОВАНИЕ ОТДЕЛЬНЫХ КАМПАНИЙ
------------------------------------------------------------------------------

2.1. Проверка одной конкретной кампании
python3 check_links.py \
  --token "..." \
  --client-login "..." \
  --campaign-id 704059435

2.2. Проверка нескольких кампаний (через несколько флагов)
python3 check_links.py \
  --token "..." \
  --client-login "..." \
  --campaign-id 704059435 \
  --campaign-id 704059999

2.3. Проверка нескольких кампаний (через список)
python3 check_links.py \
  --token "..." \
  --client-login "..." \
  --campaign-ids 704059435,704059999

------------------------------------------------------------------------------
3. ПРОВЕРКА С ПОВТОРНОЙ ПРОВЕРКОЙ (ANTI-JS / TIMER REDIRECTS)
------------------------------------------------------------------------------

3.1. Повторная проверка ВСЕХ ссылок:
     2 попытки, между ними 7 секунд
python3 check_links.py \
  --token "..." \
  --client-login "..." \
  --recheck-delay 7 \
  --recheck-attempts 2

3.2. Повторная проверка только для партнёрских доменов
python3 check_links.py \
  --token "..." \
  --client-login "..." \
  --recheck-delay 7 \
  --recheck-attempts 2 \
  --recheck-only-domains "ad.admitad.com,offerwall.admitad.com,tb.gdeslon.ru,bankpro.su"

3.3. Тест одной кампании + повторная проверка партнёрских ссылок
python3 check_links.py \
  --token "..." \
  --client-login "..." \
  --campaign-id 704059435 \
  --recheck-delay 7 \
  --recheck-attempts 2 \
  --recheck-only-domains "ad.admitad.com"

------------------------------------------------------------------------------
4. РЕЖИМЫ БЕЗ TELEGRAM (ЛОКАЛЬНЫЕ ПРОГОНЫ)
------------------------------------------------------------------------------

4.1. Telegram отключён автоматически, если не передавать параметры
python3 check_links.py \
  --token "..." \
  --client-login "..." \
  --campaign-id 704059435

4.2. Явное отключение Telegram (если добавлен флаг --no-telegram)
python3 check_links.py \
  --token "..." \
  --client-login "..." \
  --campaign-id 704059435 \
  --no-telegram

------------------------------------------------------------------------------
5. ВСПОМОГАТЕЛЬНЫЕ ПАРАМЕТРЫ
------------------------------------------------------------------------------

--timeout N
  Таймаут HTTP-запроса к URL (по умолчанию 10 сек)

--api-timeout N
  Таймаут запросов к API Яндекс.Директа (по умолчанию 30 сек)

--skip-campaigns-file path
  Файл со списком ID кампаний, которые нужно пропускать

--output-file name.txt
  Базовое имя файла отчёта (реальный файл создаётся в папке _logs)

------------------------------------------------------------------------------
ПРИМЕЧАНИЯ
------------------------------------------------------------------------------
• Повторная проверка полезна для ссылок с JS / countdown редиректами.
• Финальный результат берётся по ПОСЛЕДНЕЙ попытке проверки.
• Если кампания указана, но не активна (не ON + ACCEPTED) —
  будет выведено предупреждение.
• Логи всегда сохраняются локально, даже если Telegram отключён.

===============================================================================
"""