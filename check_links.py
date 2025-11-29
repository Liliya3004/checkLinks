import argparse
import os
import sys
from typing import Dict, Iterable, List, Optional, Tuple, Set
import time
from collections import defaultdict

import requests

API_URL_BASE = "https://api.direct.yandex.com/json/v5"


class YandexDirectClient:
    def __init__(self, token: str, client_login: str, language: str = "ru", api_timeout: int = 30) -> None:
        self.token = token
        self.client_login = client_login
        self.language = language
        self.api_timeout = api_timeout

    def _request(self, service: str, method: str, params: Dict) -> Dict:
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
            # Любая сетевая ошибка к API — в осмысленный RuntimeError
            raise RuntimeError(f"Ошибка сети при запросе {service}.{method}: {exc}") from exc

        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            raise RuntimeError(
                f"API error {payload['error'].get('error_code')}: {payload['error'].get('error_detail')}"
            )
        return payload.get("result", {})

    def iter_active_campaign_ids(self) -> Iterable[Tuple[int, str]]:
        params = {
            "SelectionCriteria": {"States": ["ON"], "Statuses": ["ACCEPTED"]},
            "FieldNames": ["Id", "Name", "State", "Status"],
        }
        result = self._request("campaigns", "get", params)
        for campaign in result.get("Campaigns", []):
            yield int(campaign["Id"]), campaign.get("Name", "")

    def iter_ads(self, campaign_id: int) -> Iterable[Dict]:
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
                yield ad

            limited_by = result.get("LimitedBy")
            if limited_by is None:
                break
            params["Page"]["Offset"] = limited_by


def extract_urls_from_ad(ad: Dict) -> List[str]:
    urls: List[str] = []
    for key in ("TextAd", "DynamicTextAd", "TextAdBuilderAd"):
        sub = ad.get(key)
        if sub:
            href = sub.get("Href")
            if href:
                urls.append(href)
    return urls


def check_url(url: str, timeout: int = 10) -> Tuple[Optional[int], Optional[str]]:
    try:
        response = requests.get(url, allow_redirects=True, timeout=timeout)
        return response.status_code, None
    except requests.RequestException as exc:  # pragma: no cover - network errors depend on runtime
        return None, str(exc)


def parse_args(argv: List[str]) -> argparse.Namespace:
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
        help="Файл для сохранения результатов проверки (по умолчанию results.txt).",
    )
    parser.add_argument(
        "--telegram-token",
        default=os.getenv("TELEGRAM_BOT_TOKEN"),
        help="Токен бота Telegram (по умолчанию TELEGRAM_BOT_TOKEN).",
    )
    parser.add_argument(
        "--telegram-chat-id",
        default=os.getenv("TELEGRAM_CHAT_ID"),
        help="ID чата/юзера для отправки отчёта (по умолчанию TELEGRAM_CHAT_ID).",
    )
    parser.add_argument(
        "--api-timeout",
        type=int,
        default=30,
        help="Таймаут запросов к API Яндекс.Директа в секундах (по умолчанию 30).",
    )
    return parser.parse_args(argv)


def send_telegram_message(token: str, chat_id: str, text: str) -> bool:
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


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    start_time = time.time()

    # Генерируем имя файла с timestamp, если не указано вручную
    now_str_file = time.strftime("%Y-%m-%d_%H-%M-%S")
    base_name, ext = os.path.splitext(args.output_file)
    args.output_file = f"{base_name}_{now_str_file}{ext}"

    if not args.token or not args.client_login:
        print("Необходимо передать токен и Client-Login через параметры или переменные окружения.", file=sys.stderr)
        return 1

    client = YandexDirectClient(
        token=args.token,
        client_login=args.client_login,
        api_timeout=args.api_timeout,
    )

    any_issue = False
    lines: List[str] = []  # полный лог для файла

    # HTTP-ошибки по ссылкам: кампания -> множество объявлений
    issues_http: Dict[int, Set[int]] = defaultdict(set)
    # Ошибки API по кампаниям
    issues_api: Dict[int, str] = {}

    for campaign_id, name in client.iter_active_campaign_ids():
        header = f"Кампания: {name} (ID {campaign_id})"
        print(header)
        lines.append(header)

        try:
            for ad in client.iter_ads(campaign_id):
                ad_id = int(ad.get("Id"))
                for url in extract_urls_from_ad(ad):
                    status, error = check_url(url, timeout=args.timeout)

                    if status == 200:
                        msg = f"  Объявление {ad_id}: ссылка {url} отвечает 200 (OK)"
                    elif status is None:
                        any_issue = True
                        err_text = f"ошибка запроса {url}: {error}"
                        msg = f"  Объявление {ad_id}: {err_text}"
                        issues_http[campaign_id].add(ad_id)
                    else:
                        any_issue = True
                        err_text = f"ссылка {url} отвечает {status}"
                        msg = f"  Объявление {ad_id}: {err_text}"
                        issues_http[campaign_id].add(ad_id)

                    print(msg)
                    lines.append(msg)

        except RuntimeError as e:
            any_issue = True
            err_text = f"ошибка обращения к API для кампании {campaign_id}: {e}"
            msg = f"  [API ERROR] {err_text}"
            print(msg)
            lines.append(msg)
            issues_api[campaign_id] = err_text
            # переходим к следующей кампании

    summary_line = (
        "Найдены ссылки с отличным от 200 ответом."
        if any_issue
        else "Все ссылки возвращают 200."
    )
    print(summary_line)
    lines.append(summary_line)

    # --- запись полного лога в файл ---
    now_str = time.strftime("%Y-%m-%d %H:%M:%S")

    log_content = []
    log_content.append(f"Отчёт проверки ссылок")
    log_content.append(f"Дата и время запуска: {now_str}")
    log_content.append("")
    log_content.extend(lines)

    try:
        with open(args.output_file, "w", encoding="utf-8") as f:
            f.write("\n".join(log_content))
        print(f"\nРезультат сохранён: {args.output_file}")
    except Exception as e:
        print(f"Ошибка записи файла: {e}", file=sys.stderr)

    # --- подготовка и отправка красивого отчёта в Telegram ---
    if args.telegram_token and args.telegram_chat_id:
        if issues_http or issues_api:
            now_str = time.strftime("%Y-%m-%d %H:%M:%S")
            total_campaigns_http = len(issues_http)
            total_ads_http = sum(len(ads) for ads in issues_http.values())

            report_lines: List[str] = []
            report_lines.append("Отчёт проверки ссылок")
            report_lines.append(f"Время запуска: {now_str}")
            report_lines.append("")
            report_lines.append("Ошибки проверки ссылок (ответ не 200):")

            if issues_http:
                report_lines.append(
                    f"- кампаний с ошибками: {total_campaigns_http}, объявлений с ошибками: {total_ads_http}"
                )
                report_lines.append("")

                for camp_id, ad_ids in sorted(issues_http.items()):
                    report_lines.append(f"Кампания {camp_id}:")
                    for ad_id in sorted(ad_ids):
                        report_lines.append(f"  - Объявление {ad_id}")
                    report_lines.append("")
            else:
                report_lines.append("- HTTP-ошибок по ссылкам нет.")
                report_lines.append("")

            if issues_api:
                report_lines.append("Ошибки API Яндекс.Директа:")
                for camp_id, err in sorted(issues_api.items()):
                    report_lines.append(f"Кампания {camp_id}: {err}")
                report_lines.append("")

            report_lines.append(f"Полный лог: {args.output_file}")

            text = "\n".join(report_lines)
            if len(text) > 4000:
                text = text[:3990] + "\n…обрезано, см. полный лог в файле."

            sent = send_telegram_message(args.telegram_token, args.telegram_chat_id, text)
            print("\nКраткий отчёт:")
            print(text)
            if sent:
                print("Краткий отчёт отправлен в Telegram.")
            else:
                print("Не удалось отправить краткий отчёт в Telegram, см. сообщение об ошибке выше.")
        else:
            ok_text = "Отчёт проверки ссылок: все ссылки возвращают 200. Ошибок не найдено."
            sent = send_telegram_message(args.telegram_token, args.telegram_chat_id, ok_text)
            print("\nСообщение для Telegram:")
            print(ok_text)
            if sent:
                print("Сообщение об отсутствии ошибок отправлено в Telegram.")
            else:
                print("Не удалось отправить сообщение в Telegram, см. сообщение об ошибке выше.")
    else:
        print("TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не заданы, отчёт в Telegram не отправлен.")

    duration = time.time() - start_time
    print(f"Время выполнения программы: {duration:.2f} сек.")
    finish_str = time.strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(args.output_file, "a", encoding="utf-8") as f:
            f.write("\n")
            f.write(f"Дата и время окончания: {finish_str}\n")
            f.write(f"Время выполнения: {duration:.2f} сек.\n")
    except Exception as e:
        print(f"Ошибка записи завершения лога: {e}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
