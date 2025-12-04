import argparse
import os
import sys
from typing import Dict, Iterable, List, Optional, Tuple
import time
from collections import defaultdict

import requests

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
                # Только реально крутящиеся объявления
                if state != "ON":
                    continue
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
        response = requests.get(url, headers=headers, allow_redirects=True, timeout=timeout)
        return response.status_code, None
    except requests.RequestException as exc:
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
        # parse_mode специально НЕ включаем, чтобы не ловить ошибки парсинга
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


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    start_time = time.time()

    # Генерируем имя файла с timestamp
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

    # HTTP-ошибки по ссылкам:
    # кампания_id -> список проблем:
    # (ad_id, url, status_code_or_None, error_text_or_None)
    issues_http: Dict[int, List[Tuple[int, str, Optional[int], Optional[str]]]] = defaultdict(list)
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

                    if status is not None and 200 <= status < 300:
                        # Любой 2xx считаем ОК (включая 202 и т.п.)
                        msg = f"  Объявление {ad_id}: ссылка {url} отвечает {status} (OK)"
                    elif status is None:
                        any_issue = True
                        err_text = f"ошибка запроса {error}" if error else "ошибка запроса, подробности отсутствуют"
                        msg = f"  Объявление {ad_id}: ссылка {url}: {err_text}"
                        issues_http[campaign_id].append((ad_id, url, None, err_text))
                    else:
                        any_issue = True
                        desc = HTTP_STATUS_DESCRIPTIONS.get(status)
                        if desc:
                            msg = f"  Объявление {ad_id}: ссылка {url} отвечает {status} ({desc})"
                        else:
                            msg = f"  Объявление {ad_id}: ссылка {url} отвечает {status}"
                        issues_http[campaign_id].append((ad_id, url, status, desc))

                    print(msg)
                    lines.append(msg)

        except RuntimeError as e:
            any_issue = True
            err_text = f"ошибка обращения к API для кампании {campaign_id}: {e}"
            msg = f"  [API ERROR] {err_text}"
            print(msg)
            lines.append(msg)
            issues_api[campaign_id] = err_text

    summary_line = (
        "Найдены ссылки с отличным от 2xx ответом."
        if any_issue
        else "Все ссылки возвращают 2xx."
    )
    print(summary_line)
    lines.append(summary_line)

    # --- запись полного лога в файл ---
    now_str = time.strftime("%Y-%m-%d %H:%M:%S")

    log_content: List[str] = []
    log_content.append("Отчёт проверки ссылок")
    log_content.append(f"Дата и время запуска: {now_str}")
    log_content.append("")
    log_content.extend(lines)

    try:
        with open(args.output_file, "w", encoding="utf-8") as f:
            f.write("\n".join(log_content))
        print(f"\nРезультат сохранён: {args.output_file}")
    except Exception as e:
        print(f"Ошибка записи файла: {e}", file=sys.stderr)

    # --- подготовка и отправка отчёта в Telegram ---
    if args.telegram_token and args.telegram_chat_id:
        now_str = time.strftime("%Y-%m-%d %H:%M:%S")
        if issues_http or issues_api:
            total_campaigns_http = len(issues_http)
            total_ads_http = sum(len(ads) for ads in issues_http.values())

            report_lines: List[str] = []
            report_lines.append(f"✨ Отчёт проверки ссылок — {now_str}")
            report_lines.append("")
            report_lines.append("❌ Ошибки найдены")
            report_lines.append(f"📂 Кампаний с ошибками: {total_campaigns_http}")
            report_lines.append(f"📣 Объявлений с ошибками: {total_ads_http}")
            report_lines.append("")

            if issues_http:
                # Группируем проблемы по типам: 404, другие коды, без кода (ошибка сети и т.п.)
                group_404: Dict[int, List[Tuple[int, str, Optional[int], Optional[str]]]] = defaultdict(list)
                group_other: Dict[int, List[Tuple[int, str, Optional[int], Optional[str]]]] = defaultdict(list)
                group_no_code: Dict[int, List[Tuple[int, str, Optional[int], Optional[str]]]] = defaultdict(list)

                for camp_id, problems in issues_http.items():
                    for ad_id, url, status_code, err_text in problems:
                        if status_code is None:
                            group_no_code[camp_id].append((ad_id, url, status_code, err_text))
                        elif status_code == 404:
                            group_404[camp_id].append((ad_id, url, status_code, err_text))
                        else:
                            group_other[camp_id].append((ad_id, url, status_code, err_text))

                report_lines.append("📌 Проблемные кампании:")

                if group_404:
                    report_lines.append("🔴 Ответ 404 (страница не найдена):")
                    for camp_id, problems in sorted(group_404.items()):
                        report_lines.append(f"- Кампания {camp_id}:")
                        for ad_id, url, status_code, err_text in problems:
                            report_lines.append(
                                f"  • Объявление {ad_id}: ссылка {url} отвечает 404 (страница не найдена)."
                            )
                        report_lines.append("")

                if group_other:
                    report_lines.append("🟠 Другие коды ошибок:")
                    for camp_id, problems in sorted(group_other.items()):
                        report_lines.append(f"- Кампания {camp_id}:")
                        for ad_id, url, status_code, err_text in problems:
                            code_str = str(status_code) if status_code is not None else "?"
                            desc = err_text or HTTP_STATUS_DESCRIPTIONS.get(status_code, "")
                            if desc:
                                report_lines.append(
                                    f"  • Объявление {ad_id}: ссылка {url} отвечает {code_str} ({desc})."
                                )
                            else:
                                report_lines.append(
                                    f"  • Объявление {ad_id}: ссылка {url} отвечает {code_str}."
                                )
                        report_lines.append("")

                if group_no_code:
                    report_lines.append("⚪ Код не получен (проверьте вручную):")
                    for camp_id, problems in sorted(group_no_code.items()):
                        report_lines.append(f"- Кампания {camp_id}:")
                        for ad_id, url, status_code, err_text in problems:
                            text_err = err_text or "код не получен, проверьте вручную"
                            report_lines.append(
                                f"  • Объявление {ad_id}: ссылка {url} — {text_err}."
                            )
                        report_lines.append("")

            if issues_api:
                report_lines.append("⚠ Ошибки API Яндекс.Директа:")
                for camp_id, err in sorted(issues_api.items()):
                    report_lines.append(f"- Кампания {camp_id}: {err}")
                report_lines.append("")

            report_lines.append(f"📄 Полный лог: {args.output_file}")

            text = "\n".join(report_lines)
            if len(text) > 4000:
                text = text[:3990] + "\n…обрезано, см. полный лог в файле."

            sent_msg = send_telegram_message(args.telegram_token, args.telegram_chat_id, text)
            print("\nКраткий отчёт:")
            print(text)
            if sent_msg:
                print("Краткий отчёт отправлен в Telegram.")
            else:
                print("Не удалось отправить краткий отчёт в Telegram, см. сообщение об ошибке выше.")

            caption = "Полный лог проверки ссылок во вложении."
            sent_doc = send_telegram_document(args.telegram_token, args.telegram_chat_id, args.output_file, caption)
            if sent_doc:
                print("Файл лога отправлен в Telegram.")
            else:
                print("Не удалось отправить файл лога в Telegram, см. сообщение об ошибке выше.")
        else:
            ok_text = f"✨ Отчёт проверки ссылок — {now_str}\n\n🟢 Ошибок не найдено. Все ссылки отвечают 2xx."
            sent_msg = send_telegram_message(args.telegram_token, args.telegram_chat_id, ok_text)
            print("\nСообщение для Telegram:")
            print(ok_text)
            if sent_msg:
                print("Сообщение об отсутствии ошибок отправлено в Telegram.")
            else:
                print("Не удалось отправить сообщение в Telegram, см. сообщение об ошибке выше.")

            caption = "Полный лог проверки ссылок (ошибок не найдено)."
            sent_doc = send_telegram_document(args.telegram_token, args.telegram_chat_id, args.output_file, caption)
            if sent_doc:
                print("Файл лога отправлен в Telegram.")
            else:
                print("Не удалось отправить файл лога в Telegram, см. сообщение об ошибке выше.")
    else:
        print("TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не заданы, отчёт в Telegram не отправлен.")

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
