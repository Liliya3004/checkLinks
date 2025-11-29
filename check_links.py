import argparse
import os
import sys
from typing import Dict, Iterable, List, Optional, Tuple

import requests

API_URL_BASE = "https://api.direct.yandex.com/json/v5"


class YandexDirectClient:
    def __init__(self, token: str, client_login: str, language: str = "ru") -> None:
        self.token = token
        self.client_login = client_login
        self.language = language

    def _request(self, service: str, method: str, params: Dict) -> Dict:
        url = f"{API_URL_BASE}/{service}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Client-Login": self.client_login,
            "Accept-Language": self.language,
        }
        response = requests.post(url, json={"method": method, "params": params}, headers=headers)
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
        "--output-file",
        default="results.txt",
        help="Файл для сохранения результатов проверки (по умолчанию results.txt).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="Таймаут проверки ссылки в секундах.",
    )
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    if not args.token or not args.client_login:
        print("Необходимо передать токен и Client-Login через параметры или переменные окружения.", file=sys.stderr)
        return 1

    client = YandexDirectClient(token=args.token, client_login=args.client_login)
    any_issue = False
    lines: List[str] = []  # собираем всё сюда

    for campaign_id, name in client.iter_active_campaign_ids():
        header = f"Кампания: {name} (ID {campaign_id})"
        print(header)
        lines.append(header)

        for ad in client.iter_ads(campaign_id):
            ad_id = ad.get("Id")
            for url in extract_urls_from_ad(ad):
                status, error = check_url(url, timeout=args.timeout)

                if status == 500:
                    msg = f"  Объявление {ad_id}: ссылка {url} отвечает 500 (OK)"
                elif status is None:
                    any_issue = True
                    msg = f"  Объявление {ad_id}: ошибка запроса {url}: {error}"
                else:
                    any_issue = True
                    msg = f"  Объявление {ad_id}: ссылка {url} отвечает {status}"

                print(msg)
                lines.append(msg)

    summary_line = (
        "Найдены ссылки с отличным от 500 ответом."
        if any_issue
        else "Все ссылки возвращают 500."
    )

    print(summary_line)
    lines.append(summary_line)

    # === Сохранение файла ===
    try:
        with open(args.output_file, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        print(f"\nРезультат сохранён: {args.output_file}")
    except Exception as e:
        print(f"Ошибка записи файла: {e}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
