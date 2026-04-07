import asyncio
import json
import os
import re
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen, Request
from urllib.parse import quote

from telethon import TelegramClient
from telethon.sessions import StringSession

api_id = int(os.getenv("API_ID", "2040"))
api_hash = os.getenv("API_HASH", "b18441a1ff607e10a989891a5462e627")
session_string = os.getenv("SESSION_STRING", "")

sources_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRJXb9M9fgCWtRWdmo-8Uv3wkkwbnP6L71Nfwwt9V7HCF5zSWheBduunl0WA9gykUGWqjq6I-sqKw92/pub?gid=1276245514&single=true&output=csv"

if session_string:
    client = TelegramClient(StringSession(session_string), api_id, api_hash)
else:
    client = TelegramClient("session", api_id, api_hash)


def load_key_value_csv(url):
    with urlopen(url) as response:
        content = response.read().decode("utf-8-sig")

    data = {}
    for row in content.splitlines():
        parts = [p.strip().strip('"') for p in row.split(",", 1)]
        if len(parts) == 2 and parts[0]:
            data[parts[0]] = parts[1]
    return data


def load_list(url):
    with urlopen(url) as response:
        content = response.read().decode("utf-8-sig")
    return [row.strip().strip('"').lower() for row in content.splitlines() if row.strip()]


def load_weights(url):
    with urlopen(url) as response:
        content = response.read().decode("utf-8-sig")

    rules = []
    for row in content.splitlines():
        parts = [p.strip().strip('"') for p in row.split(",", 1)]
        if len(parts) != 2:
            continue

        phrase = parts[0].lower()
        if not phrase:
            continue

        try:
            weight = int(parts[1])
        except ValueError:
            continue

        rules.append((phrase, weight))

    return rules


def load_seen_from_csv(url):
    with urlopen(url) as response:
        content = response.read().decode("utf-8-sig")

    lines = content.splitlines()
    if not lines:
        return set()

    return set(line.strip() for line in lines[1:] if line.strip())


def append_seen_to_sheet(writer_url, unique_id):
    payload = json.dumps({"message_id": unique_id}).encode("utf-8")

    request = Request(
        writer_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    with urlopen(request) as response:
        raw = response.read().decode("utf-8")

    result = json.loads(raw)

    if not result.get("ok"):
        raise RuntimeError(f"Ошибка записи seen_id: {result}")

    return result


def normalize_text(text):
    text = (text or "").lower().replace("ё", "е")
    text = re.sub(r"[^\w\s\-+/]", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text, flags=re.UNICODE).strip()
    return text


def phrase_in_text(text, phrase):
    text_n = normalize_text(text)
    phrase_n = normalize_text(phrase)

    if not phrase_n:
        return False

    if " " in phrase_n:
        return phrase_n in text_n

    pattern = rf"(?<!\w){re.escape(phrase_n)}(?!\w)"
    return re.search(pattern, text_n, flags=re.UNICODE) is not None


def contains_any(text, phrases):
    return any(phrase_in_text(text, phrase) for phrase in phrases)


def base_priority(text, skip_words, high_words, medium_words):
    if contains_any(text, skip_words):
        return "skip"

    if contains_any(text, high_words):
        return "high"

    if contains_any(text, medium_words):
        return "medium"

    return "neutral"


def calc_score(text, weight_rules):
    score = 0
    matched = []

    for phrase, weight in weight_rules:
        if phrase_in_text(text, phrase):
            score += weight
            matched.append(f"{phrase} ({weight:+d})")

    return score, matched


def classify(text, skip_words, high_words, medium_words, weight_rules, threshold):
    priority = base_priority(text, skip_words, high_words, medium_words)

    if priority == "skip":
        return "skip", -999, []

    score, matched = calc_score(text, weight_rules)

    if priority == "high":
        score += 3
        matched.append("base_high (+3)")
    elif priority == "medium":
        score += 1
        matched.append("base_medium (+1)")

    if score >= threshold:
        return "send", score, matched

    return "skip", score, matched


def send(text, bot_token, chat_id):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = f"chat_id={chat_id}&text={quote(text)}&disable_web_page_preview=true"
    full_url = f"{url}?{data}"

    with urlopen(full_url) as response:
        return response.read().decode("utf-8")


def is_recent_enough(message_date, cutoff_date):
    if message_date.tzinfo is None:
        message_date = message_date.replace(tzinfo=timezone.utc)
    return message_date >= cutoff_date


def format_date(dt):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone().strftime("%d.%m.%Y %H:%M")


def build_message(template, formatted_date, channel, score, link, preview, matched_text):
    header = template.get("header", "🔥 Новая вакансия")
    show_score = template.get("show_score", "yes").strip().lower() == "yes"
    show_matches = template.get("show_matches", "yes").strip().lower() == "yes"

    parts = [header, ""]
    parts.append(f"📅 {formatted_date}")
    parts.append(f"📡 {channel}")

    if show_score:
        parts.append(f"🎯 score: {score}")

    parts.append(f"🔗 {link}")
    parts.append("")
    parts.append(f"📝 {preview}")

    if show_matches:
        parts.append("")
        parts.append(f"🧠 {matched_text}")

    return "\n".join(parts)


async def process_channel(channel, config, seen_ids):
    print(f"\n📡 Обрабатываю: {channel}")

    async for message in client.iter_messages(channel, limit=config["limit"]):
        text = message.message or ""
        if not text.strip():
            continue

        if not is_recent_enough(message.date, config["cutoff"]):
            continue

        unique_id = f"{channel}:{message.id}"
        if unique_id in seen_ids:
            continue

        decision, score, matched = classify(
            text,
            config["skip"],
            config["high"],
            config["medium"],
            config["weights"],
            config["threshold"]
        )

        if decision == "skip":
            continue

        formatted_date = format_date(message.date)
        link = f"https://t.me/{channel}/{message.id}"
        preview = text.replace("\n", " ")[:config["preview_length"]]
        matched_text = ", ".join(matched[:8]) if matched else "нет"

        msg = build_message(
            config["template"],
            formatted_date,
            channel,
            score,
            link,
            preview,
            matched_text
        )

        print(f"📅 {formatted_date}")
        print(f"📡 {channel}")
        print(f"🎯 score: {score}")
        print(f"🔗 {link}")
        print(f"📝 {preview}...")
        print(f"🧠 {matched_text}")
        print("-" * 60)

        send(msg, config["bot_token"], config["group_chat_id"])
        append_seen_to_sheet(config["seen_writer_url"], unique_id)
        seen_ids.add(unique_id)
        config["sent"] += 1


async def main():
    print("🚀 запуск...\n")

    sources = load_key_value_csv(sources_url)

    skip_words = load_list(sources["skip_url"])
    high_words = load_list(sources["high_url"])
    medium_words = load_list(sources["medium_url"])
    channels = load_list(sources["channels_url"])
    settings = load_key_value_csv(sources["settings_url"])
    weight_rules = load_weights(sources["weights_url"])
    template = load_key_value_csv(sources["template_url"])
    seen_ids = load_seen_from_csv(sources["seen_ids_url"])

    config = {
        "skip": skip_words,
        "high": high_words,
        "medium": medium_words,
        "weights": weight_rules,
        "template": template,
        "group_chat_id": settings["group_chat_id"],
        "bot_token": settings["bot_token"],
        "limit": int(settings.get("limit", 300)),
        "preview_length": int(settings.get("preview_length", 300)),
        "threshold": int(settings.get("score_threshold", 3)),
        "cutoff": datetime.now(timezone.utc) - timedelta(days=int(settings.get("days_back", 90))),
        "sent": 0,
        "seen_writer_url": sources["seen_writer_url"],
    }

    print(f"каналов: {len(channels)}")
    print(f"limit: {config['limit']}")
    print(f"threshold: {config['threshold']}")
    print(f"weights: {len(weight_rules)}")
    print(f"беру не старше: {config['cutoff'].strftime('%d.%m.%Y %H:%M UTC')}")
    print(f"уже обработано: {len(seen_ids)}\n")

    tasks = [process_channel(channel, config, seen_ids) for channel in channels]
    await asyncio.gather(*tasks)

    print(f"\n✅ отправлено: {config['sent']}")


async def runner():
    await client.start()
    await main()
    await client.disconnect()


asyncio.run(runner())
