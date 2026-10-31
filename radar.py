#!/usr/bin/env python3
"""Радар приложений: сбор данных из публичных чартов App Store, расчёт роста и тем, локальная страница.

Команды:
  python3 radar.py check     проверить, какие источники Apple отвечают
  python3 radar.py collect   собрать снимок чартов за сегодня
  python3 radar.py build     пересобрать страницу из накопленных данных
  python3 radar.py serve     открыть страницу в браузере
  python3 radar.py all       collect + build + serve (по умолчанию)

Только стандартная библиотека Python 3. Данные лежат в файле radar.db рядом со скриптом.
"""
import datetime as dt
import json
import os
import re
import sqlite3
import statistics
import sys
import threading
import time
import urllib.parse
import urllib.request
import webbrowser
from collections import Counter, defaultdict
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, "radar.db")
SITE = os.path.join(HERE, "site")

# ---------------------------------------------------------------- настройки
COUNTRIES = {"us": "США", "de": "Германия", "br": "Бразилия", "tr": "Турция"}
# Игры не берём: нас интересуют обычные приложения
GENRES = {
    6000: "Бизнес", 6001: "Погода", 6002: "Утилиты", 6003: "Путешествия", 6004: "Спорт",
    6005: "Соцсети", 6006: "Справочники", 6007: "Продуктивность", 6008: "Фото и видео",
    6009: "Новости", 6010: "Навигация", 6011: "Музыка", 6012: "Образ жизни",
    6013: "Здоровье и фитнес", 6015: "Финансы", 6016: "Развлечения", 6017: "Образование",
    6018: "Книги", 6020: "Медицина", 6023: "Еда и напитки", 6024: "Покупки",
}
RU_GENRE = {  # названия категорий из lookup -> по-русски
    "Business": "Бизнес", "Weather": "Погода", "Utilities": "Утилиты", "Travel": "Путешествия",
    "Sports": "Спорт", "Social Networking": "Соцсети", "Reference": "Справочники",
    "Productivity": "Продуктивность", "Photo & Video": "Фото и видео", "News": "Новости",
    "Navigation": "Навигация", "Music": "Музыка", "Lifestyle": "Образ жизни",
    "Health & Fitness": "Здоровье и фитнес", "Finance": "Финансы", "Entertainment": "Развлечения",
    "Education": "Образование", "Books": "Книги", "Medical": "Медицина", "Food & Drink": "Еда и напитки",
    "Shopping": "Покупки", "Games": "Игры",
}
CHART_LIMIT = 200          # приложений в каждом чарте категории
MAX_AGE_DAYS = 365         # «новое» приложение = вышло не раньше этого срока
DOWNLOADS_PER_RATING = 70  # грубо: одна оценка на ~70 загрузок. Это ориентир, не факт
REVENUE_PER_DOWNLOAD = 0.30  # грубо: $ на одну загрузку у бесплатных приложений. Ориентир
GIANT_RATINGS = 5000       # «крупный игрок» = больше стольких оценок
UA = {"User-Agent": "radar-app/1.0 (personal research tool)"}
TILES = ["#86CB95", "#7AAEDC", "#B0A4DB", "#7CC4BC", "#D9B060", "#DDA3BF", "#CDBF9C", "#E3897A"]
STOP = set("""app apps the and for with your you free pro lite plus best new top what who how
this that from have into more most pure simple easy fast smart super mobile online official
для как это что или про все ваш мой приложение""".split())


# ---------------------------------------------------------------- сеть
def http_json(url, retries=5):
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:  # noqa
            last = e
            time.sleep(4 * (i + 1))
    raise RuntimeError(f"{url}: {last}")


def fetch_chart(cc, genre_id, limit=CHART_LIMIT):
    """Топ бесплатных приложений категории. Возвращает список id по порядку (1, 2, 3 ...)."""
    url = f"https://itunes.apple.com/{cc}/rss/topfreeapplications/limit={limit}/genre={genre_id}/json"
    time.sleep(0.4)
    data = http_json(url)
    entries = data.get("feed", {}).get("entry", []) or []
    if isinstance(entries, dict):
        entries = [entries]
    ids = []
    for e in entries:
        try:
            ids.append(int(e["id"]["attributes"]["im:id"]))
        except Exception:
            continue
    return ids


def fetch_chart_overall(cc, limit=100):
    url = f"https://rss.applemarketingtools.com/api/v2/{cc}/apps/top-free/{limit}/apps.json"
    data = http_json(url)
    return [int(r["id"]) for r in data.get("feed", {}).get("results", [])]


def fetch_lookup(ids, cc):
    out = {}
    ids = list(ids)
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        url = "https://itunes.apple.com/lookup?" + urllib.parse.urlencode(
            {"id": ",".join(map(str, chunk)), "country": cc, "entity": "software"})
        time.sleep(3)
        data = http_json(url)
        for r in data.get("results", []):
            if r.get("trackId"):
                out[int(r["trackId"])] = r
    return out


# ---------------------------------------------------------------- база
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS apps(id INTEGER PRIMARY KEY, name TEXT, dev TEXT, genre TEXT,
        release TEXT, updated TEXT, price REAL, icon TEXT);
    CREATE TABLE IF NOT EXISTS chart(day TEXT, cc TEXT, genre INTEGER, app_id INTEGER, rank INTEGER,
        PRIMARY KEY(day, cc, genre, app_id));
    CREATE TABLE IF NOT EXISTS stats(day TEXT, cc TEXT, app_id INTEGER, rating_count INTEGER, rating_avg REAL,
        PRIMARY KEY(day, cc, app_id));
    """)
    return conn


def collect(conn, today=None, fetch_chart_fn=None, fetch_overall_fn=None, fetch_lookup_fn=None, log=print):
    today = today or dt.date.today().isoformat()
    fetch_chart_fn = fetch_chart_fn or fetch_chart
    fetch_overall_fn = fetch_overall_fn or fetch_chart_overall
    fetch_lookup_fn = fetch_lookup_fn or fetch_lookup
    total = 0
    for cc in COUNTRIES:
        ids_seen = {}
        ok_genres = 0
        for gid, gname in GENRES.items():
            try:
                ids = fetch_chart_fn(cc, gid)
            except Exception as e:  # noqa
                log(f"  ! {cc}/{gname}: чарт не получен ({str(e)[:90]})")
                continue
            ok_genres += 1
            conn.execute("DELETE FROM chart WHERE day=? AND cc=? AND genre=?", (today, cc, gid))
            for rank, aid in enumerate(ids, 1):
                conn.execute("INSERT OR REPLACE INTO chart VALUES(?,?,?,?,?)", (today, cc, gid, aid, rank))
                ids_seen.setdefault(aid, True)
        if ok_genres == 0:  # запасной вариант: общий топ
            try:
                ids = fetch_overall_fn(cc)
                conn.execute("DELETE FROM chart WHERE day=? AND cc=? AND genre=0", (today, cc))
                for rank, aid in enumerate(ids, 1):
                    conn.execute("INSERT OR REPLACE INTO chart VALUES(?,?,?,?,?)", (today, cc, 0, aid, rank))
                    ids_seen.setdefault(aid, True)
                log(f"  {cc}: категории недоступны, взят общий топ ({len(ids)})")
            except Exception as e:  # noqa
                log(f"  ! {cc}: ни один чарт не доступен ({str(e)[:90]})")
                continue
        info = fetch_lookup_fn(list(ids_seen), cc)
        for aid, r in info.items():
            conn.execute("INSERT OR REPLACE INTO apps VALUES(?,?,?,?,?,?,?,?)", (
                aid, r.get("trackName", ""), r.get("artistName", ""), r.get("primaryGenreName", ""),
                (r.get("releaseDate") or "")[:10], (r.get("currentVersionReleaseDate") or "")[:10],
                float(r.get("price") or 0), r.get("artworkUrl100", "")))
            if r.get("userRatingCount") is not None:
                conn.execute("INSERT OR REPLACE INTO stats VALUES(?,?,?,?,?)", (
                    today, cc, aid, int(r.get("userRatingCount") or 0), float(r.get("averageUserRating") or 0)))
        total += len(ids_seen)
        log(f"  {COUNTRIES[cc]}: приложений в чартах {len(ids_seen)}, данных получено {len(info)}")
    conn.commit()
    return total


# ---------------------------------------------------------------- анализ
GENERIC = set("""live tracker player scanner voice video music chat social audio reader stories story map radar weather
forecast digital offline car room book books camera photo photos editor maker creator generator manager helper
assistant studio wallet card cards game games play world time daily day tool tools kit hub pass pay go one
my me you it get use scan browser translate translator keyboard widget widgets lock call calls message messages
mail email note notes todo list lists watch show shows tv radio news sports sport food coffee pizza""".split())


def phrases(name):
    toks = [t for t in tokens(name)]
    out = set(t for t in toks if t not in GENERIC)
    for a, b in zip(toks, toks[1:]):
        if not (a in GENERIC and b in GENERIC):
            out.add(a + " " + b)
    return out

def tokens(name):
    name = re.sub(r"[-–—:|•·]", " ", name.lower())
    out = []
    for w in re.findall(r"[a-zа-яё0-9]+", name):
        if w in STOP or len(w) < 3 and w != "ai" or w.isdigit():
            continue
        out.append(w)
    return out


def pct(new, old):
    if old is None or old <= 0 or new is None:
        return 0
    return int(round((new / old - 1) * 100))


def ensure_series(conn, cc, aid, today_d):
    rows = conn.execute("SELECT day, rating_count FROM stats WHERE cc=? AND app_id=? ORDER BY day", (cc, aid)).fetchall()
    return [(dt.date.fromisoformat(d), c) for d, c in rows]


def daily_rates(points, today_d, window=91):
    """Оценок в сутки по дням: разность накопленных оценок / число дней между снимками."""
    rates = {}
    for (d0, c0), (d1, c1) in zip(points, points[1:]):
        gap = (d1 - d0).days
        if gap <= 0:
            continue
        r = max(0, (c1 - c0)) / gap
        for k in range(gap):
            rates[d0 + dt.timedelta(days=k + 1)] = r
    series = []
    for i in range(window):
        day = today_d - dt.timedelta(days=window - 1 - i)
        series.append(round(rates[day], 1) if day in rates else None)
    return series


def mean(vals):
    vals = [v for v in vals if v is not None]
    return sum(vals) / len(vals) if vals else None


def build_data(conn, today=None, log=print):
    today_d = dt.date.fromisoformat(today) if today else dt.date.today()
    today_s = today_d.isoformat()
    last_day = conn.execute("SELECT MAX(day) FROM chart").fetchone()[0]
    if not last_day:
        raise RuntimeError("В базе нет данных. Сначала запустите сбор.")
    last_d = dt.date.fromisoformat(last_day)
    n_days = conn.execute("SELECT COUNT(DISTINCT day) FROM chart").fetchone()[0]

    apps_rows = {r[0]: r for r in conn.execute("SELECT id,name,dev,genre,release,updated,price,icon FROM apps")}
    # лучший ранг и страна в последний день
    best = {}
    for aid, cc, gid, rank in conn.execute("SELECT app_id, cc, genre, rank FROM chart WHERE day=?", (last_day,)):
        cur = best.get(aid)
        if cur is None or rank < cur[1] or (rank == cur[1] and cc == "us"):
            best[aid] = (cc, rank, gid)
    # ранг неделю назад
    prev_rank = {}
    week_ago = (last_d - dt.timedelta(days=7)).isoformat()
    prev_day = conn.execute("SELECT MAX(day) FROM chart WHERE day<=?", (week_ago,)).fetchone()[0]
    if prev_day is None:
        prev_day = conn.execute("SELECT MIN(day) FROM chart WHERE day<?", (last_day,)).fetchone()[0]
    if prev_day:
        for aid, cc, gid, rank in conn.execute("SELECT app_id, cc, genre, rank FROM chart WHERE day=?", (prev_day,)):
            cur = prev_rank.get((aid, cc))
            if cur is None or rank < cur:
                prev_rank[(aid, cc)] = rank

    apps = []
    all_known = {}
    for aid, (cc, rank, gid) in best.items():
        r = apps_rows.get(aid)
        if not r or not r[4]:
            continue
        release = dt.date.fromisoformat(r[4])
        age = max(1, (last_d - release).days)
        pts = ensure_series(conn, cc, aid, last_d)
        ratings = pts[-1][1] if pts else 0
        series = daily_rates(pts, last_d)
        known = [v for v in series if v is not None]
        history_days = len(known)
        if history_days >= 2:
            per_day = mean(series[-7:]) or mean(known) or 0
        else:
            per_day = ratings / age  # пока нет истории: средний темп за всю жизнь
        genre_ru = RU_GENRE.get(r[3], r[3])
        info = {
            "id": f"a{aid}", "aid": aid, "name": r[1], "dev": r[2], "cat": genre_ru, "age": age,
            "cn": cc.upper(), "rank": rank, "ratings": ratings, "perDay": int(round(per_day)), "price": r[6],
            "history": history_days, "series": series, "release": release,
        }
        all_known[aid] = info
        if age <= MAX_AGE_DAYS:
            apps.append(info)

    # рост по окнам: средний темп последних W дней против предыдущих W
    for a in apps:
        s = a["series"]
        # окна ограничены накопленной историей: пока дней мало, сравниваем половину с половиной
        def g(w):
            W = min(w, a["history"] // 2)
            if W < 2:
                return 0
            return pct(mean(s[-W:]), mean(s[-2 * W:-W]))
        a["g"] = [g(14), g(30), g(90)] if a["history"] >= 4 else [0, 0, 0]
        pr = prev_rank.get((a["aid"], a["cn"].lower()))
        a["prev"] = pr if pr else a["rank"]
        dl = a["perDay"] * 30 * DOWNLOADS_PER_RATING
        a["dl"] = int(dl)
        a["m"] = int(dl * a["price"]) if a["price"] > 0 else int(dl * REVENUE_PER_DOWNLOAD)
        a["words"] = tokens(a["name"])[:4]
        # сигнал
        if a["history"] >= 4:
            sig = "fast" if a["g"][0] >= 25 else ("cool" if a["g"][0] <= -15 else "watch")
        else:
            sig = "fast" if (a["age"] <= 60 and a["rank"] <= 50) else "watch"
        a["signal"] = sig
        # начало ускорения
        onset = -1
        L = len(s)
        known_idx = [i for i, v in enumerate(s) if v is not None]
        if len(known_idx) >= 6:
            base = statistics.median([s[i] for i in known_idx[:max(3, len(known_idx) // 3)]]) or 0.1
            for i in known_idx:
                if s[i] >= 2 * max(base, 0.5):
                    onset = a["age"] - (L - 1) + i
                    break
        a["onset"] = onset
        a["k"] = 1
        a["tile"] = TILES[a["aid"] % len(TILES)]
        why = [f"Вышло {a['age']} дн. назад, сейчас #{a['rank']} в чарте категории ({COUNTRIES[a['cn'].lower()]})"]
        if prev_day and a["prev"] != a["rank"]:
            d = a["prev"] - a["rank"]
            why.append(f"Позиция {'выросла' if d > 0 else 'упала'} на {abs(d)} мест с {prev_day}")
        why.append(f"Набирает около {a['perDay']} оценок в сутки" + ("" if a["history"] >= 2 else " (пока средний темп за всю жизнь, истории мало)"))
        if a["history"] < 4:
            why.append(f"Данные накапливаются: снимков {n_days}, для роста нужно хотя бы 4 дня")
        a["why"] = why

    # темы по словам и парам слов в названиях
    by_token = defaultdict(set)
    for a in apps:
        for w in phrases(a["name"]):
            by_token[w].add(a["aid"])
    by_aid = {a["aid"]: a for a in apps}
    giants_apps = [set(tokens(i["name"])) for i in all_known.values() if i["ratings"] > GIANT_RATINGS]
    themes = []
    cand = [w for w, st in by_token.items() if len(st) >= 3]
    # сначала пары слов (они точнее), потом одиночные, внутри по числу приложений
    chosen = sorted(cand, key=lambda w: (-(" " in w), -len(by_token[w])))[:30]
    assigned = {}
    for w in chosen:
        members = [by_aid[x] for x in by_token[w] if by_aid[x]["aid"] not in assigned]
        if len(members) < 3:
            continue
        for m in members:
            assigned[m["aid"]] = w
        ws = set(w.split())
        giants = sum(1 for gt in giants_apps if ws <= gt)
        weeks = [0] * 8
        for m in members:
            wk = (last_d - m["release"]).days // 7
            if 0 <= wk < 8:
                weeks[7 - wk] += 1
        cats = Counter(m["cat"] for m in members)
        per_cc = Counter(m["cn"] for m in members)
        hot = per_cc.most_common(1)[0][0]
        gaps = [COUNTRIES[c] for c in COUNTRIES if per_cc.get(c.upper(), 0) == 0]
        co = Counter()
        for m in members:
            for t in set(tokens(m["name"])):
                if t not in ws and t not in GENERIC:
                    co[t] += 1
        kws = []
        for t, _n in co.most_common(3):
            rel = [m for m in members if t in tokens(m["name"])]
            total_pd = sum(m["perDay"] for m in rel)
            gi = sum(1 for m in rel if m["ratings"] > GIANT_RATINGS)
            kws.append({"w": f"{w} {t}", "demand": "высокий" if total_pd >= 100 else "средний" if total_pd >= 30 else "низкий",
                        "diff": "высокая" if gi >= 4 else "средняя" if gi >= 2 else "низкая"})
        themes.append({
            "id": "t_" + re.sub(r"\W", "", w), "name": w.capitalize(), "cat": cats.most_common(1)[0][0],
            "weeks": weeks, "giants": giants,
            "perDay": int(statistics.median([m["perDay"] for m in members])) if members else 0,
            "hot": COUNTRIES[hot.lower()], "gaps": ", ".join(gaps) if gaps else "везде есть",
            "kws": kws,
        })
    for a in apps:
        w = assigned.get(a["aid"])
        a["theme"] = "t_" + re.sub(r"\W", "", w) if w else "t_misc"
    if any(a["theme"] == "t_misc" for a in apps):
        misc = [a for a in apps if a["theme"] == "t_misc"]
        weeks = [0] * 8
        for m in misc:
            wk = (last_d - m["release"]).days // 7
            if 0 <= wk < 8:
                weeks[7 - wk] += 1
        themes.append({"id": "t_misc", "name": "Без общей темы", "cat": Counter(m["cat"] for m in misc).most_common(1)[0][0],
                       "weeks": weeks, "giants": 0, "perDay": int(statistics.median([m["perDay"] for m in misc])),
                       "hot": "—", "gaps": "—", "kws": [], "hidden": True})
    # убрать служебные поля
    out_apps = []
    for a in apps:
        out_apps.append({k: a[k] for k in ("id", "name", "dev", "cat", "theme", "age", "rank", "prev", "ratings", "perDay", "signal",
                                           "onset", "k", "tile", "why", "words", "m", "g", "dl", "cn", "series")} | {"store": "ios"})
    out_apps.sort(key=lambda x: -x["m"])
    return {"apps": out_apps, "themes": themes, "day": last_day, "snapshots": n_days}


# ---------------------------------------------------------------- страница
def option_html(pairs, with_all=False):
    html = ['<option value="all">Все категории</option>'] if with_all else []
    for v, label in pairs:
        html.append(f'<option value="{v}">{label}</option>')
    return "\n".join(html)


def build_site(data, template_path=None):
    template_path = template_path or os.path.join(HERE, "template.dc.html")
    s = open(template_path, encoding="utf-8").read()
    cats = sorted({a["cat"] for a in data["apps"]})
    ccs = sorted({a["cn"] for a in data["apps"]})
    country_names = {c.upper(): n for c, n in COUNTRIES.items()}
    block = ["const APPS = " + json.dumps(data["apps"], ensure_ascii=False) + ";",
             "const THEMES = " + json.dumps(data["themes"], ensure_ascii=False) + ";",
             "const COUNTRIES = " + json.dumps(country_names, ensure_ascii=False) + ";"]
    a = s.index("// DATA-BEGIN")
    b = s.index("// DATA-END") + len("// DATA-END")
    s = s[:a] + "\n".join(block) + s[b:]

    def swap(tag, html):
        nonlocal s
        pat = re.compile(rf"<!--{tag}-->.*?<!--/{tag}-->", re.S)
        assert pat.search(s), tag
        s = pat.sub(lambda m: html, s, count=1)
    swap("CATS-ALL", option_html([(c, c) for c in cats], True))
    swap("CATS-EX", option_html([(c, c) for c in cats]))
    swap("COUNTRIES", option_html([(c, country_names[c]) for c in country_names]))
    s = s.replace("Демо-данные", "Данные Apple · ориентир")
    if data["snapshots"] < 4:
        note = ('<p style="font-size:13px;color:#D9B060;max-width:780px">Собрано снимков: %d. Рост и сигналы станут точными примерно через 4 дня сбора. '
                'Пока доход и загрузки считаются по среднему темпу оценок за всю жизнь приложения и завышены у крупных брендов; смотри на позиции и новинки.</p>' % data["snapshots"])
    else:
        note = ""
    s = re.sub(r"<!--NOTICE-->.*?<!--/NOTICE-->", lambda m: note, s, flags=re.S)
    day = dt.date.fromisoformat(data["day"]).strftime("%d.%m.%Y")
    s = s.replace("App Store, США · сегодня, 06:10", f"App Store · снимок {day} · снимков: {data['snapshots']}")
    os.makedirs(SITE, exist_ok=True)
    open(os.path.join(SITE, "Main.dc.html"), "w", encoding="utf-8").write(s)
    src = os.path.join(HERE, "support.js")
    open(os.path.join(SITE, "support.js"), "wb").write(open(src, "rb").read())
    return os.path.join(SITE, "Main.dc.html")


# ---------------------------------------------------------------- команды
def cmd_check():
    print("Проверяю доступ к Apple...")
    tests = [
        ("Чарт категории (США, Здоровье)", lambda: len(fetch_chart("us", 6013, 10))),
        ("Общий топ (США)", lambda: len(fetch_chart_overall("us", 10))),
        ("Данные приложения (lookup)", lambda: len(fetch_lookup([389801252], "us"))),
    ]
    for name, fn in tests:
        try:
            print(f"  OK   {name}: получено {fn()}")
        except Exception as e:  # noqa
            print(f"  FAIL {name}: {str(e)[:120]}")


def cmd_collect():
    conn = db()
    print("Собираю снимок чартов App Store...")
    total = collect(conn)
    print(f"Готово. Приложений в чартах: {total}")


def cmd_build():
    conn = db()
    data = build_data(conn)
    path = build_site(data)
    print(f"Страница собрана: {path}. Приложений: {len(data['apps'])}, тем: {len(data['themes'])}")


def cmd_serve(open_browser=True):
    os.chdir(SITE)
    srv = None
    for port in range(8765, 8790):
        try:
            srv = ThreadingHTTPServer(("127.0.0.1", port), SimpleHTTPRequestHandler)
            break
        except OSError:
            continue
    if srv is None:
        print("Не удалось найти свободный порт. Закройте другие окна «Радара» и повторите.")
        return
    url = f"http://127.0.0.1:{port}/Main.dc.html"
    print(f"Радар открыт: {url}\nЧтобы закрыть, нажмите Ctrl+C или закройте это окно.")
    if open_browser:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"
    if cmd == "check":
        cmd_check()
    elif cmd == "collect":
        cmd_collect()
    elif cmd == "build":
        cmd_build()
    elif cmd == "serve":
        cmd_serve()
    elif cmd == "open":
        cmd_build()
        cmd_serve()
    elif cmd == "all":
        cmd_collect()
        cmd_build()
        cmd_serve()
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
