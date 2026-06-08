#!/usr/bin/env python3
"""
WickFill Optimizer v3.208
- ∞ Бесконечный режим: оптимизация крутится без остановки, рестарт после каждого цикла
- Скользящее окно: каждые N минут (по таймфрейму) добавляет свечу, убирает первую
- Live-алерт: если на новой закрытой свече сигнал по лучшим параметрам — шлёт email
- Динамический график: /chart обновляется автоматически каждые 30с
- v3.168: межцикловая встряска — после 15 циклов без улучшения рескрамбл stop/tp/bool/cat + расширенный BH
- v3.170: поля символ/таймфрейм/дни запоминают последние значения через localStorage
- v3.173: тело live-свечи не пунктирное (только фитиль); таймер до закрытия свечи под лейблами TP/SL/цены; антиперекрытие правых лейблов
- v3.197: диагностический лог [alert] — показывает up_wick%, dn_wick%, wick_dir, nb для каждого сигнала
- v3.203: _find_auto_config — убран локальный фолбек, только GitHub; при пустом GitHub — старт с нуля
- v3.204: детерминированная граница окна на графике — cutoff по последней свече датасета вместо time.time(); _simulate принимает now_ts для стабильных days_limit
- v3.205: _clamp_tp зажимает sl_pct/tp_pct к текущим границам UI; seed при загрузке зажимается через _clamp_sl_tp_to_bounds — оптимизатор не выходит за wf_sl_min/max, wf_tp_min/max
- v3.206: таблица лучшей комбинации и stat-grid показывают «Вход след.св.» (use_next_bar да/нет)
- v3.202: /recent_configs — убран локальный fallback, только GitHub
- v3.201: fix всех SyntaxError — literal newlines в строках, совместимость Python 3.12+
- v3.200: fix SyntaxError line 992 — literal newline in print end= replaced with \r
- v3.199: добавлены UI-поля wf_tp_min/wf_tp_max — ограничение диапазона тейка, аналогично sl
- v3.198: fix — _GRIDS["sl_pct"] пересчитывается после изменения sl_min/sl_max (раньше оптимизатор игнорировал ограничение снизу)
- v3.196: fix bounce — sweep/ret/rep/clu/near_level теперь зеркалятся для нижнего фитиля лонг
"""

import json, time, threading, random, math, os, base64
import math as _math  # используется в fitness внутри _simulate
import multiprocessing
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import sys as _sys
# python3.14t (free-threaded, no GIL) не поддерживает ProcessPoolExecutor
if hasattr(_sys, '_is_gil_enabled') and not _sys._is_gil_enabled():
    from concurrent.futures import ThreadPoolExecutor as PoolExecutor
    _POOL_TYPE = "thread"
else:
    from concurrent.futures import ProcessPoolExecutor as PoolExecutor
    _POOL_TYPE = "process"
import requests
import smtplib, email.mime.text, email.mime.multipart

GATE_API = "https://api.gateio.ws/api/v4"
APP_VERSION = "3.208"

def _ts():
    """Возвращает метку времени для логов: [HH:MM:SS]"""
    return time.strftime("[%H:%M:%S]")

TF_SECONDS = {
    "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "4h": 14400, "8h": 28800, "1d": 86400
}

# ═══════════════════════════════════════════════════════════════
# PARAM SPACE
# ═══════════════════════════════════════════════════════════════
PARAM_SPACE = {
    "sl_pct":             {"min": 0.4,  "max": 0.8,  "step": 0.05, "type": "float", "label": "Стоп-лосс (%)"},
    "tp_pct":             {"min": 0.5,  "max": 2,  "step": 0.05, "type": "float", "label": "Тейк-профит (%)"},
    "min_wick_pct":       {"min": 30.0, "max": 90.0, "step": 5.0,  "type": "float", "label": "Мин. фитиль (% диапазона)"},
    "min_wick_pct_price": {"min": 0.05, "max": 0.5,  "step": 0.05, "type": "float", "label": "Мин. фитиль (% цены)"},
    "wick_dir":           {"values": ["both", "upper", "lower", "bounce"], "type": "cat",  "label": "Направление фитиля"},
    "filter_body_rat":    {"values": [True, False], "type": "bool", "label": "Фильтр: тело < фитиль"},
    "filter_consec":      {"values": [False, True], "type": "bool", "label": "Фильтр: не 2 сигнала подряд"},
    "use_confirm_candle": {"values": [True, False], "type": "bool", "label": "Подтверждающая свеча"},
    "confirm_body_pct":   {"min": 2.0,  "max": 30.0, "step": 2.0,  "type": "float", "label": "Мин. тело подтв. свечи (%)"},
    "use_rsi_filter":     {"values": [True, False], "type": "bool", "label": "RSI — включить фильтр"},
    "rsi_len":            {"min": 2,    "max": 8,    "step": 1,    "type": "int",   "label": "RSI — период"},
    "rsi_long_max":       {"min": 0.0, "max": 100.0, "step": 1.0,  "type": "float", "label": "RSI — порог лонга"},
    "rsi_short_min":      {"min": 0.0, "max": 100.0, "step": 1.0,  "type": "float", "label": "RSI — порог шорта"},
    "use_level_filter":   {"values": [True, False], "type": "bool", "label": "Уровни HH/LL"},
    "level_lookback":     {"min": 3,    "max": 20,   "step": 1,    "type": "int",   "label": "Уровни — история (св.)"},
    "level_toler_pct":    {"min": 0.1,  "max": 0.5,  "step": 0.1,  "type": "float", "label": "Уровни — допуск (%)"},
    "use_geo_filter":     {"values": [True, False], "type": "bool", "label": "Перцентиль фитиля"},
    "geo_lookback":       {"min": 10,   "max": 30,   "step": 5,    "type": "int",   "label": "Перцентиль — окно"},
    "geo_min_pct":        {"min": 50.0, "max": 90.0, "step": 5.0,  "type": "float", "label": "Перцентиль — мин (%)"},
    "use_css_filter":     {"values": [True, False], "type": "bool", "label": "CSS фильтр"},
    "css_min_score":      {"min": 30.0, "max": 90.0, "step": 5.0,  "type": "float", "label": "CSS — мин. балл"},
    "css_wt_wick":        {"min": 20.0, "max": 60.0, "step": 10.0, "type": "float", "label": "CSS — вес фитиля"},
    "css_wt_close":       {"min": 10.0, "max": 40.0, "step": 10.0, "type": "float", "label": "CSS — вес закрытия"},
    "css_wt_body":        {"min": 10.0, "max": 40.0, "step": 10.0, "type": "float", "label": "CSS — вес тела"},
    "css_wt_range":       {"min": 10.0, "max": 40.0, "step": 10.0, "type": "float", "label": "CSS — вес диапазона"},
    "css_wt_price":       {"min": 0.0,  "max": 30.0, "step": 10.0, "type": "float", "label": "CSS — вес фитиль/цена"},
    "use_be":             {"values": [False, True], "type": "bool", "label": "Безубыток"},
    "be_trigger_pct":     {"min": 0.1,  "max": 0.8,  "step": 0.1,  "type": "float", "label": "BE — триггер (%)"},
    "be_offset_pct":      {"min": 0.0,  "max": 0.2,  "step": 0.05, "type": "float", "label": "BE — смещение (%)"},
    "use_next_bar":       {"values": [True, False], "type": "bool", "label": "Вход на следующей свече"},
    "use_return_filter":  {"values": [True, False], "type": "bool", "label": "Гео-1 возврат к телу"},
    "ret_lookback":       {"min": 30,   "max": 120,  "step": 10,   "type": "int",   "label": "Гео-1 — история"},
    "ret_n":              {"min": 1,    "max": 5,    "step": 1,    "type": "int",   "label": "Гео-1 — возврат N баров"},
    "ret_wick_sim":       {"min": 50.0, "max": 90.0, "step": 10.0, "type": "float", "label": "Гео-1 — схожесть (%)"},
    "min_return_pct":     {"min": 50.0, "max": 90.0, "step": 5.0,  "type": "float", "label": "Гео-1 — мин. WR (%)"},
    "use_repeat_filter":  {"values": [True, False], "type": "bool", "label": "Гео-3 проверенный уровень"},
    "rep_lookback":       {"min": 50,   "max": 150,  "step": 25,   "type": "int",   "label": "Гео-3 — история"},
    "rep_zone_pct":       {"min": 0.2,  "max": 0.6,  "step": 0.1,  "type": "float", "label": "Гео-3 — зона (±%)"},
    "rep_min_win":        {"min": 1,    "max": 3,    "step": 1,    "type": "int",   "label": "Гео-3 — мин. отработок"},
    "use_cluster_filter": {"values": [True, False], "type": "bool", "label": "Гео-4 кластер фитилей"},
    "cluster_lookback":   {"min": 30,   "max": 110,   "step": 10,   "type": "int",   "label": "Гео-4 — история"},
    "cluster_pct":        {"min": 0.15, "max": 0.4,  "step": 0.05, "type": "float", "label": "Гео-4 — зона (±%)"},
    "cluster_min":        {"min": 2,    "max": 4,    "step": 1,    "type": "int",   "label": "Гео-4 — мин. фитилей"},
    "use_close_filter":   {"values": [False, True], "type": "bool", "label": "Позиция закрытия"},
    "close_long_min_pct": {"min": 10.0, "max": 90.0, "step": 10.0, "type": "float", "label": "Закрытие лонг — верхние N%"},
    "close_short_max_pct":{"min": 10.0, "max": 90.0, "step": 10.0, "type": "float", "label": "Закрытие шорт — нижние N%"},
    "use_quiet_filter":   {"values": [True, False], "type": "bool", "label": "Тихая зона ATR"},
    "quiet_atr_len":      {"min": 5,    "max": 50,   "step": 5,    "type": "int",   "label": "ATR — период"},
    "quiet_max_ratio":    {"min": 1.1,  "max": 5.0,  "step": 0.1,  "type": "float", "label": "ATR — макс. ratio"},
    "quiet_min_ratio":    {"min": 0.1,  "max": 1,  "step": 0.1,  "type": "float", "label": "ATR — мин. ratio"},
    "use_sweep_filter":   {"values": [True, False], "type": "bool", "label": "Sweep ликвидность"},
    "sweep_len":          {"min": 5,    "max": 20,   "step": 5,    "type": "int",   "label": "Sweep — период"},
    "sweep_toler_pct":    {"min": 0.3,  "max": 1.0,  "step": 0.1,  "type": "float", "label": "Sweep — допуск (%)"},
    "use_ms_filter":      {"values": [False, True], "type": "bool", "label": "Структура рынка HH/HL"},
    "ms_lookback":        {"min": 20,   "max": 60,   "step": 10,   "type": "int",   "label": "Структура — период"},
    "use_ema_filter":     {"values": [False, True], "type": "bool", "label": "EMA тренд-фильтр"},
    "ema_period":         {"min": 20,   "max": 200,  "step": 20,   "type": "int",   "label": "EMA — период"},
}

def _param_grid(spec):
    if spec["type"] in ("bool", "cat"):
        return list(spec["values"])
    mn, mx, st = spec["min"], spec["max"], spec["step"]
    vals = []
    v = mn
    while v <= mx + st * 0.001:
        vals.append(int(round(v)) if spec["type"] == "int" else round(v, 10))
        v += st
    return vals

_GRIDS = {k: _param_grid(v) for k, v in PARAM_SPACE.items()}
_KEYS  = list(PARAM_SPACE.keys())

FILTER_GROUPS = {
    "sweep_len": "use_sweep_filter", "sweep_toler_pct": "use_sweep_filter",
    "geo_lookback": "use_geo_filter", "geo_min_pct": "use_geo_filter",
    "rsi_len": "use_rsi_filter", "rsi_long_max": "use_rsi_filter", "rsi_short_min": "use_rsi_filter",
    "level_lookback": "use_level_filter", "level_toler_pct": "use_level_filter",
    "css_min_score": "use_css_filter", "css_wt_wick": "use_css_filter",
    "css_wt_close": "use_css_filter", "css_wt_body": "use_css_filter",
    "css_wt_range": "use_css_filter", "css_wt_price": "use_css_filter",
    "confirm_body_pct": "use_confirm_candle",
    "be_trigger_pct": "use_be", "be_offset_pct": "use_be",
    "ret_lookback": "use_return_filter", "ret_n": "use_return_filter",
    "ret_wick_sim": "use_return_filter", "min_return_pct": "use_return_filter",
    "rep_lookback": "use_repeat_filter", "rep_zone_pct": "use_repeat_filter", "rep_min_win": "use_repeat_filter",
    "cluster_lookback": "use_cluster_filter", "cluster_pct": "use_cluster_filter", "cluster_min": "use_cluster_filter",
    "close_long_min_pct": "use_close_filter", "close_short_max_pct": "use_close_filter",
    "quiet_atr_len": "use_quiet_filter", "quiet_max_ratio": "use_quiet_filter", "quiet_min_ratio": "use_quiet_filter",
    "ms_lookback": "use_ms_filter",
    "ema_period":  "use_ema_filter",
}

# ═══════════════════════════════════════════════════════════════
# GLOBAL STATE
# ═══════════════════════════════════════════════════════════════
opt_state = {
    "running": False, "done": False, "infinite": False,
    "cycle": 0,       # номер цикла бесконечного режима
    "cycle_step": 0, "cycle_total": 0,  # прогресс внутри цикла (стартов + BH итераций)
    "progress": 0, "total": 0, "generation": 0, "pass_num": 0,
    "current_param": "", "logs": [], "best": None, "top20": [], "valid": None, "windows": [], "min_stable_days": None,
    "started_at": "", "elapsed": 0.0, "error": "", "cycle_times": [], "avg_cycle_s": None,
    "chart_candles": [], "chart_signals": [], "chart_symbol": "", "chart_tf": "",
    "chart_path": "", "chart_updated_at": 0,
    # sliding window
    "sw_running": False, "sw_last_update": 0, "sw_candle_count": 0,
    # live signal alert
    "last_signal_t": 0,   # timestamp последней свечи с сигналом (чтобы не дублировать)
    # fetch progress (0-100, -1 = не идёт)
    "fetch_pct": -1, "fetch_symbol": "",
}
opt_lock = threading.Lock()

# ── Multi-symbol state ──────────────────────────────────────────
# opt_states[symbol] — per-symbol snapshot updated after each cycle
opt_states   = {}   # {symbol_key: {eq, wr, dd, trades, cycle, running, chart_updated_at, ...}}
opt_states_lock = threading.Lock()
_multi_symbols  = []   # ordered list of symbols in current session
_active_chart_symbol = ""  # which symbol's chart is shown in iframe

alert_state = {
    "running": False, "error": "", "last_scan": "",
    "signals": [], "sent": 0,
}
alert_lock = threading.Lock()

# Кеш текущей незакрытой свечи — обновляется фоновым потоком
_live_candle_cache = {}   # {"symbol_tf": {t,o,h,l,c,_fetched_at}}
_live_candle_lock  = threading.Lock()

def _live_candle_updater():
    """Фоновый поток: каждые 3 секунды обновляет незакрытую свечу.
    Если chart_candles устарели (последняя свеча > 2 интервалов назад) —
    перегружает исторические свечи с API.
    При сетевых ошибках делает паузу с экспоненциальным backoff (до 60с)."""
    _last_refresh = 0  # время последней полной перезагрузки истории
    _net_errors = 0    # счётчик последовательных сетевых ошибок

    while True:
        sleep_time = 3
        try:
            with opt_lock:
                symbol   = opt_state.get("chart_symbol", "")
                tf       = opt_state.get("chart_tf", "")
                cc       = opt_state.get("chart_candles", [])
                best     = opt_state.get("best", None)
                running  = opt_state.get("running", False)

            if symbol and tf:
                interval_sec = TF_SECONDS.get(tf, 3600)
                now = int(time.time())

                # Проверяем свежесть исторических данных
                last_t = cc[-1]["t"] if cc else 0
                stale = (now - last_t) > interval_sec * 2  # старше 2 интервалов

                # Если данные устарели и оптимизатор не бежит — перегружаем историю
                if stale and not running and best and (now - _last_refresh) > 60:
                    print(f"{_ts()} [SW] Данные устарели (last={last_t}, now={now}), перегружаю историю...", flush=True)
                    try:
                        fresh = _fetch_candles(symbol, tf, 3)
                        if fresh and len(fresh) > 10:
                            # Пересчитываем сигналы с лучшими параметрами
                            best_p = best.get("params", {})
                            if best_p:
                                sim = _simulate(fresh, best_p, 0, _collect=True)
                                sigs = sim["_signals"] if sim else []
                            else:
                                sigs = []
                            new_cc = [{"t":c["t"],"o":c["open"],"h":c["high"],
                                       "l":c["low"],"c":c["close"]} for c in fresh]
                            with opt_lock:
                                # Проверяем что символ не сменился пока грузили данные
                                if opt_state.get("chart_symbol", "") == symbol:
                                    opt_state["chart_candles"] = new_cc
                                    opt_state["chart_signals"]  = sigs
                                    cc = new_cc
                                    _last_refresh = now
                                    _net_errors = 0
                                    print(f"{_ts()} [SW] ✅ Перезагружено {len(fresh)} свечей", flush=True)
                                else:
                                    print(f"{_ts()} [SW] ⚠ Символ сменился во время загрузки, данные отброшены", flush=True)
                    except Exception as e:
                        _net_errors += 1
                        print(f"{_ts()} [SW] ❌ Ошибка перезагрузки: {e}", flush=True)

                # Обновляем незакрытую свечу
                c = _fetch_current_candle(symbol, tf)
                if c:
                    _net_errors = 0  # сброс счётчика ошибок при успехе
                    key = f"{symbol}_{tf}"
                    with _live_candle_lock:
                        _live_candle_cache[key] = dict(c, _fetched_at=time.time())
                    with opt_lock:
                        # Проверяем что символ не сменился пока шёл запрос к API
                        if opt_state.get("chart_symbol", "") != symbol:
                            pass  # символ сменился — пропускаем, в следующей итерации возьмём новый
                        else:
                            cc2 = list(opt_state.get("chart_candles", []))
                            if cc2:
                                live_c = {"t":c["t"],"o":c["open"],"h":c["high"],
                                          "l":c["low"],"c":c["close"],"live":True}
                                last_closed = next((x for x in reversed(cc2) if not x.get("live")), None)
                                last_closed_t = last_closed["t"] if last_closed else 0
                                if cc2[-1].get("live"):
                                    # Обновляем существующую live-свечу только если t совпадает
                                    if c["t"] == cc2[-1]["t"]:
                                        cc2[-1] = live_c
                                        opt_state["chart_candles"] = cc2
                                    elif c["t"] > cc2[-1]["t"]:
                                        # Новый интервал — убираем старую live и добавляем новую
                                        cc2.pop()
                                        opt_state["chart_candles"] = cc2 + [live_c]
                                elif c["t"] > last_closed_t:
                                    # Строго больше: не дублируем закрытую свечу
                                    # Проверяем gap — пропущены ли свечи за время офлайна
                                    gap_candles = round((c["t"] - last_closed_t) / interval_sec) - 1
                                    if gap_candles >= 1:
                                        # Есть пропуск — дозагружаем недостающие закрытые свечи
                                        try:
                                            limit = min(gap_candles + 3, 100)
                                            r_gap = requests.get(f"{GATE_API}/futures/usdt/candlesticks",
                                                params={"contract": symbol, "interval": tf, "limit": limit}, timeout=8)
                                            if r_gap.status_code == 200:
                                                gap_data = r_gap.json()
                                                # Берём только закрытые свечи новее last_closed_t и старее c["t"]
                                                for gc in gap_data:
                                                    gc_t = int(gc.get("t", 0))
                                                    if gc_t > last_closed_t and gc_t < c["t"]:
                                                        cc2.append({"t": gc_t, "o": float(gc["o"]),
                                                                     "h": float(gc["h"]), "l": float(gc["l"]),
                                                                     "c": float(gc["c"])})
                                                # Дедупликация и сортировка
                                                seen = {}
                                                for x in cc2:
                                                    seen[x["t"]] = x
                                                cc2 = sorted(seen.values(), key=lambda x: x["t"])
                                                print(f"{_ts()} [SW] Gap-fill: вставлено {gap_candles} пропущ. свечей", flush=True)
                                        except Exception as _ge:
                                            print(f"{_ts()} [SW] Gap-fill ошибка: {_ge}", flush=True)
                                    opt_state["chart_candles"] = cc2 + [live_c]
                else:
                    _net_errors += 1

            # Адаптивная пауза: при сетевых ошибках увеличиваем интервал до 60с
            if _net_errors > 0:
                sleep_time = min(3 * (2 ** min(_net_errors - 1, 4)), 60)
                if _net_errors == 1:
                    print(f"{_ts()} [SW] ⚠ Нет соединения, пауза {sleep_time}с...", flush=True)
            else:
                sleep_time = 3

        except Exception as e:
            _net_errors += 1
            sleep_time = min(3 * (2 ** min(_net_errors - 1, 4)), 60)
            print(f"{_ts()} [SW] ⚠ {e}", flush=True)
        time.sleep(sleep_time)

# Запускаем фоновый поток сразу
threading.Thread(target=_live_candle_updater, daemon=True).start()

# ═══════════════════════════════════════════════════════════════
# SIMULATE
# ═══════════════════════════════════════════════════════════════
def _simulate(candles_list, p, days_limit, init_deposit=100.0, risk_pct=20.0,
              max_pos=6000.0, _collect=False, trade_from_ts=None, now_ts=None):
    sl_p=p["sl_pct"]; tp_p=p["tp_pct"]; mwp=p["min_wick_pct"]; mwpp=p["min_wick_pct_price"]
    wd=p["wick_dir"]; fbr=p["filter_body_rat"]; fcon=p["filter_consec"]
    ucc=p["use_confirm_candle"]; cbp=p["confirm_body_pct"]
    urf=p["use_rsi_filter"]; rl=p["rsi_len"]; rlmax=p["rsi_long_max"]; rsmin=p["rsi_short_min"]
    ulf=p["use_level_filter"]; ll=p["level_lookback"]; ltol=p["level_toler_pct"]
    ugf=p["use_geo_filter"]; gl=p["geo_lookback"]; gmin=p["geo_min_pct"]
    ucss=p["use_css_filter"]; css_mn=p["css_min_score"]
    ww=p["css_wt_wick"]; wc=p["css_wt_close"]; wb=p["css_wt_body"]; wr_w=p["css_wt_range"]; wp_w=p["css_wt_price"]
    be_trig=p["be_trigger_pct"]; be_off=p["be_offset_pct"]
    nb=p["use_next_bar"]
    uretf=p["use_return_filter"]; ret_lb=p["ret_lookback"]; ret_n=p["ret_n"]
    ret_sim=p["ret_wick_sim"]; ret_minwr=p["min_return_pct"]
    urepf=p["use_repeat_filter"]; rep_lb=p["rep_lookback"]; rep_zone=p["rep_zone_pct"]; rep_min=p["rep_min_win"]
    ucluf=p["use_cluster_filter"]; clu_lb=p["cluster_lookback"]; clu_pct=p["cluster_pct"]; clu_min=p["cluster_min"]
    uclof=p["use_close_filter"]; clo_lng=p["close_long_min_pct"]; clo_sht=p["close_short_max_pct"]
    uqf=p["use_quiet_filter"]; q_atr=p["quiet_atr_len"]; q_max=p["quiet_max_ratio"]; q_min=p["quiet_min_ratio"]
    uswf=p["use_sweep_filter"]; sw_len=p["sweep_len"]; sw_tol=p["sweep_toler_pct"]
    umsf=p["use_ms_filter"]; ms_lb=p["ms_lookback"]
    uemaf=p.get("use_ema_filter", False); ema_per=p.get("ema_period", 50)

    if not candles_list or len(candles_list) < max(ll if ulf else 0, gl if ugf else 0, rl if urf else 0, q_atr if uqf else 0, sw_len if uswf else 0, ms_lb if umsf else 0, ema_per if uemaf else 0, ret_lb if uretf else 0, rep_lb if urepf else 0, clu_lb if ucluf else 0, 20) + 10:
        return None
    if days_limit > 0:
        _ref_ts = now_ts if now_ts else time.time()
        cutoff = _ref_ts - days_limit * 86400
        candles_list = [c for c in candles_list if c.get("t", 0) >= cutoff]
    if len(candles_list) < max(ll, gl, rl) + 10:
        return None
    n = len(candles_list)

    # Используем предвычисленный массив closes если доступен (worker-процесс)
    global _worker_closes
    if _worker_closes is not None and len(_worker_closes) == n and days_limit == 0:
        closes = _worker_closes
    else:
        closes = [c["close"] for c in candles_list]
    def _rsi(closes, period):
        if len(closes) < period + 1: return [50.0]*len(closes)
        gains, losses = [], []
        for i in range(1, len(closes)):
            d = closes[i]-closes[i-1]; gains.append(max(d,0)); losses.append(max(-d,0))
        rsi_vals=[50.0]*len(closes)
        ag=sum(gains[:period])/period; al=sum(losses[:period])/period
        for i in range(period, len(closes)):
            ag=(ag*(period-1)+gains[i-1])/period; al=(al*(period-1)+losses[i-1])/period
            rs=ag/al if al>0 else 100; rsi_vals[i]=100-100/(1+rs)
        return rsi_vals
    rsi_series = _rsi(closes, rl)

    def _atr(candles, period):
        trs=[]
        for i in range(1,len(candles)):
            hi=candles[i]["high"]; lo=candles[i]["low"]; pc=candles[i-1]["close"]
            trs.append(max(hi-lo, abs(hi-pc), abs(lo-pc)))
        atr_vals=[0.0]*len(candles)
        if len(trs)<period: return atr_vals
        atr_vals[period]=sum(trs[:period])/period
        for i in range(period+1,len(candles)):
            atr_vals[i]=(atr_vals[i-1]*(period-1)+trs[i-1])/period
        return atr_vals
    atr_series = _atr(candles_list, max(q_atr,2))

    # EMA series (exponential moving average)
    ema_series = [0.0] * n
    if uemaf and n >= ema_per:
        k = 2.0 / (ema_per + 1)
        ema_series[ema_per - 1] = sum(closes[:ema_per]) / ema_per
        for _ei in range(ema_per, n):
            ema_series[_ei] = closes[_ei] * k + ema_series[_ei - 1] * (1 - k)

    def _calc_return_rate(i, is_up_wick):
        # OPT: плоские массивы вместо candles_list[ki]["high"] и т.д.
        total=0.0; returns=0.0
        if i<=ret_n+1: return None
        max_look=min(ret_lb, i-ret_n-1)
        for k in range(ret_n+1, max_look+1):
            ki=i-k
            if ki<0: continue
            k_rng=_all_rng[ki]
            if k_rng<=0: continue
            if is_up_wick:
                if _all_upw[ki]/k_rng*100>=ret_sim:
                    target=_all_hi[ki]-_all_upw[ki]  # max(open,close)
                    total+=1
                    for j in range(1,ret_n+1):
                        fi=ki+j
                        if fi<n and _all_lo[fi]<=target: returns+=1; break
            else:
                if _all_dnw[ki]/k_rng*100>=ret_sim:
                    target=_all_lo[ki]+_all_dnw[ki]  # min(open,close)
                    total+=1
                    for j in range(1,ret_n+1):
                        fi=ki+j
                        if fi<n and _all_hi[fi]>=target: returns+=1; break
        return (returns/total*100) if total>0 else None

    # OPT: предвычисляем вспомогательные массивы для _count_tested_level / _count_wick_cluster
    # Вместо доступа к candles_list[ki]["high"] и т.д. в горячем цикле — используем плоские массивы
    # Они будут определены ниже (после _all_hi/_all_lo/_all_upw/_all_dnw/_all_rng),
    # но функции замкнуты на нелокальный массив через nonlocal-ссылку (заполнится до вызова).
    # Здесь определяем функции с захватом через closure.

    def _count_tested_level(i, level_price, is_up_search):
        # OPT: использует _all_hi, _all_lo, _all_upw, _all_dnw, _all_rng вместо candles_list[ki]
        wins=0
        if i<3: return wins
        zone_tol=level_price*rep_zone/100.0; max_look=min(rep_lb,i-2)
        closes = _worker_closes  # предвычисленный массив close в воркере (или None)
        for k in range(2, max_look+1):
            ki=i-k
            if ki<0: continue
            k_rng=_all_rng[ki]
            if k_rng<=0: continue
            if is_up_search:
                k_upw=_all_upw[ki]
                if k_upw/k_rng*100>=mwp:
                    if abs(_all_hi[ki]-level_price)<=zone_tol:
                        fi=ki+1
                        fi_close = (closes[fi] if closes is not None else candles_list[fi]["close"]) if fi<n else 0
                        fi_open  = candles_list[fi]["open"] if fi<n else 0
                        fi_hi    = _all_hi[fi] if fi<n else 0
                        # close[fi] < max(open[fi], close_prev_wick_top) — используем open свечи fi
                        body_top = _all_hi[ki] - _all_upw[ki]  # = max(open,close) свечи ki
                        if fi<n and fi_close < body_top: wins+=1
            else:
                k_dnw=_all_dnw[ki]
                if k_dnw/k_rng*100>=mwp:
                    if abs(_all_lo[ki]-level_price)<=zone_tol:
                        fi=ki+1
                        body_bot = _all_lo[ki] + _all_dnw[ki]  # = min(open,close) свечи ki
                        fi_close = (closes[fi] if closes is not None else candles_list[fi]["close"]) if fi<n else 0
                        if fi<n and fi_close > body_bot: wins+=1
        return wins

    def _count_wick_cluster(i, level_price, is_up_search):
        # OPT: использует предвычисленные плоские массивы
        cnt=0; zone_tol=level_price*clu_pct/100.0; max_look=min(clu_lb,i-1)
        for k in range(1, max_look+1):
            ki=i-k
            if ki<0: continue
            k_rng=_all_rng[ki]
            if k_rng<=0: continue
            if is_up_search:
                if _all_upw[ki]/k_rng*100>=mwp:
                    if abs(_all_hi[ki]-level_price)<=zone_tol: cnt+=1
            else:
                if _all_dnw[ki]/k_rng*100>=mwp:
                    if abs(_all_lo[ki]-level_price)<=zone_tol: cnt+=1
        return cnt

    # Предвычисляем массивы high/low/upwick/dnwick для быстрых скользящих окон
    _all_hi  = [c["high"] for c in candles_list]
    _all_lo  = [c["low"]  for c in candles_list]
    _all_upw = [c["high"]-max(c["open"],c["close"]) for c in candles_list]
    _all_dnw = [min(c["open"],c["close"])-c["low"]  for c in candles_list]
    _all_rng = [c["high"]-c["low"] for c in candles_list]  # OPT: для _css без доступа к candles_list[j]

    # OPT: скользящие deque для max(_all_hi[s:i]) и min(_all_lo[s:i]) — O(1) вместо O(window)
    from collections import deque as _deque
    def _build_sliding_max(arr, window):
        """Возвращает массив, где result[i] = max(arr[max(0,i-window):i])."""
        dq = _deque()  # хранит индексы, убывающие по значению
        out = [0.0] * len(arr)
        for i, v in enumerate(arr):
            while dq and arr[dq[-1]] <= v:
                dq.pop()
            dq.append(i)
            if dq[0] <= i - window:
                dq.popleft()
            out[i] = arr[dq[0]]
        return out

    def _build_sliding_min(arr, window):
        dq = _deque()
        out = [0.0] * len(arr)
        for i, v in enumerate(arr):
            while dq and arr[dq[-1]] >= v:
                dq.pop()
            dq.append(i)
            if dq[0] <= i - window:
                dq.popleft()
            out[i] = arr[dq[0]]
        return out

    # Предвычисляем скользящие max/min для каждого фильтра с окном
    _slide_hi_ll  = _build_sliding_max(_all_hi, ll)   if ulf  else None
    _slide_lo_ll  = _build_sliding_min(_all_lo, ll)   if ulf  else None
    _slide_upw_gl = _build_sliding_max(_all_upw, gl)  if ugf  else None  # не нужен, geo считает percentile
    _slide_hi_sw  = _build_sliding_max(_all_hi, sw_len) if uswf else None
    _slide_lo_sw  = _build_sliding_min(_all_lo, sw_len) if uswf else None
    # Для geo filter: percentile — нужен полный слайс, оставляем как есть (окно gl <= 30, дёшево)
    # Для ms filter: sliding max/min с окном ms_lb
    _slide_hi_ms1 = _build_sliding_max(_all_hi, ms_lb)   if umsf else None
    _slide_lo_ms1 = _build_sliding_min(_all_lo, ms_lb)   if umsf else None
    _slide_hi_ms2 = _build_sliding_max(_all_hi, ms_lb*2) if umsf else None
    _slide_lo_ms2 = _build_sliding_min(_all_lo, ms_lb*2) if umsf else None

    equity=init_deposit; max_eq=init_deposit; max_dd=0.0
    trades=0; wins=0; losses_n=0; pnls=[]
    in_trade=False; t_dir=0; t_ep=0.0; t_tp=0.0; t_sl=0.0
    t_orig_sl=0.0; t_pos=0.0; t_entry_bar=-1
    be_triggered=False; be_trig_lvl=0.0
    pending_sig=0; sig_bar=-1; last_sig=0
    _csigs=[]

    start_i=max(ll if ulf else 0, gl if ugf else 0, rl if urf else 0, q_atr if uqf else 0, sw_len if uswf else 0, ms_lb if umsf else 0, ema_per if uemaf else 0, ret_lb if uretf else 0, rep_lb if urepf else 0, clu_lb if ucluf else 0, 20)+2
    start_i=min(start_i,n-1)
    # trade_from_ts: не торговать до этого timestamp (индикаторы всё равно прогреваются)
    if trade_from_ts is not None:
        for _ti in range(start_i, n):
            if candles_list[_ti].get('t', 0) >= trade_from_ts:
                start_i = _ti
                break

    for i in range(start_i, n):
        c=candles_list[i]; hi=c["high"]; lo=c["low"]; op=c["open"]; cl=c["close"]
        rng=hi-lo; up_w=hi-max(op,cl); dn_w=min(op,cl)-lo; body=abs(cl-op)
        atr_now=atr_series[i]

        if in_trade and i>t_entry_bar:
            hit_tp=(t_dir==1 and hi>=t_tp) or (t_dir==-1 and lo<=t_tp)
            hit_sl=(t_dir==1 and lo<=t_sl) or (t_dir==-1 and hi>=t_sl)
            if hit_tp or hit_sl:
                tp_win=hit_tp and not hit_sl
                if hit_tp and hit_sl: tp_win=abs(op-t_tp)<=abs(op-t_sl)
                exit_p=t_tp if tp_win else t_sl
                move=(exit_p-t_ep)/t_ep*100 if t_dir==1 else (t_ep-exit_p)/t_ep*100
                rr_r=move/t_orig_sl if t_orig_sl>0 else 0
                pnl=t_pos*risk_pct/100*rr_r
                equity+=pnl; pnls.append(pnl); trades+=1
                if tp_win: wins+=1
                else: losses_n+=1
                if equity>max_eq: max_eq=equity
                dd=(max_eq-equity)/max_eq*100 if max_eq>0 else 0
                if dd>max_dd: max_dd=dd
                if _collect and _csigs:
                    _csigs[-1]["exit_bar"]=i; _csigs[-1]["win"]=tp_win; _csigs[-1]["exit_p"]=exit_p
                in_trade=False; be_triggered=False

        up_w_pct=up_w/rng*100 if rng>0 else 0
        dn_w_pct=dn_w/rng*100 if rng>0 else 0
        up_w_pp=up_w/cl*100 if cl>0 else 0
        dn_w_pp=dn_w/cl*100 if cl>0 else 0

        is_up_w=up_w_pct>=mwp and up_w_pp>=mwpp
        is_dn_w=dn_w_pct>=mwp and dn_w_pp>=mwpp
        if wd=="upper": is_dn_w=False
        if wd=="lower": is_up_w=False
        # bounce: нижний фитиль = лонг, верхний = шорт (отталкивание)
        # eff_long_w / eff_short_w — физический фитиль для лонг/шорт сигнала
        if wd=="bounce":
            is_up_w, is_dn_w = is_dn_w, is_up_w
            eff_long_w=dn_w; eff_long_w_pct=dn_w_pct; eff_long_w_pp=dn_w_pp
            eff_short_w=up_w; eff_short_w_pct=up_w_pct; eff_short_w_pp=up_w_pp
        else:
            eff_long_w=up_w; eff_long_w_pct=up_w_pct; eff_long_w_pp=up_w_pp
            eff_short_w=dn_w; eff_short_w_pct=dn_w_pct; eff_short_w_pp=dn_w_pp

        # body_ok использует эффективный фитиль для каждого направления
        body_ok_up=(not fbr) or (body<eff_long_w)
        body_ok_dn=(not fbr) or (body<eff_short_w)

        rsi_now=rsi_series[i]
        rsi_ok_l=(not urf) or rsi_now<=rlmax
        rsi_ok_s=(not urf) or rsi_now>=rsmin

        # OPT: level filter — sliding deque O(1) вместо max/min по slice
        if ulf:
            prev_hi=_slide_hi_ll[i-1] if i>0 else hi
            prev_lo=_slide_lo_ll[i-1] if i>0 else lo
        else:
            prev_hi=hi; prev_lo=lo
        near_hi=(not ulf) or (abs(hi-prev_hi)/prev_hi*100<=ltol if prev_hi>0 else False)
        near_lo=(not ulf) or (abs(lo-prev_lo)/prev_lo*100<=ltol if prev_lo>0 else False)

        if ugf:
            _s=max(0,i-gl)
            hist_up=_all_upw[_s:i]; hist_dn=_all_dnw[_s:i]
            geo_up=sum(1 for w in hist_up if eff_long_w>w)/len(hist_up)*100 if hist_up else 0
            geo_dn=sum(1 for w in hist_dn if eff_short_w>w)/len(hist_dn)*100 if hist_dn else 0
            geo_ok_l=geo_up>=gmin; geo_ok_s=geo_dn>=gmin
        else:
            geo_ok_l=geo_ok_s=True

        # OPT: _css использует _all_rng вместо candles_list[j]["high"]-candles_list[j]["low"]
        def _css(is_long):
            wick=eff_long_w if is_long else eff_short_w
            w_pct=eff_long_w_pct if is_long else eff_short_w_pct
            s1=min(w_pct/mwp*100,100) if mwp>0 else 100
            cp=(cl-lo)/rng*100 if rng>0 else 50
            s2=cp if is_long else 100-cp; s2=max(min(s2,100),0)
            s3=max(min((1-body/wick)*100,100),0) if wick>0 else 0
            _cs=max(0,i-20); hist_rng=_all_rng[_cs:i]
            s4=sum(1 for r2 in hist_rng if rng>r2)/len(hist_rng)*100 if hist_rng else 50
            wp_v=eff_long_w_pp if is_long else eff_short_w_pp
            s5=min(wp_v/mwpp*100,100) if mwpp>0 else 100
            tw=ww+wc+wb+wr_w+wp_w
            return (s1*ww+s2*wc+s3*wb+s4*wr_w+s5*wp_w)/tw if tw>0 else 0

        if ucss:
            css_ok_l=is_up_w and _css(True)>=css_mn
            css_ok_s=is_dn_w and _css(False)>=css_mn
        else:
            css_ok_l=css_ok_s=True

        if uqf and atr_now>0:
            _qs=max(0,i-q_atr); hist_atr=atr_series[_qs:i]
            valid_atr=[v for v in hist_atr if v>0]
            avg_atr=sum(valid_atr)/len(valid_atr) if valid_atr else atr_now
            ratio=atr_now/avg_atr if avg_atr>0 else 1
            quiet_ok=q_min<=ratio<=q_max
        else:
            quiet_ok=True

        # OPT: sweep filter — sliding deque O(1)
        # bounce: лонг = нижний фитиль пробивает нижний уровень и возвращается; шорт = верхний
        if uswf:
            sw_hi=_slide_hi_sw[i-1] if i>0 else hi
            sw_lo=_slide_lo_sw[i-1] if i>0 else lo
            if wd=="bounce":
                sweep_ok_l=lo<=sw_lo*(1+sw_tol/100) and cl>sw_lo
                sweep_ok_s=hi>=sw_hi*(1-sw_tol/100) and cl<sw_hi
            else:
                sweep_ok_l=hi>=sw_hi*(1-sw_tol/100) and cl<sw_hi
                sweep_ok_s=lo<=sw_lo*(1+sw_tol/100) and cl>sw_lo
        else:
            sweep_ok_l=sweep_ok_s=True

        # OPT: ms filter — sliding deque O(1) вместо max/min по двум slices
        if umsf and i>=ms_lb*2:
            swing_hi  = _slide_hi_ms1[i-1]
            swing_lo  = _slide_lo_ms1[i-1]
            prev_s_hi = _slide_hi_ms2[i-ms_lb-1] if i-ms_lb-1>=0 else swing_hi
            prev_s_lo = _slide_lo_ms2[i-ms_lb-1] if i-ms_lb-1>=0 else swing_lo
            ms_up=swing_hi>prev_s_hi and swing_lo>prev_s_lo
            ms_down=swing_hi<prev_s_hi and swing_lo<prev_s_lo
            ms_ok_l=ms_down; ms_ok_s=ms_up
        else:
            ms_ok_l=ms_ok_s=True

        # EMA trend filter: лонг только выше EMA, шорт только ниже
        if uemaf and i>=ema_per and ema_series[i]>0:
            ema_ok_l = cl > ema_series[i]
            ema_ok_s = cl < ema_series[i]
        else:
            ema_ok_l = ema_ok_s = True

        if uretf:
            # bounce: лонг ищет нижние фитили в истории (is_up_wick=False), шорт — верхние
            if wd=="bounce":
                ret_up=_calc_return_rate(i,False) if is_up_w else None
                ret_dn=_calc_return_rate(i,True)  if is_dn_w else None
            else:
                ret_up=_calc_return_rate(i,True)  if is_up_w else None
                ret_dn=_calc_return_rate(i,False) if is_dn_w else None
            ret_ok_l=ret_up is not None and ret_up>=ret_minwr
            ret_ok_s=ret_dn is not None and ret_dn>=ret_minwr
        else:
            ret_ok_l=ret_ok_s=True

        if urepf:
            # bounce: лонг проверяет уровень lo (нижний фитиль), шорт — hi
            if wd=="bounce":
                rep_ok_l=is_up_w and _count_tested_level(i,lo,False)>=rep_min
                rep_ok_s=is_dn_w and _count_tested_level(i,hi,True)>=rep_min
            else:
                rep_ok_l=is_up_w and _count_tested_level(i,hi,True)>=rep_min
                rep_ok_s=is_dn_w and _count_tested_level(i,lo,False)>=rep_min
        else:
            rep_ok_l=rep_ok_s=True

        if ucluf:
            # bounce: лонг ищет кластер нижних фитилей на lo, шорт — верхних на hi
            if wd=="bounce":
                clu_ok_l=is_up_w and _count_wick_cluster(i,lo,False)>=clu_min
                clu_ok_s=is_dn_w and _count_wick_cluster(i,hi,True)>=clu_min
            else:
                clu_ok_l=is_up_w and _count_wick_cluster(i,hi,True)>=clu_min
                clu_ok_s=is_dn_w and _count_wick_cluster(i,lo,False)>=clu_min
        else:
            clu_ok_l=clu_ok_s=True

        if uclof and rng>0:
            close_pos=(cl-lo)/rng*100
            # bounce: лонг (нижний фитиль) — закрытие должно быть выше (верхние N%), шорт — ниже
            if wd=="bounce":
                clo_ok_l=close_pos>=clo_lng; clo_ok_s=close_pos<=clo_sht
            else:
                clo_ok_l=close_pos>=clo_lng; clo_ok_s=close_pos<=clo_sht
        else:
            clo_ok_l=clo_ok_s=True

        # bounce: near_lo для лонга (нижний фитиль у нижнего уровня), near_hi для шорта
        if wd=="bounce":
            near_l=near_lo; near_s=near_hi
        else:
            near_l=near_hi; near_s=near_lo

        long_sig_base=(is_up_w and body_ok_up and rsi_ok_l and near_l
                       and geo_ok_l and css_ok_l and quiet_ok
                       and sweep_ok_l and ms_ok_l and ema_ok_l and ret_ok_l
                       and rep_ok_l and clu_ok_l and clo_ok_l)
        short_sig_base=(is_dn_w and body_ok_dn and rsi_ok_s and near_s
                        and geo_ok_s and css_ok_s and quiet_ok
                        and sweep_ok_s and ms_ok_s and ema_ok_s and ret_ok_s
                        and rep_ok_s and clu_ok_s and clo_ok_s)

        if fcon:
            long_sig_base=long_sig_base and last_sig!=1
            short_sig_base=short_sig_base and last_sig!=-1
        if long_sig_base: last_sig=1
        elif short_sig_base: last_sig=-1

        def _confirm():
            if not ucc: return True
            return (body/rng*100>=cbp) if rng>0 else False

        if nb:
            if not in_trade and pending_sig!=0 and i==sig_bar+1:
                if _confirm():
                    ep=cl; pos=min(equity,max_pos)
                    if pending_sig==1:
                        t_dir=1;t_ep=ep;t_tp=ep*(1+tp_p/100);t_sl=ep*(1-sl_p/100)
                        t_orig_sl=sl_p;t_pos=pos;be_trig_lvl=ep*(1+be_trig/100)
                        be_triggered=False;in_trade=True;t_entry_bar=i
                    elif pending_sig==-1:
                        t_dir=-1;t_ep=ep;t_tp=ep*(1-tp_p/100);t_sl=ep*(1+sl_p/100)
                        t_orig_sl=sl_p;t_pos=pos;be_trig_lvl=ep*(1-be_trig/100)
                        be_triggered=False;in_trade=True;t_entry_bar=i
                    if _collect and in_trade:
                        _csigs.append({"bar_i":i,"dir":t_dir,"ep":t_ep,"tp":t_tp,"sl":t_sl,"t":c["t"],"exit_bar":None,"win":None})
                pending_sig=0

            if in_trade and i>t_entry_bar:
                opp=(short_sig_base and t_dir==1) or (long_sig_base and t_dir==-1)
                if opp and _confirm():
                    exit_p=cl
                    move=(exit_p-t_ep)/t_ep*100 if t_dir==1 else (t_ep-exit_p)/t_ep*100
                    rr_r=move/t_orig_sl if t_orig_sl>0 else 0
                    pnl=t_pos*risk_pct/100*rr_r; is_win=pnl>0
                    equity+=pnl;pnls.append(pnl);trades+=1
                    if is_win: wins+=1
                    else: losses_n+=1
                    if equity>max_eq: max_eq=equity
                    dd=(max_eq-equity)/max_eq*100 if max_eq>0 else 0
                    if dd>max_dd: max_dd=dd
                    if _collect and _csigs:
                        _csigs[-1]["exit_bar"]=i;_csigs[-1]["win"]=is_win
                        _csigs[-1]["exit_p"]=exit_p;_csigs[-1]["sig_close"]=True
                    in_trade=False;be_triggered=False
                    pending_sig=-1 if short_sig_base else 1;sig_bar=i

            if long_sig_base and not in_trade: pending_sig=1;sig_bar=i
            elif short_sig_base and not in_trade: pending_sig=-1;sig_bar=i
        else:
            if in_trade and i>t_entry_bar:
                opp=(short_sig_base and t_dir==1) or (long_sig_base and t_dir==-1)
                if opp and _confirm():
                    exit_p=cl
                    move=(exit_p-t_ep)/t_ep*100 if t_dir==1 else (t_ep-exit_p)/t_ep*100
                    rr_r=move/t_orig_sl if t_orig_sl>0 else 0
                    pnl=t_pos*risk_pct/100*rr_r; is_win=pnl>0
                    equity+=pnl;pnls.append(pnl);trades+=1
                    if is_win: wins+=1
                    else: losses_n+=1
                    if equity>max_eq: max_eq=equity
                    dd=(max_eq-equity)/max_eq*100 if max_eq>0 else 0
                    if dd>max_dd: max_dd=dd
                    if _collect and _csigs:
                        _csigs[-1]["exit_bar"]=i;_csigs[-1]["win"]=is_win
                        _csigs[-1]["exit_p"]=exit_p;_csigs[-1]["sig_close"]=True
                    in_trade=False;be_triggered=False
                    if short_sig_base and _confirm():
                        ep=cl;pos=min(equity,max_pos)
                        t_dir=-1;t_ep=ep;t_tp=ep*(1-tp_p/100);t_sl=ep*(1+sl_p/100)
                        t_orig_sl=sl_p;t_pos=pos;be_trig_lvl=ep*(1-be_trig/100)
                        be_triggered=False;in_trade=True;t_entry_bar=i
                        if _collect: _csigs.append({"bar_i":i,"dir":-1,"ep":ep,"tp":t_tp,"sl":t_sl,"t":c["t"],"exit_bar":None,"win":None})
                    elif long_sig_base and _confirm():
                        ep=cl;pos=min(equity,max_pos)
                        t_dir=1;t_ep=ep;t_tp=ep*(1+tp_p/100);t_sl=ep*(1-sl_p/100)
                        t_orig_sl=sl_p;t_pos=pos;be_trig_lvl=ep*(1+be_trig/100)
                        be_triggered=False;in_trade=True;t_entry_bar=i
                        if _collect: _csigs.append({"bar_i":i,"dir":1,"ep":ep,"tp":t_tp,"sl":t_sl,"t":c["t"],"exit_bar":None,"win":None})

            if long_sig_base and not in_trade and _confirm():
                ep=cl;pos=min(equity,max_pos)
                t_dir=1;t_ep=ep;t_tp=ep*(1+tp_p/100);t_sl=ep*(1-sl_p/100)
                t_orig_sl=sl_p;t_pos=pos;be_trig_lvl=ep*(1+be_trig/100)
                be_triggered=False;in_trade=True;t_entry_bar=i
                if _collect: _csigs.append({"bar_i":i,"dir":1,"ep":ep,"tp":t_tp,"sl":t_sl,"t":c["t"],"exit_bar":None,"win":None})
            elif short_sig_base and not in_trade and _confirm():
                ep=cl;pos=min(equity,max_pos)
                t_dir=-1;t_ep=ep;t_tp=ep*(1-tp_p/100);t_sl=ep*(1+sl_p/100)
                t_orig_sl=sl_p;t_pos=pos;be_trig_lvl=ep*(1-be_trig/100)
                be_triggered=False;in_trade=True;t_entry_bar=i
                if _collect: _csigs.append({"bar_i":i,"dir":-1,"ep":ep,"tp":t_tp,"sl":t_sl,"t":c["t"],"exit_bar":None,"win":None})

    if in_trade:
        last_c=candles_list[-1]; exit_p=last_c["close"]
        move=(exit_p-t_ep)/t_ep*100 if t_dir==1 else (t_ep-exit_p)/t_ep*100
        rr_r=move/t_orig_sl if t_orig_sl>0 else 0
        pnl_ot=t_pos*risk_pct/100*rr_r*0.5  # коэф. 0.5: исход неизвестен, не искажаем equity
        equity+=pnl_ot; pnls.append(pnl_ot)
        is_win_ot=pnl_ot>0
        trades+=1
        if is_win_ot: wins+=1
        else: losses_n+=1
        if equity>max_eq: max_eq=equity
        dd_ot=(max_eq-equity)/max_eq*100 if max_eq>0 else 0
        if dd_ot>max_dd: max_dd=dd_ot
        if _collect and _csigs:
            _csigs[-1]["exit_bar"]=None;_csigs[-1]["win"]=is_win_ot;_csigs[-1]["open_end"]=True
            _csigs[-1]["exit_p"]=exit_p

    wr_val=wins/trades*100 if trades>0 else 0
    avg_pnl=sum(pnls)/len(pnls) if pnls else 0
    profit_factor=(sum(x for x in pnls if x>0)/abs(sum(x for x in pnls if x<0))
                   if any(x<0 for x in pnls) else float("inf"))

    if trades<15: fitness=-9999.0
    elif max_dd>=50.0: fitness=-9999.0
    else:
        net_return=equity-100.0

        # --- Calmar: логарифмически нормирован ---
        # DD>=20% — полный обрыв calmar
        min_dd_floor=max(1.0, 15.0/_math.sqrt(max(trades,1)))
        effective_dd=max(max_dd,min_dd_floor)
        if max_dd>=20.0:
            calmar_score=0.0
        else:
            calmar_score=_math.log(max(net_return/effective_dd, 1.0)+1.0)
        dd_penalty=max(0.0,max_dd-15.0)*0.2

        # --- WR: линейный бонус с 50%, без экспоненциального буста ---
        # Убран буст WR 86%+ — он толкал к редким "идеальным" сделкам (оверфиттинг)
        # При RR 1:5 достаточно WR 50%, при RR 1:2 — WR 60%
        wr_bonus=max(0.0,wr_val-50.0)*0.08

        # --- Депозит: log(equity) × нормализация плотности сделок ---
        # Цель: не давать системе получать полный profit_bonus за 3 сделки за 90 дней.
        # Ожидаемый темп: 1 сделка на каждые ~7 дней (умеренная стратегия).
        # Если дней > 0 — считаем ожидаемое кол-во сделок пропорционально периоду,
        # но мягко: минимальный порог 15, чтобы короткие окна (3-10 дней) не штрафовались.
        if days_limit and days_limit > 0:
            _expected = max(15.0, days_limit / 7.0)
        else:
            _expected = 25.0  # fallback: дней нет — ожидаем 25 сделок
        _density_norm = min(1.0, trades / _expected)
        profit_bonus=_math.log(max(equity,1.0))*4.0*_density_norm

        # --- Сделки: поощряем 15-40, плавно штрафуем >60 ---
        # Статистическая надёжность важнее: 25 сделок лучше 5
        if trades<=40:
            trade_bonus=_math.log(max(trades/15.0,1.0)+1)*1.5
        elif trades<=60:
            trade_bonus=_math.log(max(40/15.0,1.0)+1)*1.5-(trades-40)*0.05
        else:
            trade_bonus=_math.log(max(40/15.0,1.0)+1)*1.5-20*0.05-(trades-60)*0.15

        # --- RR (Risk/Reward): средний выигрыш / средний проигрыш ---
        # Стратегия RR 1:5 + WR 50% лучше RR 1:1 + WR 80%
        wins_pnl=[x for x in pnls if x>0]
        loss_pnl=[abs(x) for x in pnls if x<0]
        if wins_pnl and loss_pnl:
            avg_win=sum(wins_pnl)/len(wins_pnl)
            avg_loss=sum(loss_pnl)/len(loss_pnl)
            rr=avg_win/max(avg_loss,0.0001)
            rr_bonus=_math.log(max(rr,1.0)+1)*1.5
        else:
            rr_bonus=0.0

        pf_val=min(profit_factor,4.0) if profit_factor!=float("inf") else 4.0
        pf_bonus=pf_val*1.2

        fitness=calmar_score*2.0+profit_bonus+wr_bonus+trade_bonus+rr_bonus+pf_bonus-dd_penalty

    return {
        "equity": round(equity,2), "trades": trades, "wins": wins, "losses": losses_n,
        "winrate": round(wr_val,1), "max_dd": round(max_dd,2),
        "profit_factor": round(profit_factor,2) if profit_factor!=float("inf") else 999.0,
        "avg_pnl": round(avg_pnl,4), "fitness": round(fitness,4),
        "params": dict(p), "_signals": _csigs if _collect else None,
    }

# ═══════════════════════════════════════════════════════════════
# WORKER POOL
# ═══════════════════════════════════════════════════════════════
_worker_candles = None
_worker_days    = None
_worker_risk    = 20.0
# Предвычисленные массивы (закэшированы один раз на процесс)
_worker_opens   = None
_worker_highs   = None
_worker_lows    = None
_worker_closes  = None

def _worker_init(candles, days, risk):
    global _worker_candles, _worker_days, _worker_risk
    global _worker_opens, _worker_highs, _worker_lows, _worker_closes
    _worker_candles = candles
    _worker_days    = days
    _worker_risk    = risk
    # Предвычисляем массивы цен один раз — не надо делать list comprehension в каждой симуляции
    _worker_opens  = [c["open"]  for c in candles]
    _worker_highs  = [c["high"]  for c in candles]
    _worker_lows   = [c["low"]   for c in candles]
    _worker_closes = [c["close"] for c in candles]

def _worker_evaluate(ind):
    r = _simulate(_worker_candles, ind, _worker_days, risk_pct=_worker_risk)
    if r: return r
    return {"fitness":-9999.0,"equity":100.0,"trades":0,"wins":0,"losses":0,
            "winrate":0,"max_dd":0,"profit_factor":0,"avg_pnl":0,"params":ind}

# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def _default_individual():
    ind={}
    for k,spec in PARAM_SPACE.items():
        if spec["type"] in ("bool","cat"): ind[k]=spec["values"][0]
        elif spec["type"]=="int": ind[k]=int(round((spec["min"]+spec["max"])/2))
        else:
            mid=(spec["min"]+spec["max"])/2; st=spec["step"]
            ind[k]=round(round(mid/st)*st,10)
    return ind

def _random_individual():
    ind={}
    for k,spec in PARAM_SPACE.items():
        if spec["type"] in ("bool","cat"): ind[k]=random.choice(spec["values"])
        else: ind[k]=random.choice(_GRIDS[k])
    return ind

def _si(val, default):
    try: return int(float(val))
    except: return default

def _sf(val, default):
    try: v=float(val); return v if v==v else default
    except: return default

def _update_top20(top20_list, result):
    top20_list.append(result)
    # Сортируем по validated_fitness (учитывает стабильность) если оно есть, иначе по fitness
    top20_list.sort(key=lambda x: -(x.get("validated_fitness") or x["fitness"]))
    seen=set(); deduped=[]
    for item in top20_list:
        key=round(item.get("validated_fitness") or item["fitness"], 6)
        if key not in seen: seen.add(key); deduped.append(item)
    return deduped[:7]

# ═══════════════════════════════════════════════════════════════
# FETCH CANDLES
# ═══════════════════════════════════════════════════════════════
def _fetch_candles(symbol, tf, days):
    interval_sec = TF_SECONDS.get(tf, 3600)
    LIMIT = 999          # Gate.io futures максимум за один запрос
    now   = int(time.time())
    since = now - days * 86400
    total_needed = (now - since) // interval_sec + 2
    all_candles = []
    current_from = since
    last_http_error = None
    last_exception  = None
    # Сообщаем UI что начался fetch
    try:
        with opt_lock:
            opt_state["fetch_pct"] = 0
            opt_state["fetch_symbol"] = symbol
    except Exception:
        pass
    print(f"{_ts()} [fetch] {symbol} {tf} {days}д — нужно ~{total_needed} свечей...", flush=True)
    while current_from < now:
        pct = int((current_from - since) / max(now - since, 1) * 100)
        print("[fetch] {}% ({} св.)".format(pct, len(all_candles)), end="\r", flush=True)
        try:
            with opt_lock:
                opt_state["fetch_pct"] = pct
        except Exception:
            pass
        _fetch_attempt = 0
        _fetch_max_attempts = 5
        _fetch_ok = False
        while _fetch_attempt < _fetch_max_attempts:
            try:
                r = requests.get(f"{GATE_API}/futures/usdt/candlesticks",
                    params={"contract": symbol, "interval": tf,
                            "from": current_from, "limit": LIMIT}, timeout=15)
                if r.status_code != 200:
                    last_http_error = f"HTTP {r.status_code}: {r.text[:200]}"
                    if r.status_code in (429, 502, 503, 504):
                        _wait = min(2 ** _fetch_attempt * 2, 60)
                        print(f"\n{_ts()} [fetch] ⚠ {last_http_error}, повтор через {_wait}с...", flush=True)
                        time.sleep(_wait)
                        _fetch_attempt += 1
                        continue
                    print(f"\n{_ts()} [fetch] ❌ {last_http_error}", flush=True)
                    _fetch_ok = False
                    break
                data = r.json()
                if not isinstance(data, list):
                    last_http_error = f"Неожиданный ответ API: {str(data)[:200]}"
                    print(f"\n{_ts()} [fetch] ❌ {last_http_error}", flush=True)
                    _fetch_ok = False
                    break
                if not data:
                    _fetch_ok = True
                    break
                for c in data:
                    t = int(c.get("t", 0))
                    if t < since - interval_sec: continue  # мягкий порог
                    all_candles.append({"t": t, "open": float(c["o"]),
                        "high": float(c["h"]), "low": float(c["l"]), "close": float(c["c"])})
                last_t = int(data[-1].get("t", 0))
                next_from = last_t + interval_sec
                if next_from <= current_from:
                    current_from = now  # форсируем выход из внешнего цикла
                    _fetch_ok = True; break
                current_from = next_from
                if last_t >= now - interval_sec:
                    current_from = now  # форсируем выход из внешнего цикла
                    _fetch_ok = True; break
                time.sleep(0.05)
                _fetch_ok = True
                break
            except Exception as e:
                last_exception = str(e)
                _wait = min(2 ** _fetch_attempt * 3, 60)
                print(f"\n{_ts()} [fetch] ⚠ Ошибка (попытка {_fetch_attempt+1}/{_fetch_max_attempts}): {e}, повтор через {_wait}с...", flush=True)
                time.sleep(_wait)
                _fetch_attempt += 1
        if not _fetch_ok and _fetch_attempt >= _fetch_max_attempts:
            print(f"\n{_ts()} [fetch] ❌ Превышено кол-во попыток. Последняя ошибка: {last_exception or last_http_error}", flush=True)
            break
    seen = set(); result = []
    for c in sorted(all_candles, key=lambda x: x["t"]):
        if c["t"] not in seen: seen.add(c["t"]); result.append(c)
    print(f"\n{_ts()} [fetch] ✅ Готово: {len(result)} свечей (ожидалось ~{total_needed})", flush=True)
    # Сигнализируем UI: загрузка завершена (100%), затем сбрасываем
    try:
        with opt_lock:
            opt_state["fetch_pct"] = 100
    except Exception:
        pass
    # Возвращаем причину ошибки вместе с результатом через глобал (для лога оптимизатора)
    global _last_fetch_error
    _last_fetch_error = last_http_error or last_exception or None
    return result

def _fetch_latest_candle(symbol, tf):
    """Загружает последние 2 свечи, возвращает последнюю закрытую."""
    try:
        r = requests.get(f"{GATE_API}/futures/usdt/candlesticks",
            params={"contract":symbol,"interval":tf,"limit":2}, timeout=8)
        if r.status_code != 200: return None
        data = r.json()
        if not data or len(data) < 1: return None
        # Берём предпоследнюю (она гарантированно закрыта)
        c = data[-2] if len(data) >= 2 else data[-1]
        return {"t":int(c.get("t",0)),"open":float(c["o"]),
                "high":float(c["h"]),"low":float(c["l"]),"close":float(c["c"])}
    except:
        return None

def _fetch_current_candle(symbol, tf):
    """Возвращает текущую (возможно незакрытую) свечу.
    Gate.io при запросе без параметра 'to' отдаёт последние N свечей,
    где последняя — текущая (незакрытая). Сравниваем её timestamp
    с предпоследней чтобы убедиться что это новый интервал.
    При сетевых ошибках делает до 3 попыток с паузами."""
    _max_retries = 3
    for _attempt in range(_max_retries):
        try:
            interval_sec = TF_SECONDS.get(tf, 3600)
            r = requests.get(f"{GATE_API}/futures/usdt/candlesticks",
                params={"contract": symbol, "interval": tf, "limit": 3}, timeout=8)
            if r.status_code != 200:
                print(f"[live_candle] HTTP {r.status_code}", flush=True)
                if _attempt < _max_retries - 1:
                    time.sleep(2 ** _attempt)
                    continue
                return None
            data = r.json()
            if not data or len(data) < 2:
                print(f"[live_candle] мало данных: {data}", flush=True)
                return None
            last = data[-1]
            prev = data[-2]
            last_t = int(last.get("t", 0))
            prev_t = int(prev.get("t", 0))
            # Текущая незакрытая свеча — это последняя, у неё t >= prev_t + interval_sec
            # ИЛИ просто берём её всегда — она либо закрыта либо нет,
            # в любом случае это самая свежая информация
            now = int(time.time())
            candle_open_t = (now // interval_sec) * interval_sec
            # Если последняя свеча из API — это уже текущий интервал, берём её
            if last_t >= candle_open_t:
                c = last
            else:
                # Gate.io ещё не выдал текущую — строим из тикера
                r2 = requests.get(f"{GATE_API}/futures/usdt/tickers",
                    params={"contract": symbol}, timeout=5)
                if r2.status_code != 200: return None
                td = r2.json()
                if not td: return None
                price = float(td[0].get("last", 0))
                open_p = float(last.get("c", price))
                print(f"[live_candle] тикер: price={price} open={open_p} t={candle_open_t}", flush=True)
                return {"t": candle_open_t, "open": open_p,
                        "high": max(open_p, price), "low": min(open_p, price),
                        "close": price, "live": True}
            result = {"t": last_t, "open": float(c["o"]),
                      "high": float(c["h"]), "low": float(c["l"]),
                      "close": float(c["c"]), "live": True}
            print(f"[live_candle] OK t={last_t} c={result['close']} (now={now} interval_t={candle_open_t})", flush=True)
            return result
        except Exception as e:
            _wait = 2 ** _attempt
            print(f"[live_candle] exception (попытка {_attempt+1}/{_max_retries}): {e}, пауза {_wait}с...", flush=True)
            if _attempt < _max_retries - 1:
                time.sleep(_wait)
    return None

# ═══════════════════════════════════════════════════════════════
# COORDINATE DESCENT
# ═══════════════════════════════════════════════════════════════
# OPT: обратный маппинг use_X -> [dep_param1, dep_param2, ...] для батчинга
_USE_TO_DEPS = {}
for _dep_k, _use_k in FILTER_GROUPS.items():
    _USE_TO_DEPS.setdefault(_use_k, []).append(_dep_k)

def _coordinate_descent_from(start_ind, pmap_fn, olog, t0,
                              top20_global, start_label, max_passes=8,
                              stop_flag=None, grids=None):
    current = dict(start_ind)
    best_result = pmap_fn([current])[0]
    pass_num = 0

    # Если стартовая точка не набирает минимум сделок — пробуем найти хоть что-то
    # за один быстрый круг, иначе пропускаем этот старт
    _dead_start = best_result["fitness"] <= -9000

    while True:
        if stop_flag and stop_flag(): break
        pass_num += 1
        _grids = grids if grids is not None else _GRIDS

        # OPT: use_* параметры пропускаем отдельно — они обрабатываются вместе
        # с зависимыми параметрами в одном батче (см. ниже)
        keys_shuffled = list(_KEYS); random.shuffle(keys_shuffled)
        # Разбиваем на use_* и остальные
        use_keys_set = set(_USE_TO_DEPS.keys())
        # use_* будут обработаны внутри блока зависимых параметров
        # Строим порядок: сначала use_* параметр (если ещё не обработан), потом его зависимые
        processed_use = set()
        ordered_keys = []
        for k in keys_shuffled:
            uk = FILTER_GROUPS.get(k)  # родительский use_* для зависимого параметра
            if uk and uk not in processed_use:
                # Первый зависимый параметр этой группы — вставляем use_* перед ним
                ordered_keys.append(uk)
                processed_use.add(uk)
            if k not in use_keys_set:  # не добавляем use_* дважды
                ordered_keys.append(k)
            elif k not in processed_use:
                ordered_keys.append(k)
                processed_use.add(k)

        steps_in_pass = sum(len(_grids[k]) for k in ordered_keys if k in _grids)
        with opt_lock:
            opt_state["pass_num"]=pass_num; opt_state["total"]=steps_in_pass; opt_state["progress"]=0

        step_in_pass=0; improved_in_pass=False
        _pass_t0 = time.time()
        _visited_use = set()  # OPT: не обрабатываем use_* дважды

        for param_idx, key in enumerate(ordered_keys):
            if stop_flag and stop_flag(): break
            if key not in PARAM_SPACE: continue  # защита
            label=PARAM_SPACE[key]["label"]; grid=_grids.get(key, _GRIDS.get(key, []))
            if not grid: continue
            with opt_lock:
                opt_state["current_param"]=label; opt_state["generation"]=param_idx+1

            _param_t0 = time.time()

            # OPT: если это use_* параметр — объединяем с зависимыми в один батч
            # Смысл: вместо отдельного pmap([use=True, use=False]) + отдельного pmap([dep1_val1,...])
            # делаем один pmap для use=False + все значения dep1 при use=True
            # Это сокращает число pmap-вызовов для групп с 2+ зависимыми параметрами
            if key in use_keys_set and key not in _visited_use:
                _visited_use.add(key)
                deps = _USE_TO_DEPS.get(key, [])
                # Кандидат с use=False (все зависимые неважны)
                candidates = [{**current, key: False}]
                # Кандидаты с use=True + все значения первого зависимого параметра
                # (остальные зависимые будут оптимизированы в своих итерациях)
                if deps and current.get(key, True):
                    first_dep = deps[0]
                    dep_grid = _grids.get(first_dep, _GRIDS.get(first_dep, []))
                    for val in dep_grid:
                        candidates.append({**current, key: True, first_dep: val})
                else:
                    candidates.append({**current, key: True})

                results = pmap_fn(candidates); results.sort(key=lambda x: -x["fitness"])
                if _eco_mode and not (stop_flag and stop_flag()):
                    for _ in range(9):  # 9×50мс = 450мс, прерывается по stop_flag
                        if stop_flag and stop_flag(): break
                        time.sleep(0.05)
                _param_dt = round(time.time() - _param_t0, 3)
                param_best = results[0]
                best_use_val = param_best["params"][key]

                if param_best["fitness"] > best_result["fitness"]:
                    delta = param_best["equity"] - best_result["equity"]
                    # Применяем весь найденный набор параметров (use + первый dep если улучшился)
                    for upd_k, upd_v in param_best["params"].items():
                        if upd_k == key or upd_k in deps:
                            current[upd_k] = upd_v
                    best_result = param_best; improved_in_pass = True
                    val_str = "да" if best_use_val else "нет"
                    olog(f"    ✅ {label}: {val_str} → ${param_best['equity']:.2f} (+{delta:.2f}$) | WR {param_best['winrate']:.1f}% | Сд {param_best['trades']} | DD {param_best['max_dd']:.1f}%","found")

                _plog("param", key=key, n_cands=len(candidates), sec=_param_dt, pass_n=pass_num, start=start_label)
                step_in_pass += len(grid)
                with opt_lock:
                    opt_state["progress"] = step_in_pass
                    opt_state["elapsed"] = round(time.time()-t0, 1)
                continue

            # OPT: зависимый параметр — пропускаем если use_* = False (симуляция всё равно игнорирует)
            use_key = FILTER_GROUPS.get(key)
            if use_key and not current.get(use_key, True):
                step_in_pass += len(grid)
                with opt_lock:
                    opt_state["progress"] = step_in_pass
                    opt_state["elapsed"] = round(time.time()-t0, 1)
                continue

            candidates=[{**current, key:val} for val in grid]
            # Добавляем кандидата с отключённым родительским use_* (если ещё не оптимизировали его)
            if use_key and use_key not in _visited_use and current.get(use_key, True):
                candidates.append({**current, use_key:False})

            results=pmap_fn(candidates); results.sort(key=lambda x:-x["fitness"])
            if _eco_mode and not (stop_flag and stop_flag()):
                for _ in range(9):  # прерываемый sleep 450мс
                    if stop_flag and stop_flag(): break
                    time.sleep(0.05)
            _param_dt = round(time.time() - _param_t0, 3)
            param_best=results[0]; best_val=param_best["params"][key]

            if param_best["fitness"]>best_result["fitness"]:
                delta=param_best["equity"]-best_result["equity"]
                current[key]=best_val
                # Если лучший кандидат отключил use_*, обновляем его тоже
                if use_key and param_best["params"].get(use_key) == False:
                    current[use_key] = False
                    _visited_use.add(use_key)
                best_result=param_best; improved_in_pass=True
                val_str=("да" if best_val else "нет") if isinstance(best_val,bool) else (f"{best_val:.2f}" if isinstance(best_val,float) else str(best_val))
                olog(f"    ✅ {label}: {val_str} → ${param_best['equity']:.2f} (+{delta:.2f}$) | WR {param_best['winrate']:.1f}% | Сд {param_best['trades']} | DD {param_best['max_dd']:.1f}%","found")

            _plog("param", key=key, n_cands=len(candidates), sec=_param_dt, pass_n=pass_num, start=start_label)

            step_in_pass+=len(grid)
            with opt_lock:
                opt_state["progress"]=step_in_pass
                opt_state["elapsed"]=round(time.time()-t0,1)

        _pass_dt = round(time.time() - _pass_t0, 3)
        _plog("pass_done", pass_n=pass_num, sec=_pass_dt, improved=improved_in_pass, start=start_label)

        if stop_flag and stop_flag(): break

        if not improved_in_pass: break
        # Мёртвый старт: если за первый круг не нашли ни одной валидной стратегии — уходим
        if _dead_start and best_result["fitness"] <= -9000: break
        _dead_start = False  # после первого улучшения снимаем флаг
        if pass_num>=max_passes: break

    # Обновляем top20 только финальным результатом старта
    top20_global = _update_top20(top20_global, best_result)
    return best_result, current, top20_global

# ═══════════════════════════════════════════════════════════════
# EMAIL
# ═══════════════════════════════════════════════════════════════
def _send_telegram(cfg, text):
    token = cfg.get("tg_token","")
    chat_id = cfg.get("tg_chat_id","")
    if not token or not chat_id: return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    # Retry 3 раза при сетевых ошибках (таймаут, connection reset и т.п.)
    for _attempt in range(3):
        try:
            resp = requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, timeout=15)
            if resp.ok:
                return True
            err_text = resp.text
            # 429 Too Many Requests — подождать и повторить
            if resp.status_code == 429:
                retry_after = int(resp.json().get("parameters", {}).get("retry_after", 5))
                print(f"[tg] 429 flood wait {retry_after}s", flush=True)
                time.sleep(retry_after)
                continue
            with opt_lock: opt_state["error"]=f"Telegram: {err_text}"
            print(f"[tg] ERROR {resp.status_code}: {err_text[:200]}", flush=True)
            return False
        except Exception as e:
            print(f"[tg] attempt {_attempt+1}/3 failed: {e}", flush=True)
            if _attempt < 2:
                time.sleep(3)
            else:
                with opt_lock: opt_state["error"]=f"Telegram: {e}"
                return False
    return False

def _send_ntfy(cfg, text):
    """Резервный канал уведомлений через ntfy.sh (open-source push)."""
    topic = cfg.get("ntfy_topic", "")
    if not topic: return False
    server = cfg.get("ntfy_server", "https://ntfy.sh")
    url = f"{server}/{topic}"
    # Убираем HTML-теги для plain text
    import re as _re
    plain = _re.sub(r"<[^>]+>", "", text).strip()
    try:
        resp = requests.post(url, data=plain.encode("utf-8"),
                             headers={"Title": "WickFill", "Priority": "high",
                                      "Tags": "chart_increasing"},
                             timeout=10)
        if resp.ok:
            return True
        print(f"[ntfy] ERROR {resp.status_code}: {resp.text[:100]}", flush=True)
        return False
    except Exception as e:
        print(f"[ntfy] failed: {e}", flush=True)
        return False

def _send_alert(cfg, text):
    """Шлёт в Telegram и/или ntfy — оба канала независимо."""
    tg_ok = _send_telegram(cfg, text)
    ntfy_ok = _send_ntfy(cfg, text)
    return tg_ok or ntfy_ok

def _send_signal_email(cfg, symbol, tf, direction, entry, tp, sl, candle_t, leverage=None):
    dir_str="🔵 ЛОНГ" if direction==1 else "🟡 ШОРТ"
    # Показываем время ЗАКРЫТИЯ свечи (открытие + интервал), в московском времени (UTC+3)
    close_t = candle_t + TF_SECONDS.get(tf, 3600)
    moscow_offset = 3 * 3600  # UTC+3
    dt = time.strftime("%Y-%m-%d %H:%M", time.gmtime(close_t + moscow_offset))
    lev_str = f"\n⚡ Плечо: <b>{int(leverage)}×</b>" if leverage and int(leverage) > 1 else ""
    text = (
        f"🔔 <b>WickFill Сигнал</b>\n\n"

        f"{dir_str} <b>{symbol}</b> {tf}\n"
        f"🕐 {dt}\n\n"

        f"📥 Вход: <b>{entry:.6g}</b>\n"
        f"✅ Тейк-профит: <b>{tp:.6g}</b>\n"
        f"❌ Стоп-лосс: <b>{sl:.6g}</b>"
        f"{lev_str}"
    )
    return _send_alert(cfg, text)

# ═══════════════════════════════════════════════════════════════
# GATE.IO AUTO-TRADING — USDT-M фьючерсы
# ═══════════════════════════════════════════════════════════════
import hmac, hashlib

def _gate_sign(api_secret, method, url_path, query_string, body_str, ts):
    """Подписывает запрос по алгоритму Gate.io v4."""
    body_hash = hashlib.sha512((body_str or "").encode()).hexdigest()
    msg = "\n".join([method, url_path, query_string, body_hash, str(ts)])
    sig = hmac.new(api_secret.encode(), msg.encode(), hashlib.sha512).hexdigest()
    return sig

def _gate_request(cfg, method, path, params=None, body=None):
    """Выполняет подписанный запрос к Gate.io Futures API."""
    api_key    = cfg.get("gate_key", "")
    api_secret = cfg.get("gate_secret", "")
    if not api_key or not api_secret:
        return None, "Gate API ключи не заданы"
    ts          = str(int(time.time()))
    query_str   = "&".join(f"{k}={v}" for k, v in (params or {}).items())
    body_str    = json.dumps(body) if body else ""
    sig         = _gate_sign(api_secret, method, path, query_str, body_str, ts)
    url         = f"https://fx-api.gateio.ws{path}" + (f"?{query_str}" if query_str else "")
    headers     = {
        "Content-Type":  "application/json",
        "KEY":           api_key,
        "SIGN":          sig,
        "Timestamp":     ts,
    }
    try:
        resp = requests.request(method, url, headers=headers,
                                data=body_str.encode() if body_str else None, timeout=10)
        data = resp.json()
        if not resp.ok:
            return None, data.get("message") or data.get("label") or str(data)
        return data, None
    except Exception as e:
        return None, str(e)

def _gate_get_balance(cfg):
    """Возвращает доступный баланс USDT фьючерсного кошелька."""
    data, err = _gate_request(cfg, "GET", "/api/v4/futures/usdt/accounts")
    if err: return None, err
    available = float(data.get("available", 0))
    return available, None

def _gate_cancel_all_orders(cfg, contract):
    """Отменяет все открытые обычные и триггерные ордера по контракту."""
    # 1. Обычные лимитные ордера
    _gate_request(cfg, "DELETE", "/api/v4/futures/usdt/orders",
                  params={"contract": contract, "side": "ask"})
    _gate_request(cfg, "DELETE", "/api/v4/futures/usdt/orders",
                  params={"contract": contract, "side": "bid"})
    # 2. Триггерные ордера (price_orders) — TP и SL
    data, err = _gate_request(cfg, "GET", "/api/v4/futures/usdt/price_orders",
                              params={"contract": contract, "status": "open"})
    if not err and isinstance(data, list):
        for o in data:
            oid = o.get("id")
            if oid:
                _gate_request(cfg, "DELETE", f"/api/v4/futures/usdt/price_orders/{oid}")


def _gate_close_position(cfg, contract):
    """Отменяет все ордера и закрывает открытую позицию по контракту (если есть)."""
    # Сначала отменяем все висящие ордера (TP/SL от предыдущей сделки)
    _gate_cancel_all_orders(cfg, contract)
    # Получаем текущую позицию
    data, err = _gate_request(cfg, "GET", f"/api/v4/futures/usdt/positions/{contract}")
    if err: return True, None  # нет позиции — ок
    size = int(data.get("size", 0))
    if size == 0: return True, None  # уже закрыта
    # Закрываем противоположным маркет-ордером
    order = {
        "contract": contract,
        "size":     -size,
        "price":    "0",
        "tif":      "ioc",
        "reduce_only": True,
        "text":     "t-wickfill-close"
    }
    _, err = _gate_request(cfg, "POST", "/api/v4/futures/usdt/orders", body=order)
    return err is None, err

def _gate_set_leverage(cfg, contract, leverage):
    """Устанавливает кредитное плечо для контракта."""
    _, err = _gate_request(cfg, "POST", f"/api/v4/futures/usdt/positions/{contract}/leverage",
                           params={"leverage": str(int(leverage))})
    return err is None, err

def _gate_round_price(price, contract):
    """Округляет цену до шага Gate (order_price_round).
    Известные шаги: BTC=0.1, ETH=0.01, SOL=0.001, остальные=0.0001 или берём из API.
    """
    _TICK = {
        "BTC_USDT": 0.1,   "ETH_USDT": 0.01,  "SOL_USDT": 0.001,
        "BNB_USDT": 0.01,  "XRP_USDT": 0.0001,"DOGE_USDT": 0.00001,
        "LTC_USDT": 0.001, "AVAX_USDT": 0.001,"LINK_USDT": 0.001,
        "DOT_USDT": 0.001, "UNI_USDT": 0.001, "ATOM_USDT": 0.001,
        "OP_USDT":  0.0001,"ARB_USDT": 0.0001,"SUI_USDT": 0.0001,
        "APT_USDT": 0.001, "INJ_USDT": 0.001, "TON_USDT": 0.001,
        "TRX_USDT": 0.00001,"ADA_USDT": 0.00001,"MATIC_USDT": 0.00001,
    }
    tick = _TICK.get(contract, 0.1)
    import math
    rounded = round(round(price / tick) * tick, 10)
    # Форматируем без лишних нулей
    decimals = max(0, -int(math.floor(math.log10(tick)))) if tick < 1 else 0
    return f"{rounded:.{decimals}f}"


def _gate_place_order(cfg, contract, direction, size, tp_price, sl_price):
    """Выставляет рыночный ордер с TP и SL через price_orders (триггерные)."""
    is_long = (direction == 1)
    close_size = -(int(size)) if is_long else int(size)
    tp_price_str = _gate_round_price(tp_price, contract)
    sl_price_str = _gate_round_price(sl_price, contract)

    # 1. Основной маркет-ордер
    order = {
        "contract": contract,
        "size":     int(size) if is_long else -int(size),
        "price":    "0",
        "tif":      "ioc",
        "text":     "t-wickfill"
    }
    data, err = _gate_request(cfg, "POST", "/api/v4/futures/usdt/orders", body=order)
    if err: return False, err

    # 2. TP — триггерный ордер (price_orders)
    # Лонг: срабатывает когда цена >= tp_price (rule=1)
    # Шорт: срабатывает когда цена <= tp_price (rule=2)
    tp_trigger = {
        "initial": {
            "contract":    contract,
            "size":        close_size,
            "price":       "0",
            "tif":         "ioc",
            "reduce_only": True,
            "text":        "t-wickfill-tp"
        },
        "trigger": {
            "strategy_type": 0,
            "price_type":    0,
            "price":         tp_price_str,
            "rule":          1 if is_long else 2,
            "expiration":    86400
        }
    }
    tp_data, tp_err = _gate_request(cfg, "POST", "/api/v4/futures/usdt/price_orders", body=tp_trigger)
    if tp_err:
        # Логируем но не прерываем
        pass

    # 3. SL — триггерный ордер (price_orders)
    # Лонг: срабатывает когда цена <= sl_price (rule=2)
    # Шорт: срабатывает когда цена >= sl_price (rule=1)
    sl_trigger = {
        "initial": {
            "contract":    contract,
            "size":        close_size,
            "price":       "0",
            "tif":         "ioc",
            "reduce_only": True,
            "text":        "t-wickfill-sl"
        },
        "trigger": {
            "strategy_type": 0,
            "price_type":    0,
            "price":         sl_price_str,
            "rule":          2 if is_long else 1,
            "expiration":    86400
        }
    }
    sl_data, sl_err = _gate_request(cfg, "POST", "/api/v4/futures/usdt/price_orders", body=sl_trigger)

    tp_status  = f"TP={tp_price} {'✅' if not tp_err else '❌'+str(tp_err)}"
    sl_status  = f"SL={sl_price} {'✅' if not sl_err else '❌'+str(sl_err)}"
    return True, f"{tp_status} | {sl_status}"

def _gate_execute_signal(cfg, symbol, direction, ep, tp, sl, leverage, position_pct, fixed_margin_usdt=None, fixed_notional_usdt=None):
    """Полный цикл: закрыть старую → выставить новую."""
    # Gate контракт: BTC_USDT → BTC_USDT (совпадает)
    contract = symbol.replace("/", "_").upper()
    log_lines = []
    # 1. Закрываем старую позицию
    ok, err = _gate_close_position(cfg, contract)
    if not ok:
        return False, f"Ошибка закрытия позиции: {err}"
    log_lines.append("✓ Старая позиция закрыта (или не было)")
    # 2. Получаем баланс
    balance, err = _gate_get_balance(cfg)
    if err or balance is None:
        return False, f"Ошибка получения баланса: {err}"
    log_lines.append(f"✓ Баланс: {balance:.2f} USDT")
    # 3. Устанавливаем плечо
    ok, err = _gate_set_leverage(cfg, contract, leverage)
    if not ok:
        log_lines.append(f"⚠ Плечо: {err}")  # не критично — продолжаем
    else:
        log_lines.append(f"✓ Плечо: {int(leverage)}×")
    # 4. Рассчитываем размер позиции
    if fixed_notional_usdt is not None:
        # Пользователь вводит размер позиции (уже с плечом)
        notional = fixed_notional_usdt
    elif fixed_margin_usdt is not None:
        # Пользователь вводит маржу (без плеча) — умножаем
        notional = fixed_margin_usdt * leverage
    else:
        margin   = balance * (position_pct / 100.0)
        notional = margin * leverage
    # Размер в контрактах.
    # Gate USDT Futures: 1 контракт = quanto_multiplier единиц базового актива.
    # Стоимость 1 контракта = ep * quanto_multiplier (в USDT).
    # size = notional / (ep * quanto_multiplier)
    _QM_TABLE = {
        "BTC_USDT": 0.0001, "ETH_USDT": 0.01,  "SOL_USDT": 0.1,
        "BNB_USDT": 0.01,   "XRP_USDT": 10.0,  "DOGE_USDT": 100.0,
        "ADA_USDT": 10.0,   "MATIC_USDT": 10.0,"DOT_USDT": 1.0,
        "LTC_USDT": 0.1,    "AVAX_USDT": 0.1,  "LINK_USDT": 1.0,
        "UNI_USDT": 1.0,    "ATOM_USDT": 1.0,  "TRX_USDT": 1000.0,
        "OP_USDT": 1.0,     "ARB_USDT": 10.0,  "SUI_USDT": 1.0,
        "APT_USDT": 0.1,    "INJ_USDT": 0.1,   "TON_USDT": 1.0,
    }
    _qm = _QM_TABLE.get(contract, 0)
    if _qm == 0:
        # Если не в таблице — запрашиваем у Gate
        try:
            _ci = requests.get(f"{GATE_API}/futures/usdt/contracts/{contract}", timeout=5).json()
            _qm = float(_ci.get("quanto_multiplier", 0) or 0)
        except Exception:
            _qm = 0
    if _qm > 0:
        size = max(1, round(notional / (ep * _qm)))
        log_lines.append(f"  [debug] notional={notional:.2f} ep={ep:.2f} qm={_qm} → size={size}")
    else:
        # Последний фоллбэк: предполагаем 1 контракт = 1 USD
        size = max(1, round(notional))
        log_lines.append(f"  [debug] qm=0 fallback: notional={notional:.2f} → size={size}")
    log_lines.append(f"✓ Размер: {size} контр. (~{notional:.1f} USDT)")
    # 5. Выставляем ордер
    ok, order_log = _gate_place_order(cfg, contract, direction, size, tp, sl)
    if not ok:
        return False, f"Ошибка ордера: {order_log}\n" + "\n".join(log_lines)
    dir_str = "ЛОНГ" if direction == 1 else "ШОРТ"
    log_lines.append(f"✓ Ордер: {dir_str} {contract} {order_log or ''}")
    return True, "\n".join(log_lines)


def _build_chart_html(candles, signals, best_result, symbol, tf, risk_pct_ui=20.0):
    import json as _j
    p=best_result.get("params",{})
    eq=best_result.get("equity",100.0); wr=best_result.get("winrate",0.0)
    dd=best_result.get("max_dd",0.0); pf_raw=best_result.get("profit_factor",0.0)
    pf=999 if pf_raw in (999.0,float("inf")) else pf_raw
    trades=best_result.get("trades",0)
    params_rows=""
    for k,v in p.items():
        vs=("да" if v else "нет") if isinstance(v,bool) else (f"{v:.2f}" if isinstance(v,float) else str(v))
        label=PARAM_SPACE.get(k,{}).get("label",k)
        params_rows+=f"<tr><td>{label}</td><td><b>{vs}</b></td></tr>"
    candles_json=_j.dumps(candles,ensure_ascii=False)
    signals_json=_j.dumps(signals,ensure_ascii=False)
    tf_sec=TF_SECONDS.get(tf,3600)
    updated=time.strftime("%Y-%m-%d %H:%M:%S")

    return f"""<!DOCTYPE html>
<html lang="ru"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>WickFill · {symbol} · {tf}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
:root{{
  --cream:#f7f3ee;--cream2:#ede8e0;--cream3:#e2dbd0;
  --bark:#4a3f34;--text:#1a1310;--text2:#504438;--text3:#7a6e63;
  --border:rgba(92,79,67,.15);--border2:rgba(92,79,67,.08);
  --green:#A3BF6F;--red:#FF8234;--yellow:#c8902a;
  --green-light:rgba(58,125,82,.1);--red-light:rgba(160,48,48,.1);
}}
html,body{{height:100%;background:#111111;color:#F5F5F5;font-family:'DM Sans',system-ui,sans-serif;font-size:13px;overflow:hidden;display:flex;flex-direction:column}}
[data-theme="light"] body{{background:#FAE6D8;color:#1e1209}}
[data-theme="dark"] body{{background:#111111;color:#F5F5F5}}
[data-theme="dark"] #tooltip{{background:rgba(26,26,26,.97);border:1px solid rgba(245,245,245,.12);color:#F5F5F5;box-shadow:0 4px 16px rgba(0,0,0,.5)}}
[data-theme="light"] #tooltip{{background:rgba(237,232,225,.98);border:1px solid rgba(60,45,30,.14);color:#2b2620;box-shadow:0 4px 16px rgba(40,30,20,.10)}}
.body{{display:flex;flex:1;min-height:0}}
#canvas-wrap{{flex:1;position:relative;overflow:hidden}}
canvas{{display:block;width:100%;height:100%}}
#tooltip{{position:absolute;display:none;pointer-events:none;
  background:rgba(30,26,23,.97);border:1px solid rgba(255,255,255,.12);
  border-radius:10px;padding:7px 11px;font-size:.7rem;line-height:1.75;
  white-space:nowrap;z-index:20;color:#d4c8bc;
  box-shadow:0 4px 16px rgba(0,0,0,.4)}}
.legend{{display:none}}
.live-badge{{padding:2px 7px;
  background:var(--green-light);border:1px solid rgba(58,125,82,.3);
  border-radius:10px;font-size:.65rem;color:var(--green);
  animation:pulse 2s infinite;display:inline-block}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.5}}}}
::-webkit-scrollbar{{width:5px}}
::-webkit-scrollbar-thumb{{background:var(--cream3);border-radius:3px}}
</style></head><body>
<div class="body">
  <div id="canvas-wrap">
    <canvas id="c"></canvas>
    <div id="tooltip"></div>

    <div style="position:absolute;top:8px;left:10px;display:flex;align-items:center;gap:8px;font-size:.7rem;color:#9a8e83">
      <span class="live-badge" id="liveBadge">⬤ LIVE</span>
      <span style="font-weight:600;color:#d4c8bc">{symbol} · {tf}</span>
      <span>{len(candles)} св. · {trades} сд.</span>
      <span style="color:#7a6e68">{(__import__('datetime').datetime.utcfromtimestamp(candles[0]['t'])+__import__('datetime').timedelta(hours=3)).strftime('%d.%m %H:%M')} — {(__import__('datetime').datetime.utcfromtimestamp(candles[-1]['t'])+__import__('datetime').timedelta(hours=3)).strftime('%d.%m %H:%M')}</span>
    </div>
  </div>
</div>
<script>
// Read theme from URL param and apply before render
(function(){{
  const p=new URLSearchParams(location.search);
  const t=p.get('theme')||'light';
  document.documentElement.setAttribute('data-theme',t);
}})();
</script>
<script>
const CANDLES={candles_json};
const SIGNALS={signals_json};
const TF_SEC={tf_sec};
const canvas=document.getElementById('c');
const ctx=canvas.getContext('2d');
const wrap=document.getElementById('canvas-wrap');
let viewStart=Math.max(0,CANDLES.length-120),viewLen=Math.min(120,CANDLES.length);
let isDragging=false,dragX=0,dragVS=0,sidebarOpen=true;
function toggleSidebar(){{const sb=document.getElementById('sidebar');sidebarOpen=!sidebarOpen;sb.classList.toggle('hidden',!sidebarOpen);requestAnimationFrame(render);}}
let _lastW=0,_lastH=0;
function render(){{
  const dpr=window.devicePixelRatio||1;
  let W=wrap.clientWidth,H=wrap.clientHeight;
  // Fallback: если wrap ещё не отрисован — используем последние известные размеры
  if(!W||!H){{if(_lastW&&_lastH){{W=_lastW;H=_lastH;}}else return;}}
  _lastW=W;_lastH=H;
  canvas.width=W*dpr;canvas.height=H*dpr;canvas.style.width=W+'px';canvas.style.height=H+'px';
  ctx.scale(dpr,dpr);
  const end=Math.min(viewStart+viewLen,CANDLES.length),vis=CANDLES.slice(viewStart,end);
  if(!vis.length) return;
  let mn=Infinity,mx=-Infinity;
  for(const c of vis){{mn=Math.min(mn,c.l);mx=Math.max(mx,c.h);}}
  for(const s of SIGNALS){{if(s.bar_i>=viewStart&&s.bar_i<end){{mn=Math.min(mn,s.sl);mx=Math.max(mx,s.tp);}}}}
  const pad=(mx-mn)*0.08;mn-=pad;mx+=pad;if(mx<=mn)mx=mn+1;
  const PAD_L=6,PAD_R=72,PAD_T=28,PAD_B=54,drawW=W-PAD_L-PAD_R,drawH=H-PAD_T-PAD_B;
  const cw=drawW/vis.length,gap=Math.max(0.5,cw*0.15);
  const py=price=>PAD_T+(mx-price)/(mx-mn)*drawH;
  const cx=i=>PAD_L+(i+0.5)*cw;
  // Theme-aware colors
  const isDark=document.documentElement.getAttribute('data-theme')==='dark';
  const clrBg        = isDark ? '#111111' : '#FAE6D8';
  const clrAxis      = isDark ? 'rgba(255,255,255,.12)' : 'rgba(30,40,60,.12)';
  const clrGrid      = isDark ? 'rgba(255,255,255,.05)' : 'rgba(30,40,60,.05)';
  const clrPriceText = isDark ? '#9a8e83' : '#6a7a8e';
  const clrTimeText  = isDark ? '#7a6e63' : '#848d9e';
  // Background fill chart area
  ctx.fillStyle=clrBg;ctx.fillRect(0,0,W,H);
  // Time axis separator
  ctx.strokeStyle=clrAxis;ctx.lineWidth=1;
  ctx.beginPath();ctx.moveTo(PAD_L,H-PAD_B);ctx.lineTo(W-PAD_R,H-PAD_B);ctx.stroke();
  // Price axis separator
  ctx.beginPath();ctx.moveTo(W-PAD_R,PAD_T);ctx.lineTo(W-PAD_R,H-PAD_B);ctx.stroke();
  ctx.font='10px system-ui';ctx.textAlign='left';
  for(let g=0;g<=7;g++){{
    const price=mn+(mx-mn)*g/7,y=py(price);
    ctx.strokeStyle=clrGrid;ctx.lineWidth=1;ctx.beginPath();ctx.moveTo(PAD_L,y);ctx.lineTo(W-PAD_R,y);ctx.stroke();
    ctx.fillStyle=clrPriceText;ctx.fillText(price.toPrecision(6),W-PAD_R+4,y+3);
  }}
  // Индекс live-свечи (глобальный) — не рисуем сигналы на незакрытой свече
  const _liveBarGlobal = (CANDLES.length > 0 && CANDLES[CANDLES.length-1].live) ? CANDLES.length-1 : -1;
  // Active open trade — find regardless of viewport (labels always visible)
  // Исключаем сигналы на live-свече — до закрытия свечи TP/SL не рисуем
  const activeSig=SIGNALS.find(s=>s.open_end===true && s.bar_i!==_liveBarGlobal);
  for(const s of SIGNALS){{
    const vi=s.bar_i-viewStart;if(vi<-1||vi>=vis.length) continue;
    const viC=Math.max(0,vi),eiR=s.exit_bar!==null?s.exit_bar-viewStart:vis.length-1;
    const ei=Math.min(Math.max(viC,eiR),vis.length-1);
    const x1=PAD_L+viC*cw,x2=PAD_L+(ei+1)*cw,isLong=s.dir===1;
    ctx.fillStyle='rgba(58,125,82,0.08)';ctx.fillRect(x1,Math.min(py(s.ep),py(s.tp)),x2-x1,Math.abs(py(s.ep)-py(s.tp)));
    ctx.fillStyle='rgba(160,48,48,0.08)';ctx.fillRect(x1,Math.min(py(s.ep),py(s.sl)),x2-x1,Math.abs(py(s.ep)-py(s.sl)));
    // Полоски на границах TP/SL — строго в пределах ширины заливки
    ctx.setLineDash([]);ctx.lineWidth=0.8;
    ctx.strokeStyle=isLong?'rgba(58,125,82,0.45)':'rgba(160,48,48,0.45)';
    ctx.beginPath();ctx.moveTo(x1,py(s.tp));ctx.lineTo(x2,py(s.tp));ctx.stroke();
    ctx.strokeStyle=isLong?'rgba(160,48,48,0.45)':'rgba(58,125,82,0.45)';
    ctx.beginPath();ctx.moveTo(x1,py(s.sl));ctx.lineTo(x2,py(s.sl));ctx.stroke();
    ctx.strokeStyle=isLong?'#A3BF6F':'#FF8234';ctx.lineWidth=1.2;ctx.setLineDash([]);ctx.beginPath();ctx.moveTo(x1,py(s.ep));ctx.lineTo(x2,py(s.ep));ctx.stroke();
  }}
  // TP/SL dashed lines and labels for active open trade — always drawn regardless of viewport
  // Таймер до закрытия свечи
  const _now=Date.now()/1000;
  const _liveC=CANDLES[CANDLES.length-1];
  let _candleTimer='';
  if(_liveC){{
    const _candleEnd=_liveC.t+TF_SEC;
    const _rem=Math.max(0,Math.ceil(_candleEnd-_now));
    const _mm=Math.floor(_rem/60),_ss=_rem%60;
    _candleTimer=_mm+'m '+_ss.toString().padStart(2,'0')+'s';
  }}
  // Антиперекрытие правых лейблов: собираем Y-зоны занятых лейблов
  const _usedRightY=[];
  function _fitRightLabel(y, h){{
    const half=h/2+1;
    for(const [a,b] of _usedRightY){{ if(y+half>a && y-half<b) return false; }}
    _usedRightY.push([y-half, y+half]);
    return true;
  }}
  if(activeSig){{
    const isLong=activeSig.dir===1;
    const tpY=py(activeSig.tp),slY=py(activeSig.sl);
    const aViC=Math.max(0,activeSig.bar_i-viewStart);
    const ax1=PAD_L+aViC*cw, ax2=W-PAD_R;
    ctx.setLineDash([4,3]);
    ctx.strokeStyle=isLong?'#A3BF6F':'#FF8234';ctx.lineWidth=1;
    ctx.beginPath();ctx.moveTo(ax1,tpY);ctx.lineTo(ax2,tpY);ctx.stroke();
    ctx.strokeStyle='#b0a090';
    ctx.beginPath();ctx.moveTo(ax1,slY);ctx.lineTo(ax2,slY);ctx.stroke();
    ctx.setLineDash([]);
    ctx.font='bold 9px system-ui';ctx.textAlign='left';
    // TP label + timer
    const tpLblH=_candleTimer?26:14;
    if(_fitRightLabel(tpY, tpLblH)){{
      ctx.fillStyle=isLong?'rgba(163,191,111,0.9)':'rgba(255,130,52,0.9)';
      ctx.beginPath();ctx.roundRect(W-PAD_R+1,tpY-tpLblH/2,PAD_R-2,tpLblH,3);ctx.fill();
      ctx.fillStyle='#fff';ctx.fillText('TP '+activeSig.tp.toPrecision(5),W-PAD_R+4,tpY-(_candleTimer?5:0)+3);
      if(_candleTimer){{ctx.font='8px system-ui';ctx.fillStyle='rgba(255,255,255,0.75)';ctx.fillText(_candleTimer,W-PAD_R+4,tpY+10);ctx.font='bold 9px system-ui';}}
    }}
    // SL label + timer
    const slLblH=_candleTimer?26:14;
    if(_fitRightLabel(slY, slLblH)){{
      ctx.fillStyle='rgba(140,120,100,0.75)';
      ctx.beginPath();ctx.roundRect(W-PAD_R+1,slY-slLblH/2,PAD_R-2,slLblH,3);ctx.fill();
      ctx.fillStyle='#fff';ctx.fillText('SL '+activeSig.sl.toPrecision(5),W-PAD_R+4,slY-(_candleTimer?5:0)+3);
      if(_candleTimer){{ctx.font='8px system-ui';ctx.fillStyle='rgba(255,255,255,0.75)';ctx.fillText(_candleTimer,W-PAD_R+4,slY+10);ctx.font='bold 9px system-ui';}}
    }}
    ctx.font='10px system-ui';
  }}
  // Current price label — always visible, with anti-overlap
  const lastC=vis[vis.length-1];
  if(lastC){{
    const curPrice=lastC.c,curY=py(curPrice),isUp=lastC.c>=lastC.o;
    const cpCol=isUp?'#A3BF6F':'#FF8234';
    ctx.setLineDash([2,3]);ctx.strokeStyle=cpCol+'80';ctx.lineWidth=1;
    ctx.beginPath();ctx.moveTo(PAD_L,curY);ctx.lineTo(W-PAD_R,curY);ctx.stroke();
    ctx.setLineDash([]);
    const cpLblH=_candleTimer?26:14;
    if(_fitRightLabel(curY, cpLblH)){{
      ctx.fillStyle=cpCol;ctx.font='bold 9px system-ui';ctx.textAlign='left';
      ctx.beginPath();ctx.roundRect(W-PAD_R+1,curY-cpLblH/2,PAD_R-2,cpLblH,3);ctx.fill();
      ctx.fillStyle='#fff';ctx.fillText(curPrice.toPrecision(6),W-PAD_R+4,curY-(_candleTimer?5:0)+3);
      if(_candleTimer){{ctx.font='8px system-ui';ctx.fillStyle='rgba(255,255,255,0.75)';ctx.fillText(_candleTimer,W-PAD_R+4,curY+10);ctx.font='bold 9px system-ui';}}
    }}
  }}
  for(let i=0;i<vis.length;i++){{
    const c=vis[i],x=cx(i),bull=c.c>=c.o,isLive=c.live===true;
    const col=bull?'#A3BF6F':'#FF8234';
    ctx.globalAlpha=isLive?0.55:1.0;
    ctx.strokeStyle=col;ctx.fillStyle=col;ctx.lineWidth=Math.max(1,cw*0.1);
    // Live candle: фитиль пунктирный, тело — сплошное
    if(isLive) ctx.setLineDash([3,2]);
    ctx.beginPath();ctx.moveTo(x,py(c.h));ctx.lineTo(x,py(c.l));ctx.stroke();
    ctx.setLineDash([]); // тело всегда сплошное
    const bTop=py(Math.max(c.o,c.c)),bBot=py(Math.min(c.o,c.c)),bH=Math.max(1,bBot-bTop),bW=Math.max(1,cw-gap*2);
    ctx.fillRect(x-bW/2,bTop,bW,bH);
    ctx.globalAlpha=1.0;
  }}
  // Рисуем сигналы: стрелки и лейблы всегда (антиперекрытие через _labelFits)
  const _showLabels = true;
  // Антиперекрытие лейблов: храним занятые X-зоны
  const _usedLabelX = [];
  function _labelFits(lx, tw) {{
    const half = tw/2 + 5;
    for (const [u0, u1] of _usedLabelX) {{ if (lx+half > u0 && lx-half < u1) return false; }}
    _usedLabelX.push([lx-half, lx+half]);
    return true;
  }}
  for(const s of SIGNALS){{
    const vi=s.bar_i-viewStart;if(vi<0||vi>=vis.length) continue;
    // Не рисуем сигнал если он на live-свече — она ещё не закрыта
    if(s.bar_i===_liveBarGlobal) continue;
    const x=cx(vi),isLong=s.dir===1;
    const c_sig=vis[vi];
    const arrowSz=Math.max(4,Math.min(7,cw*0.45));
    const arrowOff=Math.max(14,Math.min(22,cw*2.2));
    const isOpenEnd=s.open_end===true,isWin=s.win===true;
    ctx.fillStyle=isLong?'#4a7fc1':'#c8902a';
    ctx.strokeStyle=isOpenEnd?'#c0a888':s.win===null?'#c0a888':isWin?'#A3BF6F':'#FF8234';
    ctx.lineWidth=1.5;ctx.beginPath();
    if(isLong){{const ay=py(c_sig.l)+arrowOff;ctx.moveTo(x,ay-arrowSz);ctx.lineTo(x-arrowSz,ay);ctx.lineTo(x+arrowSz,ay);}}
    else{{const ay=py(c_sig.h)-arrowOff;ctx.moveTo(x,ay+arrowSz);ctx.lineTo(x-arrowSz,ay);ctx.lineTo(x+arrowSz,ay);}}
    ctx.closePath();ctx.fill();ctx.stroke();
    if(_showLabels&&!isOpenEnd&&s.exit_bar!==null&&s.win!==null){{
      const exitPrice=s.exit_p??( s.win?s.tp:s.sl);
      const pct=isLong?(exitPrice-s.ep)/s.ep*100:(s.ep-exitPrice)/s.ep*100;
      const lbl=(pct>=0?'+':'')+pct.toFixed(2)+'%';
      const vi_exit=s.exit_bar-viewStart,x_exit=(vi_exit>=0&&vi_exit<vis.length)?cx(vi_exit):x;
      const lx=(x+x_exit)/2;
      const ly=isLong?py(c_sig.l)+arrowOff+arrowSz+16:py(c_sig.h)-arrowOff-arrowSz-16;
      ctx.font=`bold ${{Math.max(9,Math.min(11,cw*1.5))}}px system-ui`;ctx.textAlign='center';
      const tw=ctx.measureText(lbl).width;
      ctx.fillStyle=pct>=0?'rgba(58,125,82,0.9)':'rgba(160,48,48,0.9)';
      ctx.beginPath();ctx.roundRect(lx-tw/2-3,ly-11,tw+6,14,3);ctx.fill();
      ctx.fillStyle='#fff';ctx.fillText(lbl,lx,ly);
    }}
  }}
  ctx.fillStyle=clrTimeText;ctx.font='10px system-ui';ctx.textAlign='center';
  const step=Math.max(1,Math.floor(vis.length/8));
  const isMobile=W<500;
  const mskOffset=3*3600*1000;
  for(let i=0;i<vis.length;i+=step){{
    const t=new Date((vis[i].t+TF_SEC)*1000+mskOffset);
    let lbl;
    if(isMobile){{
      lbl=t.getUTCHours().toString().padStart(2,'0')+':'+t.getUTCMinutes().toString().padStart(2,'0');
    }}else{{
      lbl=(t.getUTCMonth()+1)+'/'+t.getUTCDate()+' '+t.getUTCHours().toString().padStart(2,'0')+':'+t.getUTCMinutes().toString().padStart(2,'0');
    }}
    const lx=cx(i);
    const hw=ctx.measureText(lbl).width/2;
    if(lx+hw>W-PAD_R) continue; // skip labels that would overflow into price scale
    ctx.fillText(lbl,lx,H-PAD_B+16);
  }}
}}
wrap.addEventListener('wheel',e=>{{e.preventDefault();const rect=wrap.getBoundingClientRect(),ox=e.clientX-rect.left;const delta=e.deltaY>0?1.18:0.84,ratio=(ox-6)/wrap.clientWidth,pivot=viewStart+ratio*viewLen;viewLen=Math.max(15,Math.min(CANDLES.length,Math.round(viewLen*delta)));viewStart=Math.max(0,Math.min(CANDLES.length-viewLen,Math.round(pivot-ratio*viewLen)));render();}},{{passive:false}});
wrap.addEventListener('mousedown',e=>{{isDragging=true;dragX=e.clientX;dragVS=viewStart;}});
window.addEventListener('mousemove',e=>{{if(!isDragging)return;const cw2=wrap.clientWidth/viewLen,dx=Math.round((e.clientX-dragX)/cw2);viewStart=Math.max(0,Math.min(CANDLES.length-viewLen,dragVS-dx));render();}});
window.addEventListener('mouseup',()=>isDragging=false);
// Touch support
let _t1x=0,_t1VS=0,_tPinchD=0,_tPinchVL=0,_tPinchVS=0;
wrap.addEventListener('touchstart',e=>{{
  e.preventDefault();
  if(e.touches.length===1){{_t1x=e.touches[0].clientX;_t1VS=viewStart;}}
  else if(e.touches.length===2){{
    const dx=e.touches[0].clientX-e.touches[1].clientX,dy=e.touches[0].clientY-e.touches[1].clientY;
    _tPinchD=Math.sqrt(dx*dx+dy*dy);_tPinchVL=viewLen;_tPinchVS=viewStart;
  }}
}},{{passive:false}});
wrap.addEventListener('touchmove',e=>{{
  e.preventDefault();
  if(e.touches.length===1){{
    const cw2=wrap.clientWidth/viewLen,dx=Math.round((e.touches[0].clientX-_t1x)/cw2);
    viewStart=Math.max(0,Math.min(CANDLES.length-viewLen,_t1VS-dx));render();
  }} else if(e.touches.length===2){{
    const dx=e.touches[0].clientX-e.touches[1].clientX,dy=e.touches[0].clientY-e.touches[1].clientY;
    const d=Math.sqrt(dx*dx+dy*dy),scale=_tPinchD/d;
    viewLen=Math.max(15,Math.min(CANDLES.length,Math.round(_tPinchVL*scale)));
    viewStart=Math.max(0,Math.min(CANDLES.length-viewLen,_tPinchVS));render();
  }}
}},{{passive:false}});
wrap.addEventListener('touchend',e=>{{e.preventDefault();}},{{passive:false}});
const tip=document.getElementById('tooltip');
const PAD_L_C=6,PAD_R_C=72;
wrap.addEventListener('mousemove',e=>{{
  if(_touchActive)return;  // планшет с тачем — не мешаем
  const rect=wrap.getBoundingClientRect(),offsetX=e.clientX-rect.left;
  _showTipAt(offsetX, e.offsetY);
  // Позиция справа от курсора (стандартное поведение мыши)
  if(tip.style.display==='block'){{
    const W=wrap.clientWidth,tx=offsetX+14;
    tip.style.left=(tx+tip.offsetWidth>W?tx-tip.offsetWidth-20:tx)+'px';
    tip.style.top=Math.max(0,e.offsetY-10)+'px';
  }}
}});
wrap.addEventListener('mouseleave',()=>{{if(!_touchActive)tip.style.display='none';}});
// Touch tooltip для мобильных/планшетов
let _touchActive=false,_tipHideTimer=null;
function _showTipAt(offsetX,offsetY){{
  const W=wrap.clientWidth,drawW=W-PAD_L_C-PAD_R_C;
  if(offsetX>=W-PAD_R_C){{tip.style.display='none';return;}}
  const vis=CANDLES.slice(viewStart,viewStart+Math.min(viewLen,CANDLES.length-viewStart)),cw2=drawW/vis.length;
  const i=Math.min(vis.length-1,Math.max(0,Math.floor((offsetX-PAD_L_C)/cw2)));
  if(i<0||i>=vis.length){{tip.style.display='none';return;}}
  const c=vis[i],gi=viewStart+i,sig=SIGNALS.find(s=>s.bar_i===gi);
  const mskMs=(c.t+TF_SEC)*1000+3*3600*1000,d=new Date(mskMs);
  const dt=d.getUTCDate().toString().padStart(2,'0')+'.'+(d.getUTCMonth()+1).toString().padStart(2,'0')+'.'+d.getUTCFullYear()+' '+d.getUTCHours().toString().padStart(2,'0')+':'+d.getUTCMinutes().toString().padStart(2,'0')+' МСК';
  let html=`<b>${{dt}}</b><br>O ${{c.o.toPrecision(6)}} H ${{c.h.toPrecision(6)}}<br>L ${{c.l.toPrecision(6)}} C ${{c.c.toPrecision(6)}}`;
  if(sig){{const dir=sig.dir===1?'🔵 Лонг':'🟡 Шорт',res=sig.open_end?'⛔ не закрыт':sig.win?'✅ TP':'❌ SL';html+=`<br><br>${{dir}} ${{res}}<br>Вход ${{sig.ep.toPrecision(6)}}<br>TP ${{sig.tp.toPrecision(6)}}<br>SL ${{sig.sl.toPrecision(6)}}`;}}
  tip.innerHTML=html;tip.style.display='block';
  const W2=wrap.clientWidth;
  const ty=Math.max(0,offsetY-tip.offsetHeight-16);
  const tx=Math.min(W2-tip.offsetWidth-4,Math.max(4,offsetX-tip.offsetWidth/2));
  tip.style.left=tx+'px';tip.style.top=ty+'px';
}}
wrap.addEventListener('touchstart',()=>{{
  _touchActive=true;
  if(_tipHideTimer){{clearTimeout(_tipHideTimer);_tipHideTimer=null;}}
}},{{passive:true}});
wrap.addEventListener('touchmove',e=>{{
  if(e.touches.length!==1){{tip.style.display='none';return;}}
  const rect=wrap.getBoundingClientRect();
  _showTipAt(e.touches[0].clientX-rect.left, e.touches[0].clientY-rect.top);
}},{{passive:true}});
wrap.addEventListener('touchend',()=>{{
  _tipHideTimer=setTimeout(()=>{{tip.style.display='none';_touchActive=false;}},1200);
}},{{passive:true}});
window.addEventListener('resize',render);
// ── Live candle: обновляем незакрытую свечу каждые 2 секунды ──
const LIVE_SYMBOL = '{symbol}';
const LIVE_TF     = '{tf}';
let _liveFailCount = 0;
let _liveTimer = null;

function fetchLiveCandle() {{
  // Отменяем предыдущий таймер и ставим новый — защита от накопления
  if (_liveTimer) clearTimeout(_liveTimer);
  _liveTimer = setTimeout(fetchLiveCandle, 2000);

  const url = '/live_candle?symbol=' + encodeURIComponent(LIVE_SYMBOL) + '&tf=' + encodeURIComponent(LIVE_TF) + '&_=' + Date.now();
  fetch(url)
    .then(r => r.json())
    .then(d => {{
      _liveFailCount = 0;
      if (!d.ok) return;
      const last = CANDLES[CANDLES.length - 1];
      // Был ли пользователь у правого края ДО изменений
      const wasAtEnd = (viewStart + viewLen >= CANDLES.length - 1);
      if (last && last.live) {{
        if (d.t === last.t) {{
          // Та же свеча — обновляем HLC, open не трогаем (чтобы не было разрыва)
          last.h = Math.max(last.h, d.h); last.l = Math.min(last.l, d.l); last.c = d.c;
        }} else if (d.t > last.t) {{
          // Новый интервал: закрываем старую live, добавляем новую.
          // viewStart сдвигаем ровно на 1 — чтобы viewLen остался постоянным и не было разрыва.
          delete last.live;
          CANDLES.push({{t:d.t, o:d.o, h:d.h, l:d.l, c:d.c, live:true}});
          if (wasAtEnd) viewStart = Math.max(0, CANDLES.length - viewLen);
        }}
      }} else {{
        // Первое появление live-свечи
        if (last && !last.live && d.t === last.t) {{
          // t совпадает с последней закрытой — помечаем её live, обновляем HLC
          // open НЕ трогаем — он должен совпадать с close предыдущей свечи (нет разрыва)
          last.live = true;
          last.h = Math.max(last.h, d.h); last.l = Math.min(last.l, d.l); last.c = d.c;
          // длина не изменилась — viewport не трогаем
        }} else if (last && !last.live && d.t > last.t) {{
          // t больше — новая live-свеча сверх закрытых
          CANDLES.push({{t:d.t, o:d.o, h:d.h, l:d.l, c:d.c, live:true}});
          if (wasAtEnd) viewStart = Math.max(0, CANDLES.length - viewLen);
        }}
      }}
      // Badge
      const badge = document.getElementById('liveBadge');
      if (badge) {{
        const stale = d.age !== undefined && d.age > 10;
        badge.style.color = stale ? '#e09030' : '';
        badge.textContent = (stale ? '⚠ ' : '⬤ ') + 'LIVE  ' + d.c.toPrecision(7);
      }}
      // Всегда перерисовываем через rAF — даже если changed=false (для badge)
      requestAnimationFrame(render);
    }}).catch(e => {{
      _liveFailCount++;
      console.warn('[live_candle] err #' + _liveFailCount, e);
    }});
}}

// Старт
fetchLiveCandle();

// Полный перезапрос страницы раз в 5 минут — только если пользователь у правого края
setTimeout(() => {{ if (viewStart + viewLen >= CANDLES.length - 2) location.reload(); }}, 300000);

// postMessage — обновляем CANDLES/SIGNALS без перезагрузки страницы
window.addEventListener('message', e => {{
  if (!e.data || e.data.type !== 'chart_update') return;
  const wasAtEnd = (viewStart + viewLen >= CANDLES.length - 1);
  const oldLen = CANDLES.length;
  // Сохраняем текущую live-свечу чтобы не было моргания при замене массива
  const prevLive = CANDLES.length > 0 && CANDLES[CANDLES.length-1].live
    ? {{...CANDLES[CANDLES.length-1]}} : null;
  CANDLES.length = 0; e.data.candles.forEach(c => CANDLES.push(c));
  SIGNALS.length = 0; e.data.signals.forEach(s => SIGNALS.push(s));
  // Если последняя свеча в новых данных — закрытая, а у нас была live актуальная — добавляем обратно
  if (prevLive && CANDLES.length > 0 && !CANDLES[CANDLES.length-1].live) {{
    const lastT = CANDLES[CANDLES.length-1].t;
    if (prevLive.t >= lastT) {{
      if (prevLive.t === lastT) CANDLES.pop();
      CANDLES.push(prevLive);
    }}
  }}
  // Сдвигаем viewStart только если были у правого края; viewLen не трогаем
  if (wasAtEnd) {{
    viewStart = Math.max(0, CANDLES.length - viewLen);
  }}
  render();
}});

render();
</script></body></html>"""

def _save_chart(candles, signals, best_result, symbol, tf, risk_pct_ui=20.0):
    # Локальное сохранение файла отключено — график доступен через /chart
    return None

# ═══════════════════════════════════════════════════════════════
# CHECK SIGNAL ON LAST CANDLE & SEND EMAIL
# ═══════════════════════════════════════════════════════════════
def _check_trade_close(prev_signals, new_signals, alert_cfg, symbol, tf):
    """Находит сделки, которые только что закрылись, и шлёт Telegram-уведомление."""
    if not alert_cfg or not prev_signals or not new_signals:
        return
    # Карта открытых позиций из предыдущего прогона (exit_bar == None или open_end)
    prev_open = {s["bar_i"]: s for s in prev_signals
                 if s.get("exit_bar") is None or s.get("open_end")}
    if not prev_open:
        return
    moscow_offset = 3 * 3600
    for s in new_signals:
        bar_i = s["bar_i"]
        if bar_i not in prev_open:
            continue
        # Была открытой — теперь закрыта?
        if s.get("exit_bar") is not None and not s.get("open_end"):
            is_win   = s.get("win", False)
            exit_p   = s.get("exit_p") or (s["tp"] if is_win else s["sl"])
            is_long  = s["dir"] == 1
            pct      = ((exit_p - s["ep"]) / s["ep"] * 100 if is_long
                        else (s["ep"] - exit_p) / s["ep"] * 100)
            dir_str  = "🔵 ЛОНГ" if is_long else "🟡 ШОРТ"
            res_str  = "✅ Тейк-профит" if is_win else "❌ Стоп-лосс"
            pct_str  = ("+" if pct >= 0 else "") + f"{pct:.2f}%"
            # Берём время закрытия свечи входа (t свечи + интервал таймфрейма), а не time.time()
            exit_candle_t = s.get("t", int(time.time())) + TF_SECONDS.get(tf, 3600)
            dt = time.strftime("%Y-%m-%d %H:%M", time.gmtime(exit_candle_t + moscow_offset))
            text = (
                f"🔔 <b>WickFill — Сделка закрыта</b>\n\n"

                f"{dir_str} <b>{symbol}</b> {tf}\n"
                f"{res_str}  <b>{pct_str}</b>\n\n"

                f"📥 Вход:   <b>{s['ep']:.6g}</b>\n"
                f"📤 Выход:  <b>{exit_p:.6g}</b>\n"
                f"🕐 {dt} (МСК)"
            )
            ok = _send_alert(alert_cfg, text)
            status = "✓" if ok else "✕"
            print(f"[trade_close] {status} {symbol} {tf} {'ЛОНГ' if is_long else 'ШОРТ'} {pct_str} {res_str}", flush=True)

def _check_new_candle_signal(candles, best_params, risk_pct, alert_cfg, symbol=None, tf=None, precomp_signals=None):
    """Проверяет последнюю свечу. Если сигнал — шлёт telegram + открывает сделку."""
    if not best_params or not alert_cfg: return
    if len(candles) < 5: return

    with opt_lock:
        if symbol is None:
            symbol = opt_state.get("chart_symbol", "?")
        if tf is None:
            tf = opt_state.get("chart_tf", "?")
        last_signal_t = opt_state.get("last_signal_t", 0)

    if symbol and symbol in opt_states:
        with opt_states_lock:
            last_signal_t = opt_states[symbol].get("last_signal_t", last_signal_t)

    # Используем уже готовые сигналы если переданы, иначе считаем
    if precomp_signals is not None:
        sigs = precomp_signals
    else:
        sim = _simulate(candles, best_params, 0, _collect=True, risk_pct=risk_pct)
        if not sim or not sim["_signals"]: return
        sigs = sim["_signals"]

    if not sigs: return

    # Ищем сигнал по времени последней свечи — bar_i из разных _simulate может не совпадать
    # (SW-окно и окно оптимизатора могут быть разной длины)
    last_candle_t = candles[-1]["t"]
    nb = bool(best_params.get("use_next_bar", False))

    for s in sigs:
        candle_t = s.get("t") or 0
        if candle_t != last_candle_t:
            continue  # не последняя свеча
        if candle_t <= last_signal_t:
            continue  # уже отправляли
        ep = s["ep"]; tp = s["tp"]; sl = s["sl"]; direction = s["dir"]
        # Читаем плечо из лучшего конфига
        with opt_lock:
            _best_for_lev = opt_state.get("all_time_best") or opt_state.get("best") or {}
        _sig_leverage = (_best_for_lev.get("leverage") or
                         (_best_for_lev.get("params") or {}).get("leverage") or 1)
        if symbol and symbol in opt_states:
            with opt_states_lock:
                _sym_best = opt_states[symbol].get("best") or {}
            _sig_leverage = (_sym_best.get("leverage") or
                             (_sym_best.get("params") or {}).get("leverage") or _sig_leverage)
        # 1. Телеграм/ntfy уведомление
        tg_ok = _send_signal_email(alert_cfg, symbol, tf, direction, ep, tp, sl, candle_t,
                                    leverage=_sig_leverage)
        # Сохраняем сигнал
        with opt_lock:
            opt_state["last_signal_t"] = candle_t
        if symbol and symbol in opt_states:
            with opt_states_lock:
                if symbol in opt_states:
                    opt_states[symbol]["last_signal_t"] = candle_t
        with alert_lock:
            alert_state["sent"] += 1
            alert_state["signals"].insert(0, {
                "symbol": symbol, "tf": tf, "dir": direction,
                "ep": ep, "tp": tp, "sl": sl, "t": candle_t,
                "ts": time.strftime("%H:%M:%S", time.gmtime(candle_t + TF_SECONDS.get(tf, 3600) + 3*3600))
            })
            alert_state["signals"] = alert_state["signals"][:50]
        print(f"[alert] Сигнал: {symbol} {tf} {'ЛОНГ' if direction==1 else 'ШОРТ'} ep={ep:.6g} tg={'✓' if tg_ok else '✕'}")
        # Диагностика: показываем последнюю свечу чтобы понять откуда взялось направление
        try:
            _dc = candles[-1]
            _dhi = _dc["high"]; _dlo = _dc["low"]; _dop = _dc["open"]; _dcl = _dc["close"]
            _drng = _dhi - _dlo
            _dup_w = _dhi - max(_dop, _dcl); _ddn_w = min(_dop, _dcl) - _dlo
            _dup_pct = _dup_w / _drng * 100 if _drng > 0 else 0
            _ddn_pct = _ddn_w / _drng * 100 if _drng > 0 else 0
            _wd = best_params.get("wick_dir", "?")
            _nb = best_params.get("use_next_bar", False)
            print(f"[alert] candle: O={_dop:.4g} H={_dhi:.4g} L={_dlo:.4g} C={_dcl:.4g} "
                  f"up_wick={_dup_pct:.1f}% dn_wick={_ddn_pct:.1f}% wick_dir={_wd} nb={_nb}")
        except Exception:
            pass

        # 2. Автоторговля Gate.io
        gate_key    = alert_cfg.get("gate_key", "")
        gate_secret = alert_cfg.get("gate_secret", "")
        gate_pct    = float(alert_cfg.get("gate_pct", 0))
        gate_auto_on = alert_cfg.get("gate_auto_enabled", False)
        if gate_key and gate_secret and gate_pct > 0 and gate_auto_on:
            with opt_lock:
                best = opt_state.get("all_time_best") or opt_state.get("best") or {}
            leverage = best.get("leverage", 1) or 1
            auto_tp_pct = float(alert_cfg.get("gate_auto_tp_pct", 0))
            auto_sl_pct = float(alert_cfg.get("gate_auto_sl_pct", 0))
            trade_tp = round(ep * (1 + auto_tp_pct/100) if direction==1 else ep * (1 - auto_tp_pct/100), 6) if auto_tp_pct > 0 else tp
            trade_sl = round(ep * (1 - auto_sl_pct/100) if direction==1 else ep * (1 + auto_sl_pct/100), 6) if auto_sl_pct > 0 else sl
            ok_trade, trade_log = _gate_execute_signal(
                alert_cfg, symbol, direction, ep, trade_tp, trade_sl, leverage, gate_pct
            )
            status = "✓" if ok_trade else "✕"
            print(f"[gate] {status} {symbol} {'ЛОНГ' if direction==1 else 'ШОРТ'}: {trade_log}", flush=True)
            with opt_lock:
                opt_state.setdefault("logs", []).append({
                    "ts": time.strftime("%H:%M:%S"),
                    "msg": f"[gate] {status} {symbol} {'ЛОНГ' if direction==1 else 'ШОРТ'} × {int(leverage)} — {trade_log.splitlines()[-1]}",
                    "level": "ok" if ok_trade else "error"
                })
        break

# ═══════════════════════════════════════════════════════════════
# SLIDING WINDOW THREAD — обновляет свечи каждые N секунд
# ═══════════════════════════════════════════════════════════════
_sw_candles = []   # общий список свечей (защищён opt_lock)
_sw_params  = {}
_sw_cfg     = {}   # email cfg
_sw_risk    = 20.0

# Per-symbol SW state для мультирежима
_sw_state      = {}  # {symbol: {"candles":[], "params":{}, "risk":20, "running":False, "opt_states_ref": None}}
_sw_state_lock = threading.Lock()
_sw_threads    = {}  # {symbol: Thread}

def _try_slide_window(symbol, tf, olog):
    """Проверяет появление новой закрытой свечи и бесшовно сдвигает окно.
    Вызывается между циклами оптимизатора. Не блокирует, не ждёт."""
    global _sw_candles
    try:
        new_c = _fetch_latest_candle(symbol, tf)
        if not new_c:
            return False
        with opt_lock:
            cur = list(_sw_candles)
        if not cur or new_c["t"] <= cur[-1]["t"]:
            return False  # свеча та же — ничего не делаем
        new_candles = cur[1:] + [new_c]
        with opt_lock:
            _sw_candles = new_candles
        olog(f"🕯 Новая свеча t={new_c['t']} c={new_c['close']:.6g} — окно сдвинуто", "info")
        return True
    except Exception as e:
        print(f"[slide] Ошибка: {e}")
        return False


def _sliding_window_thread(symbol, tf, n_candles, alert_cfg, risk_pct):
    """Каждый TF-интервал: загружает последнюю закрытую свечу, добавляет, убирает первую.
    В мультирежиме использует _sw_state[symbol], в одиночном — глобальный opt_state."""
    global _sw_candles, _sw_params, _sw_risk
    interval_sec = TF_SECONDS.get(tf, 3600)
    is_multi = symbol in _sw_state  # мультирежим если символ зарегистрирован в _sw_state

    print(f"[sw:{symbol}] Запущен. ТФ={tf} окно={n_candles} интервал={interval_sec}с multi={is_multi}")

    def _get_running():
        if is_multi:
            with _sw_state_lock:
                return _sw_state.get(symbol, {}).get("running", False)
        else:
            with opt_lock:
                return opt_state["sw_running"]

    def _set_running(val):
        if is_multi:
            with _sw_state_lock:
                if symbol in _sw_state:
                    _sw_state[symbol]["running"] = val
        else:
            with opt_lock:
                opt_state["sw_running"] = val

    def _get_candles_params():
        if is_multi:
            with _sw_state_lock:
                s = _sw_state.get(symbol, {})
                return list(s.get("candles") or []), dict(s.get("params") or {}) or None
        else:
            with opt_lock:
                return list(_sw_candles), dict(_sw_params) if _sw_params else None

    def _set_candles(new_c):
        if is_multi:
            with _sw_state_lock:
                if symbol in _sw_state:
                    _sw_state[symbol]["candles"] = new_c
        else:
            global _sw_candles
            with opt_lock:
                _sw_candles = new_c

    def _update_chart(chart_candles_fmt, chart_signals_data, br, chart_path_val):
        """Обновляет chart в opt_state (всегда) и в opt_states[symbol] (для мультирежима)."""
        ts = int(time.time())
        with opt_lock:
            # Обновляем глобальный opt_state только если это активный символ
            if not is_multi or symbol == _active_chart_symbol:
                opt_state["chart_candles"]   = chart_candles_fmt
                opt_state["chart_signals"]   = chart_signals_data
                opt_state["chart_updated_at"] = ts
                if chart_path_val:
                    opt_state["chart_path"] = chart_path_val
        if is_multi:
            with opt_states_lock:
                s = opt_states.get(symbol, {})
                s["chart_candles"]    = chart_candles_fmt
                s["chart_signals"]    = chart_signals_data
                s["chart_updated_at"] = ts
                if chart_path_val:
                    s["chart_path"] = chart_path_val

    _set_running(True)

    # Синхронизируем окно со свежими данными при старте
    days_needed = max(1, round(n_candles * interval_sec / 86400) + 1)
    print(f"[sw:{symbol}] Синхронизация свежих свечей ({days_needed}д)...")
    fresh = _fetch_candles(symbol, tf, days_needed)
    if fresh and len(fresh) >= n_candles:
        _set_candles(fresh[-n_candles:])
        candles_for_log, _ = _get_candles_params()
        print(f"[sw:{symbol}] Синхронизировано: {len(candles_for_log)} свечей")
    else:
        print(f"[sw:{symbol}] Синхронизация не удалась, используем старые данные")

    while True:
        if not _get_running(): break

        now = int(time.time())
        next_close = ((now // interval_sec) + 1) * interval_sec
        wait_sec = next_close - now
        sleep_total = wait_sec + 15  # +15с — биржа иногда задерживает финализацию свечи
        print(f"[sw:{symbol}] Следующая свеча через {sleep_total}с")

        for _ in range(sleep_total):
            if not _get_running(): break
            time.sleep(1)

        if not _get_running(): break

        # Retry: биржа может задержать свечу — пробуем до 5 раз с интервалом 5с
        new_c = None
        for _retry in range(5):
            _cand = _fetch_latest_candle(symbol, tf)
            _cur_candles, _ = _get_candles_params()
            if _cand and _cur_candles and _cand["t"] > _cur_candles[-1]["t"]:
                new_c = _cand
                break
            print(f"[sw:{symbol}] Свеча ещё не финализирована, retry {_retry+1}/5...")
            time.sleep(5)
        if new_c is None:
            new_c = _fetch_latest_candle(symbol, tf)

        if not new_c:
            print(f"[sw:{symbol}] Не удалось загрузить новую свечу"); continue

        candles, best_p = _get_candles_params()

        if not candles:
            print(f"[sw:{symbol}] Свечи ещё не загружены"); continue

        if new_c["t"] <= candles[-1]["t"]:
            print(f"[sw:{symbol}] Свеча t={new_c['t']} уже есть, пропускаем"); continue

        new_candles = candles[1:] + [new_c]

        if best_p:
            sim = _simulate(new_candles, best_p, 0, _collect=True, risk_pct=risk_pct)
            chart_signals_data = sim["_signals"] if sim else []
            chart_candles_fmt = [{"t":c["t"],"o":c["open"],"h":c["high"],"l":c["low"],"c":c["close"]} for c in new_candles]
            cur_c2 = _fetch_current_candle(symbol, tf)
            if cur_c2 and cur_c2["t"] > new_candles[-1]["t"]:
                chart_candles_fmt = chart_candles_fmt + [{"t":cur_c2["t"],"o":cur_c2["open"],"h":cur_c2["high"],"l":cur_c2["low"],"c":cur_c2["close"],"live":True}]

            # prev_signals для проверки закрытия сделки
            prev_signals_for_close = []
            with opt_lock:
                if not is_multi or symbol == _active_chart_symbol:
                    prev_signals_for_close = list(opt_state.get("chart_signals") or [])
            if is_multi:
                with opt_states_lock:
                    prev_signals_for_close = list(opt_states.get(symbol, {}).get("chart_signals") or []) or prev_signals_for_close

            _set_candles(new_candles)

            br = {}
            if is_multi:
                with opt_states_lock:
                    br = dict(opt_states.get(symbol, {}).get("best") or {})
            else:
                with opt_lock:
                    br = dict(opt_state.get("best") or {})

            chart_path_val = _save_chart(chart_candles_fmt, chart_signals_data, br or {"params":best_p,"equity":100,"winrate":0,"max_dd":0,"profit_factor":0,"trades":0}, symbol, tf, risk_pct)
            _update_chart(chart_candles_fmt, chart_signals_data, br, chart_path_val)

            with opt_lock:
                opt_state["sw_last_update"]  = int(time.time())
                opt_state["sw_candle_count"] = len(new_candles)

            print(f"[sw:{symbol}] Свеча добавлена t={new_c['t']} c={new_c['close']:.4g}")

            # Читаем alert_cfg динамически — пользователь мог заполнить поля после старта
            _live_alert_cfg = None
            with opt_lock:
                _live_alert_cfg = opt_state.get("alert_cfg") or alert_cfg
            if is_multi:
                with opt_states_lock:
                    _live_alert_cfg = opt_states.get(symbol, {}).get("alert_cfg") or _live_alert_cfg
            if _live_alert_cfg and prev_signals_for_close:
                _check_trade_close(prev_signals_for_close, chart_signals_data, _live_alert_cfg, symbol, tf)
            if _live_alert_cfg:
                # Берём сигналы из opt_state — то что видит JS на графике
                # SW-тред и основной оптимизатор могут иметь разные параметры
                _alert_sigs = None
                if is_multi:
                    with opt_states_lock:
                        _alert_sigs = list(opt_states.get(symbol, {}).get("chart_signals") or [])
                if not _alert_sigs:
                    with opt_lock:
                        _alert_sigs = list(opt_state.get("chart_signals") or [])
                # Fallback на SW-сигналы если opt_state ещё не обновился
                if not _alert_sigs:
                    _alert_sigs = chart_signals_data
                _check_new_candle_signal(new_candles, best_p, risk_pct, _live_alert_cfg, symbol=symbol, tf=tf, precomp_signals=_alert_sigs)
        else:
            _set_candles(new_candles)

    _set_running(False)
    print(f"[sw:{symbol}] Остановлен")

# ═══════════════════════════════════════════════════════════════
# OPTIMIZER MAIN LOOP
# ═══════════════════════════════════════════════════════════════
_opt_stop_flag = threading.Event()
_opt_thread = None
_last_fetch_error = None
_eco_mode = False  # режим экономии: 1 воркер + задержки между итерациями

# ═══════════════════════════════════════════════════════════════
# PERF LOG — замеры для диагностики торможения
# ═══════════════════════════════════════════════════════════════
_perf_log = []          # список dict-записей
_perf_lock = threading.Lock()
_perf_t0   = 0.0       # время старта сессии

def _plog(event, **kw):
    """Добавить запись в perf-лог. Потокобезопасно."""
    entry = {"t": round(time.time() - _perf_t0, 3), "ev": event}
    entry.update(kw)
    with _perf_lock:
        _perf_log.append(entry)

def _perf_save(symbol, tf):
    """Сохранить perf-лог в файл рядом с конфигами."""
    with _perf_lock:
        data = list(_perf_log)
    if not data:
        return
    ts = time.strftime("%Y%m%d_%H%M%S")
    sym = symbol.replace("_","").replace("/","").lower()
    fname = f"wickfill_perf_{sym}_{tf}.txt"
    lines = [f"WickFill perf-log  symbol={symbol}  tf={tf}  saved={time.strftime('%Y-%m-%d %H:%M:%S')}\n",
             f"{'время':>8}  {'событие':<28}  детали\n",
             "-"*80 + "\n"]
    prev_t = 0.0
    for e in data:
        t = e["t"]; dt = t - prev_t; prev_t = t
        ev = e["ev"]
        details = "  ".join(f"{k}={v}" for k,v in e.items() if k not in ("t","ev"))
        flag = ""
        # Помечаем записи где прошло много времени
        if dt > 5:   flag = f"  ⚠ +{dt:.1f}s"
        if dt > 30:  flag = f"  🔴 +{dt:.1f}s ЗАТЫК"
        lines.append(f"{t:>8.1f}s  {ev:<28}  {details}{flag}\n")
    txt = "".join(lines)
    # 1. GitHub first (logs/)
    gh_path = f"logs/{fname}"
    try:
        gh_ok = _gh_put_file(gh_path, txt, f"perf-log: {fname}")
        if gh_ok:
            print(f"[perf] ✅ Загружен на GitHub: {gh_path}", flush=True)
            return
    except Exception as e:
        print(f"[perf] ⚠ GitHub ошибка: {type(e).__name__}: {e}", flush=True)

    # 2. Локальный фолбек + очередь
    saved = False
    for d in _AUTO_DIRS:
        if not os.path.isdir(d): continue
        try:
            fpath = os.path.join(d, fname)
            with open(fpath, "w", encoding="utf-8") as f:
                f.write(txt)
            print(f"[perf] Сохранён локально: {fpath}", flush=True)
            _gh_enqueue(fpath, gh_path, txt, f"perf-log: {fname}")
            saved = True
            break
        except Exception as e:
            print(f"[perf] Ошибка записи {d}: {e}", flush=True)
    if not saved:
        print(f"[perf] Не удалось сохранить лог\n{txt[:2000]}", flush=True)

def _run_one_cycle(candles, days, risk_pct, olog, t0, tf="1h", n_restarts=8,
                   prev_best_params=None, prev_top20=None, pool=None, n_workers=1,
                   shake=False):
    """Запускает один полный цикл оптимизации. Возвращает (final_result, final_params, top20)."""
    global _sw_params

    # Для таймфреймов < 1h ограничиваем максимальный TP до 1.2%
    _small_tf = TF_SECONDS.get(tf, 3600) < 3600
    _grids_local = dict(_GRIDS)
    if _small_tf:
        _grids_local["tp_pct"] = [v for v in _GRIDS["tp_pct"] if v <= 1.2]

    def pmap(candidates):
        if not candidates:
            return []
        chunk = max(1, len(candidates) // (n_workers * 2))
        _pt0 = time.time()
        result = list(pool.map(_worker_evaluate, candidates, chunksize=chunk))
        _dt = round(time.time() - _pt0, 3)
        _plog("pmap", n=len(candidates), workers=n_workers, sec=_dt,
              sec_per_cand=round(_dt/len(candidates),4) if candidates else 0)
        return result

    def stop_flag():
        return _opt_stop_flag.is_set()

    def _rand_ind():
        ind = {}
        for k, spec in PARAM_SPACE.items():
            if spec["type"] in ("bool", "cat"): ind[k] = random.choice(spec["values"])
            else: ind[k] = random.choice(_grids_local[k])
        return ind

    def _clamp_tp(ind):
        if not ind: return ind
        ind = dict(ind)
        # Зажимаем tp_pct для малых TF
        if _small_tf and ind.get("tp_pct", 0) > 1.2:
            ind["tp_pct"] = 1.2
        # Зажимаем sl_pct и tp_pct к текущим границам UI (sl_min/max, tp_min/max)
        sl_lo = PARAM_SPACE["sl_pct"]["min"]; sl_hi = PARAM_SPACE["sl_pct"]["max"]
        tp_lo = PARAM_SPACE["tp_pct"]["min"]; tp_hi = PARAM_SPACE["tp_pct"]["max"]
        if "sl_pct" in ind:
            ind["sl_pct"] = max(sl_lo, min(sl_hi, ind["sl_pct"]))
        if "tp_pct" in ind:
            ind["tp_pct"] = max(tp_lo, min(tp_hi, ind["tp_pct"]))
        return ind

    def _clamp_result(r):
        """Обрезает tp_pct в result-объекте (params + пересчёт не нужен — просто обрезаем сетку)."""
        if not _small_tf or not r: return r
        if r.get("params", {}).get("tp_pct", 0) <= 1.2: return r
        r2 = dict(r); r2["params"] = dict(r["params"]); r2["params"]["tp_pct"] = 1.2
        return r2

    top20_global = [_clamp_result(r) for r in prev_top20] if prev_top20 else []

    # «Пол» — загруженный seed: никогда не показываем результат хуже него в UI
    _seed_floor = top20_global[0] if top20_global else None
    _seed_floor_fit = (_seed_floor.get("validated_fitness") or _seed_floor["fitness"]) if _seed_floor else -1e18

    # ── При встряске расширяем BH ────────────────────────────────────────────
    # shake=True: больше итераций, шире шаг мутации, обязательно рескрамблим
    # bool/cat и stop/tp в стартовых точках чтобы дать шанс "заблокированным" параметрам
    _BH_MAX_eff     = 20 if shake else 12
    _BH_PATIENCE_eff = 8 if shake else 4
    _BH_STEP_FRAC   = 0.6 if shake else 0.25   # max шаг = fraction * len(grid)
    _PERTURB_FRAC   = 0.55 if shake else 0.35  # доля ключей под perturbation

    _SHAKE_KEYS_BOOL_CAT = [k for k, s in PARAM_SPACE.items() if s["type"] in ("bool", "cat")]
    _SHAKE_KEYS_NUMERIC  = ["stop_pct", "tp_pct"]  # параметры с сильным lock-in эффектом

    def _shake_individual(ind):
        """Форсированно рескрамблим stop/tp + все bool/cat; остальное не трогаем."""
        ind2 = dict(ind)
        for k in _SHAKE_KEYS_BOOL_CAT + _SHAKE_KEYS_NUMERIC:
            if k not in PARAM_SPACE: continue
            spec = PARAM_SPACE[k]; grid = _grids_local[k]
            ind2[k] = random.choice(grid)  # random.choice работает и для bool/cat и для числовых
        return ind2
    # ────────────────────────────────────────────────────────────────────────

    # Фаза 1: многоточечный старт
    if prev_best_params:
        if shake:
            # При встряске: лучший как база, но с рескрамблем stop/tp/bool/cat,
            # плюс увеличиваем число рестартов на 2 чтобы шире покрыть пространство
            shaken_base = _shake_individual(_clamp_tp(prev_best_params))
            start_points = [_clamp_tp(prev_best_params), shaken_base] + [_rand_ind() for _ in range(n_restarts - 1)]
            olog(f"━━ ФАЗА 1 [ВСТРЯСКА]: база + shake(stop/tp/bool) + {n_restarts-1} случайных ━", "ok")
        else:
            start_points = [_clamp_tp(prev_best_params)] + [_rand_ind() for _ in range(n_restarts - 1)]
            olog(f"━━ ФАЗА 1: лучший предыдущего цикла + {n_restarts-1} случайных ━", "ok")
    else:
        start_points = [_default_individual()] + [_rand_ind() for _ in range(n_restarts - 1)]
        olog(f"━━ ФАЗА 1: {n_restarts} стартов ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "ok")

    local_bests = []
    # BH_MAX / BH_PATIENCE определены выше через _BH_MAX_eff / _BH_PATIENCE_eff (shake-aware)
    with opt_lock:
        opt_state["cycle_step"] = 0
        opt_state["cycle_total"] = len(start_points) + _BH_MAX_eff
    for i, start_ind in enumerate(start_points):
        if stop_flag(): break
        label = "Старт #1 (предыдущий лучший)" if (i==0 and prev_best_params) else f"Старт #{i+1}"
        olog(f"── {label} ──", "activity")  # только для строки активности, не в лог
        with opt_lock: opt_state["generation"]=i+1
        _plog("phase1_start", start=i+1, label=label)
        _st0 = time.time()
        result, cur, top20_global = _coordinate_descent_from(
            start_ind, pmap, olog, t0, top20_global, label, max_passes=4, stop_flag=stop_flag, grids=_grids_local)
        _plog("phase1_done", start=i+1, sec=round(time.time()-_st0,1),
              equity=round(result["equity"],2), wr=round(result["winrate"],1))
        local_bests.append((result["fitness"], result, cur))
        olog(f"  {label} → ${result['equity']:.2f} WR {result['winrate']:.1f}% DD {result['max_dd']:.1f}%",
             "found" if result["equity"]>100 else "info")
        with opt_lock:
            opt_state["top20"] = top20_global
            opt_state["elapsed"] = round(time.time()-t0, 1)
            opt_state["cycle_step"] = i + 1
            # best и all_time_best НЕ трогаем здесь — они обновляются только в конце цикла

    if stop_flag(): return None, None, top20_global

    local_bests.sort(key=lambda x: -x[0])
    best_f, best_r1, best_p1 = local_bests[0]

    # Фаза 2: Basin Hopping от лучшей точки
    # OPT: early-stop после N итераций подряд без улучшения (экономит ~60% времени BH)
    BH_MAX = _BH_MAX_eff; BH_PATIENCE = _BH_PATIENCE_eff
    olog(f"━━ ФАЗА 2: Basin Hopping (макс {BH_MAX} итераций, patience={BH_PATIENCE}{', ШАГ×2.4 ВСТРЯСКА' if shake else ''}) ━━━━━━━━", "ok")
    bh_current=dict(best_p1); bh_best=best_r1; final_result=best_r1; final_params=best_p1
    bh_no_improve = 0  # OPT: счётчик подряд идущих неудач
    try:
      for bh_i in range(BH_MAX):
        if stop_flag(): break
        # OPT: early-stop по patience
        if bh_no_improve >= BH_PATIENCE:
            olog(f"  ⏭ BH early-stop: {BH_PATIENCE} итераций без улучшения", "info")
            break
        # OPT: при perturbation учитываем FILTER_GROUPS — не меняем зависимые параметры
        # если их родительский use_* = False (симуляция всё равно их игнорирует,
        # но coordinate_descent потом не найдёт улучшения и тратит время впустую)
        perturbed=dict(bh_current)
        keys_to_perturb = random.sample(_KEYS, max(1, int(len(_KEYS)*_PERTURB_FRAC)))
        for k in keys_to_perturb:
            # Пропускаем зависимый параметр если его родитель отключён
            parent = FILTER_GROUPS.get(k)
            if parent and not perturbed.get(parent, True):
                continue
            spec=PARAM_SPACE[k]; grid=_grids_local[k]
            if spec["type"] in ("bool","cat"): perturbed[k]=random.choice(spec["values"])
            else:
                idx=grid.index(bh_current[k]) if bh_current[k] in grid else len(grid)//2
                step=random.randint(1,max(1,int(len(grid)*_BH_STEP_FRAC)))
                perturbed[k]=grid[min(max(0,idx+random.choice([-step,step])),len(grid)-1)]
        with opt_lock: opt_state["current_param"]=f"Basin Hopping {bh_i+1}/{BH_MAX}"
        _plog("bh_start", bh=bh_i+1)
        _bht0 = time.time()
        bh_r, bh_p, top20_global = _coordinate_descent_from(
            perturbed, pmap, olog, t0, top20_global, f"BH-{bh_i+1}", max_passes=4, stop_flag=stop_flag, grids=_grids_local)
        improved = bh_r["fitness"] > bh_best["fitness"]
        _plog("bh_done", bh=bh_i+1, sec=round(time.time()-_bht0,1),
              equity=round(bh_r["equity"],2), improved=improved)
        if improved:
            bh_best=bh_r; bh_current=bh_p; final_result=bh_r; final_params=bh_p
            bh_no_improve = 0  # OPT: сбрасываем счётчик при успехе
            olog(f"  ✅ BH {bh_i+1}: ЛУЧШЕ ${bh_r['equity']:.2f}","found")
            with opt_lock:
                opt_state["top20"] = top20_global
                # best и all_time_best НЕ трогаем здесь — только в конце цикла
        else:
            bh_no_improve += 1  # OPT: увеличиваем счётчик неудач
        with opt_lock:
            opt_state["cycle_step"] = len(start_points) + bh_i + 1

    finally:
        pass  # пул управляется снаружи (run_optimizer), не закрываем здесь

    if top20_global and top20_global[0]["fitness"] > final_result["fitness"]:
        final_result = top20_global[0]
        final_params = dict(final_result["params"])
    # Гарантируем ограничение TP для малых TF в итоговом результате
    final_result = _clamp_result(final_result)
    final_params = dict(final_result["params"])

    # --- Валидация стабильности финального результата цикла ---
    # Прогоняем по 3 окнам (старая треть / средняя / свежая треть)
    # Штрафуем validated_fitness если окна сильно расходятся с трейном
    train_wr_cycle = final_result.get("winrate", 0)
    now_ts_cycle = time.time()
    def _quick_window(d_from, d_to):
        cutoff_f = now_ts_cycle - d_from * 86400
        cutoff_t = now_ts_cycle - d_to * 86400
        sl = [c for c in candles if cutoff_f <= c.get("t", 0) < cutoff_t]
        if len(sl) < 8: return None
        return _simulate(sl, final_params, 0, risk_pct=risk_pct)
    window_size_c = days / 3.0
    ok_windows = 0; total_windows = 0
    for wi in range(3):
        wres = _quick_window(days - wi * window_size_c, days - (wi + 1) * window_size_c)
        if wres and wres["trades"] >= 5:
            total_windows += 1
            if train_wr_cycle > 0 and wres["winrate"] >= train_wr_cycle * 0.65:
                ok_windows += 1
    stability_ratio = (ok_windows / total_windows) if total_windows > 0 else 1.0
    # validated_fitness учитывает стабильность: нестабильная стратегия штрафуется до 50%
    stability_multiplier = 0.5 + 0.5 * stability_ratio
    final_result["stability_ratio"] = round(stability_ratio, 2)
    final_result["validated_fitness"] = round(final_result["fitness"] * stability_multiplier, 4)
    olog(f"  📐 Стабильность: {ok_windows}/{total_windows} окон ({'✅' if stability_ratio >= 0.67 else '⚠️'} {stability_ratio:.0%}) → vfit={final_result['validated_fitness']:.2f}", "ok" if stability_ratio >= 0.67 else "warn")

    # Обновляем validated_fitness для всего top20
    for r in top20_global:
        if "validated_fitness" not in r:
            r["stability_ratio"] = 1.0
            r["validated_fitness"] = r["fitness"]

    return final_result, final_params, top20_global

# ═══════════════════════════════════════════════════════════════
# AUTO SAVE / LOAD CONFIG
# ═══════════════════════════════════════════════════════════════
def _script_dir():
    """Безопасно возвращает папку скрипта — без краша если __file__ == '<stdin>'."""
    try:
        p = os.path.abspath(__file__)
        d = os.path.dirname(p)
        return d if d else os.getcwd()
    except Exception:
        return os.getcwd()

_WICKFILL_DIR = "/sdcard/Download/WickFill"
_AUTO_DIRS = [
    _WICKFILL_DIR,
    "/sdcard/Download",
    _script_dir(),
]

# ── GitHub Sync ──────────────────────────────────────────────────────────────
_GH_TOKEN  = "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"
_GH_REPO   = "mambaleylo/wickfill"
_GH_API    = "https://api.github.com"
_GH_SYNC_PENDING = []   # [(local_path, gh_path, content_str)] — очередь на синхронизацию
_GH_SYNC_LOCK = threading.Lock()

def _gh_request(method, path, payload=None):
    """Минимальный GitHub API клиент. Возвращает dict или None при ошибке."""
    import urllib.request as _ur, urllib.error as _ue
    url = f"{_GH_API}/repos/{_GH_REPO}/contents/{path}"
    headers = {"Authorization": f"token {_GH_TOKEN}", "Content-Type": "application/json"}
    data = json.dumps(payload).encode() if payload else None
    req = _ur.Request(url, data=data, method=method, headers=headers)
    try:
        with _ur.urlopen(req, timeout=10) as r:
            return json.load(r)
    except _ue.HTTPError as e:
        if e.code == 404: return None
        try: body = e.read().decode()[:300]
        except: body = ""
        print(f"{_ts()} [gh] HTTP {e.code} {method} {path}: {body}", flush=True)
        return None
    except Exception as e:
        print(f"{_ts()} [gh] {e}", flush=True)
        return None

def _gh_put_file(gh_path, content_str, message):
    """Загружает или обновляет файл на GitHub. Возвращает True при успехе."""
    existing = _gh_request("GET", gh_path)
    sha = existing.get("sha") if existing else None
    payload = {
        "message": message,
        "content": base64.b64encode(content_str.encode("utf-8")).decode(),
    }
    if sha:
        payload["sha"] = sha
    result = _gh_request("PUT", gh_path, payload)
    return result is not None and "commit" in result

def _gh_get_file(gh_path):
    """Скачивает содержимое файла с GitHub. Возвращает строку или None."""
    result = _gh_request("GET", gh_path)
    if not result or "content" not in result:
        return None
    try:
        return base64.b64decode(result["content"].replace("\n","")).decode("utf-8")

    except Exception:
        return None

def _gh_delete_file(gh_path, message="delete"):
    """Удаляет файл с GitHub. Возвращает True при успехе."""
    try:
        meta = _gh_request("GET", gh_path)
        if not meta or "sha" not in meta:
            return False
        result = _gh_request("DELETE", gh_path, {"message": message, "sha": meta["sha"]})
        return result is not None
    except Exception:
        return False

def _gh_list_folder(gh_path):
    """Список файлов в папке GitHub. Возвращает [{"name":..., "path":...}] или []."""
    result = _gh_request("GET", gh_path)
    if isinstance(result, list):
        return [{"name": f["name"], "path": f["path"]} for f in result if f.get("type") == "file"]
    return []

def _gh_sync_pending():
    """Фоновая попытка загрузить файлы из очереди на GitHub."""
    with _GH_SYNC_LOCK:
        if not _GH_SYNC_PENDING:
            return
        queue = list(_GH_SYNC_PENDING)
    ok_indices = []
    for i, (local_path, gh_path, content_str, message) in enumerate(queue):
        if _gh_put_file(gh_path, content_str, message):
            ok_indices.append(i)
            print(f"{_ts()} [gh] ✅ Синхронизировано: {gh_path}", flush=True)
    with _GH_SYNC_LOCK:
        for i in sorted(ok_indices, reverse=True):
            if i < len(_GH_SYNC_PENDING):
                _GH_SYNC_PENDING.pop(i)

def _gh_enqueue(local_path, gh_path, content_str, message):
    """Добавляет файл в очередь синхронизации."""
    with _GH_SYNC_LOCK:
        # Заменяем если уже есть такой gh_path
        for i, item in enumerate(_GH_SYNC_PENDING):
            if item[1] == gh_path:
                _GH_SYNC_PENDING[i] = (local_path, gh_path, content_str, message)
                return
        _GH_SYNC_PENDING.append((local_path, gh_path, content_str, message))

# Фоновый поток синхронизации — проверяет очередь каждые 60 секунд
def _gh_sync_worker():
    while True:
        try:
            time.sleep(60)
            _gh_sync_pending()
        except Exception:
            pass

threading.Thread(target=_gh_sync_worker, daemon=True, name="gh-sync").start()
# ── /GitHub Sync ─────────────────────────────────────────────────────────────


def _clamp_tp_result(r, tf):
    """Обрезает tp_pct > 1.5 для TF < 1h в result-объекте (модульный уровень)."""
    if not r or TF_SECONDS.get(tf, 3600) >= 3600: return r
    if r.get("params", {}).get("tp_pct", 0) <= 1.5: return r
    r2 = dict(r); r2["params"] = dict(r["params"]); r2["params"]["tp_pct"] = 1.5
    return r2

def _clamp_tp_params(p, tf):
    """Обрезает tp_pct > 1.5 для TF < 1h в dict params."""
    if not p or TF_SECONDS.get(tf, 3600) >= 3600: return p
    if p.get("tp_pct", 0) <= 1.5: return p
    p2 = dict(p); p2["tp_pct"] = 1.5
    return p2

def _clamp_sl_tp_to_bounds(p):
    """Зажимает sl_pct и tp_pct к текущим границам PARAM_SPACE (из UI-полей wf_sl_min/max, wf_tp_min/max).
    Вызывается при загрузке seed — чтобы конфиг из другой сессии не вышел за заданные границы."""
    if not p: return p
    p2 = dict(p)
    sl_min = PARAM_SPACE["sl_pct"]["min"]; sl_max = PARAM_SPACE["sl_pct"]["max"]
    tp_min = PARAM_SPACE["tp_pct"]["min"]; tp_max = PARAM_SPACE["tp_pct"]["max"]
    if "sl_pct" in p2:
        p2["sl_pct"] = max(sl_min, min(sl_max, p2["sl_pct"]))
    if "tp_pct" in p2:
        p2["tp_pct"] = max(tp_min, min(tp_max, p2["tp_pct"]))
    return p2

def _config_key(symbol, tf, days, risk_pct):
    """Уникальный ключ набора параметров для имени файла."""
    sym = symbol.replace("_","").replace("/","").lower()
    return f"{sym}_{tf}_{days}d_r{int(round(risk_pct))}"

def _config_filename(symbol, tf, days, risk_pct, equity, sl_pct=None, tp_pct=None):
    """wickfill_btcusdt_15m_3d_$234_r20_sl0.5_tp1.2.json"""
    sym = symbol.replace("_","").replace("/","").lower()
    eq  = int(round(equity))
    r   = int(round(risk_pct))
    sl_part = f"_sl{round(sl_pct,2)}" if sl_pct is not None else ""
    tp_part = f"_tp{round(tp_pct,2)}" if tp_pct is not None else ""
    return f"wickfill_{sym}_{tf}_{days}d_${eq}_r{r}{sl_part}{tp_part}.json"

def _find_auto_config(symbol, tf, days, risk_pct):
    """Ищет лучший конфиг: сначала GitHub, потом локально."""
    import glob as _glob
    days = int(days)
    sym = symbol.replace("_","").replace("/","").lower()
    r   = int(round(risk_pct))
    import re as _re_fc
    pat_re = _re_fc.compile(rf"^wickfill_{_re_fc.escape(sym)}_{_re_fc.escape(tf)}_{days}d_\$\d+_r{r}(_sl[\d.]+_tp[\d.]+)?\.json$")

    # 1. Попытка с GitHub
    try:
        gh_files = _gh_list_folder("configs")
        for f in gh_files:
            name = f["name"]
            if not pat_re.match(name): continue
            raw = _gh_get_file(f"configs/{name}")
            if not raw: continue
            data = json.loads(raw)
            if not (data.get("best") and data["best"].get("params")): continue
            if int(data.get("days", days)) != days: continue
            if abs(float(data.get("risk_pct", risk_pct)) - risk_pct) > 0.1: continue
            print(f"{_ts()} [gh] ✅ Конфиг загружен с GitHub: configs/{name}", flush=True)
            return f"github:configs/{name}", data
    except Exception as e:
        print(f"{_ts()} [gh] Ошибка загрузки конфига: {e}", flush=True)

    # Локальный фолбек убран — только GitHub
    return None, None

def _auto_save_config(symbol, tf, days, risk_pct, best, top20, olog=None):
    """Сохраняет конфиг в Downloads. Атомарная замена — никаких копий с (1)."""
    import glob as _glob, tempfile
    sym = symbol.replace("_","").replace("/","").lower()
    r   = int(round(risk_pct))
    eq  = best.get("equity", 100)
    pat = f"wickfill_{sym}_{tf}_{days}d_$*_r{r}.json"

    def _log(msg, level="info"):
        if olog: olog(msg, level)
        else:
            with opt_lock:
                opt_state["logs"].append({"ts": time.strftime("%H:%M:%S"), "msg": msg, "level": level})
                if len(opt_state["logs"]) > 500:
                    opt_state["logs"] = opt_state["logs"][-300:]

    # Попытаться создать /sdcard/Download/WickFill (и /sdcard/Download как fallback)
    for _d in ["/sdcard/Download", _WICKFILL_DIR]:
        if not os.path.isdir(_d):
            try: os.makedirs(_d, exist_ok=True)
            except Exception: pass

    # Найти папку для записи (первая существующая и доступная для записи)
    save_dir = None
    tried = []
    for d in _AUTO_DIRS:
        exists = os.path.isdir(d)
        writable = os.access(d, os.W_OK) if exists else False
        tried.append(f"{d} ({'✓' if writable else ('нет папки' if not exists else 'нет записи')})")
        if exists and writable:
            save_dir = d; break
    if not save_dir:
        save_dir = os.path.dirname(os.path.abspath(__file__))
        tried.append(f"{save_dir} (фолбек скрипта)")

    _log(f"[save] Сохраняю в: {save_dir}", "info")

    fname = _config_filename(symbol, tf, days, risk_pct, eq,
                             sl_pct=best.get("params", {}).get("sl_pct"),
                             tp_pct=best.get("params", {}).get("tp_pct"))
    fpath = os.path.join(save_dir, fname)

    data = {
        "best": best, "top20": top20,
        "symbol": symbol, "tf": tf,
        "days": days, "risk_pct": risk_pct,
        "saved_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    content_str = json.dumps(data, ensure_ascii=False, indent=2)
    gh_path = f"configs/{fname}"

    # 1. Попытка сохранить на GitHub
    # Сначала проверяем: вдруг на GitHub уже лежит лучший результат (другое устройство)
    gh_ok = False
    our_fit = best.get("validated_fitness") or best.get("fitness", -9999)
    try:
        import re as _re
        sym_key = symbol.replace("_","").replace("/","").lower()
        r_key   = int(round(risk_pct))
        _pat    = _re.compile(rf"^wickfill_{_re.escape(sym_key)}_{_re.escape(tf)}_{days}d_\$\d+_r{r_key}(_sl[\d.]+_tp[\d.]+)?\.json$")
        existing_files = _gh_list_folder("configs")
        gh_best_fit = -9999
        for _ef in existing_files:
            if _pat.match(_ef["name"]):
                try:
                    _raw = _gh_get_file(f"configs/{_ef['name']}")
                    if _raw:
                        _gd = json.loads(_raw)
                        _gb = _gd.get("best", {})
                        _gf = _gb.get("validated_fitness") or _gb.get("fitness", -9999)
                        if _gf > gh_best_fit:
                            gh_best_fit = _gf
                except Exception: pass
        if gh_best_fit > our_fit:
            _log(f"⏭ GitHub уже лучше (gh={gh_best_fit:.2f} > our={our_fit:.2f}), пропускаем сохранение", "info")
            return fpath
        # Удаляем старые конфиги с тем же sym/tf/days/risk
        for _ef in existing_files:
            if _pat.match(_ef["name"]) and _ef["name"] != fname:
                try:
                    _gh_delete_file(_ef["path"], f"replace: {_ef['name']} -> {fname}")
                    print(f"{_ts()} [gh] 🗑 Удалён старый конфиг: {_ef['name']}", flush=True)
                except Exception: pass
    except Exception as _e:
        print(f"{_ts()} [gh] ⚠ Ошибка очистки старых конфигов: {_e}", flush=True)
    try:
        gh_ok = _gh_put_file(gh_path, content_str, f"auto-save: {fname}")
        if gh_ok:
            print(f"{_ts()} [gh] ✅ Конфиг сохранён на GitHub: {gh_path}", flush=True)
            if olog: olog(f"✅ Сохранено на GitHub: {fname}", "found")
            else:
                with opt_lock:
                    opt_state["logs"].append({"ts": time.strftime("%H:%M:%S"), "msg": f"✅ GitHub: {fname}", "level": "found"})
    except Exception as e:
        print(f"{_ts()} [gh] ⚠ GitHub ошибка: {type(e).__name__}: {e}", flush=True)
        if olog: olog(f"⚠ GitHub: {type(e).__name__}: {e}", "warn")

    # 2. Если GitHub недоступен — сохранить локально и поставить в очередь
    if not gh_ok:
        tmp_path = None
        try:
            tmp_fd, tmp_path = tempfile.mkstemp(dir=save_dir, suffix=".tmp")
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                f.write(content_str)
            os.replace(tmp_path, fpath)
            # Удалить старые файлы того же набора параметров
            for d in _AUTO_DIRS:
                if not os.path.isdir(d): continue
                for old_f in _glob.glob(os.path.join(d, pat)):
                    if os.path.abspath(old_f) == os.path.abspath(fpath): continue
                    try: os.remove(old_f)
                    except Exception: pass
            try:
                import subprocess
                subprocess.Popen(["termux-media-scan", fpath],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except Exception: pass
            print(f"{_ts()} [save] ✅ Сохранён локально (нет сети): {fpath}", flush=True)
            if olog: olog(f"⚠ Сохранено локально (нет сети): {fname}", "warn")
            else:
                with opt_lock:
                    opt_state["logs"].append({"ts": time.strftime("%H:%M:%S"), "msg": f"⚠ Локально (нет сети): {fname}", "level": "warn"})
            # Добавить в очередь синхронизации
            _gh_enqueue(fpath, gh_path, content_str, f"auto-save: {fname}")
        except Exception as e:
            _log(f"⚠ Сохранение не удалось: {save_dir} → {e}", "warn")
            print(f"{_ts()} [save] ❌ Ошибка записи: {e}", flush=True)
            if tmp_path:
                try: os.remove(tmp_path)
                except Exception: pass
            return None
    return fpath

def run_optimizer(params):
    global _sw_candles, _sw_params, _sw_risk
    symbol       = params.get("wf_symbol", "BTC_USDT")
    tf           = params.get("wf_tf", "1h")
    days         = _si(params.get("wf_days"), 3)
    risk_pct     = max(1.0, min(100.0, _sf(params.get("wf_risk"), 20.0)))
    sl_min       = max(0.1, min(5.0, _sf(params.get("wf_sl_min"), 0.4)))
    sl_max       = max(sl_min, min(10.0, _sf(params.get("wf_sl_max"), 0.8)))
    PARAM_SPACE["sl_pct"]["min"] = sl_min
    PARAM_SPACE["sl_pct"]["max"] = sl_max
    # Пересчитываем сетку — _GRIDS строится один раз при импорте, нужно обновить вручную
    _GRIDS["sl_pct"] = _param_grid(PARAM_SPACE["sl_pct"])
    tp_min       = max(0.1, min(5.0, _sf(params.get("wf_tp_min"), 0.5)))
    tp_max       = max(tp_min, min(20.0, _sf(params.get("wf_tp_max"), 2.0)))
    PARAM_SPACE["tp_pct"]["min"] = tp_min
    PARAM_SPACE["tp_pct"]["max"] = tp_max
    _GRIDS["tp_pct"] = _param_grid(PARAM_SPACE["tp_pct"])
    infinite     = params.get("infinite", False)
    alert_cfg    = params.get("alert_cfg", None)  # dict или None
    n_candles    = _si(params.get("wf_n_candles"), 0)
    seed         = params.get("seed", None)        # {best, top20} из загруженного файла

    _opt_stop_flag.clear()
    _sw_risk = risk_pct

    # Сохраняем alert_cfg в opt_state — SW-тред читает его динамически
    with opt_lock:
        opt_state["alert_cfg"] = alert_cfg

    # Сбрасываем perf-лог для новой сессии
    global _perf_t0, _perf_log
    _perf_t0 = time.time()
    with _perf_lock:
        _perf_log = []
    _plog("start", symbol=symbol, tf=tf, days=days, risk=risk_pct, pool=_POOL_TYPE, cpus=os.cpu_count())

    with opt_lock:
        # Сохраняем sw_running — не обрываем уже живой тред скользящего окна
        sw_was_running = opt_state.get("sw_running", False)
        opt_state.update({
            "running": True, "done": False, "infinite": infinite,
            "cycle": 0, "progress": 0, "total": 0,
            "generation": 0, "pass_num": 0, "current_param": "",
            "logs": [], "logs_dropped": 0, "best": None, "all_time_best": None, "top20": [], "valid": None, "windows": [], "min_stable_days": None, "days": days,
            "started_at": time.strftime("%H:%M:%S"),
            "elapsed": 0.0, "error": "",
            "chart_symbol": symbol, "chart_tf": tf,
            "chart_path": "", "chart_updated_at": -1,
            "chart_candles": [], "chart_signals": [],
            "sw_last_update": 0, "sw_candle_count": 0,
            "last_signal_t": 0,
        })
        # Восстанавливаем флаг если тред скользящего окна уже жив
        if sw_was_running:
            opt_state["sw_running"] = True

    def olog(msg, level="info"):
        with opt_lock:
            opt_state["logs"].append({"ts": time.strftime("%H:%M:%S"), "msg": msg, "level": level})

    t0 = time.time()

    olog(f"🚀 Старт · {symbol} · {tf} · {days}д · риск {risk_pct:.0f}%")

    # Загрузка свечей
    candles = _fetch_candles(symbol, tf, days)
    # Сбрасываем прогресс-бар загрузки
    with opt_lock:
        opt_state["fetch_pct"] = -1
        opt_state["fetch_symbol"] = ""
    if len(candles) < 30:
        reason = _last_fetch_error or "нет данных от биржи"
        olog(f"❌ Мало свечей: {len(candles)} — {reason}", "error")
        with opt_lock: opt_state["running"]=False; opt_state["error"]=f"Мало свечей: {len(candles)}"
        return
    # Считаем сколько свечей реально попадёт в бэктест (те же условия что в _simulate)
    cutoff_check = time.time() - days * 86400
    candles_in_window = [c for c in candles if c.get("t", 0) >= cutoff_check]

    # Если задано n_candles — обрезаем окно
    if n_candles > 0 and n_candles < len(candles):
        candles = candles[-n_candles:]
        olog(f"   Окно ограничено до {n_candles} последних свечей", "info")

    _sw_candles = list(candles)
    n_sw = len(candles)   # сохраняем размер окна
    # В мультирежиме инициализируем per-symbol candles чтобы SW-тред использовал правильные данные
    if len(_multi_symbols) > 1 and symbol in _sw_state:
        with _sw_state_lock:
            _sw_state[symbol]["candles"] = list(candles)

    # Сразу показываем график со свечами (без сигналов) — не ждём завершения первого цикла
    try:
        cc_pre = [{"t":c["t"],"o":c["open"],"h":c["high"],"l":c["low"],"c":c["close"]} for c in candles]
        cur_pre0 = _fetch_current_candle(symbol, tf)
        if cur_pre0 and cur_pre0["t"] > candles[-1]["t"]:
            cc_pre = cc_pre + [{"t":cur_pre0["t"],"o":cur_pre0["open"],"h":cur_pre0["high"],"l":cur_pre0["low"],"c":cur_pre0["close"],"live":True}]
        _pre_best_stub = {"params": {}, "equity": 100, "winrate": 0, "max_dd": 0, "profit_factor": 0, "trades": 0}
        with opt_lock:
            if not opt_state.get("best"):  # не перезаписываем если seed уже дал лучший
                opt_state["chart_candles"]    = cc_pre
                opt_state["chart_signals"]    = []
                opt_state["chart_updated_at"] = int(time.time())
                opt_state["best"]             = _pre_best_stub
        olog(f"📊 Предварительный график: {len(cc_pre)} свечей (сигналы появятся после 1-го цикла)", "ok")
    except Exception as e:
        olog(f"⚠ Предварительный график не удался: {e}", "warn")

    # Запускаем прогрев пула ПАРАЛЛЕЛЬНО с остальной подготовкой (критично для Windows spawn)
    _n_workers = max(1, os.cpu_count() or 1)
    _plog("pool_create", workers=_n_workers, pool_type=_POOL_TYPE, n_candles=len(candles))
    olog(f"⚙ Запуск {'ThreadPool' if _POOL_TYPE=='thread' else 'ProcessPool'} ({_n_workers} {'потоков' if _POOL_TYPE=='thread' else 'процессов'})...", "info")
    _pool_ready = threading.Event()
    _shared_pool_holder = [None]

    def _create_pool():
        _shared_pool_holder[0] = PoolExecutor(
            max_workers=_n_workers,
            initializer=_worker_init,
            initargs=(candles, 0, risk_pct)
        )
        _pool_ready.set()

    threading.Thread(target=_create_pool, daemon=True).start()

    cycle = 0
    prev_best_params = None   # лучшие параметры предыдущего цикла
    prev_top20       = []     # накопленный top20 всех циклов
    _last_autosave_vfit = 0.0  # validated_fitness последнего автосохранения
    _global_best_ever = None  # лучший за все циклы — никогда не откатывается назад
    # ── Межцикловая стагнация ───────────────────────────────────────────────
    _stagnation_cycles  = 0   # сколько циклов подряд нет улучшения глобального рекорда
    _STAGNATION_THRESH  = 15  # порог: столько циклов без улучшения → встряска
    _last_shake_vfit    = -1e18  # validated_fitness на момент последнего рескрамбла
    # ────────────────────────────────────────────────────────────────────────
    # Автоперезагрузка свечей: каждые 4 интервала TF
    _reload_interval_sec = TF_SECONDS.get(tf, 3600) * 1
    _last_candle_reload  = time.time()
    olog(f"🔄 Автообновление свечей каждые {_reload_interval_sec//60} мин (1 × {tf})", "info")
    # Сразу заполняем из seed если он есть
    if seed and seed.get("best") and seed["best"].get("params"):
        _s = dict(seed["best"])
        if "validated_fitness" not in _s:
            _s["validated_fitness"] = _s.get("fitness", 0)
        _global_best_ever = _s
        _last_autosave_vfit = _s.get("validated_fitness", _s.get("fitness", 0))

    # Авто-загрузка конфига из Downloads (если нет ручного seed)
    if not seed:
        existing_dirs = [d for d in _AUTO_DIRS if os.path.isdir(d)]
        import glob as _glob2
        sym2 = symbol.replace("_","").replace("/","").lower()
        r2   = int(round(risk_pct))
        search_pat = f"wickfill_{sym2}_{tf}_{days}d_$*_r{r2}*.json"
        olog(f"🗂 Ищу: {search_pat}", "info")
        for d in existing_dirs:
            all_wf = _glob2.glob(os.path.join(d, "wickfill_*.json"))
            if all_wf:
                olog(f"   📁 {d}: {[os.path.basename(f) for f in all_wf]}", "info")
        auto_path, auto_data = _find_auto_config(symbol, tf, days, risk_pct)
        if auto_data:
            seed = {"best": auto_data["best"], "top20": auto_data.get("top20", [])}
            _last_autosave_vfit = auto_data["best"].get("validated_fitness", auto_data["best"].get("fitness", 0))
            olog(f"🔍 Авто-загрузка: {auto_path}", "ok")
            olog(f"   ${auto_data['best'].get('equity',0):.0f} WR {auto_data['best'].get('winrate',0):.1f}% | {len(seed['top20'])} записей top20", "ok")
        else:
            olog(f"📭 Конфиг не найден для {symbol} {tf} {days}д r{int(round(risk_pct))} — прогон с нуля", "info")

    # Если передан seed из загруженного файла — стартуем с него
    if seed and seed.get("best") and seed["best"].get("params"):
        prev_best_params = _clamp_sl_tp_to_bounds(_clamp_tp_params(dict(seed["best"]["params"]), tf))
        prev_top20       = [_clamp_tp_result(r, tf) for r in (seed.get("top20") or [])]
        olog(f"📂 Загружен seed: ${seed['best'].get('equity',0):.2f} WR {seed['best'].get('winrate',0):.1f}% | top20: {len(prev_top20)} записей", "ok")
        # Сразу строим график по загруженному конфигу — не ждём конца первого цикла
        try:
            olog(f"📊 Строю предварительный график из конфига...", "info")
            sim_pre = _simulate(candles, prev_best_params, 0, _collect=True, risk_pct=risk_pct)
            if sim_pre:
                sigs_pre = sim_pre["_signals"] or []
                cc_fmt = [{"t":c["t"],"o":c["open"],"h":c["high"],"l":c["low"],"c":c["close"]} for c in candles]
                cur_pre = _fetch_current_candle(symbol, tf)
                if cur_pre and cur_pre["t"] > candles[-1]["t"]:
                    cc_fmt = cc_fmt + [{"t":cur_pre["t"],"o":cur_pre["open"],"h":cur_pre["high"],"l":cur_pre["low"],"c":cur_pre["close"],"live":True}]
                pre_best = seed["best"]
                cp = _save_chart(cc_fmt, sigs_pre, pre_best, symbol, tf, risk_pct)
                with opt_lock:
                    opt_state["chart_candles"]    = cc_fmt
                    opt_state["chart_signals"]    = sigs_pre
                    opt_state["chart_path"]       = cp or ""
                    opt_state["chart_updated_at"] = int(time.time())
                    opt_state["best"]             = pre_best
                    opt_state["top20"]            = prev_top20
                    _sw_params = dict(prev_best_params)  # алерты сразу на базе seed
                olog(f"✅ График готов: {len(sigs_pre)} сигналов", "ok")
        except Exception as e:
            olog(f"⚠ Предварительный график не удался: {e}", "warn")

    # Ждём готовности пула (он создавался параллельно с подготовкой)
    if not _pool_ready.wait(timeout=120):
        olog("❌ Пул воркеров не запустился за 120с", "error")
        with opt_lock: opt_state["running"]=False; opt_state["error"]="Pool timeout"
        return
    _shared_pool = _shared_pool_holder[0]
    _plog("pool_ready", sec=round(time.time()-t0, 1), workers=_n_workers)

    while True:
        if _opt_stop_flag.is_set(): break
        cycle += 1
        with opt_lock: opt_state["cycle"] = cycle
        if infinite:
            olog(f"", "info")
            if cycle == 1:
                _nw = max(1, os.cpu_count() or 1)
                olog(f"⚙ {'ThreadPool' if _POOL_TYPE=='thread' else 'ProcessPool'}: {_nw} {'потоков' if _POOL_TYPE=='thread' else 'процессов'}", "found")
                olog(f"═══ ЦИКЛ #{cycle} — ПЕРВЫЙ ПРОГОН ═══════════════════════════", "ok")
            else:
                prev_eq = prev_top20[0]["equity"] if prev_top20 else 0
                olog(f"═══ ЦИКЛ #{cycle} — ПРОДОЛЖЕНИЕ (лучшее за всё время: ${prev_eq:.2f}) ═══", "ok")

        # Между циклами — автоперезагрузка свечей каждый 1 интервал TF
        if cycle > 1 and infinite and (time.time() - _last_candle_reload) >= _reload_interval_sec:
            olog(f"🔄 Перезагрузка свечей (прошло {int((time.time()-_last_candle_reload)//60)} мин)...", "info")
            try:
                fresh_reload = _fetch_candles(symbol, tf, days)
                if n_candles > 0 and n_candles < len(fresh_reload):
                    fresh_reload = fresh_reload[-n_candles:]
                if len(fresh_reload) >= 30:
                    with opt_lock:
                        _sw_candles = list(fresh_reload)
                    if len(_multi_symbols) > 1 and symbol in _sw_state:
                        with _sw_state_lock:
                            _sw_state[symbol]["candles"] = list(fresh_reload)
                    olog(f"✅ Свечи обновлены: {len(fresh_reload)} ({fresh_reload[-1]['t'] and __import__('datetime').datetime.utcfromtimestamp(fresh_reload[-1]['t']).strftime('%d.%m %H:%M') or '?'} UTC)", "ok")
                else:
                    olog(f"⚠ Перезагрузка не удалась — мало свечей ({len(fresh_reload)}), используем старые", "warn")
            except Exception as _re:
                olog(f"⚠ Ошибка перезагрузки свечей: {_re}", "warn")
            _last_candle_reload = time.time()

        # Между циклами — проверяем появление новой свечи и бесшовно сдвигаем окно
        if cycle > 1:
            _try_slide_window(symbol, tf, olog)

        # Берём актуальное окно свечей (не перегружаем с сети — SW уже обновляет)
        with opt_lock:
            current_candles = list(_sw_candles)

        _plog("cycle_start", cycle=cycle, n_candles=len(current_candles))
        cycle_t0 = time.time()

        # ── Встряска при стагнации ──────────────────────────────────────────
        _shake_now = False
        if infinite and _stagnation_cycles >= _STAGNATION_THRESH:
            _shake_now = True
            _stagnation_cycles = 0  # сбрасываем счётчик после встряски
            _last_shake_vfit = _global_best_ever.get("validated_fitness") or _global_best_ever.get("fitness", 0) if _global_best_ever else -1e18
            olog(f"⚡ ВСТРЯСКА (stagnation={_STAGNATION_THRESH} циклов): рескрамбл stop/tp/bool → расширенный BH", "warn")
        # ────────────────────────────────────────────────────────────────────

        final_result, final_params, top20 = _run_one_cycle(
            current_candles, days, risk_pct, olog, t0, tf,
            prev_best_params=prev_best_params if infinite else None,
            prev_top20=prev_top20 if infinite else None,
            pool=_shared_pool, n_workers=_n_workers,
            shake=_shake_now)
        _plog("cycle_end", cycle=cycle, sec=round(time.time()-cycle_t0,1),
              stopped=_opt_stop_flag.is_set(), has_result=final_result is not None)

        if _opt_stop_flag.is_set():
            print(f"[DBG] while-loop: stop_flag сработал на cycle={cycle}", flush=True); break

        print(f"[DBG] cycle={cycle} infinite={infinite} final_result={final_result is not None} stop={_opt_stop_flag.is_set()}", flush=True)
        cycle_elapsed = round(time.time() - cycle_t0, 1)   # вычисляем сразу — используется ниже
        if final_result:
            elapsed = round(time.time()-t0, 1)

            # Накапливаем top20 между циклами — сначала merge, потом выбираем best
            if infinite:
                merged = list(top20) + list(prev_top20)
                merged.sort(key=lambda x: -(x.get("validated_fitness") or x["fitness"]))
                seen_vf=set(); deduped=[]
                for item in merged:
                    k=round(item.get("validated_fitness") or item["fitness"], 6)
                    if k not in seen_vf: seen_vf.add(k); deduped.append(item)
                prev_top20 = deduped[:7]
            else:
                prev_top20 = top20

            # all_time_best — лучший за все циклы, никогда не откатывается назад
            # Кандидат этого цикла: лучший в prev_top20 по validated_fitness
            if prev_top20:
                cycle_best = _clamp_tp_result(max(prev_top20, key=lambda r: r.get("validated_fitness", r["fitness"])), tf)
            else:
                cycle_best = _clamp_tp_result(final_result, tf)
            # Обновляем глобальный рекорд только если equity стало лучше — никогда не откатывается
            # Сравниваем по validated_fitness (учитывает стабильность × результат)
            _cb_vfit = cycle_best.get("validated_fitness") or cycle_best.get("fitness", 0)
            _gb_vfit = _global_best_ever.get("validated_fitness") or _global_best_ever.get("fitness", 0) if _global_best_ever else -1e18
            if _global_best_ever is None or _cb_vfit > _gb_vfit:
                _global_best_ever = cycle_best
                _stagnation_cycles = 0  # рекорд улучшился — сбрасываем
            else:
                _stagnation_cycles += 1  # нет улучшения — наращиваем
            all_time_best = _global_best_ever
            prev_best_params = dict(cycle_best["params"])  # следующий цикл стартует с лучшего этого цикла

            _prev_best_eq = getattr(run_optimizer, '_prev_reported_eq', 0)
            is_new_rec = all_time_best.get("equity", 0) > _prev_best_eq
            run_optimizer._prev_reported_eq = all_time_best.get("equity", 0)
            rec_flag = "🆕" if is_new_rec else "→"
            _stag_str = f" | stagnation={_stagnation_cycles}/{_STAGNATION_THRESH}" if _stagnation_cycles > 0 else ""
            olog(f"✅ Цикл #{cycle} готов за {int(cycle_elapsed)}с | {rec_flag} ${all_time_best['equity']:.2f} WR {all_time_best['winrate']:.1f}% Сд {all_time_best['trades']} DD {all_time_best['max_dd']:.1f}%{_stag_str}", "found" if is_new_rec else "ok")

            all_time_params = dict(all_time_best["params"])
            with opt_lock:
                _sw_params = all_time_params

            # --- Walk-forward валидация (30% + скользящие окна + мин. период) ---
            now_ts = time.time()
            valid_days = days * 0.30
            with opt_lock:
                _fresh_candles = list(_sw_candles) if _sw_candles else list(current_candles)
            train_wr = all_time_best["winrate"]

            def _wf_sim(d_from, d_to=None):
                """Прогоняет конфиг на отрезке [now - d_from*86400 .. now - (d_to or 0)*86400].
                Передаём полный список свечей и используем days_limit для обрезки только снизу.
                days_limit в _simulate режет по cutoff = time.time() - days_limit*86400,
                то есть _wf_sim(6, 0) → days_limit=6 → берёт последние 6 дней.
                НО индикаторы (RSI, ATR) прогреваются на всех свечах ДО cutoff тоже,
                потому что _simulate сначала строит rsi_series/atr_series по всему списку,
                а потом внутри цикла проверяет i >= start_i (индекс первой свечи в окне).
                Проблема была в том что мы передавали УРЕЗАННЫЙ список — исправляем:
                передаём ПОЛНЫЙ _fresh_candles, а days_limit отсекает торговлю снизу."""
                cutoff_from = now_ts - d_from * 86400
                cutoff_to   = now_ts - (d_to or 0) * 86400
                # Фильтруем только верхнюю границу (d_to), нижнюю отдаём на откуп days_limit
                # Передаём полный список — индикаторы прогреваются на всей истории
                # trade_from_ts ограничивает только торговлю, не данные
                if d_to and d_to > 0:
                    sl = [c for c in _fresh_candles if c.get("t", 0) < cutoff_to]
                else:
                    sl = list(_fresh_candles)
                if len(sl) < 10: return None
                return _simulate(sl, all_time_params, 0, risk_pct=risk_pct,
                                 trade_from_ts=cutoff_from)

            # 1) Валидация на последних 30%
            valid_sim = _wf_sim(valid_days, 0)
            valid_result = None
            if valid_sim:
                valid_result = {
                    "equity":        round(valid_sim["equity"], 2),
                    "winrate":       round(valid_sim["winrate"], 1),
                    "max_dd":        round(valid_sim["max_dd"], 1),
                    "trades":        valid_sim["trades"],
                    "profit_factor": min(round(valid_sim.get("profit_factor", 0), 2), 999.0),
                    "days":          round(valid_days, 1),
                }
                olog(
                    f"🔍 Валидация ({valid_result['days']}д): "
                    f"${valid_result['equity']:.2f} | "
                    f"WR {valid_result['winrate']:.1f}% | "
                    f"DD {valid_result['max_dd']:.1f}% | "
                    f"Сд {valid_result['trades']}",
                    "ok" if valid_result["winrate"] >= train_wr * 0.75 else "warn"
                )

            # 2) Скользящие окна — 5 равных отрезков по всей истории
            window_size = days / 5.0
            windows = []
            for wi in range(5):
                d_from = days - wi * window_size
                d_to   = days - (wi + 1) * window_size
                ws = _wf_sim(d_from, d_to)
                if ws and ws["trades"] >= 3:
                    windows.append({
                        "i":       wi + 1,
                        "winrate": round(ws["winrate"], 1),
                        "equity":  round(ws["equity"], 2),
                        "trades":  ws["trades"],
                        "ok":      ws["winrate"] >= train_wr * 0.75,
                        "ts_from": round(now_ts - d_from * 86400),
                        "ts_to":   round(now_ts - d_to   * 86400),
                    })
            if windows:
                ww_str = " | ".join(f"#{w['i']} WR{w['winrate']:.0f}%{'✅' if w['ok'] else '❌'}" for w in windows)
                olog(f"📊 Окна: {ww_str}", "ok")

            # 3) Минимальный стабильный период — ищем самый короткий рабочий отрезок
            min_stable_days = None
            for pct in [0.10, 0.20, 0.33, 0.50, 0.70]:
                test_days = days * pct
                ts = _wf_sim(test_days, 0)
                if ts and ts["winrate"] >= train_wr * 0.75 and ts["trades"] >= 3:
                    min_stable_days = round(test_days, 1)
                    break  # нашли минимальный — дальше не ищем
            if min_stable_days is not None:
                olog(f"📐 Мин. стабильный период: {min_stable_days}д", "ok")

            with opt_lock:
                opt_state["valid"] = valid_result
                opt_state["windows"] = windows
                opt_state["min_stable_days"] = min_stable_days

            # Автосохранение в Downloads если результат улучшился
            new_vfit = all_time_best.get("validated_fitness", all_time_best.get("fitness", 0))
            if new_vfit > _last_autosave_vfit:
                saved = _auto_save_config(symbol, tf, days, risk_pct, all_time_best, prev_top20, olog)
                if saved:
                    _last_autosave_vfit = new_vfit
                else:
                    olog(f"⚠ Авто-сохранение не удалось (проверь папку Download)", "warn")

            # Обновляем chart — показываем сигналы за то же окно что и оптимизация
            # В мультирежиме _sw_candles перезаписывается другим символом — используем локальные candles
            is_multi_run = len(_multi_symbols) > 1
            if is_multi_run:
                chart_candles_src = list(candles)  # локальные свечи текущего символа
            else:
                with opt_lock:
                    chart_candles_src = list(_sw_candles)
            # Обрезаем свечи по тому же days_limit что и оптимизатор
            # Используем метку последней свечи как точку отсчёта (детерминировано)
            _src_ts = max((c.get("t", 0) for c in chart_candles_src), default=time.time())
            cutoff = _src_ts - days * 86400
            chart_candles_window = [c for c in chart_candles_src if c.get("t", 0) >= cutoff]
            if len(chart_candles_window) < 10:
                chart_candles_window = chart_candles_src  # fallback
            # Симулируем только закрытые свечи — live-свеча добавляется ниже только для отображения
            sim = _simulate(chart_candles_window, all_time_params, 0, _collect=True, risk_pct=risk_pct)
            chart_signals = sim["_signals"] if sim else []
            chart_candles_fmt = [{"t":c["t"],"o":c["open"],"h":c["high"],"l":c["low"],"c":c["close"]} for c in chart_candles_window]
            # Добавляем незакрытую свечу только для отображения
            cur_c = _fetch_current_candle(symbol, tf)
            if cur_c and cur_c["t"] > chart_candles_window[-1]["t"]:
                chart_candles_fmt = chart_candles_fmt + [{"t":cur_c["t"],"o":cur_c["open"],"h":cur_c["high"],"l":cur_c["low"],"c":cur_c["close"],"live":True}]
            chart_path = _save_chart(chart_candles_fmt, chart_signals, all_time_best, symbol, tf, risk_pct)
            with opt_lock:
                opt_state["chart_candles"]  = chart_candles_fmt
                opt_state["chart_signals"]  = chart_signals
                opt_state["chart_path"]     = chart_path or ""
                opt_state["chart_updated_at"] = int(time.time())
                opt_state["best"]           = all_time_best
                opt_state["all_time_best"]  = all_time_best  # всегда = глобальный рекорд
                opt_state["top20"]          = prev_top20
                opt_state["elapsed"]        = elapsed
                opt_state["done"]           = not infinite
                ct = opt_state.setdefault("cycle_times", [])
                ct.append(cycle_elapsed)
                if len(ct) > 20: ct.pop(0)  # храним последние 20
                opt_state["avg_cycle_s"] = round(sum(ct) / len(ct), 1)

            # Запуск скользящего окна (один раз после первого цикла)
            with opt_lock:
                sw_already = opt_state["sw_running"]
            if not sw_already:
                sw_thread = threading.Thread(
                    target=_sliding_window_thread,
                    args=(symbol, tf, n_sw, alert_cfg, risk_pct),
                    daemon=True
                )
                sw_thread.start()
                olog(f"🔄 Скользящее окно запущено (каждые {TF_SECONDS.get(tf,3600)}с)", "ok")

        if not infinite:
            break

        # Бесконечный режим: без паузы, сразу следующий цикл
        # Запускаем SW-тред здесь тоже — на случай если первый цикл был прерван
        with opt_lock:
            sw_already = opt_state["sw_running"]
        if not sw_already:
            sw_thread = threading.Thread(
                target=_sliding_window_thread,
                args=(symbol, tf, n_sw, alert_cfg, risk_pct),
                daemon=True
            )
            sw_thread.start()
            olog(f"🔄 Скользящее окно запущено (каждые {TF_SECONDS.get(tf,3600)}с)", "ok")

        if not _opt_stop_flag.is_set():
            olog(f"⟳ Запускаем следующий цикл улучшения...", "info")

    _shared_pool.shutdown(wait=False)
    _plog("optimizer_done", reason="stop_flag" if _opt_stop_flag.is_set() else "finished")
    _perf_save(symbol, tf)

    with opt_lock:
        opt_state["running"] = False
        opt_state["done"]    = True
    print("[opt] Завершён")

def run_optimizer_safe(params):
    import traceback
    try:
        run_optimizer(params)
    except Exception as e:
        print(f"[opt] ИСКЛЮЧЕНИЕ: {e}\n{traceback.format_exc()}", flush=True)
        with opt_lock:
            opt_state["running"] = False
            opt_state["error"] = str(e)
        try:
            _plog("crash", error=str(e))
            sym = params.get("wf_symbol","unknown")
            tf2 = params.get("wf_tf","?")
            _perf_save(sym, tf2)
        except Exception:
            pass

def _run_multi_safe(sym_list, base_params):
    """Round-robin: one cycle per symbol, repeating until stopped."""
    import traceback, copy
    global _active_chart_symbol
    print(f"[multi] Старт round-robin: {sym_list}", flush=True)
    sym_cycles = {s: 0 for s in sym_list}
    tf       = base_params.get("wf_tf", "1h")
    days     = int(base_params.get("wf_days", 30) or 30)
    risk_pct = float(base_params.get("wf_risk", 10) or 10)
    alert_cfg = base_params.get("alert_cfg") or None

    # Регистрируем все символы в _sw_state (per-symbol режим)
    with _sw_state_lock:
        for s in sym_list:
            if s not in _sw_state:
                _sw_state[s] = {"candles": [], "params": {}, "risk": risk_pct, "running": False}

    try:
        while not _opt_stop_flag.is_set():
            for sym in sym_list:
                if _opt_stop_flag.is_set():
                    break
                params = copy.deepcopy(base_params)
                params["wf_symbol"] = sym
                params["infinite"] = False
                sym_cycles[sym] = sym_cycles.get(sym, 0) + 1
                with opt_states_lock:
                    _active_chart_symbol = sym
                    if sym not in opt_states:
                        opt_states[sym] = {}
                    opt_states[sym]["running"] = True
                    opt_states[sym]["cycle"]   = sym_cycles[sym]
                print(f"[multi] Цикл #{sym_cycles[sym]} → {sym}", flush=True)
                try:
                    run_optimizer(params)
                except Exception as e:
                    print(f"[multi] ИСКЛЮЧЕНИЕ run_optimizer {sym}: {e}\n{traceback.format_exc()}", flush=True)
                # snapshot result into opt_states
                try:
                    with opt_lock:
                        best = opt_state.get("all_time_best") or opt_state.get("best")
                        valid = opt_state.get("valid")
                        windows = opt_state.get("windows", [])
                        min_stable = opt_state.get("min_stable_days")
                        days_v = opt_state.get("days", 30)
                        chart_upd = opt_state.get("chart_updated_at", -1)
                        chart_candles = list(opt_state.get("chart_candles", []))
                        chart_signals = list(opt_state.get("chart_signals", []))
                        chart_tf = opt_state.get("chart_tf","")
                        chart_path = opt_state.get("chart_path","")
                        best_params = dict(opt_state.get("best",{}).get("params",{})) if opt_state.get("best") else {}
                    with opt_states_lock:
                        s = opt_states.setdefault(sym, {})
                        s["symbol"]   = sym
                        s["cycle"]    = sym_cycles[sym]
                        s["running"]  = False
                        s["valid"]    = valid
                        s["windows"]  = windows
                        s["min_stable_days"] = min_stable
                        s["days"]     = days_v
                        s["chart_tf"] = chart_tf  # всегда обновляем tf
                        # Обновляем данные графика: берём лучшее что есть
                        if chart_upd > 0:
                            # Полный снапшот с готовым графиком
                            s["chart_updated_at"] = chart_upd
                            s["chart_candles"]    = chart_candles
                            s["chart_signals"]    = chart_signals
                            s["chart_path"]       = chart_path
                        elif chart_candles:
                            # Свечи есть но chart_updated_at не выставлен (прерван цикл)
                            # Сохраняем свечи — /chart построит график на лету
                            s["chart_candles"] = chart_candles
                            s["chart_signals"] = chart_signals
                            if "chart_updated_at" not in s:
                                s["chart_updated_at"] = -1
                        elif "chart_updated_at" not in s:
                            s["chart_updated_at"] = -1
                        if best:
                            prev_eq = s.get("eq", 0)
                            new_eq  = round(best.get("equity", 100), 2)
                            if new_eq >= prev_eq:
                                s["best"]   = best
                                s["eq"]     = new_eq
                                s["wr"]     = round(best.get("winrate", 0), 1)
                                s["dd"]     = round(best.get("max_dd", 0), 1)
                                s["trades"] = best.get("trades", 0)
                                s["pf"]     = round(min(best.get("profit_factor", 0), 999), 2)
                                s["sl"]     = best.get("params", {}).get("sl_pct", None)
                                s["tp"]     = best.get("params", {}).get("tp_pct", None)
                except Exception as e:
                    print(f"[multi] ИСКЛЮЧЕНИЕ snapshot {sym}: {e}\n{traceback.format_exc()}", flush=True)
                # Запускаем per-symbol SW-тред после первого цикла (один раз на символ)
                try:
                    if sym_cycles[sym] == 1 and best_params:
                        with _sw_state_lock:
                            _sw_state[sym]["params"] = best_params
                            _sw_state[sym]["risk"]   = risk_pct
                        already = _sw_threads.get(sym)
                        if not already or not already.is_alive():
                            n_sw = days * int(86400 / TF_SECONDS.get(tf, 3600))
                            t = threading.Thread(
                                target=_sliding_window_thread,
                                args=(sym, tf, n_sw, alert_cfg, risk_pct),
                                daemon=True
                            )
                            _sw_threads[sym] = t
                            t.start()
                            print(f"[multi] SW-тред запущен для {sym}", flush=True)
                    elif sym_cycles[sym] > 1 and best_params:
                        with _sw_state_lock:
                            if sym in _sw_state:
                                _sw_state[sym]["params"] = best_params
                except Exception as e:
                    print(f"[multi] ИСКЛЮЧЕНИЕ SW-тред {sym}: {e}\n{traceback.format_exc()}", flush=True)
    except Exception as e:
        print(f"[multi] КРИТИЧЕСКОЕ ИСКЛЮЧЕНИЕ: {e}\n{traceback.format_exc()}", flush=True)
    finally:
        # Останавливаем все per-symbol SW-треды
        with _sw_state_lock:
            for sym in sym_list:
                if sym in _sw_state:
                    _sw_state[sym]["running"] = False
        with opt_states_lock:
            for sym in sym_list:
                if sym in opt_states:
                    opt_states[sym]["running"] = False
        print("[multi] Round-robin завершён", flush=True)


def _run_sym_worker(sym, base_params, n_workers, stop_event):
    """Параллельный воркер: бесконечно оптимизирует один символ.
    Пишет результаты напрямую в opt_states[sym]. Не трогает глобальный opt_state."""
    import traceback, copy, time as _time
    tf       = base_params.get("wf_tf", "1h")
    days     = int(base_params.get("wf_days", 30) or 30)
    risk_pct = float(base_params.get("wf_risk", 10) or 10)
    alert_cfg = base_params.get("alert_cfg") or None

    def _slog(msg, level="info"):
        with opt_states_lock:
            s = opt_states.setdefault(sym, {})
            logs = s.setdefault("logs", [])
            logs.append({"ts": _time.strftime("%H:%M:%S"), "msg": f"[{sym.replace('_USDT','')}] {msg}", "level": level})
            # Ограничиваем буфер
            if len(logs) > 400:
                s["logs"] = logs[-200:]

    print(f"{_ts()} [par] {sym}: воркер запущен ({n_workers} воркеров)", flush=True)
    _slog(f"⚙ Параллельный режим · {n_workers} {'процессов' if _POOL_TYPE=='proc' else 'потоков'} · {tf} · {days}д", "info")

    # Загружаем свечи
    candles = _fetch_candles(sym, tf, days)
    with opt_lock:
        opt_state["fetch_pct"] = -1
        opt_state["fetch_symbol"] = ""
    if len(candles) < 30:
        _slog(f"❌ Мало свечей: {len(candles)}", "error")
        with opt_states_lock:
            opt_states.setdefault(sym, {})["running"] = False
        return

    # Создаём пул с выделенными воркерами
    try:
        pool = PoolExecutor(max_workers=n_workers, initializer=_worker_init, initargs=(candles, 0, risk_pct))
    except Exception as e:
        _slog(f"❌ Ошибка пула: {e}", "error")
        with opt_states_lock:
            opt_states.setdefault(sym, {})["running"] = False
        return

    cycle = 0
    prev_best_params = None
    prev_top20 = []
    global_best = None
    global_best_vfit = -1e18
    last_autosave_vfit = -1e18
    local_candles = list(candles)
    sw_thread_started = False
    sw_candles_ref = [list(candles)]  # mutable ref для SW-треда

    # Загружаем seed из автосохранения
    try:
        _, auto_data = _find_auto_config(sym, tf, days, risk_pct)
        if auto_data and auto_data.get("best"):
            b = auto_data["best"]
            prev_best_params = dict(b.get("params", {})) if b.get("params") else None
            global_best = b
            global_best_vfit = b.get("validated_fitness", b.get("fitness", 0))
            last_autosave_vfit = global_best_vfit
            _slog(f"[load] Загружен конфиг: ${b.get('equity',100):.2f}", "ok")
    except Exception:
        pass

    try:
        while not stop_event.is_set() and not _opt_stop_flag.is_set():
            cycle += 1
            with opt_states_lock:
                s = opt_states.setdefault(sym, {})
                s["cycle"] = cycle
                s["running"] = True

            # Между циклами сдвигаем окно свечей
            if cycle > 1:
                try:
                    new_c = _fetch_latest_candle(sym, tf)
                    if new_c and local_candles and new_c["t"] > local_candles[-1]["t"]:
                        local_candles = local_candles[1:] + [new_c]
                        sw_candles_ref[0] = list(local_candles)
                        _slog(f"🕯 Новая свеча, окно сдвинуто", "info")
                except Exception:
                    pass

            _slog(f"═══ ЦИКЛ #{cycle} ═══", "ok")
            cycle_t0 = _time.time()

            try:
                result, params_out, top20 = _run_one_cycle(
                    local_candles, days, risk_pct, _slog, cycle_t0, tf,
                    prev_best_params=prev_best_params,
                    prev_top20=prev_top20,
                    pool=pool, n_workers=n_workers
                )
            except Exception as e:
                _slog(f"❌ Ошибка цикла: {e}", "error")
                print(f"[par] {sym} цикл {cycle} ошибка: {e}\n{traceback.format_exc()}", flush=True)
                _time.sleep(2)
                continue

            if stop_event.is_set() or _opt_stop_flag.is_set():
                break

            cycle_elapsed = round(_time.time() - cycle_t0, 1)

            if result:
                # Накапливаем top20
                merged = list(top20) + list(prev_top20)
                merged.sort(key=lambda x: -(x.get("validated_fitness") or x["fitness"]))
                seen_vf = set(); deduped = []
                for item in merged:
                    k = round(item.get("validated_fitness") or item["fitness"], 6)
                    if k not in seen_vf: seen_vf.add(k); deduped.append(item)
                prev_top20 = deduped[:7]

                if prev_top20:
                    cycle_best = _clamp_tp_result(max(prev_top20, key=lambda r: r.get("validated_fitness", r["fitness"])), tf)
                else:
                    cycle_best = _clamp_tp_result(result, tf)

                cb_vfit = cycle_best.get("validated_fitness") or cycle_best.get("fitness", 0)
                if global_best is None or cb_vfit > global_best_vfit:
                    global_best = cycle_best
                    global_best_vfit = cb_vfit

                prev_best_params = dict(cycle_best["params"])

                best = global_best
                _slog(f"✅ Цикл #{cycle} за {int(cycle_elapsed)}с | ${best['equity']:.2f} WR {best['winrate']:.1f}% DD {best['max_dd']:.1f}%", "found")

                # Обновляем graph данные
                cutoff = _time.time() - days * 86400
                chart_src = [c for c in local_candles if c.get("t", 0) >= cutoff] or local_candles
                try:
                    sim = _simulate(chart_src, dict(best["params"]), 0, _collect=True, risk_pct=risk_pct)
                    chart_signals = sim["_signals"] if sim else []
                except Exception:
                    chart_signals = []
                chart_candles_fmt = [{"t":c["t"],"o":c["open"],"h":c["high"],"l":c["low"],"c":c["close"]} for c in chart_src]

                with opt_states_lock:
                    s = opt_states.setdefault(sym, {})
                    s["best"]             = best
                    s["eq"]               = round(best.get("equity", 100), 2)
                    s["wr"]               = round(best.get("winrate", 0), 1)
                    s["dd"]               = round(best.get("max_dd", 0), 1)
                    s["trades"]           = best.get("trades", 0)
                    s["pf"]               = round(min(best.get("profit_factor", 0), 999), 2)
                    s["sl"]               = best.get("params", {}).get("sl_pct")
                    s["tp"]               = best.get("params", {}).get("tp_pct")
                    s["chart_candles"]    = chart_candles_fmt
                    s["chart_signals"]    = chart_signals
                    s["chart_tf"]         = tf
                    s["chart_updated_at"] = int(_time.time())
                    s["symbol"]           = sym

                # Автосохранение — только если vfit улучшился
                try:
                    new_vfit = best.get("validated_fitness", best.get("fitness", 0))
                    if new_vfit > last_autosave_vfit:
                        saved = _auto_save_config(sym, tf, days, risk_pct, best, prev_top20, _slog)
                        if saved:
                            last_autosave_vfit = new_vfit
                except Exception:
                    pass

                # Запускаем скользящее окно (один раз)
                if not sw_thread_started:
                    sw_thread_started = True
                    n_sw = len(local_candles)
                    _sw_t = threading.Thread(
                        target=_sliding_window_thread,
                        args=(sym, tf, n_sw, alert_cfg, risk_pct),
                        daemon=True
                    )
                    _sw_t.start()
                    _slog(f"🔄 Скользящее окно запущено", "ok")

            else:
                _slog(f"⚠ Цикл #{cycle} без результата", "warn")

    except Exception as e:
        _slog(f"❌ Критическая ошибка: {e}", "error")
        print(f"[par] {sym} критическая ошибка: {e}\n{traceback.format_exc()}", flush=True)
    finally:
        try:
            pool.shutdown(wait=False)
        except Exception:
            pass
        with opt_states_lock:
            opt_states.setdefault(sym, {})["running"] = False
        print(f"[par] {sym}: воркер завершён", flush=True)


def _run_multi_parallel(sym_list, base_params):
    """Запускает параллельную оптимизацию: каждый символ в своём треде.
    Число символов ограничено числом ядер. Ядра делятся поровну."""
    import traceback
    global _active_chart_symbol

    cpu = max(1, os.cpu_count() or 1)
    n_syms = min(len(sym_list), cpu)
    if n_syms < len(sym_list):
        print(f"[par] Ограничено ядрами: запускаем {n_syms} из {len(sym_list)} символов", flush=True)
        sym_list = sym_list[:n_syms]

    workers_per_sym = max(1, cpu // n_syms)
    print(f"[par] {n_syms} символов × {workers_per_sym} воркеров (CPU={cpu})", flush=True)

    with opt_states_lock:
        _active_chart_symbol = sym_list[0]
        for sym in sym_list:
            s = opt_states.setdefault(sym, {})
            s["running"] = True
            s["cycle"] = 0
            s.setdefault("logs", [])

    stop_events = {sym: threading.Event() for sym in sym_list}
    threads = []
    for sym in sym_list:
        t = threading.Thread(
            target=_run_sym_worker,
            args=(sym, base_params, workers_per_sym, stop_events[sym]),
            daemon=True
        )
        threads.append(t)
        t.start()
        print(f"[par] Запущен тред для {sym}", flush=True)

    # Ждём всех (или глобальный стоп)
    try:
        while True:
            alive = any(t.is_alive() for t in threads)
            if not alive:
                break
            if _opt_stop_flag.is_set():
                for ev in stop_events.values():
                    ev.set()
                for t in threads:
                    t.join(timeout=5)
                break
            threading.Event().wait(timeout=1)
    except Exception as e:
        print(f"[par] Ошибка координатора: {e}\n{traceback.format_exc()}", flush=True)
    finally:
        for ev in stop_events.values():
            ev.set()
        with opt_states_lock:
            for sym in sym_list:
                opt_states.setdefault(sym, {})["running"] = False
        print("[par] Все параллельные треды завершены", flush=True)

# ═══════════════════════════════════════════════════════════════
# HTML UI
# ═══════════════════════════════════════════════════════════════
HTML = r"""<!DOCTYPE html>
<html lang="ru"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0,maximum-scale=1.0">
<meta http-equiv="Cache-Control" content="no-store, no-cache, must-revalidate, max-age=0">
<meta http-equiv="Pragma" content="no-cache">
<meta http-equiv="Expires" content="0">
<script>
document.documentElement.setAttribute("data-theme",localStorage.getItem("wf_theme")||"light");
if(window.innerWidth<=700){
  var _mfix=function(){
    var s=document.documentElement.style;
    s.overflow='auto';s.height='auto';s.touchAction='pan-y';
    var b=document.body;
    if(b){b.style.overflow='auto';b.style.height='auto';b.style.touchAction='pan-y';b.style.overscrollBehavior='auto';}
    var a=document.querySelector('.app');
    if(a){a.style.height='auto';a.style.overflow='visible';a.style.minHeight='100dvh';}
  };
  if(document.body) _mfix(); else document.addEventListener('DOMContentLoaded',_mfix);
}
</script>
<title>WickFill · Optimizer</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --card-bg:#fff8f3;
  --input-bg:rgba(255,248,243,0.8);
  --cream:#FAE6D8;
  --cream2:#f2ddd0;
  --cream3:#e8cfc0;
  --sand:#d4a882;
  --sand2:#a06040;
  --warm:#7a6050;
  --bark:#2a1f12;
  --text:#1e1209;
  --text2:#4a3520;
  --text3:#8a7060;
  --glass:rgba(250,230,216,0.96);
  --glass2:rgba(245,222,206,0.82);
  --blur:saturate(180%) blur(20px);
  --shadow:0 2px 16px rgba(80,40,10,0.08);
  --shadow2:0 8px 32px rgba(80,40,10,0.13);
  --radius:18px;
  --radius-sm:14px;
  --accent:#FF8234;
  --green:#7a9e3a;
  --green-light:#eaf2d8;
  --red:#FF8234;
  --red-light:#fff0e8;
  --orange:#FF8234;
  --accent:#A3BF6F;
  --blue:#2a4e78;
  --blue-light:#d8e6f2;
  --yellow:#7a5a20;
  --yellow-light:#f0e8d4;
  --border:rgba(100,65,30,0.13);
  --border2:rgba(100,65,30,0.07);
}

[data-theme="dark"]{
  --card-bg:#1a1a1a;
  --input-bg:#111111;
  --cream:#111111;
  --cream2:#1a1a1a;
  --cream3:#222222;
  --sand:#2e2e2e;
  --sand2:#444444;
  --warm:#888888;
  --bark:#F5F5F5;
  --text:#F5F5F5;
  --text2:#cccccc;
  --text3:#888888;
  --glass:rgba(17,17,17,0.95);
  --glass2:rgba(26,26,26,0.90);
  --shadow:0 2px 20px rgba(0,0,0,0.6);
  --shadow2:0 8px 40px rgba(0,0,0,0.7);
  --accent:#8B2508;
  --green:#A3BF6F;
  --green-light:rgba(163,191,111,0.15);
  --red:#FF8234;
  --red-light:rgba(255,130,52,0.15);
  --orange:#FF8234;
  --accent:#A3BF6F;
  --blue:#5a7fa0;
  --blue-light:rgba(90,127,160,0.12);
  --yellow:#b09050;
  --yellow-light:rgba(176,144,80,0.12);
  --border:rgba(245,245,245,0.10);
  --border2:rgba(245,245,245,0.06);
}

[data-theme="dark"] .card,
[data-theme="dark"] .sidebar,
[data-theme="dark"] .topbar,
[data-theme="dark"] #recentPanel{
  background:#1a1a1a !important;
}
[data-theme="dark"] input[type=text],
[data-theme="dark"] input[type=password],
[data-theme="dark"] input[type=number],
[data-theme="dark"] select{
  background:#111111 !important;
  color:#F5F5F5 !important;
  border-color:rgba(245,245,245,0.12) !important;
}
[data-theme="dark"] input:focus,
[data-theme="dark"] select:focus{
  border-color:#8B2508 !important;
  background:#1a1a1a !important;
}
[data-theme="dark"] .btn-primary{
  background:#8B2508 !important;
  box-shadow:0 4px 16px rgba(139,37,8,.4),inset 0 1px 0 rgba(255,255,255,.08) !important;
}
[data-theme="dark"] .btn-primary:hover:not(:disabled){
  background:#a52e0a !important;
  box-shadow:0 6px 20px rgba(139,37,8,.5) !important;
}
[data-theme="dark"] .chart-area{
  background:#111111 !important;
}
[data-theme="dark"] .eco-row{
  background:rgba(139,37,8,0.1) !important;
  border-color:rgba(139,37,8,0.25) !important;
}
[data-theme="dark"] .eco-row b{ color:#e07060 !important; }
[data-theme="dark"] .eco-row small{ color:#a05040 !important; }
[data-theme="dark"] .toggle{ background:#8B2508 !important; }
[data-theme="dark"] .dot-live,
[data-theme="dark"] .topbar-logo .dot-live{
  background:#A3BF6F !important;
  box-shadow:0 0 0 2px rgba(122,184,74,0.25) !important;
}
[data-theme="dark"] .stat-cell.good{
  background:rgba(122,184,74,0.1) !important;
  border-color:rgba(122,184,74,0.2) !important;
}
[data-theme="dark"] .stat-cell.good .stat-v{ color:#A3BF6F !important; }
[data-theme="dark"] .sym-card.active{ border-color:#8B2508 !important; }
[data-theme="dark"] .api-pill,
[data-theme="dark"] .pill.green{
  background:rgba(122,184,74,0.12) !important;
  color:#A3BF6F !important;
  border-color:rgba(122,184,74,0.2) !important;
}

html,body{
  height:100%;
  background:var(--cream);
  color:var(--text);
  font-family:'DM Sans',sans-serif;
  font-size:14px;
  overflow:hidden;
  overscroll-behavior:none;
  touch-action:pan-y;
}

/* Subtle noise texture */
body::before{
  content:'';position:fixed;inset:0;
  background-image:url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)' opacity='0.03'/%3E%3C/svg%3E");
  pointer-events:none;z-index:0;opacity:.4;
}

body>*{position:relative;z-index:1}

/* ── Layout ── */
.app{display:flex;flex-direction:column;height:100vh;height:100dvh;gap:0;overflow:hidden}

/* ── Topbar ── */
.topbar{
  display:flex;align-items:center;gap:10px;
  padding:12px 20px;
  background:var(--card-bg);
  backdrop-filter:var(--blur);
  -webkit-backdrop-filter:var(--blur);
  border-bottom:1px solid var(--border);
  flex-shrink:0;
}
.topbar-logo{
  display:flex;align-items:center;gap:8px;
  font-weight:600;font-size:.95rem;letter-spacing:-.01em;color:var(--bark);
}
.topbar-logo .dot-live{
  width:7px;height:7px;border-radius:50%;
  background:#A3BF6F;flex-shrink:0;
  box-shadow:0 0 0 2px rgba(163,191,111,.25);
}
.topbar-spacer{flex:1}
.topbar-meta{display:flex;align-items:center;gap:8px;flex-wrap:wrap}

/* Pill badge */
/* ── Topbar chips — single source of truth ── */
.tb{
  box-sizing:border-box!important;
  display:inline-flex!important;align-items:center!important;justify-content:center!important;gap:4px!important;
  height:28px!important;padding:0 10px!important;border-radius:7px!important;
  font-size:.71rem!important;font-weight:500!important;letter-spacing:.01em!important;line-height:28px!important;
  border:1px solid var(--border)!important;
  background:var(--glass2)!important;
  color:var(--text2)!important;
  white-space:nowrap!important;
  cursor:default;
  font-family:inherit!important;
  vertical-align:middle;
  -webkit-appearance:none!important;appearance:none!important;
  transition:background .15s,border-color .15s,color .15s;
  margin:0!important;
}
.tb.btn{cursor:pointer}
.tb svg{flex-shrink:0;opacity:.7;display:block}
.tb.green{background:var(--green-light)!important;border-color:rgba(74,124,89,.2)!important;color:var(--green)!important}
.tb.btn:hover{background:var(--cream2)!important;border-color:var(--sand)!important;color:var(--bark)!important}
.tb.btn:hover svg{opacity:1}
.tb.success{background:var(--green-light)!important;border-color:rgba(74,124,89,.2)!important;color:var(--green)!important}
.tb.success:hover{filter:brightness(.93)}
.tb.danger{color:var(--red)!important}
.tb.danger:hover{background:var(--red-light)!important;border-color:rgba(139,58,58,.25)!important}
/* legacy aliases */
.pill,.icon-btn{box-sizing:border-box}

/* ── Main 2-col grid ── */
.main{display:flex;flex:1;min-height:0;gap:0}

/* ── Left sidebar ── */
.sidebar{
  width:320px;flex-shrink:0;
  background:var(--card-bg);
  backdrop-filter:var(--blur);
  -webkit-backdrop-filter:var(--blur);
  border-right:1px solid var(--border);
  overflow-y:auto;padding:18px 16px;
  display:flex;flex-direction:column;gap:14px;
  touch-action:pan-y;
}

/* Card */
.card{
  background:var(--card-bg);
  border:1px solid var(--border);
  border-radius:var(--radius);
  padding:14px 15px;
}
.card-title{
  font-size:.67rem;font-weight:600;
  text-transform:uppercase;letter-spacing:.07em;
  color:var(--text3);margin-bottom:11px;
}

/* Field */
.field{display:flex;flex-direction:column;gap:4px;min-width:0}
.field label{font-size:.72rem;color:var(--text3);font-weight:500}
.field-row{display:grid;grid-template-columns:1fr 1fr;gap:6px}
.field-inset{display:flex;flex-direction:column;gap:3px}
.field-inset label{
  font-size:.58rem;color:var(--text3);text-transform:uppercase;letter-spacing:.04em;
  pointer-events:none;line-height:1;padding-left:2px;
}
.field-inset input,.field-inset select{
  padding:8px 10px;font-size:.9rem;
  border-radius:10px;
}

input[type=text],input[type=password],input[type=number],select{
  padding:8px 11px;
  background:var(--input-bg,rgba(255,248,243,0.8));
  border:1px solid var(--border);
  border-radius:10px;
  color:var(--text);
  font-size:.85rem;
  font-family:'DM Sans',sans-serif;
  width:100%;
  transition:border-color .18s;
  -webkit-appearance:none;appearance:none;
}
input:focus,select:focus{outline:none;border-color:#FF8234;background:var(--card-bg)}
input[type=number]::-webkit-inner-spin-button,
input[type=number]::-webkit-outer-spin-button{-webkit-appearance:none;margin:0}
input[type=number]{-moz-appearance:textfield}

/* Slider */
.slider-wrap{display:flex;align-items:center;gap:10px;min-width:0;overflow:hidden}
.slider-wrap input[type=range]{
  flex:1;min-width:0;height:3px;accent-color:var(--bark);
  -webkit-appearance:none;appearance:none;
  background:linear-gradient(to right, var(--bark) 0%, var(--bark) var(--pct,50%), var(--cream3) var(--pct,50%), var(--cream3) 100%);
  border-radius:2px;cursor:pointer;
}
.slider-wrap input[type=range]::-webkit-slider-thumb{
  -webkit-appearance:none;width:16px;height:16px;
  border-radius:50%;background:var(--cream);
  border:2px solid var(--bark);
  box-shadow:0 1px 4px rgba(92,79,67,.2);
  transition:transform .15s;
}
.slider-wrap input[type=range]::-webkit-slider-thumb:hover{transform:scale(1.2)}
.slider-val{
  min-width:36px;text-align:right;
  font-size:.82rem;font-weight:600;
  color:var(--bark);font-family:'DM Mono',monospace;
}

/* Toggle */
.toggle-wrap{
  display:none;
}
.toggle-wrap:hover{background:var(--cream2)}
.toggle-text{flex:1;font-size:.82rem;color:var(--text2)}
.toggle-text small{display:block;font-size:.68rem;color:var(--text3);margin-top:1px}
.toggle-sw{
  width:38px;height:22px;border-radius:11px;
  background:var(--cream3);border:1.5px solid var(--sand);
  position:relative;transition:all .22s;flex-shrink:0;
}
.toggle-sw::after{
  content:'';position:absolute;
  width:14px;height:14px;border-radius:50%;
  background:var(--cream);top:2px;left:2px;
  box-shadow:0 1px 3px rgba(0,0,0,.15);
  transition:left .22s;
}
.toggle-sw.on{background:var(--bark);border-color:var(--bark)}
.toggle-sw.on::after{left:20px}

/* Divider */
.div{height:1px;background:var(--border2);margin:2px 0}

/* ── Primary button ── */
.btn-primary{
  width:100%;padding:11px 16px;
  background:#FF8234;
  border:none;border-radius:var(--radius-sm);
  color:#fff;font-size:.9rem;font-weight:600;
  font-family:'DM Sans',sans-serif;
  cursor:pointer;letter-spacing:-.01em;
  box-shadow:0 4px 16px rgba(255,130,52,.3),inset 0 1px 0 rgba(255,255,255,.15);
  transition:all .18s ease;
  display:flex;align-items:center;justify-content:center;gap:7px;
}
.btn-primary:hover:not(:disabled){
  background:#e86d1e;
  box-shadow:0 6px 20px rgba(255,130,52,.38);transform:translateY(-1px);
}
.btn-primary:disabled{opacity:.45;cursor:not-allowed;transform:none}

/* Secondary / ghost */
.btn-ghost{
  width:100%;padding:9px 16px;
  background:transparent;
  border:1.5px solid var(--border);
  border-radius:var(--radius-sm);
  color:var(--text2);font-size:.85rem;font-weight:500;
  font-family:'DM Sans',sans-serif;
  cursor:pointer;
  display:flex;align-items:center;justify-content:center;gap:7px;
  transition:all .18s;
}
.btn-ghost:hover{background:var(--cream2);border-color:var(--sand2);color:var(--bark)}
.btn-ghost.red{border-color:rgba(139,58,58,.3);color:var(--red)}
.btn-ghost.red:hover{background:var(--red-light);border-color:rgba(139,58,58,.4)}
.btn-ghost.green2{border-color:rgba(74,124,89,.3);color:var(--green)}
.btn-ghost.green2:hover{background:var(--green-light);border-color:rgba(74,124,89,.4)}

/* Action buttons row */
.action-row{display:flex;gap:7px}
.action-row .btn-ghost{flex:1}
#restartBtnMob{display:none}

/* Progress */
.prog-wrap{display:flex;flex-direction:column;gap:5px}
.prog-track{background:var(--cream3);border-radius:3px;height:4px;overflow:hidden}
.prog-fill{height:100%;background:linear-gradient(90deg,var(--warm),var(--bark));border-radius:3px;width:0%;transition:width .4s ease}
.prog-meta{display:flex;justify-content:space-between;font-size:.68rem;color:var(--text3)}
.prog-param{font-size:.68rem;color:var(--text3);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}

/* Best stats */
.stats-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:6px}
.stat-cell{
  background:var(--cream2);
  border:1px solid var(--border2);
  border-radius:10px;padding:9px 6px;text-align:center;
  transition:background .3s, border-color .3s;
  position:relative;overflow:hidden;
}
.stat-cell.good{background:#edf3e0;border-color:rgba(110,138,62,.25)}
.stat-cell.bad{background:var(--red-light);border-color:rgba(139,40,40,.2)}
.stat-cell.warn{background:var(--yellow-light);border-color:rgba(122,90,32,.2)}
@keyframes stat-flash{0%{opacity:.5;transform:scale(.97)}100%{opacity:1;transform:scale(1)}}
.stat-cell.flash{animation:stat-flash .28s ease-out}
.stat-v{font-size:.92rem;font-weight:700;color:var(--bark);font-family:'DM Mono',monospace;line-height:1}
.stat-cell.good .stat-v{color:#6e8a3e}
.stat-cell.bad .stat-v{color:var(--red)}
.stat-cell.warn .stat-v{color:var(--yellow)}
.stat-l{font-size:.58rem;color:var(--text3);margin-top:3px;text-transform:uppercase;letter-spacing:.04em}

/* ── Telegram field ── */
.tg-grid{display:flex;flex-direction:column;gap:7px}
.tg-row{display:flex;gap:7px}
.tg-row input{flex:1}
.btn-tg-test{
  padding:0 14px;
  background:rgba(74,101,128,.1);
  border:1px solid rgba(74,101,128,.2);
  border-radius:10px;color:var(--blue);
  font-size:.75rem;font-weight:500;
  cursor:pointer;white-space:nowrap;
  transition:all .18s;
}
.btn-tg-test:hover{background:var(--blue-light);border-color:rgba(74,101,128,.35)}

/* ── Right panel ── */
.right{flex:1;display:flex;flex-direction:column;min-height:0;overflow:hidden}

/* Top strip: cycles cards LEFT + logs RIGHT, single row */
.top-strip{
  display:flex;flex-direction:row;min-height:0;
  border-bottom:1px solid var(--border2);
  flex-shrink:0;
  height:auto;
  min-height:90px;
  max-height:160px;
}
.cycles-col{
  flex-shrink:0;
  display:flex;flex-direction:column;
  padding:8px 14px 8px;
  border-right:1px solid var(--border2);
  gap:5px;min-width:0;
  max-width:50%;
}
.cycles-col-header{
  display:flex;align-items:center;justify-content:space-between;gap:6px;flex-shrink:0;
}
.log-col{
  flex:1;display:flex;flex-direction:column;min-width:0;overflow:hidden;
}
.log-col-header{
  display:flex;align-items:center;justify-content:space-between;
  padding:6px 12px 4px;flex-shrink:0;
  font-size:.65rem;color:var(--text3);font-weight:600;
  text-transform:uppercase;letter-spacing:.06em;
}

/* Chart area — fills all remaining space */
.chart-area{
  flex:1;display:flex;flex-direction:column;min-height:0;overflow:hidden;position:relative;
  background:var(--cream);
}
.chart-placeholder{
  flex:1;display:flex;align-items:center;justify-content:center;
  flex-direction:column;gap:8px;
  color:var(--text3);font-size:.78rem;
  background:var(--cream);
}
#chartFrame{
  width:100%;height:100%;flex:1;border:none;display:none;min-height:0;
  background:var(--cream);
}

/* Best combination table — inside log-col below log */
#top20Wrap.in-strip{
  border-top:1px solid var(--border2);
  flex-shrink:0;
}
#top20Wrap.in-strip .table-hdr{padding:5px 12px;font-size:.62rem;}
#top20Wrap.in-strip table{font-size:.72rem;}
#top20Wrap.in-strip th,#top20Wrap.in-strip td{padding:4px 8px;}

/* Cycles strip */
.cycles-bar{display:none} /* legacy — replaced by cycles-col */
.cycles-label{font-size:.65rem;font-weight:600;text-transform:uppercase;letter-spacing:.07em;color:var(--text3)}
.cc-strip{display:flex;gap:6px;flex-wrap:nowrap;overflow-x:auto;overflow-y:hidden;padding-bottom:2px;flex:1;align-items:flex-start;}
.sym-btn{font-size:.65rem;font-weight:600;padding:3px 9px;border-radius:20px;border:1.5px solid var(--border2);background:var(--cream3);color:var(--text2);cursor:pointer;transition:all .15s;white-space:nowrap}
.sym-btn.active{background:var(--bark);color:var(--cream);border-color:var(--bark)}
.sym-btn.running{animation:cc-glow 1.6s ease-in-out infinite}
.sym-card{min-width:120px;max-width:160px;padding:9px 10px;border-radius:10px;border:1.5px solid var(--border2);background:var(--glass);position:relative;overflow:hidden;cursor:pointer;transition:border-color .2s}
.sym-card:hover{border-color:var(--sand2)}
.sym-card.active{border-color:#FF8234}
.sym-card.pos{border-color:rgba(74,124,89,.4);background:var(--green-light)}
.sym-card.neg{border-color:rgba(139,58,58,.3);background:var(--red-light)}
.sym-card.running{border-color:rgba(92,79,67,.3);animation:cc-glow 1.6s ease-in-out infinite}
.sym-name{font-size:.58rem;font-weight:700;color:var(--text3);text-transform:uppercase;letter-spacing:.05em;margin-bottom:2px}
.sym-eq{font-size:1rem;font-weight:700;font-family:"DM Mono",monospace;color:var(--bark)}
.sym-eq.pos{color:var(--green)}.sym-eq.neg{color:var(--red)}
.sym-meta{font-size:.58rem;color:var(--text3);margin-top:2px;line-height:1.4}
.cc-strip::-webkit-scrollbar{height:3px}
.cc-strip::-webkit-scrollbar-thumb{background:var(--cream3);border-radius:2px}

.cc{
  flex-shrink:0;width:96px;height:76px;
  background:var(--glass2);
  border:1px solid var(--border);
  border-radius:12px;padding:7px 9px;
  position:relative;overflow:hidden;
  transition:all .2s;
  display:flex;flex-direction:column;justify-content:space-between;
}
.cc.running{border-color:rgba(92,79,67,.3);animation:cc-glow 1.6s ease-in-out infinite}
.cc.pos{border-color:rgba(74,124,89,.3);background:var(--green-light)}
.cc.neg{border-color:rgba(139,58,58,.3);background:var(--red-light)}
@keyframes cc-glow{0%,100%{box-shadow:0 0 0 rgba(92,79,67,0)}50%{box-shadow:0 0 12px rgba(92,79,67,.15)}}
.cc-n{font-size:.6rem;color:var(--text3);font-weight:600;text-transform:uppercase;letter-spacing:.05em}
.cc-eq{font-size:1.05rem;font-weight:700;font-family:'DM Mono',monospace;line-height:1.15;color:var(--bark)}
.cc-eq.pos{color:var(--green)}.cc-eq.neg{color:var(--red)}.cc-eq.run{color:var(--bark)}
.cc-d{font-size:.62rem;font-weight:600;margin-top:1px}
.cc-d.pos{color:var(--green)}.cc-d.neg{color:var(--red)}.cc-d.flat{color:var(--text3)}
.cc-m{font-size:.6rem;color:var(--text3);margin-top:2px;line-height:1.4}
.cc-bar{position:absolute;bottom:0;left:0;height:2.5px;background:var(--green);transition:width .5s ease;border-radius:0 2px 0 0}
.cc-bar.neg{background:var(--red)}

/* Log area */
.log-area{flex:1;overflow-y:auto;padding:12px 18px;display:flex;flex-direction:column;gap:3px;min-height:0}
.log-area::-webkit-scrollbar{width:4px}
.log-area::-webkit-scrollbar-thumb{background:var(--cream3);border-radius:2px}

.log-line{
  font-size:.73rem;font-family:'DM Mono',monospace;
  color:var(--text3);line-height:1.6;padding:1px 0;
}
.log-line.ok{color:var(--bark)}
.log-line.found{color:var(--green)}
.log-line.error{color:var(--red)}
.log-line.warn{color:var(--yellow)}
.log-line.info{color:var(--text3)}

.activity-line{
  font-size:.72rem;font-family:'DM Mono',monospace;
  color:var(--text3);padding:3px 0;
  display:flex;align-items:center;gap:6px;
}
.spin{animation:spin .9s linear infinite;display:inline-block}
@keyframes spin{to{transform:rotate(360deg)}}

/* ── Bottom: Top-7 table ── */
.table-panel{
  border-top:1px solid var(--border);
  flex-shrink:0;max-height:220px;overflow-y:auto;
}
.table-panel::-webkit-scrollbar{width:4px}
.table-panel::-webkit-scrollbar-thumb{background:var(--cream3);border-radius:2px}
.table-hdr{
  padding:10px 18px 8px;
  font-size:.68rem;font-weight:600;
  text-transform:uppercase;letter-spacing:.07em;
  color:var(--text3);background:var(--glass);
  border-bottom:1px solid var(--border2);
  position:sticky;top:0;z-index:2;
  display:flex;align-items:center;justify-content:space-between;
}

table{width:100%;border-collapse:collapse;font-size:.72rem}
thead th{
  padding:7px 10px;text-align:left;
  color:var(--text3);font-weight:500;
  font-size:.67rem;
  background:var(--cream);
  border-bottom:1px solid var(--border2);
  position:sticky;top:0;white-space:nowrap;
}
tbody td{padding:7px 10px;border-bottom:1px solid var(--border2);color:var(--text2);font-family:'DM Mono',monospace;font-size:.7rem}
tbody tr:hover td{background:var(--cream)}
tbody tr:first-child td{color:var(--bark);font-weight:600}

/* Params collapse */
.params-toggle{
  font-size:.7rem;color:var(--text3);cursor:pointer;
  padding:5px 0;display:flex;align-items:center;gap:4px;
  transition:color .15s;
}
.params-toggle:hover{color:var(--bark)}
.params-box{
  display:none;margin-top:5px;padding:9px 11px;
  background:var(--cream);border:1px solid var(--border2);
  border-radius:10px;font-size:.68rem;font-family:'DM Mono',monospace;
  line-height:1.9;max-height:140px;overflow-y:auto;color:var(--text2);
}
.params-box::-webkit-scrollbar{width:3px}
.params-box::-webkit-scrollbar-thumb{background:var(--cream3);border-radius:2px}
.params-box span{color:var(--text3)}

/* Alert status */
.alert-msg{font-size:.71rem;padding:4px 0;line-height:1.5;color:var(--text3)}
.alert-msg.ok{color:var(--green)}
.alert-msg.err{color:var(--red)}

/* ── Details (Telegram) ── */
details summary{
  cursor:pointer;list-style:none;
  display:flex;align-items:center;gap:6px;
  font-size:.75rem;font-weight:600;
  text-transform:uppercase;letter-spacing:.05em;
  color:var(--text3);padding:4px 0;
  transition:color .15s;
}
details summary:hover{color:var(--bark)}
details summary::before{content:'›';font-size:1rem;transition:transform .2s}
details[open] summary::before{transform:rotate(90deg)}
details summary::-webkit-details-marker{display:none}

/* ── Responsive mobile ── */
@media(max-width:700px){
  /* Шапка — скрыта */
  .topbar{display:none}

  /* Весь интерфейс — flex-колонка, СКРОЛЛИТСЯ */
  html,body{overflow:auto !important;height:auto !important;min-height:100dvh !important;touch-action:pan-y !important;overscroll-behavior:auto !important}
  .app{height:auto !important;min-height:100dvh;overflow:visible !important;display:flex;flex-direction:column}
  .main{flex-direction:column;flex:1;min-height:0;overflow:visible !important}

  /* ── САЙДБАР: вся ширина, без overflow:hidden ── */
  .sidebar{
    width:100%;border-right:none;border-bottom:1px solid var(--border);
    padding:10px 12px;gap:8px;
    overflow:visible;
    flex-shrink:0;
    touch-action:pan-y;
  }

  /* Карточка настроек */
  .card{padding:10px 12px}
  .card-title{display:none}
  .field-row{gap:6px;margin-bottom:6px !important}
  .field label{font-size:.68rem}
  input[type=text],input[type=number],select{padding:9px 11px;font-size:.88rem}

  /* Слайдеры */
  .slider-wrap{gap:6px}
  .slider-val{min-width:28px;font-size:.78rem}
  .field .slider-wrap{margin-top:0}
  .field>label{margin-bottom:1px;line-height:1.2}

  /* Прогресс — показываем (важно на мобиле!) */
  .prog-wrap{display:flex !important}
  .prog-meta{font-size:.65rem}
  .prog-param{font-size:.62rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}

  /* Кнопки */
  .btn-primary{padding:12px 14px;font-size:.92rem}
  .btn-ghost{padding:10px 10px;font-size:.82rem}
  .action-row{gap:5px}
  #restartBtnMob{display:flex !important}
  #swStopBtn{display:none !important}

  /* Скрываем не нужные элементы */
  #infiniteRow{display:none}
  #bestSection{display:none !important}
  #validSection{display:block !important}
  #mob-best-row{display:none !important}
  .sidebar details{display:none}
  .sidebar .div{display:none}

  /* ── Недавние конфиги — свёрнуты по умолчанию на мобиле ── */
  #recentBody{max-height:0px}
  #recentArrow{transform:rotate(0deg)}

  /* ── ПРАВАЯ ПАНЕЛЬ ── */
  .right{flex:1;min-height:0;overflow:visible;display:flex;flex-direction:column}

  /* Top strip — вертикально, без overflow:hidden */
  .top-strip{flex-direction:column;height:auto;max-height:none;flex:none;overflow:visible;}
  .cycles-col{max-width:100%;border-right:none;border-bottom:1px solid var(--border2);padding:6px 10px;overflow:visible;flex-shrink:0;}
  .cc-strip{flex-wrap:nowrap;overflow-x:auto;-webkit-overflow-scrolling:touch;touch-action:pan-x;}
  .log-col{flex:1;min-height:0;overflow:visible;}
  .log-area{min-height:120px;max-height:200px;overflow-y:auto;touch-action:pan-y;}

  /* График */
  .chart-area{height:260px;flex:none;}
  #chartFrame{height:260px;min-height:0;display:block;}

  /* Циклы — компактная лента */
  .cycles-bar{padding:6px 10px 4px;flex-shrink:0}
  .cycles-label{display:none}
  .cc{width:86px;padding:7px 9px}
  .cc-eq{font-size:.92rem}
  .cc-n{font-size:.58rem}

  /* Мобильные кнопки Топ / Логи */
  #mob-top-toggle{display:flex !important;flex-shrink:0}

  /* На мобиле осветляем тёмный график */
  #chartFrame{filter:brightness(1.35) contrast(0.92);}

  /* Таблица топ — обычный блок, скроллится вместе со страницей */
  #top20Wrap{
    display:none;
    position:static;
    max-height:none;
    background:var(--cream);
    z-index:auto;
  }
  .table-panel{max-height:none;overflow:visible;}
}
</style></head><body>

<div class="app">

<!-- ── Topbar ── -->
<header class="topbar">
  <div class="topbar-logo">
    <span class="dot-live" id="apidot2"></span>
    WickFill <span style="font-weight:300;color:var(--text3)">Optimizer</span>
    <span style="font-size:.72rem;font-weight:400;color:var(--text3)" id="versionSpan">v</span>
  </div>
  <div class="topbar-spacer"></div>
  <div class="topbar-meta">
    <span class="tb" id="speedPill" style="display:none">
      <svg width="10" height="10" viewBox="0 0 12 12" fill="currentColor"><polygon points="6,1 7.5,5 12,5 8.5,7.5 9.8,12 6,9 2.2,12 3.5,7.5 0,5 4.5,5" opacity=".75"/></svg>
      <span id="speedPillText">—</span>
    </span>
    <span id="statusBadge2"></span>
    <span class="tb green" id="swBadge" style="display:none">
      <svg width="11" height="11" viewBox="0 0 14 14" fill="none"><path d="M2 7a5 5 0 009.5 2" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><path d="M2 7a5 5 0 019.5-2" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>
      SW
    </span>
    <span class="tb btn" id="latencyPill" onclick="checkApi()" title="Задержка API Gate.io">
      <svg width="10" height="10" viewBox="0 0 12 12" fill="none"><circle cx="6" cy="6" r="5" stroke="currentColor" stroke-width="1.4"/><path d="M6 3v3.5l2 1.5" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"/></svg>
      <span id="latencyText">— мс</span>
    </span>
    <button class="tb btn" id="themeBtn" onclick="toggleTheme()" title="Тема">
      <svg width="12" height="12" viewBox="0 0 14 14" fill="currentColor"><path d="M7 1a6 6 0 100 12A6 6 0 007 1zm0 1.5A4.5 4.5 0 117 11V2.5z"/></svg>
      Тема
    </button>
    <button class="tb btn success" onclick="termuxUpdate()" title="Перезапустить скрипт с GitHub">
      <svg width="11" height="11" viewBox="0 0 14 14" fill="none"><path d="M2 7a5 5 0 009.5-2" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><path d="M12 2l-.5 3-3-.5" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>
      Restart
    </button>
  </div>
</header>

<!-- ── Main ── -->
<div class="main">

  <!-- ── Sidebar ── -->
  <aside class="sidebar">

    <!-- Recent configs quick-select -->
    <div id="recentPanel" style="display:none;margin-bottom:8px;border-radius:10px;background:var(--glass2);border:1px solid var(--border2);overflow:hidden">
      <div onclick="var b=document.getElementById('recentBody');var a=document.getElementById('recentArrow');var open=b.style.maxHeight!=='0px';b.style.maxHeight=open?'0px':'260px';a.style.transform=open?'rotate(0deg)':'rotate(180deg)'" style="display:flex;align-items:center;gap:6px;padding:8px 10px;cursor:pointer;user-select:none">
        <span style="font-size:.68rem;font-weight:600;letter-spacing:.06em;color:var(--text3);text-transform:uppercase;flex:1">Недавние конфиги</span>
        <span id="recentArrow" style="font-size:.65rem;color:var(--text3);transition:transform .2s;transform:rotate(180deg)">▼</span>
      </div>
      <div id="recentBody" style="max-height:600px;overflow-y:auto;transition:max-height .3s ease;padding:0 6px 6px">
        <div id="recentList" style="display:flex;flex-direction:column;gap:4px"></div>
      </div>
    </div>

    <!-- Settings card -->
    <div class="card">
      <div class="field-row" style="margin-bottom:6px;align-items:flex-end">
        <div class="field-inset" style="flex:3">
          <label>Символы (через запятую)</label>
          <input type="text" id="wf_symbol" value="DOGE" placeholder="BTC, ETH, SOL" style="width:100%">
        </div>
        <div class="field-inset" style="flex:1">
          <label>Стоп мин (%)</label>
          <input type="number" id="wf_sl_min" min="0.1" max="5" step="0.1" value="0.4" style="width:100%">
        </div>
        <div class="field-inset" style="flex:1">
          <label>Стоп макс (%)</label>
          <input type="number" id="wf_sl_max" min="0.1" max="10" step="0.1" value="0.8" style="width:100%">
        </div>
        <div class="field-inset" style="flex:1">
          <label>Тейк мин (%)</label>
          <input type="number" id="wf_tp_min" min="0.1" max="5" step="0.1" value="0.5" style="width:100%">
        </div>
        <div class="field-inset" style="flex:1">
          <label>Тейк макс (%)</label>
          <input type="number" id="wf_tp_max" min="0.1" max="20" step="0.1" value="2.0" style="width:100%">
        </div>
      </div>
      <div class="field-row" style="margin-bottom:0">
        <div class="field-inset" style="flex:3">
          <label>Таймфрейм</label>
          <select id="wf_tf_sel">
            <option value="5m">5m</option>
            <option value="15m">15m</option>
            <option value="30m">30m</option>
            <option value="1h" selected>1h</option>
            <option value="4h">4h</option>
            <option value="1d">1d</option>
          </select>
        </div>
        <div class="field-inset" style="flex:1">
          <label>История (дни)</label>
          <input type="number" id="wf_days" min="3" max="90" placeholder="дни" step="1" value="20" style="width:100%">
        </div>
      </div>
      <input type="hidden" id="wf_risk" value="10">
    </div>

    <!-- Бесконечный режим всегда включён -->

    <!-- Progress (hidden by default) -->
    <div class="prog-wrap" id="progWrap" style="display:none">
      <div class="prog-meta">
        <span id="progLabel" style="color:var(--bark);font-size:.72rem;font-weight:500">Запуск...</span>
        <span id="progTime">0с</span>
      </div>
      <div class="prog-track"><div class="prog-fill" id="progBar"></div></div>
      <div class="prog-param" id="progParam"></div>
    </div>

    <!-- Main action buttons: Старт + Эко на одной строке; Стоп заменяет Старт -->
    <div style="display:flex;align-items:stretch;gap:8px;margin-bottom:4px">
      <button class="btn-primary" id="wfBtn" onclick="startOpt()" style="flex:1;min-width:0">
        <span>🔍</span> Запустить оптимизацию
      </button>
      <button class="btn-ghost red" id="wfStopBtn" style="display:none;flex:1;min-width:0" onclick="stopOpt()">
        <svg width="11" height="11" viewBox="0 0 12 12" fill="currentColor" style="flex-shrink:0"><rect x="1" y="1" width="10" height="10" rx="2"/></svg> Стоп
      </button>
      <!-- Eco toggle compact -->
      <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;gap:3px;padding:6px 10px;border-radius:10px;background:var(--glass2);border:1px solid var(--border2);cursor:pointer;user-select:none;flex-shrink:0" onclick="var c=document.getElementById('ecoModeChk');c.checked=!c.checked;fetch('/set_eco?v='+(c.checked?'1':'0'));var sw=document.getElementById('ecoSw');sw.classList.toggle('on',c.checked)" title="1 ядро + паузы между итерациями. Меньше нагрев, дольше работает.">
        <input type="checkbox" id="ecoModeChk" style="display:none">
        <div id="ecoSw" class="toggle-sw" style="display:block"></div>
        <div style="font-size:.62rem;color:var(--text3);margin-top:2px">🍃 Эко</div>
      </div>
    </div>

    <div class="action-row">
      <button class="btn-ghost" id="swStopBtn" style="display:none" onclick="stopSW()">
        <svg width="11" height="11" viewBox="0 0 12 12" fill="currentColor" style="flex-shrink:0"><rect x="1" y="1" width="10" height="10" rx="2"/></svg> SW
      </button>
      <button class="btn-ghost success" id="restartBtnMob" onclick="termuxUpdate()" title="pkill → cp → python screener_pro.py из Downloads">↺ Restart</button>
    </div>

    <!-- Best result (desktop) -->
    <div id="mob-best-row" style="display:none;align-items:center;gap:8px;flex-wrap:wrap;padding:6px 2px;border-radius:10px;background:var(--glass2);border:1px solid var(--border2)">
      <span id="mob-eq" style="font-weight:700;font-family:'DM Mono',monospace;font-size:1rem;color:var(--green);padding:0 8px">—</span>
      <span id="mob-wr" style="font-size:.78rem;color:var(--text2)">WR —</span>
      <span id="mob-dd" style="font-size:.78rem;color:var(--text2)">DD —</span>
      <span id="mob-tr" style="font-size:.78rem;color:var(--text3)">— сд</span>
      <span id="mob-sl" style="font-size:.78rem;color:var(--text3)">SL —</span>
      <span id="mob-tp" style="font-size:.78rem;color:var(--text3)">TP —</span>
      <span style="flex:1"></span>
    </div>


    <!-- Best result (desktop) -->
    <div id="bestSection" style="display:none">
      <div class="div"></div>
      <div class="card-title" style="margin-bottom:8px">Лучший результат</div>
      <div class="stats-grid" id="bestGrid"></div>
      <div id="bestParamsWrap" style="display:none;margin-top:8px">
        <div id="bestParams" style="font-size:.75rem;color:var(--text2);line-height:1.7"></div>
      </div>
    </div>
    <div id="validSection" style="display:none"></div>

    <div class="div"></div>

    <!-- Telegram alerts -->
    <details>
      <summary>🔔 Telegram алерты</summary>
      <div style="margin-top:10px" class="tg-grid">
        <div class="field">
          <label>Токен бота</label>
          <input type="text" id="al_tg_token" placeholder="123456:AAF..." value="">
        </div>
        <div class="field">
          <label>Chat ID</label>
          <div class="tg-row">
            <input type="text" id="al_tg_chat" placeholder="123456789" value="">
            <button class="btn-tg-test" id="testMailBtn" onclick="sendTestEmail()">Тест</button>
          </div>
        </div>
        <div class="field" style="margin-top:6px">
          <label>ntfy.sh топик <span style="font-weight:400;color:var(--text3)">(резерв)</span></label>
          <div class="tg-row">
            <input type="text" id="al_ntfy_topic" placeholder="wickfill_мой_топик">
            <button class="btn-tg-test" onclick="sendTestNtfy()">Тест</button>
          </div>
        </div>
        <div class="alert-msg" id="alertStatusMsg"></div>
      </div>
    </details>

    <div class="div"></div>

    <!-- Gate.io auto-trading -->
    <details id="gateDetails">
      <summary>⚡ Gate.io автоторговля</summary>
      <div style="margin-top:10px" class="tg-grid">
        <div class="field">
          <label>API Key</label>
          <input type="text" id="gate_key" placeholder="вставьте API Key" autocomplete="off">
        </div>
        <div class="field">
          <label>API Secret</label>
          <input type="password" id="gate_secret" placeholder="вставьте API Secret" autocomplete="off">
        </div>
        <div class="field">
          <label>Размер позиции (% от баланса)</label>
          <div class="tg-row">
            <input type="number" id="gate_pct" placeholder="10" min="1" max="100" step="1" value="10" style="width:80px">
            <span style="font-size:.75rem;color:var(--text3);align-self:center">%</span>
            <button class="btn-tg-test" id="gateTestBtn" onclick="testGateConnection()">Тест</button>
          </div>
        </div>
        <!-- TP/SL для автоторговли -->
        <div class="field">
          <label>TP / SL для автосигналов</label>
          <div style="display:flex;gap:6px;align-items:center">
            <span style="font-size:.75rem;color:var(--text3)">TP%</span>
            <input type="number" id="gate_auto_tp_pct" placeholder="из сигнала" min="0.1" max="1000" step="0.1"
              style="width:70px;background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:4px 6px;color:var(--text1);font-size:.8rem">
            <span style="font-size:.75rem;color:var(--text3)">SL%</span>
            <input type="number" id="gate_auto_sl_pct" placeholder="из сигнала" min="0.1" max="1000" step="0.1"
              style="width:70px;background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:4px 6px;color:var(--text1);font-size:.8rem">
            <span style="font-size:.65rem;color:var(--text3);line-height:1.2">пусто =<br>из сигнала</span>
          </div>
        </div>
        <!-- Галочка автоторговли -->
        <div style="display:flex;align-items:center;gap:8px;margin-top:4px;padding:8px;background:var(--bg2);border-radius:8px;border:1px solid var(--border)">
          <input type="checkbox" id="gate_auto_enabled" style="width:18px;height:18px;accent-color:var(--green);cursor:pointer" onchange="getAlertCfg();saveAlertCfg&&saveAlertCfg()">
          <label for="gate_auto_enabled" style="cursor:pointer;font-size:.85rem;color:var(--text1);font-weight:600">
            🤖 Автоторговля по сигналам
          </label>
          <span id="gate_auto_status" style="margin-left:auto;font-size:.7rem;color:var(--text3)">выкл</span>
        </div>
        <div class="alert-msg" id="gateStatusMsg"></div>
        <div style="font-size:.7rem;color:var(--text3);margin-top:4px;line-height:1.4">
          Плечо из стратегии. TP/SL: если % не заданы — берётся из сигнала (ценовая шкала).<br>
          При новом сигнале старая позиция закрывается.
        </div>
        <div style="display:flex;gap:6px;margin-top:8px;align-items:center">
          <label style="font-size:.7rem;color:var(--text3);white-space:nowrap">TP $</label>
          <input id="gate_tp" type="number" placeholder="авто 10%" step="0.1"
            style="flex:1;background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:4px 6px;color:var(--text1);font-size:.8rem;width:0">
          <label style="font-size:.7rem;color:var(--text3);white-space:nowrap">SL $</label>
          <input id="gate_sl" type="number" placeholder="авто 10%" step="0.1"
            style="flex:1;background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:4px 6px;color:var(--text1);font-size:.8rem;width:0">
        </div>
        <div style="display:flex;gap:6px;margin-top:6px">
          <button class="btn-tg-test" style="flex:1;background:rgba(58,125,82,0.15);color:var(--green);border-color:var(--green)" onclick="gateTestTrade(1)">🔵 Лонг $5×5</button>
          <button class="btn-tg-test" style="flex:1;background:rgba(160,48,48,0.12);color:#c0514a;border-color:#c0514a" onclick="gateTestTrade(-1)">🔴 Шорт $5×5</button>
        </div>
      </div>
    </details>

  </aside>

  <!-- ── Right panel ── -->
  <div class="right">

    <!-- Top strip: cycles | logs -->
    <div class="top-strip">

      <!-- Cycles column -->
      <div class="cycles-col">
        <div class="cycles-col-header">
          <span class="cycles-label" id="ccLabel">Циклы</span>
          <span id="swStatus2" style="font-size:.65rem;color:var(--text3)"></span>
        </div>
        <div class="cc-strip" id="ccStrip"></div>
      </div>

      <!-- Log column -->
      <div class="log-col">
        <div class="log-col-header">Логи</div>
        <div class="log-area" id="wfLog" style="flex:1;padding:4px 12px 6px;"></div>
      </div>

    </div><!-- /top-strip -->

    <!-- Best combination table (below top-strip) -->
    <div class="table-panel in-strip" id="top20Wrap" style="display:none">
      <div class="table-hdr">Лучшая комбинация</div>
      <table>
        <thead>
          <tr>
            <th>Депозит</th><th>WR%</th>
            <th>Сделок</th><th>DD%</th><th>PF</th><th>SL%</th><th>TP%</th><th title="Вход на следующей свече">Вход</th><th title="Риск / Стоп-лосс">Плечо×</th>
          </tr>
        </thead>
        <tbody id="top20Body"></tbody>
      </table>
    </div>

    <!-- Chart — fills remaining space -->
    <div class="chart-area">
      <div id="symSwitcher" style="position:absolute;top:8px;left:50%;transform:translateX(-50%);z-index:20;display:none;gap:4px;flex-wrap:wrap;justify-content:center;pointer-events:auto;background:var(--cream2);border:1px solid var(--border2);border-radius:20px;padding:4px 8px;max-width:90%"></div>
      <div class="chart-placeholder" id="chartPlaceholder">
        <span style="font-size:2rem;opacity:.2">📊</span>
        <span>График появится после первого цикла</span>
      </div>
      <iframe id="chartFrame" src="about:blank"></iframe>
    </div>

  </div><!-- /right -->
</div><!-- /main -->
</div><!-- /app -->

<script>
let polling=null, startTs=0, lastLogCount=0, chartOpened=false;
let _lastChartTs={};   // per-symbol: {sym: timestamp} — чтобы не мигал при смене символа
let _chartFrameLoaded=false;
const infiniteMode=true;
function toggleInfinite(){} // режим всегда бесконечный

/* ── API check ── */
function checkApi(){
  const pill=document.getElementById('latencyPill');
  const txt=document.getElementById('latencyText');
  if(txt) txt.textContent='...'; else if(pill) pill.textContent='...';
  fetch('/ping').then(r=>r.json()).then(d=>{
    if(d.ok){if(txt)txt.textContent=d.ms+'мс';else if(pill)pill.textContent=d.ms+'мс';pill&&pill.classList.toggle('green',true);}
    else{if(txt)txt.textContent=d.error||'err';else if(pill)pill.textContent=d.error||'err';pill&&pill.classList.toggle('green',false);}
  }).catch(()=>{if(txt)txt.textContent='офлайн';else if(pill)pill.textContent='офлайн';pill&&pill.classList.toggle('green',false);});
}
checkApi();setInterval(checkApi,60000);
// Каждую секунду обновляем текст "нет соединения Xs"
setInterval(()=>{ if(_connLost) _setConnStatus(false); },1000);
// Стандартные browser events как доп. триггер
window.addEventListener('online', ()=>{ if(_connLost){ poll(); } });
window.addEventListener('offline', ()=>{
  if(!_connLost){ _connLost=true; _connLostAt=Date.now(); _setConnStatus(false); }
});

// Нормализует символы: "BTC, ETH" → "BTC_USDT, ETH_USDT" (добавляет _USDT если нет суффикса)
function _normalizeSymbols(raw){
  return raw.split(/[,\s]+/).map(s=>{
    s=s.trim().toUpperCase();
    if(!s) return null;
    // Уже содержит пару — оставляем как есть
    if(s.includes('_')) return s;
    return s+'_USDT';
  }).filter(Boolean).join(', ');
}

// Авто-загрузка конфига при вводе поля "История (дни)"
function _tryAutoLoad(){
  const days=parseInt(document.getElementById('wf_days').value);
  if(!days||days<1) return;
  const rawSym=document.getElementById('wf_symbol').value.trim()||'BTC';
  const sym=_normalizeSymbols(rawSym).split(',')[0].trim(); // первый символ для авто-загрузки
  const tf=document.getElementById('wf_tf_sel').value;
  const risk=parseFloat(document.getElementById('wf_risk').value)||10;
  fetch(`/load_result?symbol=${encodeURIComponent(sym)}&tf=${encodeURIComponent(tf)}&days=${days}&risk=${risk}`)
    .then(r=>r.json()).then(d=>{
      if(!d.ok) return; // нет конфига — тихо игнорируем
      window._loadedSeed={best:d.best,top20:d.top20||[],tf:d.tf||tf};
      // Не перезаписываем поле символа — пользователь пишет кратко (BTC), сервер знает полное имя
      // d.tf намеренно НЕ применяем к UI — пользователь выбирает TF сам
      if(d.risk_pct) document.getElementById('wf_risk').value=d.risk_pct;
      if(d.best) renderBest(d.best,d.top20||[]);
      _slStatus(`✓ Авто: $${d.best?.equity?.toFixed(0)} WR${d.best?.winrate?.toFixed(0)}% · ${d.file||''}`,true);
      // Показываем конфиг только если оптимизатор не работает
      if(!polling){ _chartFrameLoaded=false; _loadChartFrame(); }
    }).catch(()=>{});
}
window.addEventListener('DOMContentLoaded', function(){
  const daysEl=document.getElementById('wf_days');
  daysEl.addEventListener('blur', _tryAutoLoad);
  daysEl.addEventListener('keydown', function(e){ if(e.key==='Enter') _tryAutoLoad(); });
  // При смене TF — сбрасываем старый seed и подгружаем конфиг под новый TF
  const tfSel=document.getElementById('wf_tf_sel');
  if(tfSel) tfSel.addEventListener('change', function(){
    window._loadedSeed=null;
    _tryAutoLoad();
  });
  // При смене символа — тоже сбрасываем seed
  const symEl=document.getElementById('wf_symbol');
  if(symEl) symEl.addEventListener('blur', function(){
    window._loadedSeed=null;
    _tryAutoLoad();
  });

  // Восстанавливаем сохранённые ключи
  const _textFields = ['gate_key','gate_secret','gate_pct','gate_auto_tp_pct','gate_auto_sl_pct','al_tg_token','al_tg_chat','al_ntfy_topic'];
  const _checkFields = ['gate_auto_enabled'];
  _textFields.forEach(id => {
    const saved = localStorage.getItem('wf_'+id);
    if(saved) { const el=document.getElementById(id); if(el) el.value=saved; }
  });
  _checkFields.forEach(id => {
    const saved = localStorage.getItem('wf_'+id);
    if(saved !== null) { const el=document.getElementById(id); if(el) el.checked=(saved==='true'); }
  });

  // Авто-сохранение при изменении
  _textFields.forEach(id => {
    const el=document.getElementById(id);
    if(el) el.addEventListener('input', () => localStorage.setItem('wf_'+id, el.value));
  });
  _checkFields.forEach(id => {
    const el=document.getElementById(id);
    if(el) el.addEventListener('change', () => localStorage.setItem('wf_'+id, el.checked));
  });

  // Восстанавливаем параметры последнего запуска (символы, таймфрейм, дни)
  const _runFields = ['wf_symbol','wf_days','wf_sl_min','wf_sl_max','wf_tp_min','wf_tp_max'];
  _runFields.forEach(id => {
    const saved = localStorage.getItem('wf_last_'+id);
    if(saved) { const el=document.getElementById(id); if(el) el.value=saved; }
  });
  const savedTf = localStorage.getItem('wf_last_wf_tf_sel');
  if(savedTf) { const el=document.getElementById('wf_tf_sel'); if(el) el.value=savedTf; }

  // Авто-сохранение параметров запуска при изменении
  _runFields.forEach(id => {
    const el=document.getElementById(id);
    if(el) el.addEventListener('input', () => localStorage.setItem('wf_last_'+id, el.value));
  });
  const tfEl=document.getElementById('wf_tf_sel');
  if(tfEl) tfEl.addEventListener('change', () => localStorage.setItem('wf_last_wf_tf_sel', tfEl.value));
});

function getAlertCfg(){
  const t=document.getElementById('al_tg_token').value.trim();
  const c=document.getElementById('al_tg_chat').value.trim();
  const gk=document.getElementById('gate_key').value.trim();
  const gs=document.getElementById('gate_secret').value.trim();
  const gp=parseFloat(document.getElementById('gate_pct').value)||0;
  const ntfy=document.getElementById('al_ntfy_topic').value.trim();
  const base=(t&&c)?{tg_token:t,tg_chat_id:c}:{};
  if(ntfy) base.ntfy_topic=ntfy;
  const gauto=document.getElementById('gate_auto_enabled')?.checked||false;
  const gtp=parseFloat(document.getElementById('gate_auto_tp_pct')?.value)||0;
  const gsl=parseFloat(document.getElementById('gate_auto_sl_pct')?.value)||0;
  // BUG FIX: Gate работает независимо от заполненности Telegram-полей
  // Раньше если base={} (telegram не заполнен), gate ключи не добавлялись и сделки не открывались
  // Всегда передаём gate ключи в cfg — исполнение контролируется флагом gate_auto_enabled
  if(gk&&gs&&gp>0) Object.assign(base,{gate_key:gk,gate_secret:gs,gate_pct:gp,gate_auto_tp_pct:gtp,gate_auto_sl_pct:gsl,gate_auto_enabled:gauto});
  // Обновляем статус галочки
  const st=document.getElementById('gate_auto_status');
  if(st) st.textContent=gauto&&gk&&gs&&gp>0?'🟢 вкл':'⚪ выкл';
  // Если нет ни telegram ни gate — return null. Если есть хотя бы что-то — возвращаем
  return Object.keys(base).length?base:null;
}

function sendTestNtfy(){
  const topic=document.getElementById('al_ntfy_topic').value.trim();
  if(!topic){alert('Введи ntfy топик');return;}
  const msg=document.getElementById('alertStatusMsg');
  if(msg){msg.textContent='Отправляю...';msg.style.color='';}
  fetch('/test_ntfy',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({ntfy_topic:topic})})
    .then(r=>r.json()).then(d=>{
      if(msg){msg.textContent=d.ok?'✅ ntfy доставлен':'❌ '+d.error;msg.style.color=d.ok?'':'#e05050';}
    }).catch(e=>{if(msg){msg.textContent='❌ '+e;msg.style.color='#e05050';}});
}
function testGateConnection(){
  const gk=document.getElementById('gate_key').value.trim();
  const gs=document.getElementById('gate_secret').value.trim();
  const st=document.getElementById('gateStatusMsg');
  const btn=document.getElementById('gateTestBtn');
  if(!gk||!gs){st.className='alert-msg err';st.textContent='Заполните Key и Secret';return;}
  btn.disabled=true;btn.textContent='...';
  fetch('/gate_test',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({gate_key:gk,gate_secret:gs})})
    .then(r=>r.text()).then(t=>{
      btn.disabled=false;btn.textContent='Тест';
      let d; try{d=JSON.parse(t);}catch(e){st.className='alert-msg err';st.textContent='✕ Сервер: '+t.slice(0,120);return;}
      if(d.ok){st.className='alert-msg ok';st.textContent='✓ Баланс: '+d.balance+' USDT';}
      else{st.className='alert-msg err';st.textContent='✕ '+(d.msg||'ошибка');}
    }).catch(e=>{btn.disabled=false;btn.textContent='Тест';st.className='alert-msg err';st.textContent='✕ '+e;});
}

function gateTestTrade(dir){
  const gk=document.getElementById('gate_key').value.trim();
  const gs=document.getElementById('gate_secret').value.trim();
  const sym=document.getElementById('wf_symbol').value.trim()||'BTC_USDT';
  const st=document.getElementById('gateStatusMsg');
  if(!gk||!gs){st.className='alert-msg err';st.textContent='Заполните Key и Secret';return;}
  const dirStr=dir===1?'лонг':'шорт';
  st.className='alert-msg';st.textContent=`⏳ Открываю тест ${dirStr}...`;
  const tpVal=parseFloat(document.getElementById('gate_tp').value)||null;
  const slVal=parseFloat(document.getElementById('gate_sl').value)||null;
  fetch('/gate_test_trade',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({gate_key:gk,gate_secret:gs,symbol:sym,dir:dir,notional:5,leverage:5,tp_fixed:tpVal,sl_fixed:slVal})})
    .then(r=>r.json()).then(d=>{
      if(d.ok){st.className='alert-msg ok';st.textContent='✓ '+d.msg;}
      else{st.className='alert-msg err';st.textContent='✕ '+(d.msg||'ошибка');}
    }).catch(e=>{st.className='alert-msg err';st.textContent='✕ '+e;});
}

function sendTestEmail(){
  const cfg=getAlertCfg();
  const st=document.getElementById('alertStatusMsg');
  const btn=document.getElementById('testMailBtn');
  if(!cfg){st.className='alert-msg err';st.textContent='Заполните токен и Chat ID';return;}
  btn.disabled=true;btn.textContent='...';
  fetch('/test_email',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({alert_cfg:cfg})})
    .then(r=>r.json()).then(d=>{
      btn.disabled=false;btn.textContent='Тест';
      if(d.ok){st.className='alert-msg ok';st.textContent='✓ Отправлено!';}
      else{st.className='alert-msg err';st.textContent='✕ '+(d.msg||'ошибка');}
    }).catch(e=>{btn.disabled=false;btn.textContent='Тест';st.className='alert-msg err';st.textContent='✕ '+e;});
}

/* ── Save / Load ── */

/* ── Start / Stop ── */
function _slStatus(msg,ok){ /* статус убран из UI, авто-загрузка продолжает работать */ }
// ── Multi-symbol state ──
let _symList=[], _activeChart='', _symStates={};
let _lastLoadedChartSym='';

function _renderSymCards(){
  const strip=document.getElementById('ccStrip');
  // Sort: running first, then by eq descending
  const sorted=[..._symList].sort((a,b)=>{
    const sa=_symStates[a]||{}, sb=_symStates[b]||{};
    if(sa.running&&!sb.running) return -1;
    if(!sa.running&&sb.running) return 1;
    const ea=sa.eq??100, eb=sb.eq??100;
    return eb-ea;
  });
  const risk=parseFloat(document.getElementById('wf_risk')?.value)||10;
  let html='';
  for(const sym of sorted){
    const s=_symStates[sym]||{};
    const eq=s.eq??100, wr=s.wr??0, dd=s.dd??0, tr=s.trades??0;
    const pf=s.pf??0, sl=s.sl??null, tp=s.tp??null;
    const running=s.running||false;
    const hasCycle=s.cycle>0||(eq!==100||wr>0);
    const isPos=eq>100, isNeg=eq<100;
    const isActive=sym===_activeChart;
    const cls='sym-card'+(running?' running':hasCycle?(isPos?' pos':isNeg?' neg':''):'')+(isActive?' active':'');
    const eqCls='sym-eq'+(hasCycle?(isPos?' pos':isNeg?' neg':''):'');
    const symShort=sym.replace('_USDT','').replace('_BTC','').replace('_ETH','');
    // Leverage calc
    const levRaw=sl&&sl>0?Math.round(risk/sl):null;
    const levStr=levRaw?levRaw+'×':'—';
    const levColor=levRaw>50?'var(--red)':levRaw>25?'var(--yellow)':'var(--text3)';
    // Detail lines — always 3 lines for equal card height
    const line1=hasCycle?`WR ${wr.toFixed(0)}% · ${tr}сд${dd>0?' · DD '+dd.toFixed(0)+'%':''}`:'WR — · —сд';
    const line2=sl&&tp?`SL ${sl}% · TP ${tp}%`:'SL — · TP —';
    const line3=sl&&tp?`Риск <b>10%</b> · Плечо <b style="color:${levColor}">${levStr}</b> · PF ${pf>=999?'∞':pf.toFixed(1)}`:'Риск 10% · Плечо — · PF —';
    const cycleStr=running?`<span style="color:var(--green);font-size:.55rem">⟳ Цикл ${s.cycle||'?'}</span>`:(hasCycle?`<span style="font-size:.55rem;color:var(--text3)">Цикл ${s.cycle}</span>`:'<span style="font-size:.55rem;color:var(--text3)">ожидание</span>');
    html+=`<div class="${cls}" onclick="switchChart('${sym}')" title="${sym}" style="min-width:120px;max-width:155px">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:2px">
        <span class="sym-name">${symShort}</span>${cycleStr}
      </div>
      <div class="${eqCls}">$${eq.toFixed(0)}</div>
      <div class="sym-meta">${line1}</div>
      <div class="sym-meta">${line2}</div>
      <div class="sym-meta">${line3}</div>
      <div class="cc-bar ${isPos?'':'neg'}" style="width:100%"></div>
    </div>`;
  }
  strip.innerHTML=html;
}

function _renderSymSwitcher(){
  const el=document.getElementById('symSwitcher');
  if(!el||_symList.length<=1){if(el)el.style.display='none';return;}
  el.style.display='flex';
  el.innerHTML=_symList.map(s=>{
    const st=_symStates[s]||{};
    const running=st.running;
    const eq=st.eq??100;
    const isPos=eq>100,isNeg=eq<100;
    const cls='sym-btn'+(s===_activeChart?' active':'')+(running?' running':'');
    const label=s.replace('_USDT','').replace('_BTC','');
    const eqStr=st.cycle>0?` $${eq.toFixed(0)}`:'';
    return `<button class="${cls}" onclick="switchChart('${s}')">${label}${eqStr}</button>`;
  }).join('');
}

function switchChart(sym){
  _activeChart=sym;
  _renderSymSwitcher();
  _renderSymCards();
  const frame=document.getElementById('chartFrame');
  const ph=document.getElementById('chartPlaceholder');
  const theme=document.documentElement.getAttribute('data-theme')||'light';
  // При переключении символа — всегда полная перезагрузка
  _chartFrameLoaded=false;
  _loadChartFrame(sym);
  _lastChartTs[sym]=(_symStates[sym]||{}).chart_updated_at||0;
}

function startOpt(){
  const rawSym=document.getElementById('wf_symbol').value.trim()||'BTC';
  const sym=_normalizeSymbols(rawSym);
  // Обновляем поле ввода нормализованным значением (без _USDT для читаемости оставляем как есть)
  const tf=document.getElementById('wf_tf_sel').value;
  const days=document.getElementById('wf_days').value;
  const risk=document.getElementById('wf_risk').value;
  const sl_min=parseFloat(document.getElementById('wf_sl_min').value)||0.4;
  const sl_max=parseFloat(document.getElementById('wf_sl_max').value)||0.8;
  const tp_min=parseFloat(document.getElementById('wf_tp_min').value)||0.5;
  const tp_max=parseFloat(document.getElementById('wf_tp_max').value)||2.0;
  // Сохраняем параметры запуска в localStorage
  localStorage.setItem('wf_last_wf_symbol', rawSym);
  localStorage.setItem('wf_last_wf_tf_sel', tf);
  localStorage.setItem('wf_last_wf_days', days);
  localStorage.setItem('wf_last_wf_sl_min', sl_min);
  localStorage.setItem('wf_last_wf_sl_max', sl_max);
  localStorage.setItem('wf_last_wf_tp_min', tp_min);
  localStorage.setItem('wf_last_wf_tp_max', tp_max);
  const alertCfg=getAlertCfg();
  // Используем seed только если он совпадает с текущим tf (защита от устаревшего seed)
  const _rawSeed=window._loadedSeed||null;
  const seed=(_rawSeed&&_rawSeed.tf&&_rawSeed.tf!==tf)?null:_rawSeed;
  if(_rawSeed&&!seed) console.warn('[seed] Сброшен: tf seed='+_rawSeed.tf+' != выбран='+tf);
  const eco=document.getElementById('ecoModeChk')?.checked||false;
  const body=JSON.stringify({wf_symbol:sym,wf_tf:tf,wf_days:days,wf_risk:risk,wf_sl_min:sl_min,wf_sl_max:sl_max,wf_tp_min:tp_min,wf_tp_max:tp_max,infinite:infiniteMode,alert_cfg:alertCfg,seed,eco_mode:eco});
  fetch('/scan',{method:'POST',headers:{'Content-Type':'application/json'},body})
    .then(r=>r.json()).then(d=>{
      if(!d.ok){addLogLine('[!!] '+(d.msg||'Ошибка'),'error');return;}
      // Init multi-symbol state
      _symList=d.symbols||[sym.split(',')[0].trim()];
      _activeChart=_symList[0];
      _symStates={};
      for(const s of _symList) _symStates[s]={eq:100,wr:0,dd:0,trades:0,cycle:0,running:true,chart_updated_at:-1};
      // Set label based on mode
      const ccLabel=document.getElementById('ccLabel');
      if(ccLabel) ccLabel.textContent=_symList.length>1?'Монеты':'Циклы';
      _renderSymCards();
      _renderSymSwitcher();
      lastLogCount=0;chartOpened=false;_lastChartTs={};
      _resetLog();
      document.getElementById('bestSection').style.display='none';
      document.getElementById('top20Wrap').style.display='none';
      const _vs=document.getElementById('validSection');if(_vs){_vs._everShown=false;_vs.style.display='none';}
      const _spEl=document.getElementById('speedPill');if(_spEl){_spEl._lastShown=0;_spEl.style.display='none';}
      document.getElementById('progBar').style.width='0%';
      document.getElementById('progParam').textContent='';
      document.getElementById('swStopBtn').style.display='none';
      document.getElementById('wfBtn').style.display='none';
      document.getElementById('wfStopBtn').style.display='flex';
      document.getElementById('progWrap').style.display='flex';
      const _rp=document.getElementById('recentPanel');if(_rp){_rp.style.display='none';
        const _rb2=document.getElementById('recentBody');if(_rb2)_rb2.style.maxHeight='0px';
        const _ra2=document.getElementById('recentArrow');if(_ra2)_ra2.style.transform='rotate(0deg)';}
      const _cf=document.getElementById('chartFrame');
      const _cp=document.getElementById('chartPlaceholder');
      if(_cf){_cf.style.display='none';_cf.src='about:blank';_chartFrameLoaded=false;}
      if(_cp){_cp.style.display='flex';}
      startTs=Date.now();
      function scheduleNext(){
        let interval;
        if (_connLost) {
          // При потере связи: экспоненциальный backoff, макс 15с
          interval = Math.min(_MIN_POLL_INTERVAL * Math.pow(1.8, Math.min(_connRetryCount, 6)), _MAX_POLL_INTERVAL);
        } else {
          interval = document.hidden ? 5000 : _MIN_POLL_INTERVAL;
        }
        polling=setTimeout(()=>{poll();if(polling!==null)scheduleNext();},interval);
      }
      scheduleNext();
      const st=document.getElementById('alertStatusMsg');
      if(alertCfg){st.className='alert-msg ok';st.textContent='✓ Алерты: chat '+alertCfg.tg_chat_id;}
      else{st.className='alert-msg';st.textContent='Алерты не настроены';}
    }).catch(e=>addLogLine('[!!] '+e,'error'));
}

function stopOpt(){
  fetch('/scan_stop').then(()=>{});
  if(polling){clearTimeout(polling);polling=null;}
  document.getElementById('wfBtn').style.display='';
  document.getElementById('wfStopBtn').style.display='none';
  document.getElementById('swStopBtn').style.display='flex';
  document.getElementById('progWrap').style.display='none';
  if(window._lastTop20&&window._lastTop20.length) renderTop20(window._lastTop20);
  else if(window._lastBest) renderTop20([window._lastBest]);
  _loadRecentConfigs();
  addLogLine('⏹ Остановлен','warn');
}
function stopSW(){
  fetch('/sw_stop').then(()=>{});
  document.getElementById('swStopBtn').style.display='none';
  addLogLine('⏹ Скользящее окно остановлено','warn');
}
function _loadChartFrame(sym){
  const frame=document.getElementById('chartFrame');
  const ph=document.getElementById('chartPlaceholder');
  if(!frame) return;
  // Если офлайн — помечаем что нужна перезагрузка, но не трогаем iframe
  if(_connLost){
    frame._pendingReload=true;
    frame._pendingSym=sym||_activeChart||'';
    return;
  }
  frame._pendingReload=false;
  const theme=document.documentElement.getAttribute('data-theme')||'light';
  const symParam=sym?'&symbol='+encodeURIComponent(sym):'';
  if(!_chartFrameLoaded){
    // Первая загрузка — грузим полный HTML
    _chartFrameLoaded=true;
    _lastLoadedChartSym=sym||'';
    frame.src='/chart?t='+Date.now()+'&theme='+theme+symParam;
    frame.style.display='block';
    if(ph) ph.style.display='none';
  } else {
    // Последующие — шлём только данные через postMessage, не трогаем src
    const url='/chart_data'+(sym?'?symbol='+encodeURIComponent(sym):'');
    fetch(url)
      .then(r=>r.json())
      .then(d=>{
        if(d.candles&&d.signals&&frame.contentWindow){
          frame.contentWindow.postMessage({type:'chart_update',candles:d.candles,signals:d.signals},'*');
        }
      })
      .catch(()=>{});
  }
}

function listConfigs(){
  fetch('/list_configs')
    .then(r=>r.json())
    .then(d=>{
      if(!d.ok){addLogLine('⚠ Конфиги: '+d.msg,'warn');return;}
      if(!d.files||!d.files.length){addLogLine('📂 Конфиги не найдены. Папки проверены: '+d.dirs.join(', '),'warn');return;}
      addLogLine('📂 Найдено конфигов: '+d.files.length,'info');
      d.files.forEach(f=>{
        const size=f.size_kb?` [${f.size_kb} KB]`:'';
        addLogLine(`  • ${f.name}${size} → ${f.dir}`,'info');
      });
    })
    .catch(e=>addLogLine('⚠ Ошибка загрузки конфигов: '+e,'warn'));
}

function updateScript(){
  const btn=document.getElementById('updateBtn');
  const btnMob=document.getElementById('updateBtnMob');
  [btn,btnMob].forEach(b=>{if(b){b.disabled=true;b.textContent='⏳...';}});
  fetch('/update_script',{method:'POST'})
    .then(r=>r.json())
    .then(d=>{
      if(d.ok){
        addLogLine('✅ Скрипт обновлён: '+d.path+' ('+d.size_kb+' KB)','ok');
        [btn,btnMob].forEach(b=>{if(b) b.textContent='✅ Готово';});
      } else {
        addLogLine('❌ Ошибка обновления: '+d.msg,'error');
        [btn,btnMob].forEach(b=>{if(b) b.textContent='❌ Ошибка';});
      }
      setTimeout(()=>{[btn,btnMob].forEach(b=>{if(b){b.disabled=false;b.textContent='⬇ Download';}});},3000);
    })
    .catch(e=>{
      addLogLine('❌ Ошибка: '+e,'error');
      [btn,btnMob].forEach(b=>{if(b){b.disabled=false;b.textContent='⬇ Download';}});
    });
}

/* ── Connection state tracking ── */
let _connLost = false;
let _connLostAt = 0;
let _connRetryCount = 0;
const _MAX_POLL_INTERVAL = 15000;
const _MIN_POLL_INTERVAL = 1500;

function _setConnStatus(online) {
  let ind = document.getElementById('connIndicator');
  if (!ind) {
    ind = document.createElement('div');
    ind.id = 'connIndicator';
    ind.style.cssText = 'position:fixed;bottom:14px;right:14px;z-index:9999;padding:7px 14px;border-radius:20px;font-size:.78rem;font-weight:600;display:none;box-shadow:0 2px 10px rgba(0,0,0,.15);transition:opacity .3s';
    document.body.appendChild(ind);
  }
  if (online) {
    ind.style.display = 'none';
  } else {
    ind.style.background = 'rgba(139,37,8,0.92)';
    ind.style.color = '#fff';
    ind.style.display = 'block';
    const secs = Math.round((Date.now() - _connLostAt) / 1000);
    ind.textContent = '⚡ Нет соединения' + (secs > 5 ? ' (' + secs + 'с)' : '');
  }
}

function _onReconnect() {
  _connLost = false;
  _connRetryCount = 0;
  _setConnStatus(true);
  addLogLine('✅ Соединение восстановлено', 'ok');
  // Перезагрузить график если был офлайн или отложен
  const frame = document.getElementById('chartFrame');
  if (frame && (frame._pendingReload || (frame.style.display !== 'none' && frame.src !== 'about:blank'))) {
    _chartFrameLoaded = false;
    _loadChartFrame(frame._pendingSym || _activeChart || undefined);
  }
}

/* ── Poll ── */
function poll(){
  const useMulti=_symList.length>1;
  const endpoint=useMulti?'/opt_status_all':'/opt_status';
  fetch(endpoint,{cache:'no-store'}).then(r=>r.json()).then(d=>{
    const wasLost = _connLost;
    _connLost = false;
    _connRetryCount = 0;
    if (wasLost) _onReconnect();
    // Merge multi-symbol states
    if(useMulti&&d.states){
      for(const sym of _symList){
        if(d.states[sym]) _symStates[sym]=Object.assign(_symStates[sym]||{},d.states[sym]);
      }
      // Auto-follow active (currently running) symbol if current has no chart yet
      if(d.active&&d.active!==_activeChart){
        const curSt=_symStates[_activeChart]||{};
        if(curSt.chart_updated_at<=0){
          _activeChart=d.active;
          _chartFrameLoaded=false;  // принудительная перезагрузка при авто-переключении
        }
      }
      _renderSymCards();
      _renderSymSwitcher();
      // auto-load chart for active sym when timestamp updated
      const activeSt=_symStates[_activeChart]||{};
      const knownTs=_lastChartTs[_activeChart]||0;
      if(activeSt.chart_updated_at>0&&activeSt.chart_updated_at!==knownTs){
        _lastChartTs[_activeChart]=activeSt.chart_updated_at;
        // Если в iframe сейчас другой символ — сбрасываем флаг чтобы загрузить полный HTML
        if(_lastLoadedChartSym!==_activeChart) _chartFrameLoaded=false;
        _loadChartFrame(_activeChart);
      }
    }
    const elapsed=Math.round((Date.now()-startTs)/1000);
    document.getElementById('progTime').textContent=elapsed+'с';

    // ── Прогресс загрузки свечей ──────────────────────────────────
    const fetchPct = (d.fetch_pct != null) ? d.fetch_pct : -1;
    const isFetching = fetchPct >= 0 && fetchPct < 100;
    const fetchDone  = fetchPct === 100;
    // Показываем fetch-прогресс вместо основного, пока идёт загрузка
    if (isFetching) {
      document.getElementById('progBar').style.width = fetchPct + '%';
      document.getElementById('progBar').style.background = 'linear-gradient(90deg,#5a7fa0,#4a8c6a)';
      document.getElementById('progLabel').textContent = `📡 Загрузка свечей ${d.fetch_symbol||''} · ${fetchPct}%`;
      document.getElementById('progParam').textContent = '';
    } else {
      document.getElementById('progBar').style.background = '';
      // Полоска = прогресс цикла (стартов + BH итераций), не отдельного круга
      const cycStep=d.cycle_step||0, cycTotal=d.cycle_total||0;
      const pct=cycTotal>0?Math.round(cycStep/cycTotal*100):0;
      document.getElementById('progBar').style.width=pct+'%';
      const cycleStr=d.infinite?` · Цикл #${d.cycle}`:'';
      // Лейбл: фаза (Старт / BH) + N/total + % + цикл
      const n_starts=d.generation||0;
      const bh_total=cycTotal>0?cycTotal-n_starts:0;
      let phaseLabel='';
      if(fetchDone){
        phaseLabel='Запуск...';
      } else if(cycTotal===0||cycStep===0){
        phaseLabel='Запуск...';
      } else if(cycStep<=n_starts){
        phaseLabel=`Старт ${cycStep}/${n_starts}`;
      } else {
        const bhDone=cycStep-n_starts;
        phaseLabel=`Basin Hopping ${bhDone}/${bh_total}`;
      }
      document.getElementById('progLabel').textContent=`${phaseLabel} · ${pct}%${cycleStr}`;
      if(d.current_param) document.getElementById('progParam').textContent='→ '+d.current_param;
    }

    // SW status
    const sw2=document.getElementById('swStatus2');
    if(d.sw_running){
      const upd=d.sw_last_update?new Date(d.sw_last_update*1000).toLocaleTimeString('ru'):'—';
      sw2.textContent=`SW: ${d.sw_candle_count} св · ${upd}`;
      sw2.style.color='var(--green)';
    } else {sw2.textContent='';sw2.style.color='';}

    // Badges
    const badge=document.getElementById('statusBadge2');
    const swb=document.getElementById('swBadge');
    badge.innerHTML='';
    // Быстродействие
    const sp=document.getElementById('speedPill');
    if(sp){
      if(d.avg_cycle_s!=null){
        sp.style.display='';
        const mins=Math.floor(d.avg_cycle_s/60);
        const secs=Math.round(d.avg_cycle_s%60);
        const spTxt=document.getElementById('speedPillText');
        const spLabel=(mins>0?mins+'м ':'')+secs+'с/цикл';
        if(spTxt)spTxt.textContent=spLabel; else sp.textContent='⚡ '+spLabel;
        sp.title='Среднее время одного цикла оптимизации';
        sp._lastShown=Date.now();
      } else if(!sp._lastShown||(Date.now()-sp._lastShown)>8000){
        sp.style.display='none';
      }
    }
    swb.style.display=d.sw_running?'inline-flex':'none';
    if(d.sw_running&&!d.running) document.getElementById('swStopBtn').style.display='flex';
    if(!d.sw_running) document.getElementById('swStopBtn').style.display='none';

    const logs=d.logs||[];
    if(logs.length>lastLogCount){
      for(let i=lastLogCount;i<logs.length;i++) logLine(logs[i].msg,logs[i].level,logs[i].ts);
      lastLogCount=logs.length;
    }
    const _atb=d.all_time_best||d.best;
    if(_atb&&_atb.equity!==undefined){window._lastBest=_atb;window._lastTop20=d.top20||[];renderBest(_atb);}
    if(_atb) renderTop20([_atb]);  // таблица показывает лучший за все прогоны
    if(d.valid!==undefined) renderValid(d.valid, d.all_time_best||d.best, d.windows||[], d.min_stable_days??null, d.days||30);
    if(!useMulti&&d.chart_updated_at>0){
      const _singleSym=_symList[0]||'__single__';
      if(d.chart_updated_at!==(_lastChartTs[_singleSym]||0)){
        _lastChartTs[_singleSym]=d.chart_updated_at;
        _loadChartFrame();
      }
    }
    if(d.done&&!d.running&&!d.infinite){
      if(polling){clearTimeout(polling);polling=null;}
      document.getElementById('wfBtn').disabled=false;
      document.getElementById('wfStopBtn').style.display='none';
      document.getElementById('progLabel').textContent='✓ Готово за '+d.elapsed+'с';
      _loadRecentConfigs();
    }
  }).catch(()=>{
    if (!_connLost) {
      _connLost = true;
      _connLostAt = Date.now();
      addLogLine('⚠ Потеряно соединение с сервером...', 'warn');
    }
    _connRetryCount++;
    _setConnStatus(false);
  });
}

/* ── Cycle cards ── */
let _cc={}, _ccPrevEq=null, _startBuf=null;

function _resetLog(){
  document.getElementById('ccStrip').innerHTML='';
  document.getElementById('wfLog').innerHTML='';
  const sw=document.getElementById('symSwitcher');
  if(sw){sw.style.display='none';sw.innerHTML='';}
  lastLogCount=0; _cc={}; _ccPrevEq=null; _startBuf=null;
}

function addLogLine(msg,level,ts){
  const el=document.createElement('div');
  el.className='log-line '+(level||'info');
  el.textContent=(ts?ts+' ':'')+msg;
  const wfLog=document.getElementById('wfLog');
  wfLog.insertBefore(el,wfLog.firstChild);
  // Keep activity line pinned at the very top
  const act=document.getElementById('ccActivity');
  if(act&&act!==wfLog.firstChild) wfLog.insertBefore(act,wfLog.firstChild);
}

function _setActivity(text){ /* activity line hidden */ }
function _clearActivity(){const el=document.getElementById('ccActivity');if(el)el.remove();}

function _cycleCard(n,eq,wr,dd,elapsed,done,trades,isNewRec){
  const isPos=eq>100;
  const strip=document.getElementById('ccStrip');
  let card=strip.querySelector(`[data-n="${n}"]`);
  if(!card){
    card=document.createElement('div');
    card.dataset.n=n;
    strip.insertBefore(card, strip.firstChild);
    _cc[n]=card;
  }
  card.dataset.eq=eq;
  card.className='cc '+(done?(isPos?'pos':'neg'):'running');
  const eqCls=done?(isPos?'pos':'neg'):'run';
  const recBadge=done?(isNewRec?'<span style="font-size:.55rem;color:var(--green);font-weight:700">🆕 рекорд</span>':'<span style="font-size:.55rem;color:var(--text3)">→ без изм.</span>'):'';
  card.innerHTML=
    `<div class="cc-n" style="display:flex;justify-content:space-between;align-items:center">Цикл ${n}${recBadge}</div>`+
    `<div class="cc-eq ${eqCls}">$${eq.toFixed(0)}</div>`+
    `<div class="cc-m">WR <b>${wr.toFixed(0)}%</b>`+(trades>0?` · ${trades} сд`:'')+(dd>0?` · DD ${dd.toFixed(0)}%`:'')+`</div>`+
    (elapsed?`<div class="cc-m">${elapsed}с</div>`:'')+
    `<div class="cc-bar ${isPos?'':'neg'}" style="width:100%"></div>`;
}
function logLine(msg,level,ts){
  if(!msg||!msg.trim()) return;
  // В мультирежиме цикловые карточки не нужны — используем sym-cards
  const isMulti=_symList.length>1;
  if(/WickFill Optimizer|загрузка свечей|загружено \d+|ThreadPool|ProcessPool|Сохранено|Авто-сохранение/i.test(msg)){
    addLogLine(msg.replace(/^[📡🔄⟳✅⏹\s]+/,''),level||'info',ts);return;
  }
  const cycleM=msg.match(/═+\s*ЦИКЛ\s*#(\d+)/i);
  if(cycleM){_startBuf=null;if(!isMulti)_cycleCard(parseInt(cycleM[1]),100,0,0,null,false,0,false);_setActivity('Цикл '+cycleM[1]+' — оптимизация...');return;}
  const startM=msg.match(/──\s*(Старт\s*#(\d+)[^─]*?)\s*──/);
  if(startM){return;}  // не в лог, прогресс уже в блоке над кнопкой
  if(level==='activity') return;  // служебные строки активности — не в лог
  const passM=msg.match(/Круг\s*#(\d+)\s*\|\s*Депозит:\s*\$([\d.]+)/);
  if(passM){_setActivity('Круг #'+passM[1]+' · $'+passM[2]);return;}
  const foundM=msg.match(/✅\s*.+?→\s*\$([\d.]+)\s*\(\+?([-\d.]+)\$\)\s*\|\s*WR\s*([\d.]+)%\s*\|\s*Сд\s*(\d+)\s*\|\s*DD\s*([\d.]+)%/);
  if(foundM){
    const eq=parseFloat(foundM[1]),wr=parseFloat(foundM[3]),dd=parseFloat(foundM[5]);
    if(!_startBuf||eq>_startBuf.eq)_startBuf={eq,wr,dd};
    if(!isMulti){
      const ns=Object.keys(_cc);
      if(ns.length){const lastN=parseInt(ns[ns.length-1]);if(!_cc[lastN].classList.contains('pos')&&!_cc[lastN].classList.contains('neg'))_cycleCard(lastN,eq,wr,dd,null,false,0,false);}
    }
    return;
  }
  const endM=msg.match(/Старт\s*#\d+[^→]*→\s*\$([\d.]+)\s+WR\s*([\d.]+)%\s+DD\s*([\d.]+)%/);
  if(endM){const eq=parseFloat(endM[1]),wr=parseFloat(endM[2]),dd=parseFloat(endM[3]);if(!_startBuf||eq>_startBuf.eq)_startBuf={eq,wr,dd};return;}
  const doneM=msg.match(/✅\s*Цикл\s*#(\d+)\s*готов\s*за\s*(\d+)с\s*\|\s*([🆕→]+)\s*\$([\d.]+)\s+WR\s+([\d.]+)%\s+Сд\s+(\d+)\s+DD\s+([\d.]+)%/);
  if(doneM){
    _clearActivity();
    if(!isMulti){
      const isNewRec=doneM[3].includes('🆕');
      _cycleCard(parseInt(doneM[1]),parseFloat(doneM[4]),parseFloat(doneM[5]),parseFloat(doneM[7]),doneM[2],true,parseInt(doneM[6]),isNewRec);
    }
    _startBuf=null;return;
  }
  if(/остановлен|остановлено/i.test(msg)){_clearActivity();addLogLine('⏹ '+msg.replace(/^[⏹\s]+/,''),'warn',ts);return;}
  if(level==='error') addLogLine(msg,'error',ts);
}

function renderBest(b){
  // bestSection hidden — info shown in table above
  const eq=b.equity??100,wr=b.winrate??0,dd=b.max_dd??0,pf=b.profit_factor??0,tr=b.trades??0;
  // Мобильная строка
  const mobRow=document.getElementById('mob-best-row');
  if(mobRow){
    document.getElementById('mob-eq').textContent='$'+eq.toFixed(0);
    document.getElementById('mob-eq').style.color=eq>100?'var(--green)':eq<100?'var(--red)':'var(--bark)';
    document.getElementById('mob-wr').textContent='WR '+wr.toFixed(0)+'%';
    document.getElementById('mob-dd').textContent='DD '+dd.toFixed(0)+'%';
    document.getElementById('mob-dd').style.color=dd>25?'var(--red)':'var(--text2)';
    document.getElementById('mob-tr').textContent=tr+' сд';
    document.getElementById('mob-sl').textContent='SL '+(b.params?.sl_pct??'—')+'%';
    document.getElementById('mob-tp').textContent='TP '+(b.params?.tp_pct??'—')+'%';
  }
  const stats=[
    {v:'$'+eq.toFixed(0),l:'Депозит',c:eq>110?'good':eq<95?'bad':''},
    {v:wr.toFixed(1)+'%',l:'Winrate',c:wr>=60?'good':wr>=50?'warn':wr<40?'bad':''},
    {v:tr,l:'Сделок',c:tr>=20?'good':tr<8?'bad':'warn'},
    {v:dd.toFixed(1)+'%',l:'Max DD',c:dd<15?'good':dd<30?'warn':'bad'},
    {v:pf===999?'∞':pf.toFixed(2),l:'PF',c:pf>=1.8?'good':pf>=1.2?'warn':'bad'},
    {v:(b.params?.sl_pct??'—')+'%',l:'SL',c:''},
    {v:(b.params?.tp_pct??'—')+'%',l:'TP',c:''},
    {v:b.params?.use_next_bar!=null?(b.params.use_next_bar?'✔ след.св.':'✘ тек.св.'):'—',l:'Вход',c:''},
    {v:b.params?.rsi_len??'—',l:'RSI len',c:''},
  ];
  document.getElementById('bestGrid').innerHTML=stats.map(s=>`<div class="stat-cell ${s.c}"><div class="stat-v">${s.v}</div><div class="stat-l">${s.l}</div></div>`).join('');
  // flash animation on update
  document.querySelectorAll('#bestGrid .stat-cell').forEach(el=>{
    el.classList.remove('flash');
    void el.offsetWidth;
    el.classList.add('flash');
  });
  if(b.params){
    document.getElementById('bestParamsWrap').style.display='block';
    const lines=Object.entries(b.params).map(([k,v])=>{
      const vs=typeof v==='boolean'?(v?'да':'нет'):typeof v==='number'?(Number.isInteger(v)?v:v.toFixed(2)):v;
      return `<span>${k}:</span> <b>${vs}</b>`;
    });
    document.getElementById('bestParams').innerHTML=lines.join('<br>');
  }
}
function toggleParams(){
  const el=document.getElementById('bestParams'),vis=el.style.display!=='none';
  el.style.display=vis?'none':'block';
}

function renderValid(v, best, windows, minDays, days){
  const wrap=document.getElementById('validSection');
  if(!wrap) return;
  if(!v && (!windows||!windows.length) && !minDays){
    if(!wrap._everShown) wrap.style.display='none';
    return;
  }
  wrap._everShown=true;
  wrap.style.display='block';
  const trainWr=best?.winrate??0;
  const trainEq=best?.equity??100;
  const trainDd=best?.max_dd??0;
  const trainTrades=best?.trades??0;
  const trainDays=best?.days??(days||20);
  const ratio=v&&trainWr>0?(v.winrate/trainWr):null;
  // ok = стабильная только если И валид хорош И хотя бы часть окон работает
  const okWindows=windows?windows.filter(w=>w.ok).length:0;
  const totalWindows=windows?windows.length:0;
  const windowsOk=totalWindows===0||okWindows/totalWindows>=0.4;  // хотя бы 2 из 5
  // Последнее (свежее) окно — первый элемент массива (wi=0 самое свежее)
  const lastWindow=windows&&windows.length>0?windows[0]:null;
  const lastWindowOk=!lastWindow||lastWindow.ok;
  // Если в валид-периоде 0 сделок — стратегия просто не торговала, это не провал
  const noTradesInValid = v && v.trades===0;
  // Стабильная если: валид хороший ИЛИ большинство окон зелёные — НО только если последний период не красный
  // Также считаем стабильной если 0 сделок в коротком валид-периоде но окна хорошие
  const ok=(ratio!==null&&(ratio>=0.75||windowsOk&&okWindows>=2)&&lastWindowOk)
          ||(noTradesInValid&&windowsOk&&okWindows>=2&&lastWindowOk);
  // Деградация: в целом хорошо, но последний период плохой
  const degrading=!ok&&ratio!==null&&(ratio>=0.75||windowsOk&&okWindows>=2)&&!lastWindowOk;
  // Нет сигналов в валидационном периоде — отдельный статус
  const noSignals=noTradesInValid&&!ok&&!degrading;
  const color=ok?'var(--green)':degrading?'var(--yellow)':'var(--red)';
  const bgColor=ok?'var(--green-light)':degrading?'rgba(138,106,26,0.12)':'var(--red-light)';

  const validWr = v && !noTradesInValid ? v.winrate.toFixed(0)+'%' : '—';
  const validEq = v && !noTradesInValid ? '$'+v.equity.toFixed(0) : '—';
  const validDd = v && !noTradesInValid ? v.max_dd.toFixed(0)+'%' : '—';
  const eqColor = v && !noTradesInValid ? (v.equity>=100?'var(--green)':'var(--red)') : 'var(--text3)';
  const ddColor = v && !noTradesInValid ? (v.max_dd<15?'var(--green)':v.max_dd>25?'var(--red)':'var(--yellow)') : 'var(--text3)';

  let html=`<div style="margin-top:8px;padding:10px 12px;border-radius:12px;border:1.5px solid ${color};background:${bgColor}">`;

  // Строка 0: лучшая комбинация (train) — всегда показываем
  const trainEqColor=trainEq>100?'var(--green)':trainEq<100?'var(--red)':'var(--text2)';
  const trainDdColor=trainDd<15?'var(--green)':trainDd>25?'var(--red)':'var(--yellow)';
  html+=`<div style="display:flex;gap:10px;margin-bottom:6px;font-size:.78rem;padding-bottom:6px;border-bottom:1px solid rgba(128,128,128,0.15)">
    <span style="color:var(--text3);font-size:.65rem;align-self:center">Лучшая:</span>
    <span>💰 <b style="color:${trainEqColor}">$${trainEq.toFixed(0)}</b></span>
    <span>WR <b style="color:var(--green)">${trainWr.toFixed(0)}%</b></span>
    <span>📉 DD <b style="color:${trainDdColor}">${trainDd.toFixed(0)}%</b></span>
    <span style="color:var(--text3)">${trainTrades} сд</span>
  </div>`;

  // Строка 1: статус + валид WR vs трейн WR
  const statusLabel = ratio===null ? '— Нет данных'
    : ok ? '✓ Стабильная'
    : degrading ? '⚠ Деградация'
    : noSignals ? '⏸ Нет сигналов'
    : '⚠ Нестабильная';
  html+=`<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
    <span style="color:${color};font-weight:700;font-size:.88rem">${statusLabel}</span>
    <span style="font-size:.72rem;color:var(--text3)">валид <b style="color:${color}">${validWr}</b> / трейн <b style="color:var(--text2)">${trainWr.toFixed(0)}%</b></span>
  </div>`;

  // Строка 2: Депозит · DD · Сделок (валидационный период)
  if(v && !noTradesInValid){
    html+=`<div style="display:flex;gap:10px;margin-bottom:10px;font-size:.78rem">
      <span>💰 <b style="color:${eqColor}">${validEq}</b></span>
      <span>📉 DD <b style="color:${ddColor}">${validDd}</b></span>
      <span style="color:var(--text3)">${v.trades} сд · ${v.days}д</span>
    </div>`;
  } else if(noSignals){
    html+=`<div style="font-size:.72rem;color:var(--text3);margin-bottom:8px">За последние ${v?v.days.toFixed(0):'-'}д сигналов не было — рынок не давал условий для входа</div>`;
  }

  // Строка 3: гистограмма окон — слева старое, справа свежее
  if(windows&&windows.length>0&&windows.some(w=>w.trades>0)){
    const maxWr=Math.max(...windows.map(w=>w.winrate),1);
    const fmtD=ts=>{const d=new Date(ts*1000);return (d.getMonth()+1)+'/'+(d.getDate());};
    html+=`<div style="margin-bottom:${minDays!=null?'8px':'0'}">
      <div style="font-size:.6rem;color:var(--text3);margin-bottom:5px;text-transform:uppercase;letter-spacing:.04em">История по периодам  ← старое · свежее →</div>
      <div style="display:flex;align-items:flex-end;gap:4px;height:28px">`;
    // окна идут от старого (#5) к свежему (#1) — разворачиваем
    const sorted=[...windows];  // wi=0 — свежее, wi=N — старое; показываем старое→свежее слева→направо
    for(const w of sorted){
      const h=Math.max(4,Math.round((w.winrate/Math.max(maxWr,1))*24));
      const c=w.ok?'var(--green)':'var(--red)';
      const bg=w.ok?'var(--green)':'var(--red)';
      const fromLbl=w.ts_from?fmtD(w.ts_from):'';
      const toLbl=w.ts_to?fmtD(w.ts_to):'';
      html+=`<div style="flex:1;display:flex;flex-direction:column;align-items:center" title="WR ${w.winrate}% · ${w.trades} сд · ${fromLbl}–${toLbl}">
        <div style="width:100%;height:${h}px;background:${bg};border-radius:3px 3px 0 0;transition:height .3s"></div>
      </div>`;
    }
    html+=`</div>
      <div style="display:flex;gap:4px;margin-top:3px">`;
    for(const w of sorted){
      const c=w.ok?'var(--green)':'var(--red)';
      const fromLbl=w.ts_from?fmtD(w.ts_from):'';
      const toLbl=w.ts_to?fmtD(w.ts_to):'';
      html+=`<div style="flex:1;text-align:center">
        <div style="font-size:.52rem;font-weight:700;color:${c}">${w.winrate}%</div>
        <div style="font-size:.46rem;color:var(--text3);white-space:nowrap">${fromLbl}–${toLbl}</div>
      </div>`;
    }
    html+=`</div>
    </div>`;
  }

  // Строка 4: мин. стабильный период
  if(minDays!=null){
    const stableColor=minDays>=(days||30)*0.5?'var(--green)':'var(--yellow)';
    html+=`<div style="font-size:.7rem;color:var(--text3)">Стабильна с последних <b style="color:${stableColor}">${minDays}д</b></div>`;
  }

  html+='</div>';
  wrap.innerHTML=html;
}

function renderTop20(list){
  document.getElementById('top20Wrap').style.display='block';
  const top=list.slice(0,1);
  document.getElementById('top20Body').innerHTML=top.map((r)=>{
    const eq=(r.equity??100).toFixed(0),wr=(r.winrate??0).toFixed(1),dd=(r.max_dd??0).toFixed(1);
    const pf=r.profit_factor===999?'∞':(r.profit_factor??0).toFixed(2);
    const sl=r.params?.sl_pct??'—',tp=r.params?.tp_pct??'—';
    const nb=r.params?.use_next_bar!=null?(r.params.use_next_bar?'✔ след.':'✘ тек.'):'—';
    const eqColor=parseFloat(eq)>100?'var(--green)':parseFloat(eq)<100?'var(--red)':'inherit';
    const risk=parseFloat(document.getElementById('wf_risk')?.value)||20;
    const levRaw=(typeof sl==='number'||!isNaN(parseFloat(sl)))&&parseFloat(sl)>0
      ? Math.round(risk/parseFloat(sl)) : null;
    const lev=levRaw!==null ? levRaw+'×' : '—';
    const levColor=levRaw>50?'var(--red)':levRaw>25?'var(--yellow)':'inherit';
    return `<tr><td style="font-size:1.05rem;font-weight:700;color:${eqColor}">$${eq}</td><td>${wr}</td><td>${r.trades??0}</td>
      <td style="color:${parseFloat(dd)>25?'var(--red)':'inherit'}">${dd}</td>
      <td style="color:${parseFloat(pf)>=1.5?'var(--green)':'inherit'}">${pf}</td>
      <td>${sl}</td><td>${tp}</td>
      <td style="font-size:.8rem;color:${nb.startsWith('✔')?'var(--green)':'var(--text3)'}">${nb}</td>
      <td style="font-size:.85rem;font-weight:700;color:${levColor}">${lev}</td></tr>`;
  }).join('');
}

function deleteDownload(){
  const btn=event.target;btn.disabled=true;btn.textContent='...';
  fetch('/delete_download').then(r=>r.json()).then(d=>{
    btn.textContent=d.ok?'✓':'✕';
    setTimeout(()=>{btn.disabled=false;btn.textContent='✕';},2000);
  }).catch(()=>{btn.textContent='✕';setTimeout(()=>{btn.disabled=false;btn.textContent='✕';},2000);});
}
function renameDownload(){
  const btn=event.target;btn.disabled=true;btn.textContent='...';
  fetch('/rename_download').then(r=>r.json()).then(d=>{
    btn.textContent=d.ok?'✓':'✕';if(!d.ok)alert(d.msg||'Ошибка');
    setTimeout(()=>{btn.disabled=false;btn.textContent='✏ Fix';},3000);
  }).catch(()=>{btn.textContent='✕';setTimeout(()=>{btn.disabled=false;btn.textContent='✏ Fix';},3000);});
}
function termuxUpdate(){
  const btn=event.target;btn.disabled=true;btn.textContent='⏳';
  fetch('/termux_update').then(r=>r.json()).then(d=>{
    if(d.ok){
      btn.textContent='⏳ ~5с';
      addLogLine('⏳ Перезапуск скрипта...','info');
      // Ждём пока новый сервер поднимется — пингуем каждую секунду
      let attempts=0;
      function tryReload(){
        attempts++;
        fetch('/opt_status',{cache:'no-store'}).then(()=>{
          location.href='/?v='+Date.now();
        }).catch(()=>{
          if(attempts<20) setTimeout(tryReload,1000);
          else location.href='/?v='+Date.now();
        });
      }
      setTimeout(tryReload, 3000); // первая попытка через 3с — раньше смысла нет
    } else {
      btn.disabled=false;btn.textContent='↺ Restart';
      addLogLine('⚠ Обновление: '+(d.msg||'Ошибка'),'warn');
    }
  }).catch(()=>{
    btn.textContent='⏳';
    addLogLine('⏳ Сервер перезапускается...','info');
    let attempts=0;
    function tryReload(){
      attempts++;
      fetch('/opt_status',{cache:'no-store'}).then(()=>{
        location.href='/?v='+Date.now();
      }).catch(()=>{
        if(attempts<20) setTimeout(tryReload,1000);
        else location.href='/?v='+Date.now();
      });
    }
    setTimeout(tryReload, 3000);
  });
}

/* ── Mobile toggles ── */
let _mobTopVisible=false, _mobLogVisible=false;
function toggleMobTop(){
  _mobTopVisible=!_mobTopVisible;
  const el=document.getElementById('top20Wrap');
  if(el){el.style.display=_mobTopVisible?'block':'none';}
}
function toggleMobLog(){
  _mobLogVisible=!_mobLogVisible;
  const el=document.getElementById('wfLog');
  if(el){el.classList.toggle('mob-hidden',!_mobLogVisible);}
  const btn=document.getElementById('mob-log-btn');
  if(btn) btn.textContent=_mobLogVisible?'📋 Скрыть':'📋 Логи';
}
function toggleTheme(){
  const isDark=document.documentElement.getAttribute('data-theme')==='dark';
  const next=isDark?'light':'dark';
  document.documentElement.setAttribute('data-theme',next);
  const _tbl=document.getElementById('themeBtnLabel');if(_tbl)_tbl.textContent=next==='dark'?'Тема':'Тема';
  localStorage.setItem('wf_theme',next);
  // Reload chart iframe with new theme — полная перезагрузка нужна для смены цветов
  const frame=document.getElementById('chartFrame');
  if(frame&&frame.style.display!=='none'&&frame.src&&frame.src!=='about:blank'){
    _chartFrameLoaded=false;
    frame.src='/chart?t='+Date.now()+'&theme='+next;
    _chartFrameLoaded=true;  // сразу помечаем — следующие обновления пойдут через postMessage
  }
}
function _loadRecentConfigs(){
  fetch('/recent_configs').then(r=>r.json()).then(d=>{
    if(!d.ok||!d.configs||!d.configs.length) return;
    const panel=document.getElementById('recentPanel');
    const list=document.getElementById('recentList');
    list.innerHTML='';
    d.configs.forEach(c=>{
      const sym=(c.symbol||'').replace('_USDT','').replace('USDT','').toUpperCase();
      const eq=c.equity?'$'+c.equity:'';
      const row=document.createElement('div');
      row.style.cssText='display:flex;align-items:center;gap:8px;padding:7px 10px;border-radius:9px;background:var(--glass2);border:1px solid var(--border2);cursor:pointer;transition:background .15s,opacity .25s,max-height .3s,padding .3s,margin .3s;overflow:hidden;max-height:60px';
      row.onmouseenter=function(){this.style.background='var(--cream2)'};
      row.onmouseleave=function(){this.style.background='var(--glass2)'};
      const info=document.createElement('div');
      info.style.cssText='display:flex;align-items:center;gap:8px;flex:1;min-width:0';
      info.innerHTML=`<span style="font-size:.82rem;font-weight:600;color:var(--bark);min-width:42px">${sym}</span><span style="font-size:.75rem;color:var(--text2);background:var(--cream3);padding:2px 6px;border-radius:5px">${c.tf}</span><span style="font-size:.75rem;color:var(--text3)">${c.days}д</span><span style="flex:1"></span><span style="font-size:.72rem;color:var(--text3);font-family:'DM Mono',monospace">${eq}</span>`;
      info.onclick=function(){
        const rawSym=(c.symbol||'').replace(/_?USDT$/i,'').toUpperCase();
        document.getElementById('wf_symbol').value=rawSym;
        const sel=document.getElementById('wf_tf_sel');
        for(let i=0;i<sel.options.length;i++) if(sel.options[i].value===c.tf){sel.selectedIndex=i;break;}
        document.getElementById('wf_days').value=c.days;
      };
      const del=document.createElement('button');
      del.textContent='×';
      del.title='Удалить конфиг';
      del.style.cssText='flex-shrink:0;border:none;background:none;color:var(--text3);font-size:1rem;line-height:1;cursor:pointer;padding:2px 4px;border-radius:4px;transition:color .15s,background .15s;margin-left:4px';
      del.onmouseenter=function(){this.style.color='#c0514a';this.style.background='rgba(192,81,74,0.1)'};
      del.onmouseleave=function(){this.style.color='var(--text3)';this.style.background='none'};
      del.onclick=function(e){
        e.stopPropagation();
        const fname=c.fname||'';
        if(!fname) return;
        row.style.opacity='0';
        row.style.maxHeight='0';
        row.style.padding='0 10px';
        row.style.marginBottom='0';
        setTimeout(()=>{
          row.remove();
          const remaining=list.querySelectorAll('div[data-cfg]');
          if(!remaining.length) panel.style.display='none';
        }, 300);
        fetch('/delete_config?fname='+encodeURIComponent(fname)).catch(()=>{});
      };
      row.dataset.cfg=c.fname||'';
      row.appendChild(info);
      row.appendChild(del);
      list.appendChild(row);
    });
    panel.dataset.hasConfigs='1';
    panel.style.display='block';
    // Всегда раскрываем при загрузке
    const rb=document.getElementById('recentBody');
    const ra=document.getElementById('recentArrow');
    if(rb) rb.style.maxHeight='600px';
    if(ra) ra.style.transform='rotate(180deg)';
  }).catch(()=>{});
}
document.addEventListener('DOMContentLoaded',function(){
  const btn=document.getElementById('themeBtn');
  const t=document.documentElement.getAttribute('data-theme')||'light';
  // theme icon is SVG, no text update needed
  _loadRecentConfigs();
});

</script>
<script>
(function(){
  fetch('/version',{cache:'no-store'}).then(r=>r.json()).then(d=>{
    const sp=document.getElementById('versionSpan');
    if(sp && d.version) sp.textContent='v'+d.version;
  }).catch(()=>{});
})();
</script></body></html>"""

# ═══════════════════════════════════════════════════════════════
# HTTP SERVER
# ═══════════════════════════════════════════════════════════════
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._html(HTML.encode())
        elif parsed.path == "/version":
            self._json({"version": APP_VERSION})
        elif parsed.path == "/opt_status":
            with opt_lock:
                cr = opt_state.get("chart_path","")
                st = {
                    "running":        opt_state["running"],
                    "done":           opt_state["done"],
                    "infinite":       opt_state.get("infinite",False),
                    "cycle":          opt_state.get("cycle",0),
                    "progress":       opt_state["progress"],
                    "total":          opt_state["total"],
                    "generation":     opt_state["generation"],
                    "pass_num":       opt_state.get("pass_num",0),
                    "cycle_step":     opt_state.get("cycle_step",0),
                    "cycle_total":    opt_state.get("cycle_total",0),
                    "current_param":  opt_state.get("current_param",""),
                    "best":           opt_state.get("all_time_best") or opt_state["best"],
                    "all_time_best":   opt_state.get("all_time_best"),
                    "top20":          opt_state["top20"],
                    "valid":          opt_state.get("valid", None),
                    "windows":        opt_state.get("windows", []),
                    "min_stable_days":opt_state.get("min_stable_days", None),
                    "days":           opt_state.get("days", 30),
                    "elapsed":        opt_state["elapsed"],
                    "avg_cycle_s":    opt_state.get("avg_cycle_s"),
                    "error":          opt_state["error"],
                    "logs":           list(opt_state["logs"]),
                    "chart_path":     cr,
                    "chart_updated_at": opt_state.get("chart_updated_at",0),
                    "sw_running":     opt_state.get("sw_running",False),
                    "sw_last_update": opt_state.get("sw_last_update",0),
                    "sw_candle_count":opt_state.get("sw_candle_count",0),
                    "fetch_pct":      opt_state.get("fetch_pct", -1),
                    "fetch_symbol":   opt_state.get("fetch_symbol", ""),
                }
            with alert_lock:
                st["alert_sent"] = alert_state["sent"]
            self._json(st)
        elif parsed.path == "/opt_status_all":
            with opt_states_lock:
                syms = list(_multi_symbols)
                # Лёгкий снапшот — без chart_candles/chart_signals (они тяжёлые, не нужны в поллинге)
                states_snap = {}
                for s in syms:
                    src = opt_states.get(s, {})
                    states_snap[s] = {
                        "symbol":          src.get("symbol", s),
                        "eq":              src.get("eq", 100),
                        "wr":              src.get("wr", 0),
                        "dd":              src.get("dd", 0),
                        "trades":          src.get("trades", 0),
                        "pf":              src.get("pf", 0),
                        "sl":              src.get("sl"),
                        "tp":              src.get("tp"),
                        "cycle":           src.get("cycle", 0),
                        "running":         src.get("running", False),
                        "chart_updated_at":src.get("chart_updated_at", -1),
                        "valid":           src.get("valid"),
                        "windows":         src.get("windows", []),
                        "min_stable_days": src.get("min_stable_days"),
                        "days":            src.get("days", 30),
                    }
                active = _active_chart_symbol
            # also include main opt_state for single-symbol compat
            multi_thread_alive = bool(_opt_thread and _opt_thread.is_alive())
            if len(syms) > 1:
                # Параллельный режим: агрегируем логи из opt_states всех символов
                with opt_states_lock:
                    all_logs = []
                    for s_sym in syms:
                        all_logs.extend(opt_states.get(s_sym, {}).get("logs", []))
                    all_logs.sort(key=lambda x: x.get("ts", ""))
                    main_logs = all_logs[-300:]  # последние 300 строк
                    any_running = any(opt_states.get(s, {}).get("running", False) for s in syms)
                main_running = any_running or multi_thread_alive
                # done только если тред реально мёртв И ни один символ не running
                main_done    = not multi_thread_alive and not any_running
                main_inf     = multi_thread_alive or any_running
                # Берём прогресс из opt_state — туда пишет _coordinate_descent_from
                with opt_lock:
                    main_cycle    = opt_state.get("cycle", 0)
                    main_progress = opt_state.get("progress", 0)
                    main_total    = opt_state.get("total", 0)
                    main_pass     = opt_state.get("pass_num", 0)
                    main_param    = opt_state.get("current_param", "")
                    main_elapsed  = opt_state.get("elapsed", 0)
                    main_avg      = opt_state.get("avg_cycle_s")
            else:
                with opt_lock:
                    main_logs = list(opt_state.get("logs",[]))
                    main_running = opt_state.get("running", False)
                    main_cycle = opt_state.get("cycle", 0)
                    main_progress = opt_state.get("progress", 0)
                    main_total = opt_state.get("total", 0)
                    main_pass = opt_state.get("pass_num", 0)
                    main_param = opt_state.get("current_param","")
                    main_elapsed = opt_state.get("elapsed", 0)
                    main_avg = opt_state.get("avg_cycle_s")
                    main_done = opt_state.get("done", False)
                    main_inf = opt_state.get("infinite", False)
                main_running = main_running or multi_thread_alive
                main_done    = not multi_thread_alive
                main_inf     = multi_thread_alive
            self._json({
                "symbols": syms,
                "active": active,
                "states": states_snap,
                "running": main_running,
                "done": main_done,
                "infinite": main_inf,
                "cycle": main_cycle,
                "progress": main_progress,
                "total": main_total,
                "pass_num": main_pass,
                "current_param": main_param,
                "elapsed": main_elapsed,
                "avg_cycle_s": main_avg,
                "logs": main_logs,
            })

        elif parsed.path in ("/chart", "/chart_download"):
            qs = parse_qs(parsed.query)
            req_sym = qs.get("symbol",[""])[0].upper()

            chart_candles = []; chart_signals = []; chart_symbol = ""; chart_tf = ""; chart_best = None; chart_path = ""

            if req_sym:
                with opt_states_lock:
                    is_active = (req_sym == _active_chart_symbol)
                    sym_state = dict(opt_states.get(req_sym, {}))
                print(f"[chart] req={req_sym} active={_active_chart_symbol} is_active={is_active} "
                      f"chart_upd={sym_state.get('chart_updated_at',-1)} "
                      f"candles={len(sym_state.get('chart_candles') or [])} "
                      f"best={bool(sym_state.get('best'))}", flush=True)

                # В параллельном режиме данные всегда в opt_states, независимо от активности
                if sym_state.get("chart_candles"):
                    chart_candles = list(sym_state["chart_candles"])
                    chart_signals = list(sym_state.get("chart_signals") or [])
                    chart_symbol  = sym_state.get("symbol", req_sym)
                    chart_tf      = sym_state.get("chart_tf", "")
                    chart_best    = sym_state.get("best")
                    chart_path    = sym_state.get("chart_path", "")
                elif sym_state.get("best"):
                    chart_best   = sym_state["best"]
                    chart_symbol = req_sym
                    chart_tf     = sym_state.get("chart_tf", "")

            # Для активного символа или если sym_state пустой — берём из opt_state
            if not chart_candles:
                with opt_lock:
                    active_sym = opt_state.get("chart_symbol", "")
                    # Если запрошен конкретный символ и он не совпадает с активным — не отдаём чужие данные
                    if req_sym and req_sym != active_sym:
                        print(f"[chart] {req_sym} не совпадает с opt_state chart_symbol={active_sym}, нет данных", flush=True)
                        chart_candles = []; chart_best = None
                        # Последний шанс: снапшот из opt_states (мог обновиться пока мы тут)
                        with opt_states_lock:
                            fallback = dict(opt_states.get(req_sym, {}))
                        if fallback.get("chart_candles"):
                            chart_candles = list(fallback["chart_candles"])
                            chart_signals = list(fallback.get("chart_signals") or [])
                            chart_symbol  = fallback.get("symbol", req_sym)
                            chart_tf      = fallback.get("chart_tf", "")
                            chart_best    = fallback.get("best")
                            chart_path    = fallback.get("chart_path", "")
                            print(f"[chart] {req_sym}: использован fallback из opt_states snapshot", flush=True)
                    else:
                        chart_candles = list(opt_state.get("chart_candles", []))
                        chart_signals = list(opt_state.get("chart_signals", []))
                        chart_symbol  = opt_state.get("chart_symbol", "")
                        chart_tf      = opt_state.get("chart_tf", "")
                        chart_best    = opt_state.get("best", None)
                        chart_path    = opt_state.get("chart_path", "")
            if not chart_best or not chart_candles:
                self.send_response(200)
                self.send_header("Content-Type","text/html;charset=utf-8"); self.end_headers()
                req_theme = qs.get("theme", ["light"])[0]
                _bg   = "#111111" if req_theme == "dark" else "#FAE6D8"
                _fg   = "#d4c8bc" if req_theme == "dark" else "#252b35"
                _sub  = "#7a7069" if req_theme == "dark" else "#848d9e"
                self.wfile.write(f"<html><body style='background:{_bg};color:{_fg};font-family:system-ui;padding:40px'><h2>⏳ График ещё не готов</h2><p style='color:{_sub};margin-top:10px'>Запустите оптимизацию и подождите первого цикла.</p><script>setTimeout(()=>location.reload(),5000)</script></body></html>".encode())
                return
            try:
                data = _build_chart_html(chart_candles, chart_signals, chart_best, chart_symbol, chart_tf).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type","text/html;charset=utf-8")
                self.send_header("Content-Length",str(len(data)))
                self.send_header("Cache-Control","no-store")
                if parsed.path=="/chart_download":
                    self.send_header("Content-Disposition",f'attachment;filename="wickfill_live_{chart_symbol.replace("_","").lower()}_{chart_tf}.html"')
                self.end_headers()
                self.wfile.write(data)
            except (BrokenPipeError,ConnectionResetError): pass
            except Exception as e: self.send_response(500);self.end_headers();self.wfile.write(str(e).encode())
        elif parsed.path == "/chart_data":
            qs = parse_qs(parsed.query)
            req_sym = qs.get("symbol",[""])[0].upper()
            cc = []; cs = []
            if req_sym:
                # Мультирежим: берём данные конкретного символа из opt_states
                with opt_states_lock:
                    sym_st = dict(opt_states.get(req_sym, {}))
                if sym_st.get("chart_candles"):
                    cc = list(sym_st["chart_candles"])
                    cs = list(sym_st.get("chart_signals") or [])
            if not cc:
                # Fallback: активный символ из opt_state
                with opt_lock:
                    active_sym = opt_state.get("chart_symbol", "")
                    if not req_sym or req_sym == active_sym:
                        cc = list(opt_state.get("chart_candles", []))
                        cs = list(opt_state.get("chart_signals", []))
            self._json({"ok": True, "candles": cc, "signals": cs})
        elif parsed.path == "/live_candle":
            qs = parse_qs(parsed.query)
            symbol = qs.get("symbol", ["BTC_USDT"])[0]
            tf     = qs.get("tf", ["1h"])[0]
            key = f"{symbol}_{tf}"
            with _live_candle_lock:
                c = _live_candle_cache.get(key)
            # Если кеш устарел (>15 сек) — запрашиваем напрямую, не ждём фоновый поток
            age = time.time() - c.get("_fetched_at", 0) if c else 999
            if not c or age > 15:
                fresh_c = _fetch_current_candle(symbol, tf)
                if fresh_c:
                    c = dict(fresh_c, _fetched_at=time.time())
                    with _live_candle_lock:
                        _live_candle_cache[key] = c
            if c and "open" in c:
                self._json({"ok": True, "t": c["t"], "o": c["open"],
                            "h": c["high"], "l": c["low"], "c": c["close"],
                            "age": round(time.time() - c.get("_fetched_at", time.time()))})
            else:
                self._json({"ok": False, "msg": "нет данных"})
        elif parsed.path == "/live_price":
            qs = parse_qs(parsed.query)
            symbol = qs.get("symbol", ["BTC_USDT"])[0]
            try:
                r = requests.get(
                    f"{GATE_API}/futures/usdt/tickers",
                    params={"contract": symbol}, timeout=5
                )
                if r.status_code == 200:
                    data = r.json()
                    price = float(data[0]["last"]) if data else None
                    self._json({"ok": True, "price": price, "symbol": symbol})
                else:
                    self._json({"ok": False, "error": f"HTTP {r.status_code}"})
            except Exception as e:
                self._json({"ok": False, "error": str(e)})
        elif parsed.path == "/ping":
            result={"ok":False,"ms":None,"error":""}
            try:
                t0=time.time()
                r=requests.get(f"{GATE_API}/futures/usdt/contracts",params={"limit":1},timeout=6)
                ms=int((time.time()-t0)*1000)
                result={"ok":True,"ms":ms,"error":""} if r.status_code==200 else {"ok":False,"ms":ms,"error":f"HTTP {r.status_code}"}
            except requests.exceptions.ConnectionError: result={"ok":False,"ms":None,"error":"Нет соединения"}
            except requests.exceptions.Timeout: result={"ok":False,"ms":None,"error":"Таймаут >6с"}
            except Exception as e: result={"ok":False,"ms":None,"error":str(e)}
            self._json(result)
        elif parsed.path.startswith("/set_eco"):
            global _eco_mode
            qs = parse_qs(parsed.query)
            v = qs.get("v", ["1"])[0]
            _eco_mode = v in ("1", "true", "on", "yes")
            self._json({"ok": True, "eco_mode": _eco_mode})
        elif parsed.path == "/scan_stop":
            import traceback
            print("[STOP] /scan_stop вызван:\n" + "".join(traceback.format_stack()), flush=True)

            _opt_stop_flag.set()
            # Немедленно сбрасываем флаг running — не ждём пока тред сам дойдёт до выхода
            with opt_lock:
                opt_state["running"] = False
                opt_state["done"] = True
            with opt_states_lock:
                for s in list(opt_states.keys()):
                    opt_states[s]["running"] = False
            self._json({"ok":True})
        elif parsed.path == "/recent_configs":
            # Читаем список конфигов с GitHub (configs/), локалка — только fallback
            items = []
            gh_ok = False
            try:
                import urllib.request as _ur
                _api = f"https://api.github.com/repos/{_GH_REPO}/contents/configs"
                _req = _ur.Request(_api, headers={"Authorization": f"token {_GH_TOKEN}", "Accept": "application/vnd.github.v3+json"})
                with _ur.urlopen(_req, timeout=8) as _r:
                    _files = json.loads(_r.read())
                seen = {}
                for _fi in _files:
                    _fn = _fi.get("name","")
                    if not _fn.startswith("wickfill_") or not _fn.endswith(".json"): continue
                    try:
                        _req2 = _ur.Request(_fi["download_url"], headers={"Authorization": f"token {_GH_TOKEN}"})
                        with _ur.urlopen(_req2, timeout=8) as _r2:
                            _d = json.loads(_r2.read())
                        seen[_fn] = {
                            "fname":    _fn,
                            "symbol":   _d.get("symbol", ""),
                            "tf":       _d.get("tf", ""),
                            "days":     _d.get("days", 0),
                            "risk_pct": _d.get("risk_pct", 20),
                            "equity":   round(_d.get("best", {}).get("equity", 0)),
                            "saved_at": _d.get("saved_at", ""),
                            "source":   "github",
                        }
                    except: pass
                items = sorted(seen.values(), key=lambda x: x["saved_at"], reverse=True)
                gh_ok = True
            except Exception as _e:
                print(f"{_ts()} [recent_configs] GitHub недоступен: {_e}", flush=True)
            self._json({"ok": True, "configs": items, "source": "github" if gh_ok else "unavailable"})
        elif parsed.path == "/delete_config":
            qs = parse_qs(parsed.query)
            fname = qs.get("fname", [""])[0]
            if not fname or "/" in fname or "\\" in fname or not fname.endswith(".json"):
                self._json({"ok": False, "msg": "Недопустимое имя файла"}); return
            # Удаляем с GitHub
            gh_del = False
            try:
                gh_del = _gh_delete_file(f"configs/{fname}", f"delete config: {fname}")
            except Exception as _e:
                print(f"{_ts()} [delete_config] GitHub: {_e}", flush=True)
            # Удаляем локально тоже (если есть)
            for d in _AUTO_DIRS:
                fp = os.path.join(d, fname)
                if os.path.isfile(fp):
                    try: os.remove(fp)
                    except: pass
            if gh_del: self._json({"ok": True, "source": "github"})
            else: self._json({"ok": True, "source": "local"})
        elif parsed.path == "/sw_stop":
            with opt_lock: opt_state["sw_running"]=False
            self._json({"ok":True})
        elif parsed.path == "/reset_running":
            # Только сбрасываем флаги UI — не трогаем оптимизатор
            with opt_lock:
                opt_state["error"]=""
            self._json({"ok":True})
        elif parsed.path == "/delete_download":
            import re as _re
            _pat=_re.compile(r'^screener_pro\s*\(\d+\)\.py$')
            deleted=[]
            candidate_dirs = [_WICKFILL_DIR, "/sdcard/Download", os.path.dirname(os.path.abspath(__file__))]
            for d in candidate_dirs:
                if not os.path.isdir(d): continue
                for fname in os.listdir(d):
                    if _pat.match(fname):
                        fp=os.path.join(d,fname)
                        try: os.remove(fp); deleted.append(fp)
                        except: pass
            if deleted: self._json({"ok":True,"msg":f"Удалён: {', '.join(deleted)}"})
            else: self._json({"ok":False,"msg":"Файл screener_pro (*).py не найден в Downloads"})
        elif parsed.path == "/rename_download":
            import re as _re
            script_name = "screener_pro.py"
            _pat2 = _re.compile(r'^screener_pro.+\.py$')
            candidate_dirs = ["/sdcard/Download", os.path.dirname(os.path.abspath(__file__))]
            renamed = False
            msg = ""
            for d in candidate_dirs:
                if not os.path.isdir(d): continue
                matches = [f for f in os.listdir(d) if _pat2.match(f)]
                if not matches: continue
                src = os.path.join(d, sorted(matches)[-1])
                dst = os.path.join(d, script_name)
                try:
                    # Шаг 1: явно удаляем screener_pro.py если существует
                    if os.path.exists(dst):
                        os.remove(dst)
                    # Шаг 2: переименовываем длинный файл в screener_pro.py
                    os.rename(src, dst)
                    renamed = True
                    msg = f"Удалён старый → переименован: {os.path.basename(src)} → {script_name}"
                    break
                except Exception as e:
                    msg = str(e)
            if renamed:
                self._json({"ok": True, "msg": msg})
            else:
                self._json({"ok": False, "msg": "Файл screener_pro (*).py не найден в Downloads"})
        elif parsed.path == "/termux_update":
            import subprocess, sys
            script_name=os.path.basename(os.path.abspath(__file__))
            script_path=os.path.abspath(__file__)
            raw_url=f"https://raw.githubusercontent.com/{_GH_REPO}/main/{script_name}"
            try:
                sh=os.path.expanduser("~/wickfill_update.sh")
                with open(sh,"w") as f:
                    f.write("#!/data/data/com.termux/files/usr/bin/bash\n")
                    f.write("termux-wake-lock\n")
                    f.write(f"pkill -9 -f {script_name}\n")
                    f.write("pkill -9 -f 'multiprocessing.spawn'\n")
                    f.write("pkill -9 -f 'multiprocessing.resource_tracker'\n")
                    f.write("sleep 2\n")
                    # Скачать свежий скрипт прямо с GitHub (токен для приватного репо)
                    f.write('curl -fsSL -H "Authorization: token ' + _GH_TOKEN + '" "' + raw_url + '?ts=$(date +%s)" -o \'' + script_path + '\' || { echo "curl failed, using existing"; }\n')
                    f.write(f"{sys.executable} '{script_path}'\n")
                os.chmod(sh, 0o755)
                subprocess.Popen(["bash", sh],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    start_new_session=True)
                self._json({"ok":True,"msg":"⏳ Скачиваю с GitHub и перезапускаю..."})
                def _die(): time.sleep(0.8); os._exit(0)
                threading.Thread(target=_die,daemon=True).start()
            except Exception as e: self._json({"ok":False,"msg":str(e)})
        elif parsed.path == "/load_result":
            qs=parse_qs(parsed.query)
            symbol=qs.get("symbol",["BTC_USDT"])[0]; tf=qs.get("tf",["1h"])[0]
            days=int(qs.get("days",["3"])[0]); risk_pct=float(qs.get("risk",["20"])[0])
            # Ищем ТОЛЬКО точное совпадение по (symbol, tf, days, risk) — никакого fallback на похожие конфиги
            fpath, data = _find_auto_config(symbol, tf, days, risk_pct)
            if not data:
                self._json({"ok":False,"msg":f"Конфиг не найден для {symbol} {tf}. Проверенные папки: {[d for d in _AUTO_DIRS if os.path.isdir(d)]}"}); return
            try:
                self._json({"ok":True,"best":data.get("best"),"top20":data.get("top20",[]),
                            "saved_at":data.get("saved_at",""),
                            "symbol":data.get("symbol",symbol),"tf":data.get("tf",tf),
                            "days":data.get("days",days),"risk_pct":data.get("risk_pct",risk_pct),
                            "file":os.path.basename(fpath) if fpath else "",
                            "path":fpath if fpath else ""})
            except Exception as e: self._json({"ok":False,"msg":str(e)})
        elif parsed.path == "/list_configs":
            import glob as _glob
            files_found = []
            search_dirs = _AUTO_DIRS + [os.path.dirname(os.path.abspath(__file__))]
            checked_dirs = []
            for d in search_dirs:
                if d in checked_dirs: continue
                checked_dirs.append(d)
                if not os.path.isdir(d): continue
                for fp in sorted(_glob.glob(os.path.join(d, "wickfill_*.json"))):
                    try:
                        size_kb = round(os.path.getsize(fp) / 1024, 1)
                        files_found.append({"name": os.path.basename(fp), "dir": d, "size_kb": size_kb})
                    except Exception:
                        files_found.append({"name": os.path.basename(fp), "dir": d})
            self._json({"ok": True, "files": files_found, "dirs": [d for d in checked_dirs if os.path.isdir(d)]})
        else:
            self.send_response(404); self.end_headers()

    def do_POST(self):
        parsed=urlparse(self.path)
        length=int(self.headers.get("Content-Length",0))
        body=self.rfile.read(length) if length else b""

        if parsed.path == "/save_result":
            try: params=json.loads(body)
            except: self._json({"ok":False,"msg":"bad JSON"}); return
            best=params.get("best"); top20=params.get("top20",[]); symbol=params.get("symbol","UNK"); tf=params.get("tf","1h")
            days=int(params.get("days",3)); risk_pct=float(params.get("risk_pct",20.0))
            if not best: self._json({"ok":False,"msg":"Нет данных"}); return
            saved=_auto_save_config(symbol, tf, days, risk_pct, best, top20)
            if saved: self._json({"ok":True,"file":os.path.basename(saved),"path":saved})
            else: self._json({"ok":False,"msg":"Не удалось записать файл — нет доступа к папке Download. Убедитесь что termux-setup-storage выполнен."})
            return

        if parsed.path == "/test_email":
            try: params=json.loads(body)
            except: self._json({"ok":False,"msg":"bad JSON"}); return
            cfg=params.get("alert_cfg",{})
            if not cfg: self._json({"ok":False,"msg":"Нет конфига"}); return
            ok = _send_telegram(cfg, "✅ WickFill — тест алерта работает!")
            if ok: self._json({"ok":True})
            else: self._json({"ok":False,"msg":opt_state.get("error","Ошибка Telegram")})
            return

        if parsed.path == "/test_ntfy":
            try: params=json.loads(body)
            except: self._json({"ok":False,"msg":"bad JSON"}); return
            cfg={"ntfy_topic": params.get("ntfy_topic","")}
            ok = _send_ntfy(cfg, "🔔 WickFill — тест ntfy.sh")
            self._json({"ok": ok, "error": "" if ok else "не удалось отправить"})
            return

        if parsed.path == "/gate_test":
            try: params=json.loads(body) if body.strip() else {}
            except: self._json({"ok":False,"msg":"bad JSON"}); return
            try:
                balance, err = _gate_get_balance(params)
                if err: self._json({"ok":False,"msg":err})
                else: self._json({"ok":True,"balance":round(balance,2)})
            except Exception as e:
                self._json({"ok":False,"msg":str(e)})
            return

        if parsed.path == "/gate_test_trade":
            try: params=json.loads(body) if body.strip() else {}
            except: self._json({"ok":False,"msg":"bad JSON"}); return
            try:
                symbol = params.get("symbol","BTC_USDT").replace("/","_").upper()
                if not symbol.endswith("_USDT"): symbol += "_USDT"
                direction = int(params.get("dir",1))
                price_r = requests.get(f"{GATE_API}/futures/usdt/tickers?contract={symbol}",timeout=5).json()
                price = float(price_r[0]["last"]) if price_r else None
                if not price: self._json({"ok":False,"msg":"Не удалось получить цену"}); return
                leverage=int(params.get("leverage",5)); notional=float(params.get("notional",5.0))*leverage
                # notional = margin * leverage (пользователь вводит маржу, умножаем на плечо)
                tp_fixed = params.get("tp_fixed"); sl_fixed = params.get("sl_fixed")
                tp = float(tp_fixed) if tp_fixed else round(price*(1+(10/100)) if direction==1 else price*(1-(10/100)),6)
                sl = float(sl_fixed) if sl_fixed else round(price*(1-(10/100)) if direction==1 else price*(1+(10/100)),6)
                ok,log=_gate_execute_signal(params,symbol,direction,price,tp,sl,leverage,0,fixed_notional_usdt=notional)
                if ok:
                    dir_str="ЛОНГ" if direction==1 else "ШОРТ"
                    self._json({"ok":True,"msg":f"{dir_str} {symbol} × {leverage} (${notional:.0f} notional), TP={tp}, SL={sl}\n{log}"})
                else:
                    self._json({"ok":False,"msg":(log or "ошибка").splitlines()[-1]})
            except Exception as e:
                self._json({"ok":False,"msg":str(e)})
            return

        if parsed.path == "/update_script":
            try:
                import urllib.request as _ur
                _raw_url = f"https://raw.githubusercontent.com/{_GH_REPO}/main/screener_pro.py"
                _headers = {"Authorization": f"token {_GH_TOKEN}",
                            "User-Agent": "WickFill-updater"}
                _req = _ur.Request(_raw_url, headers=_headers)
                with _ur.urlopen(_req, timeout=30) as _resp:
                    _data = _resp.read()
                _save_dirs = ["/sdcard/Download", os.path.expanduser("~/storage/downloads")]
                _saved_path = None
                for _d in _save_dirs:
                    if os.path.isdir(_d):
                        _out = os.path.join(_d, "screener_pro.py")
                        with open(_out, "wb") as _f:
                            _f.write(_data)
                        _saved_path = _out
                        break
                if _saved_path:
                    _kb = round(len(_data) / 1024, 1)
                    self._json({"ok": True, "path": _saved_path, "size_kb": _kb})
                else:
                    self._json({"ok": False, "msg": "Папка Download не найдена. Выполните termux-setup-storage"})
            except Exception as _e:
                self._json({"ok": False, "msg": str(_e)})
            return

        if parsed.path == "/scan":
            try: params=json.loads(body)
            except: self._json({"ok":False,"msg":"bad JSON"}); return
            global _opt_thread, _multi_symbols, _active_chart_symbol, _sw_threads, _sw_state, _eco_mode
            _eco_mode = bool(params.get("eco_mode", False))
            print(f"{_ts()} [SCAN] infinite={params.get('infinite')} symbol={params.get('wf_symbol')} tf={params.get('wf_tf')}", flush=True)
            if _opt_thread and _opt_thread.is_alive():
                # Проверяем реальное состояние: если opt_state["running"]==False —
                # тред завершается или завис, но уже не активен — разрешаем перезапуск
                with opt_lock:
                    _actually_running = opt_state.get("running", False)
                if _actually_running:
                    self._json({"ok":False,"msg":"Оптимизация уже запущена. Сначала нажмите Стоп."}); return
                else:
                    # Тред живой, но running=False (завершается или зависший zombie)
                    # Даём ему 2с на завершение, потом принудительно продолжаем
                    print(f"{_ts()} [SCAN] ⚠ Тред жив, но running=False — ожидаем завершения...", flush=True)
                    _opt_stop_flag.set()
                    _opt_thread.join(timeout=2)
            # Останавливаем старые SW-треды
            with _sw_state_lock:
                for s in list(_sw_state.keys()):
                    _sw_state[s]["running"] = False
            _sw_threads = {}
            _sw_state   = {}
            # Parse comma-separated symbols, добавляем _USDT если нет суффикса пары
            raw_syms = params.get("wf_symbol","BTC_USDT")
            def _norm_sym(s):
                s = s.strip().upper()
                if not s: return None
                return s if "_" in s else s + "_USDT"
            sym_list = [_norm_sym(s) for s in raw_syms.replace(","," ").split() if s.strip()]
            sym_list = [s for s in sym_list if s]
            if not sym_list: sym_list = ["BTC_USDT"]
            with opt_states_lock:
                _multi_symbols = sym_list
                _active_chart_symbol = sym_list[0]
                tf   = params.get("wf_tf", "1h")
                days = int(params.get("wf_days", 3) or 3)
                risk = float(params.get("wf_risk", 20) or 20)
                for s in sym_list:
                    # Пробуем загрузить сохранённый конфиг чтобы сразу показать eq
                    saved_eq, saved_wr, saved_dd, saved_tr, saved_pf, saved_sl, saved_tp = 100, 0, 0, 0, 0, None, None
                    try:
                        _, auto_data = _find_auto_config(s, tf, days, risk)
                        if auto_data and auto_data.get("best"):
                            b = auto_data["best"]
                            saved_eq = round(b.get("equity", 100), 2)
                            saved_wr = round(b.get("winrate", 0), 1)
                            saved_dd = round(b.get("max_dd", 0), 1)
                            saved_tr = b.get("trades", 0)
                            saved_pf = round(min(b.get("profit_factor", 0), 999), 2)
                            saved_sl = b.get("params", {}).get("sl_pct", None)
                            saved_tp = b.get("params", {}).get("tp_pct", None)
                    except Exception:
                        pass
                    opt_states[s] = {"symbol": s, "eq": saved_eq, "wr": saved_wr, "dd": saved_dd,
                                     "trades": saved_tr, "pf": saved_pf, "sl": saved_sl, "tp": saved_tp,
                                     "cycle": 0, "running": True, "chart_updated_at": -1,
                                     "valid": None, "windows": [], "min_stable_days": None, "days": days}
            _opt_stop_flag.clear()
            if len(sym_list) == 1:
                _opt_thread = threading.Thread(target=run_optimizer_safe, args=(params,), daemon=True)
                _opt_thread.start()
            else:
                # Сбрасываем opt_state для чистого прогресс-бара в параллельном режиме
                with opt_lock:
                    opt_state.update({"running": True, "done": False, "cycle": 0,
                                      "progress": 0, "total": 0, "pass_num": 0,
                                      "current_param": "", "elapsed": 0.0, "error": "",
                                      "logs": [], "best": None, "all_time_best": None})
                _opt_thread = threading.Thread(target=_run_multi_parallel, args=(sym_list, params), daemon=True)
                _opt_thread.start()
            self._json({"ok":True, "symbols": sym_list})
        else:
            self.send_response(404); self.end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin","*")
        self.send_header("Access-Control-Allow-Methods","GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers","Content-Type")
        self.end_headers()

    def _html(self, data):
        try:
            self.send_response(200)
            self.send_header("Content-Type","text/html;charset=utf-8")
            self.send_header("Cache-Control","no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Pragma","no-cache")
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError,ConnectionResetError): pass

    def _json(self, data):
        body=json.dumps(data,ensure_ascii=False).encode()
        try:
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Access-Control-Allow-Origin","*")
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError,ConnectionResetError): pass

if __name__ == "__main__":
    multiprocessing.freeze_support()
    # spawn — безопаснее для multiprocessing на всех платформах
    try:
        multiprocessing.set_start_method("spawn", force=True)
    except RuntimeError:
        pass
    port=8080
    import socket as _sock
    def _get_local_ip():
        try:
            s=_sock.socket(_sock.AF_INET,_sock.SOCK_DGRAM)
            s.connect(("8.8.8.8",80)); ip=s.getsockname()[0]; s.close(); return ip
        except: return "?.?.?.?"
    local_ip=_get_local_ip()
    class ReusableHTTPServer(HTTPServer):
        allow_reuse_address=True
        def server_bind(self):
            import socket
            self.socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            try: self.socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEPORT,1)
            except (AttributeError,OSError): pass
            super().server_bind()
    print(f"WickFill Optimizer v{APP_VERSION}")
    print(f"  Локально:  http://localhost:{port}")
    print(f"  По сети:   http://{local_ip}:{port}")
    print(f"Остановить: Ctrl+C")
    ReusableHTTPServer(("",port),Handler).serve_forever()
