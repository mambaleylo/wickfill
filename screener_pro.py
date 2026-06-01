#!/usr/bin/env python3
"""
WickFill Optimizer v3.107-perf
- ∞ Бесконечный режим: оптимизация крутится без остановки, рестарт после каждого цикла
- Скользящее окно: каждые N минут (по таймфрейму) добавляет свечу, убирает первую
- Live-алерт: если на новой закрытой свече сигнал по лучшим параметрам — шлёт email
- Динамический график: /chart обновляется автоматически каждые 30с
"""

import json, time, threading, random, math, os
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
    "sl_pct":             {"min": 0.2,  "max": 0.8,  "step": 0.05, "type": "float", "label": "Стоп-лосс (%)"},
    "tp_pct":             {"min": 0.5,  "max": 2.5,  "step": 0.05, "type": "float", "label": "Тейк-профит (%)"},
    "min_wick_pct":       {"min": 30.0, "max": 80.0, "step": 5.0,  "type": "float", "label": "Мин. фитиль (% диапазона)"},
    "min_wick_pct_price": {"min": 0.05, "max": 0.5,  "step": 0.05, "type": "float", "label": "Мин. фитиль (% цены)"},
    "wick_dir":           {"values": ["both", "upper", "lower"], "type": "cat",  "label": "Направление фитиля"},
    "filter_body_rat":    {"values": [True, False], "type": "bool", "label": "Фильтр: тело < фитиль"},
    "filter_consec":      {"values": [False, True], "type": "bool", "label": "Фильтр: не 2 сигнала подряд"},
    "use_confirm_candle": {"values": [True, False], "type": "bool", "label": "Подтверждающая свеча"},
    "confirm_body_pct":   {"min": 4.0,  "max": 30.0, "step": 2.0,  "type": "float", "label": "Мин. тело подтв. свечи (%)"},
    "use_rsi_filter":     {"values": [True, False], "type": "bool", "label": "RSI — включить фильтр"},
    "rsi_len":            {"min": 2,    "max": 8,    "step": 1,    "type": "int",   "label": "RSI — период"},
    "rsi_long_max":       {"min": 35.0, "max": 60.0, "step": 5.0,  "type": "float", "label": "RSI — порог лонга"},
    "rsi_short_min":      {"min": 35.0, "max": 60.0, "step": 5.0,  "type": "float", "label": "RSI — порог шорта"},
    "use_level_filter":   {"values": [True, False], "type": "bool", "label": "Уровни HH/LL"},
    "level_lookback":     {"min": 3,    "max": 20,   "step": 1,    "type": "int",   "label": "Уровни — история (св.)"},
    "level_toler_pct":    {"min": 0.1,  "max": 0.5,  "step": 0.1,  "type": "float", "label": "Уровни — допуск (%)"},
    "use_geo_filter":     {"values": [True, False], "type": "bool", "label": "Перцентиль фитиля"},
    "geo_lookback":       {"min": 10,   "max": 30,   "step": 5,    "type": "int",   "label": "Перцентиль — окно"},
    "geo_min_pct":        {"min": 50.0, "max": 90.0, "step": 5.0,  "type": "float", "label": "Перцентиль — мин (%)"},
    "use_css_filter":     {"values": [True, False], "type": "bool", "label": "CSS фильтр"},
    "css_min_score":      {"min": 40.0, "max": 90.0, "step": 5.0,  "type": "float", "label": "CSS — мин. балл"},
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
    "ret_lookback":       {"min": 30,   "max": 100,  "step": 10,   "type": "int",   "label": "Гео-1 — история"},
    "ret_n":              {"min": 1,    "max": 5,    "step": 1,    "type": "int",   "label": "Гео-1 — возврат N баров"},
    "ret_wick_sim":       {"min": 50.0, "max": 80.0, "step": 10.0, "type": "float", "label": "Гео-1 — схожесть (%)"},
    "min_return_pct":     {"min": 50.0, "max": 80.0, "step": 5.0,  "type": "float", "label": "Гео-1 — мин. WR (%)"},
    "use_repeat_filter":  {"values": [True, False], "type": "bool", "label": "Гео-3 проверенный уровень"},
    "rep_lookback":       {"min": 50,   "max": 150,  "step": 25,   "type": "int",   "label": "Гео-3 — история"},
    "rep_zone_pct":       {"min": 0.2,  "max": 0.6,  "step": 0.1,  "type": "float", "label": "Гео-3 — зона (±%)"},
    "rep_min_win":        {"min": 1,    "max": 3,    "step": 1,    "type": "int",   "label": "Гео-3 — мин. отработок"},
    "use_cluster_filter": {"values": [True, False], "type": "bool", "label": "Гео-4 кластер фитилей"},
    "cluster_lookback":   {"min": 30,   "max": 80,   "step": 10,   "type": "int",   "label": "Гео-4 — история"},
    "cluster_pct":        {"min": 0.15, "max": 0.4,  "step": 0.05, "type": "float", "label": "Гео-4 — зона (±%)"},
    "cluster_min":        {"min": 2,    "max": 4,    "step": 1,    "type": "int",   "label": "Гео-4 — мин. фитилей"},
    "use_close_filter":   {"values": [False, True], "type": "bool", "label": "Позиция закрытия"},
    "close_long_min_pct": {"min": 50.0, "max": 80.0, "step": 10.0, "type": "float", "label": "Закрытие лонг — верхние N%"},
    "close_short_max_pct":{"min": 20.0, "max": 50.0, "step": 10.0, "type": "float", "label": "Закрытие шорт — нижние N%"},
    "use_quiet_filter":   {"values": [True, False], "type": "bool", "label": "Тихая зона ATR"},
    "quiet_atr_len":      {"min": 5,    "max": 20,   "step": 5,    "type": "int",   "label": "ATR — период"},
    "quiet_max_ratio":    {"min": 1.1,  "max": 2.0,  "step": 0.1,  "type": "float", "label": "ATR — макс. ratio"},
    "quiet_min_ratio":    {"min": 0.3,  "max": 0.7,  "step": 0.1,  "type": "float", "label": "ATR — мин. ratio"},
    "use_sweep_filter":   {"values": [True, False], "type": "bool", "label": "Sweep ликвидность"},
    "sweep_len":          {"min": 5,    "max": 20,   "step": 5,    "type": "int",   "label": "Sweep — период"},
    "sweep_toler_pct":    {"min": 0.3,  "max": 1.0,  "step": 0.1,  "type": "float", "label": "Sweep — допуск (%)"},
    "use_ms_filter":      {"values": [False, True], "type": "bool", "label": "Структура рынка HH/HL"},
    "ms_lookback":        {"min": 20,   "max": 60,   "step": 10,   "type": "int",   "label": "Структура — период"},
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
}

# ═══════════════════════════════════════════════════════════════
# GLOBAL STATE
# ═══════════════════════════════════════════════════════════════
opt_state = {
    "running": False, "done": False, "infinite": False,
    "cycle": 0,       # номер цикла бесконечного режима
    "progress": 0, "total": 0, "generation": 0, "pass_num": 0,
    "current_param": "", "logs": [], "best": None, "top20": [], "valid": None, "windows": [], "min_stable_days": None,
    "started_at": "", "elapsed": 0.0, "error": "", "cycle_times": [], "avg_cycle_s": None,
    "chart_candles": [], "chart_signals": [], "chart_symbol": "", "chart_tf": "",
    "chart_path": "", "chart_updated_at": 0,
    # sliding window
    "sw_running": False, "sw_last_update": 0, "sw_candle_count": 0,
    # live signal alert
    "last_signal_t": 0,   # timestamp последней свечи с сигналом (чтобы не дублировать)
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
    перегружает исторические свечи с API."""
    _last_refresh = 0  # время последней полной перезагрузки истории

    while True:
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
                                opt_state["chart_candles"] = new_cc
                                opt_state["chart_signals"]  = sigs
                            cc = new_cc
                            _last_refresh = now
                            print(f"{_ts()} [SW] ✅ Перезагружено {len(fresh)} свечей", flush=True)
                    except Exception as e:
                        print(f"{_ts()} [SW] ❌ Ошибка перезагрузки: {e}", flush=True)

                # Обновляем незакрытую свечу
                c = _fetch_current_candle(symbol, tf)
                if c:
                    key = f"{symbol}_{tf}"
                    with _live_candle_lock:
                        _live_candle_cache[key] = dict(c, _fetched_at=time.time())
                    with opt_lock:
                        cc2 = list(opt_state.get("chart_candles", []))
                        if cc2:
                            live_c = {"t":c["t"],"o":c["open"],"h":c["high"],
                                      "l":c["low"],"c":c["close"],"live":True}
                            if cc2[-1].get("live"):
                                # Обновляем существующую live-свечу
                                cc2[-1] = live_c
                                opt_state["chart_candles"] = cc2
                            elif c["t"] >= cc2[-1]["t"]:
                                # >= : добавляем live даже если t совпадает с последней закрытой
                                if c["t"] == cc2[-1]["t"]:
                                    cc2.pop()  # убираем закрытую версию той же свечи
                                opt_state["chart_candles"] = cc2 + [live_c]
        except Exception as e:
            print(f"{_ts()} [SW] ⚠ {e}", flush=True)
        time.sleep(3)

# Запускаем фоновый поток сразу
threading.Thread(target=_live_candle_updater, daemon=True).start()

# ═══════════════════════════════════════════════════════════════
# SIMULATE
# ═══════════════════════════════════════════════════════════════
def _simulate(candles_list, p, days_limit, init_deposit=100.0, risk_pct=20.0,
              max_pos=6000.0, _collect=False):
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

    if not candles_list or len(candles_list) < max(ll, gl, rl, q_atr, sw_len, ms_lb) + 10:
        return None
    if days_limit > 0:
        cutoff = time.time() - days_limit * 86400
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

    start_i=max(ll,gl,rl,q_atr,sw_len,ms_lb,ret_lb,rep_lb,clu_lb)+2
    start_i=min(start_i,n-1)

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

        body_ok_up=(not fbr) or (body<up_w)
        body_ok_dn=(not fbr) or (body<dn_w)

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
            geo_up=sum(1 for w in hist_up if up_w>w)/len(hist_up)*100 if hist_up else 0
            geo_dn=sum(1 for w in hist_dn if dn_w>w)/len(hist_dn)*100 if hist_dn else 0
            geo_ok_l=geo_up>=gmin; geo_ok_s=geo_dn>=gmin
        else:
            geo_ok_l=geo_ok_s=True

        # OPT: _css использует _all_rng вместо candles_list[j]["high"]-candles_list[j]["low"]
        def _css(is_long):
            wick=dn_w if is_long else up_w; w_pct=dn_w_pct if is_long else up_w_pct
            s1=min(w_pct/mwp*100,100) if mwp>0 else 100
            cp=(cl-lo)/rng*100 if rng>0 else 50
            s2=cp if is_long else 100-cp; s2=max(min(s2,100),0)
            s3=max(min((1-body/wick)*100,100),0) if wick>0 else 0
            _cs=max(0,i-20); hist_rng=_all_rng[_cs:i]
            s4=sum(1 for r2 in hist_rng if rng>r2)/len(hist_rng)*100 if hist_rng else 50
            wp_v=dn_w_pp if is_long else up_w_pp
            s5=min(wp_v/mwpp*100,100) if mwpp>0 else 100
            tw=ww+wc+wb+wr_w+wp_w
            return (s1*ww+s2*wc+s3*wb+s4*wr_w+s5*wp_w)/tw if tw>0 else 0

        if ucss:
            css_ok_l=is_up_w and _css(False)>=css_mn
            css_ok_s=is_dn_w and _css(True)>=css_mn
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
        if uswf:
            sw_hi=_slide_hi_sw[i-1] if i>0 else hi
            sw_lo=_slide_lo_sw[i-1] if i>0 else lo
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

        if uretf:
            ret_up=_calc_return_rate(i,True)  if is_up_w else None
            ret_dn=_calc_return_rate(i,False) if is_dn_w else None
            ret_ok_l=ret_up is not None and ret_up>=ret_minwr
            ret_ok_s=ret_dn is not None and ret_dn>=ret_minwr
        else:
            ret_ok_l=ret_ok_s=True

        if urepf:
            rep_ok_l=is_up_w and _count_tested_level(i,hi,True)>=rep_min
            rep_ok_s=is_dn_w and _count_tested_level(i,lo,False)>=rep_min
        else:
            rep_ok_l=rep_ok_s=True

        if ucluf:
            clu_ok_l=is_up_w and _count_wick_cluster(i,hi,True)>=clu_min
            clu_ok_s=is_dn_w and _count_wick_cluster(i,lo,False)>=clu_min
        else:
            clu_ok_l=clu_ok_s=True

        if uclof and rng>0:
            close_pos=(cl-lo)/rng*100
            clo_ok_l=close_pos>=clo_lng; clo_ok_s=close_pos<=clo_sht
        else:
            clo_ok_l=clo_ok_s=True

        long_sig_base=(is_up_w and body_ok_up and rsi_ok_l and near_hi
                       and geo_ok_l and css_ok_l and quiet_ok
                       and sweep_ok_l and ms_ok_l and ret_ok_l
                       and rep_ok_l and clu_ok_l and clo_ok_l)
        short_sig_base=(is_dn_w and body_ok_dn and rsi_ok_s and near_lo
                        and geo_ok_s and css_ok_s and quiet_ok
                        and sweep_ok_s and ms_ok_s and ret_ok_s
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

        # --- Депозит: log(equity) ---
        profit_bonus=_math.log(max(equity,1.0))*4.0

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
    print(f"{_ts()} [fetch] {symbol} {tf} {days}д — нужно ~{total_needed} свечей...", flush=True)
    while current_from < now:
        pct = int((current_from - since) / max(now - since, 1) * 100)
        print("[fetch] {}% ({} св.)".format(pct, len(all_candles)), end="\r", flush=True)
        try:
            r = requests.get(f"{GATE_API}/futures/usdt/candlesticks",
                params={"contract": symbol, "interval": tf,
                        "from": current_from, "limit": LIMIT}, timeout=15)
            if r.status_code != 200:
                last_http_error = f"HTTP {r.status_code}: {r.text[:200]}"
                print(f"\n{_ts()} [fetch] ❌ {last_http_error}", flush=True); break
            data = r.json()
            if not isinstance(data, list):
                last_http_error = f"Неожиданный ответ API: {str(data)[:200]}"
                print(f"\n{_ts()} [fetch] ❌ {last_http_error}", flush=True); break
            if not data: break
            for c in data:
                t = int(c.get("t", 0))
                if t < since - interval_sec: continue  # мягкий порог
                all_candles.append({"t": t, "open": float(c["o"]),
                    "high": float(c["h"]), "low": float(c["l"]), "close": float(c["c"])})
            last_t = int(data[-1].get("t", 0))
            next_from = last_t + interval_sec
            if next_from <= current_from: break
            if last_t >= now - interval_sec: break
            current_from = next_from
            time.sleep(0.05)
        except Exception as e:
            last_exception = str(e)
            print(f"\n{_ts()} [fetch] ❌ Ошибка: {e}", flush=True); break
    seen = set(); result = []
    for c in sorted(all_candles, key=lambda x: x["t"]):
        if c["t"] not in seen: seen.add(c["t"]); result.append(c)
    print(f"\n{_ts()} [fetch] ✅ Готово: {len(result)} свечей (ожидалось ~{total_needed})", flush=True)
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
    с предпоследней чтобы убедиться что это новый интервал."""
    try:
        interval_sec = TF_SECONDS.get(tf, 3600)
        r = requests.get(f"{GATE_API}/futures/usdt/candlesticks",
            params={"contract": symbol, "interval": tf, "limit": 3}, timeout=8)
        if r.status_code != 200:
            print(f"[live_candle] HTTP {r.status_code}", flush=True)
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
        print(f"[live_candle] exception: {e}", flush=True)
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
    try:
        token = cfg.get("tg_token","")
        chat_id = cfg.get("tg_chat_id","")
        if not token or not chat_id: return False
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        resp = requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, timeout=10)
        if not resp.ok:
            with opt_lock: opt_state["error"]=f"Telegram: {resp.text}"
        return resp.ok
    except Exception as e:
        with opt_lock: opt_state["error"]=f"Telegram: {e}"
        return False

def _send_signal_email(cfg, symbol, tf, direction, entry, tp, sl, candle_t):
    dir_str="🔵 ЛОНГ" if direction==1 else "🟡 ШОРТ"
    # Показываем время ЗАКРЫТИЯ свечи (открытие + интервал), в московском времени (UTC+3)
    close_t = candle_t + TF_SECONDS.get(tf, 3600)
    moscow_offset = 3 * 3600  # UTC+3
    dt = time.strftime("%Y-%m-%d %H:%M", time.gmtime(close_t + moscow_offset))
    text = (
        f"🔔 <b>WickFill Сигнал</b>\n\n"
        f"{dir_str} <b>{symbol}</b> {tf}\n"
        f"🕐 {dt}\n\n"
        f"📥 Вход: <b>{entry:.6g}</b>\n"
        f"✅ Тейк-профит: <b>{tp:.6g}</b>\n"
        f"❌ Стоп-лосс: <b>{sl:.6g}</b>"
    )
    return _send_telegram(cfg, text)

# ═══════════════════════════════════════════════════════════════
# CHART HTML (live — читается с диска каждый раз)
# ═══════════════════════════════════════════════════════════════
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
  --green:#3a7d52;--red:#a03030;--yellow:#8a6a1a;
  --green-light:rgba(58,125,82,.1);--red-light:rgba(160,48,48,.1);
}}
html,body{{height:100%;background:#1e1a17;color:#d4c8bc;font-family:'DM Sans',system-ui,sans-serif;font-size:13px;overflow:hidden;display:flex;flex-direction:column}}
[data-theme="light"] body{{background:#fafafa;color:#252b35}}
[data-theme="light"] #tooltip{{background:rgba(248,249,251,.97);border:1px solid rgba(30,40,60,.12);color:#252b35;box-shadow:0 4px 16px rgba(30,40,60,.10)}}
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
  const clrBg        = isDark ? '#1e1a17' : '#fafafa';
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
  // Active open trade — find regardless of viewport (labels always visible)
  const activeSig=SIGNALS.find(s=>s.open_end===true);
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
    ctx.strokeStyle=isLong?'#4a7fc1':'#c8902a';ctx.lineWidth=1.2;ctx.setLineDash([]);ctx.beginPath();ctx.moveTo(x1,py(s.ep));ctx.lineTo(x2,py(s.ep));ctx.stroke();
  }}
  // TP/SL dashed lines and labels for active open trade — always drawn regardless of viewport
  if(activeSig){{
    const isLong=activeSig.dir===1;
    const tpY=py(activeSig.tp),slY=py(activeSig.sl);
    // x-range: from signal bar to right edge of visible area
    const aViC=Math.max(0,activeSig.bar_i-viewStart);
    const ax1=PAD_L+aViC*cw, ax2=W-PAD_R;
    ctx.setLineDash([4,3]);
    ctx.strokeStyle=isLong?'#3a7d52':'#a03030';ctx.lineWidth=1;
    ctx.beginPath();ctx.moveTo(ax1,tpY);ctx.lineTo(ax2,tpY);ctx.stroke();
    ctx.strokeStyle='#b0a090';
    ctx.beginPath();ctx.moveTo(ax1,slY);ctx.lineTo(ax2,slY);ctx.stroke();
    ctx.setLineDash([]);
    ctx.font='bold 9px system-ui';ctx.textAlign='left';
    ctx.fillStyle=isLong?'rgba(58,125,82,0.85)':'rgba(160,48,48,0.85)';
    ctx.beginPath();ctx.roundRect(W-PAD_R+1,tpY-7,PAD_R-2,14,3);ctx.fill();
    ctx.fillStyle='#fff';ctx.fillText('TP '+activeSig.tp.toPrecision(5),W-PAD_R+4,tpY+3);
    ctx.fillStyle='rgba(140,120,100,0.75)';
    ctx.beginPath();ctx.roundRect(W-PAD_R+1,slY-7,PAD_R-2,14,3);ctx.fill();
    ctx.fillStyle='#fff';ctx.fillText('SL '+activeSig.sl.toPrecision(5),W-PAD_R+4,slY+3);
    ctx.font='10px system-ui';
  }}
  // Current price label — always visible
  const lastC=vis[vis.length-1];
  if(lastC){{
    const curPrice=lastC.c,curY=py(curPrice),isUp=lastC.c>=lastC.o;
    const cpCol=isUp?'#3a7d52':'#a03030';
    ctx.setLineDash([2,3]);ctx.strokeStyle=cpCol+'80';ctx.lineWidth=1;
    ctx.beginPath();ctx.moveTo(PAD_L,curY);ctx.lineTo(W-PAD_R,curY);ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle=cpCol;ctx.font='bold 9px system-ui';ctx.textAlign='left';
    ctx.beginPath();ctx.roundRect(W-PAD_R+1,curY-7,PAD_R-2,14,3);ctx.fill();
    ctx.fillStyle='#fff';ctx.fillText(curPrice.toPrecision(6),W-PAD_R+4,curY+3);
  }}
  for(let i=0;i<vis.length;i++){{
    const c=vis[i],x=cx(i),bull=c.c>=c.o,isLive=c.live===true;
    const col=bull?'#3a7d52':'#a03030';
    ctx.globalAlpha=isLive?0.55:1.0;
    ctx.strokeStyle=col;ctx.fillStyle=col;ctx.lineWidth=Math.max(1,cw*0.1);
    if(isLive) ctx.setLineDash([3,2]);
    ctx.beginPath();ctx.moveTo(x,py(c.h));ctx.lineTo(x,py(c.l));ctx.stroke();
    ctx.setLineDash([]);
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
    const x=cx(vi),isLong=s.dir===1;
    const c_sig=vis[vi];
    const arrowSz=Math.max(4,Math.min(7,cw*0.45));
    const arrowOff=Math.max(14,Math.min(22,cw*2.2));
    const isOpenEnd=s.open_end===true,isWin=s.win===true;
    ctx.fillStyle=isLong?'#4a7fc1':'#c8902a';
    ctx.strokeStyle=isOpenEnd?'#b0a090':s.win===null?'#b0a090':isWin?'#3a7d52':'#a03030';
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
      if(_labelFits(lx, tw)){{
        ctx.fillStyle=pct>=0?'rgba(58,125,82,0.9)':'rgba(160,48,48,0.9)';
        ctx.beginPath();ctx.roundRect(lx-tw/2-3,ly-11,tw+6,14,3);ctx.fill();
        ctx.fillStyle='#fff';ctx.fillText(lbl,lx,ly);
      }}
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
  const rect=wrap.getBoundingClientRect(),offsetX=e.clientX-rect.left;
  const W=wrap.clientWidth,drawW=W-PAD_L_C-PAD_R_C;
  // Hide tooltip when hovering over price scale area
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
  const tx=offsetX+14;tip.style.left=(tx+tip.offsetWidth>W?tx-tip.offsetWidth-20:tx)+'px';tip.style.top=Math.max(0,e.offsetY-10)+'px';
}});
wrap.addEventListener('mouseleave',()=>tip.style.display='none');
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
          // Та же свеча — просто обновляем OHLC
          last.o = d.o; last.h = d.h; last.l = d.l; last.c = d.c;
        }} else if (d.t > last.t) {{
          // Новый интервал: «закрываем» старую live (убираем флаг, она остаётся как закрытая свеча)
          delete last.live;
          CANDLES.push({{t:d.t, o:d.o, h:d.h, l:d.l, c:d.c, live:true}});
        }}
      }} else {{
        // Добавляем live-свечу: если t совпадает с последней закрытой — заменяем её
        if (last && !last.live && d.t === last.t) CANDLES.pop();
        CANDLES.push({{t:d.t, o:d.o, h:d.h, l:d.l, c:d.c, live:true}});
      }}
      // Подтягиваем viewport только если пользователь был у правого края
      // или live-свеча вышла за пределы видимой области
      if (wasAtEnd || CANDLES.length - 1 >= viewStart + viewLen) {{
        viewStart = Math.max(0, CANDLES.length - viewLen);
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

// Полный перезапрос страницы раз в 5 минут
setTimeout(() => location.reload(), 300000);

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
            ok = _send_telegram(alert_cfg, text)
            status = "✓" if ok else "✕"
            print(f"[trade_close] {status} {symbol} {tf} {'ЛОНГ' if is_long else 'ШОРТ'} {pct_str} {res_str}", flush=True)

def _check_new_candle_signal(candles, best_params, risk_pct, alert_cfg):
    """Проверяет последнюю свечу. Если сигнал — шлёт email."""
    if not best_params or not alert_cfg: return
    if len(candles) < 5: return

    with opt_lock:
        last_signal_t = opt_state.get("last_signal_t", 0)
        symbol = opt_state.get("chart_symbol", "?")
        tf     = opt_state.get("chart_tf", "?")

    # Запускаем симуляцию с _collect=True только на последней части данных
    sim = _simulate(candles, best_params, 0, _collect=True, risk_pct=risk_pct)
    if not sim or not sim["_signals"]: return

    sigs = sim["_signals"]
    # Ищем сигнал на предпоследней свече — последней закрытой
    # last_bar — текущая открытая, last_bar-1 — последняя закрытая
    last_bar = len(candles) - 1
    signal_bar = last_bar - 1
    for s in sigs:
        if s["bar_i"] == signal_bar:
            candle_t = candles[signal_bar]["t"]
            if candle_t <= last_signal_t:
                return  # уже отправляли
            ep = s["ep"]; tp = s["tp"]; sl = s["sl"]; direction = s["dir"]
            ok = _send_signal_email(alert_cfg, symbol, tf, direction, ep, tp, sl, candle_t)
            if ok:
                with opt_lock:
                    opt_state["last_signal_t"] = candle_t
                with alert_lock:
                    alert_state["sent"] += 1
                    alert_state["signals"].insert(0, {
                        "symbol": symbol, "tf": tf, "dir": direction,
                        "ep": ep, "tp": tp, "sl": sl, "t": candle_t,
                        "ts": time.strftime("%H:%M:%S", time.gmtime(candle_t + TF_SECONDS.get(tf, 3600) + 3*3600))
                    })
                    alert_state["signals"] = alert_state["signals"][:50]
                print(f"[alert] Сигнал отправлен: {symbol} {tf} {'ЛОНГ' if direction==1 else 'ШОРТ'} ep={ep:.6g}")
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
        sleep_total = wait_sec + 5
        print(f"[sw:{symbol}] Следующая свеча через {sleep_total}с")

        for _ in range(sleep_total):
            if not _get_running(): break
            time.sleep(1)

        if not _get_running(): break

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

            if alert_cfg and prev_signals_for_close:
                _check_trade_close(prev_signals_for_close, chart_signals_data, alert_cfg, symbol, tf)
            if alert_cfg:
                _check_new_candle_signal(new_candles, best_p, risk_pct, alert_cfg)
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
    saved = False
    for d in _AUTO_DIRS:
        if not os.path.isdir(d): continue
        try:
            fpath = os.path.join(d, fname)
            with open(fpath, "w", encoding="utf-8") as f:
                f.write(txt)
            print(f"[perf] Сохранён: {fpath}", flush=True)
            saved = True
            break
        except Exception as e:
            print(f"[perf] Ошибка записи {d}: {e}", flush=True)
    if not saved:
        print(f"[perf] Не удалось сохранить лог\n{txt[:2000]}", flush=True)

def _run_one_cycle(candles, days, risk_pct, olog, t0, tf="1h", n_restarts=8,
                   prev_best_params=None, prev_top20=None, pool=None, n_workers=1):
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
        if _small_tf and ind and ind.get("tp_pct", 0) > 1.2:
            ind = dict(ind); ind["tp_pct"] = 1.2
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

    # Фаза 1: многоточечный старт
    if prev_best_params:
        start_points = [_clamp_tp(prev_best_params)] + [_rand_ind() for _ in range(n_restarts - 1)]
        olog(f"━━ ФАЗА 1: лучший предыдущего цикла + {n_restarts-1} случайных ━", "ok")
    else:
        start_points = [_default_individual()] + [_rand_ind() for _ in range(n_restarts - 1)]
        olog(f"━━ ФАЗА 1: {n_restarts} стартов ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "ok")

    local_bests = []
    for i, start_ind in enumerate(start_points):
        if stop_flag(): break
        label = "Старт #1 (предыдущий лучший)" if (i==0 and prev_best_params) else f"Старт #{i+1}"
        olog(f"── {label} ──", "ok")
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
            # best и all_time_best НЕ трогаем здесь — они обновляются только в конце цикла

    if stop_flag(): return None, None, top20_global

    local_bests.sort(key=lambda x: -x[0])
    best_f, best_r1, best_p1 = local_bests[0]

    # Фаза 2: Basin Hopping от лучшей точки
    # OPT: early-stop после 4 итераций подряд без улучшения (экономит ~60% времени BH)
    BH_MAX = 12; BH_PATIENCE = 4
    olog(f"━━ ФАЗА 2: Basin Hopping (макс {BH_MAX} итераций, patience={BH_PATIENCE}) ━━━━━━━━", "ok")
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
        keys_to_perturb = random.sample(_KEYS, max(1, int(len(_KEYS)*0.35)))
        for k in keys_to_perturb:
            # Пропускаем зависимый параметр если его родитель отключён
            parent = FILTER_GROUPS.get(k)
            if parent and not perturbed.get(parent, True):
                continue
            spec=PARAM_SPACE[k]; grid=_grids_local[k]
            if spec["type"] in ("bool","cat"): perturbed[k]=random.choice(spec["values"])
            else:
                idx=grid.index(bh_current[k]) if bh_current[k] in grid else len(grid)//2
                step=random.randint(1,max(1,len(grid)//4))
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

def _clamp_tp_result(r, tf):
    """Обрезает tp_pct > 1.2 для TF < 1h в result-объекте (модульный уровень)."""
    if not r or TF_SECONDS.get(tf, 3600) >= 3600: return r
    if r.get("params", {}).get("tp_pct", 0) <= 1.2: return r
    r2 = dict(r); r2["params"] = dict(r["params"]); r2["params"]["tp_pct"] = 1.2
    return r2

def _clamp_tp_params(p, tf):
    """Обрезает tp_pct > 1.2 для TF < 1h в dict params."""
    if not p or TF_SECONDS.get(tf, 3600) >= 3600: return p
    if p.get("tp_pct", 0) <= 1.2: return p
    p2 = dict(p); p2["tp_pct"] = 1.2
    return p2

def _config_key(symbol, tf, days, risk_pct):
    """Уникальный ключ набора параметров для имени файла."""
    sym = symbol.replace("_","").replace("/","").lower()
    return f"{sym}_{tf}_{days}d_r{int(round(risk_pct))}"

def _config_filename(symbol, tf, days, risk_pct, equity):
    """wickfill_btcusdt_15m_3d_$234_r20.json"""
    sym = symbol.replace("_","").replace("/","").lower()
    eq  = int(round(equity))
    r   = int(round(risk_pct))
    return f"wickfill_{sym}_{tf}_{days}d_${eq}_r{r}.json"

def _find_auto_config(symbol, tf, days, risk_pct):
    """Ищет лучший конфиг в Downloads по (symbol,tf,days,risk). Возвращает (path, data) или (None,None)."""
    import glob as _glob
    days = int(days)
    sym = symbol.replace("_","").replace("/","").lower()
    r   = int(round(risk_pct))
    pat = f"wickfill_{sym}_{tf}_{days}d_$*_r{r}.json"
    best_path, best_data, best_eq = None, None, -1
    for d in _AUTO_DIRS:
        if not os.path.isdir(d): continue
        for fpath in _glob.glob(os.path.join(d, pat)):
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if not (data.get("best") and data["best"].get("params")): continue
                if int(data.get("days", days)) != days: continue
                if abs(float(data.get("risk_pct", risk_pct)) - risk_pct) > 0.1: continue
                eq = data["best"].get("equity", 0)
                if eq > best_eq:
                    best_eq = eq; best_path = fpath; best_data = data
            except Exception:
                pass
    return best_path, best_data

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

    _log(f"💾 Сохраняю в: {save_dir}", "info")

    fname = _config_filename(symbol, tf, days, risk_pct, eq)
    fpath = os.path.join(save_dir, fname)

    data = {
        "best": best, "top20": top20,
        "symbol": symbol, "tf": tf,
        "days": days, "risk_pct": risk_pct,
        "saved_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }

    # Атомарная запись: пишем во временный файл рядом, потом os.replace()
    tmp_path = None
    try:
        tmp_fd, tmp_path = tempfile.mkstemp(dir=save_dir, suffix=".tmp")
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, fpath)
    except Exception as e:
        _log(f"⚠ Сохранение не удалось: {save_dir} → {e}", "warn")
        _log(f"  Проверенные папки: {', '.join(tried)}", "warn")
        print(f"{_ts()} [save] ❌ Ошибка записи в {save_dir}: {e}", flush=True)
        if tmp_path:
            try: os.remove(tmp_path)
            except Exception: pass
        return None

    # Удалить ВСЕ старые файлы того же набора параметров (кроме только что сохранённого)
    for d in _AUTO_DIRS:
        if not os.path.isdir(d): continue
        for old_f in _glob.glob(os.path.join(d, pat)):
            if os.path.abspath(old_f) == os.path.abspath(fpath):
                continue  # это наш новый файл — не трогаем
            try:
                os.remove(old_f)
                print(f"{_ts()} [save] 🗑 Удалён старый файл: {old_f}", flush=True)
            except Exception as e:
                print(f"{_ts()} [save] ⚠ Не удалось удалить {old_f}: {e}", flush=True)

    # Обновляем MediaStore на Android чтобы файл появился в файловых менеджерах
    try:
        import subprocess
        subprocess.Popen(["termux-media-scan", fpath],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass

    if olog: olog(f"💾 Сохранено: {fpath}", "found")
    else:
        with opt_lock:
            opt_state["logs"].append({"ts": time.strftime("%H:%M:%S"), "msg": f"💾 Сохранено: {fpath}", "level": "found"})
    print(f"{_ts()} [save] ✅ Сохранён: {fpath}", flush=True)
    return fpath

def run_optimizer(params):
    global _sw_candles, _sw_params, _sw_risk
    symbol       = params.get("wf_symbol", "BTC_USDT")
    tf           = params.get("wf_tf", "1h")
    days         = _si(params.get("wf_days"), 3)
    risk_pct     = max(1.0, min(100.0, _sf(params.get("wf_risk"), 20.0)))
    infinite     = params.get("infinite", False)
    alert_cfg    = params.get("alert_cfg", None)  # dict или None
    n_candles    = _si(params.get("wf_n_candles"), 0)
    seed         = params.get("seed", None)        # {best, top20} из загруженного файла

    _opt_stop_flag.clear()
    _sw_risk = risk_pct

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
    olog(f"📡 Загрузка свечей {symbol} {tf} за {days}д...")
    candles = _fetch_candles(symbol, tf, days)
    if len(candles) < 30:
        reason = _last_fetch_error or "нет данных от биржи"
        olog(f"❌ Мало свечей: {len(candles)} — {reason}", "error")
        with opt_lock: opt_state["running"]=False; opt_state["error"]=f"Мало свечей: {len(candles)}"
        return
    # Считаем сколько свечей реально попадёт в бэктест (те же условия что в _simulate)
    cutoff_check = time.time() - days * 86400
    candles_in_window = [c for c in candles if c.get("t", 0) >= cutoff_check]
    expected_per_day = round(86400 / TF_SECONDS.get(tf, 3600))
    olog(f"   Загружено {len(candles)} свечей → в окне {days}д: {len(candles_in_window)} (≈{expected_per_day}/день × {days}д = {expected_per_day*days})", "ok")

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
    # Автоперезагрузка свечей: каждые 4 интервала TF
    _reload_interval_sec = TF_SECONDS.get(tf, 3600) * 2
    _last_candle_reload  = time.time()
    olog(f"🔄 Автообновление свечей каждые {_reload_interval_sec//60} мин ({2} × {tf})", "info")
    # Сразу заполняем из seed если он есть
    if seed and seed.get("best") and seed["best"].get("params"):
        _s = dict(seed["best"])
        if "validated_fitness" not in _s:
            _s["validated_fitness"] = _s.get("fitness", 0)
        _global_best_ever = _s

    # Авто-загрузка конфига из Downloads (если нет ручного seed)
    if not seed:
        existing_dirs = [d for d in _AUTO_DIRS if os.path.isdir(d)]
        import glob as _glob2
        sym2 = symbol.replace("_","").replace("/","").lower()
        r2   = int(round(risk_pct))
        search_pat = f"wickfill_{sym2}_{tf}_{days}d_$*_r{r2}.json"
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
        prev_best_params = _clamp_tp_params(dict(seed["best"]["params"]), tf)
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

        # Между циклами — автоперезагрузка свечей каждые 2 интервала TF
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
        final_result, final_params, top20 = _run_one_cycle(
            current_candles, days, risk_pct, olog, t0, tf,
            prev_best_params=prev_best_params if infinite else None,
            prev_top20=prev_top20 if infinite else None,
            pool=_shared_pool, n_workers=_n_workers)
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
            all_time_best = _global_best_ever
            prev_best_params = dict(cycle_best["params"])  # следующий цикл стартует с лучшего этого цикла

            _prev_best_eq = getattr(run_optimizer, '_prev_reported_eq', 0)
            is_new_rec = all_time_best.get("equity", 0) > _prev_best_eq
            run_optimizer._prev_reported_eq = all_time_best.get("equity", 0)
            rec_flag = "🆕" if is_new_rec else "→"
            olog(f"✅ Цикл #{cycle} готов за {int(cycle_elapsed)}с | {rec_flag} ${all_time_best['equity']:.2f} WR {all_time_best['winrate']:.1f}% Сд {all_time_best['trades']} DD {all_time_best['max_dd']:.1f}%", "found" if is_new_rec else "ok")

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
                """Прогоняет конфиг на отрезке [now - d_from*86400 .. now - (d_to or 0)*86400]."""
                cutoff_from = now_ts - d_from * 86400
                cutoff_to   = now_ts - (d_to or 0) * 86400
                sl = [c for c in _fresh_candles if cutoff_from <= c.get("t", 0) < cutoff_to]
                if len(sl) < 10: return None
                return _simulate(sl, all_time_params, 0, risk_pct=risk_pct)

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
            cutoff = time.time() - days * 86400
            chart_candles_window = [c for c in chart_candles_src if c.get("t", 0) >= cutoff]
            if len(chart_candles_window) < 10:
                chart_candles_window = chart_candles_src  # fallback
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
    _slog("📡 Загрузка свечей...", "info")
    candles = _fetch_candles(sym, tf, days)
    if len(candles) < 30:
        _slog(f"❌ Мало свечей: {len(candles)}", "error")
        with opt_states_lock:
            opt_states.setdefault(sym, {})["running"] = False
        return

    _slog(f"   Загружено {len(candles)} свечей", "ok")

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
            _slog(f"💾 Загружен сохранённый конфиг: ${b.get('equity',100):.2f}", "ok")
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
<script>document.documentElement.setAttribute("data-theme",localStorage.getItem("wf_theme")||"light");</script>
<title>WickFill · Optimizer</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --cream:#fafafa;
  --cream2:#f4f5f7;
  --cream3:#eceef2;
  --sand:#dde0e8;
  --sand2:#b8bdc9;
  --warm:#7a8090;
  --bark:#232830;
  --text:#252b35;
  --text2:#4a5262;
  --text3:#848d9e;
  --glass:rgba(250,250,250,0.93);
  --glass2:rgba(244,245,247,0.84);
  --blur:saturate(180%) blur(20px);
  --shadow:0 2px 16px rgba(30,40,60,0.06);
  --shadow2:0 8px 32px rgba(30,40,60,0.10);
  --radius:18px;
  --radius-sm:12px;
  --accent:#5a6880;
  --green:#2a6e48;
  --green-light:#dff0e8;
  --red:#8b2828;
  --red-light:#f5e0e0;
  --blue:#2a4e78;
  --blue-light:#dce8f5;
  --yellow:#7a5a20;
  --yellow-light:#f5eedc;
  --border:rgba(30,40,60,0.09);
  --border2:rgba(30,40,60,0.05);
}

[data-theme="dark"]{
  --cream:#1a1612;
  --cream2:#221e19;
  --cream3:#2a2520;
  --sand:#3d3630;
  --sand2:#504840;
  --warm:#8c7b6b;
  --bark:#c9bfb0;
  --text:#f0ebe4;
  --text2:#c9bfb0;
  --text3:#8c7b6b;
  --glass:rgba(26,22,18,0.82);
  --glass2:rgba(34,30,25,0.65);
  --shadow:0 2px 20px rgba(0,0,0,0.35);
  --shadow2:0 8px 40px rgba(0,0,0,0.45);
  --accent:#a09080;
  --green:#5a9e6f;
  --green-light:rgba(90,158,111,0.12);
  --red:#c05050;
  --red-light:rgba(192,80,80,0.12);
  --blue:#5a7fa0;
  --blue-light:rgba(90,127,160,0.12);
  --yellow:#b09050;
  --yellow-light:rgba(176,144,80,0.12);
  --border:rgba(255,255,255,0.08);
  --border2:rgba(255,255,255,0.05);
}

html,body{
  height:100%;
  background:var(--cream);
  color:var(--text);
  font-family:'DM Sans',sans-serif;
  font-size:14px;
  overflow:hidden;
  overscroll-behavior:none;
  touch-action:none;
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
  background:var(--glass);
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
  background:var(--green);flex-shrink:0;
  box-shadow:0 0 0 2px var(--green-light);
}
.topbar-spacer{flex:1}
.topbar-meta{display:flex;align-items:center;gap:8px;flex-wrap:wrap}

/* Pill badge */
.pill{
  display:inline-flex;align-items:center;gap:5px;
  padding:4px 10px;border-radius:20px;
  font-size:.72rem;font-weight:500;
  border:1px solid var(--border);
  background:var(--glass2);
  color:var(--text2);
  white-space:nowrap;
}
.pill.green{background:var(--green-light);border-color:rgba(74,124,89,.2);color:var(--green)}
.pill.blue{background:var(--blue-light);border-color:rgba(74,101,128,.2);color:var(--blue)}
.pill.pulse{animation:softpulse 2s ease-in-out infinite}
@keyframes softpulse{0%,100%{opacity:1}50%{opacity:.6}}

/* ── Icon Buttons (topbar) ── */
.icon-btn{
  display:inline-flex;align-items:center;justify-content:center;gap:5px;
  padding:6px 12px;border-radius:10px;
  background:var(--glass2);
  border:1px solid var(--border);
  color:var(--text2);font-size:.75rem;font-weight:500;
  cursor:pointer;transition:all .18s ease;
  white-space:nowrap;
}
.icon-btn:hover{background:var(--cream2);border-color:var(--sand);color:var(--bark)}
.icon-btn.danger{color:var(--red)}
.icon-btn.danger:hover{background:var(--red-light);border-color:rgba(139,58,58,.25)}
.icon-btn.success{color:var(--green)}
.icon-btn.success:hover{background:var(--green-light);border-color:rgba(74,124,89,.25)}

/* ── Main 2-col grid ── */
.main{display:flex;flex:1;min-height:0;gap:0}

/* ── Left sidebar ── */
.sidebar{
  width:320px;flex-shrink:0;
  background:var(--glass);
  backdrop-filter:var(--blur);
  -webkit-backdrop-filter:var(--blur);
  border-right:1px solid var(--border);
  overflow-y:auto;padding:18px 16px;
  display:flex;flex-direction:column;gap:14px;
  touch-action:pan-y;
}

/* Card */
.card{
  background:var(--glass2);
  border:1px solid var(--border2);
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
  background:var(--cream);
  border:1px solid var(--border);
  border-radius:10px;
  color:var(--text);
  font-size:.85rem;
  font-family:'DM Sans',sans-serif;
  width:100%;
  transition:border-color .18s;
  -webkit-appearance:none;appearance:none;
}
input:focus,select:focus{outline:none;border-color:var(--sand2);background:var(--cream)}
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
  background:linear-gradient(135deg,#2a4e78 0%,#3a6496 100%);
  border:none;border-radius:var(--radius-sm);
  color:#f0f5ff;font-size:.9rem;font-weight:600;
  font-family:'DM Sans',sans-serif;
  cursor:pointer;letter-spacing:-.01em;
  box-shadow:0 2px 12px rgba(42,78,120,.22),inset 0 1px 0 rgba(255,255,255,.12);
  transition:all .18s ease;
  display:flex;align-items:center;justify-content:center;gap:7px;
}
.btn-primary:hover:not(:disabled){
  background:linear-gradient(135deg,#345e8a 0%,#4474a8 100%);
  box-shadow:0 4px 20px rgba(42,78,120,.28);transform:translateY(-1px);
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

/* Progress */
.prog-wrap{display:flex;flex-direction:column;gap:5px}
.prog-track{background:var(--cream3);border-radius:3px;height:4px;overflow:hidden}
.prog-fill{height:100%;background:linear-gradient(90deg,var(--warm),var(--bark));border-radius:3px;width:0%;transition:width .4s ease}
.prog-meta{display:flex;justify-content:space-between;font-size:.68rem;color:var(--text3)}
.prog-param{font-size:.68rem;color:var(--text3);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}

/* Best stats */
.stats-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:6px}
.stat-cell{
  background:var(--cream);
  border:1px solid var(--border2);
  border-radius:10px;padding:8px 8px;text-align:center;
}
.stat-v{font-size:.95rem;font-weight:700;color:var(--bark);font-family:'DM Mono',monospace;line-height:1}
.stat-v.good{color:var(--green)}
.stat-v.bad{color:var(--red)}
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
.sym-card.active{border-color:var(--bark)}
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

  /* Весь интерфейс — flex-колонка на весь экран */
  .app{height:100dvh;height:100vh}
  .main{flex-direction:column;flex:1;min-height:0}

  /* ── САЙДБАР: компактный верхний блок, не скроллится ── */
  .sidebar{
    width:100%;border-right:none;border-bottom:1px solid var(--border);
    padding:8px 10px;gap:6px;
    overflow:hidden;
    flex-shrink:0;
  }

  /* Карточка настроек — 2 поля + 2 слайдера ужаты */
  .card{padding:8px 10px}
  .card-title{display:none}
  .field-row{gap:6px;margin-bottom:6px !important}
  .field label{font-size:.65rem}
  input[type=text],input[type=number],select{padding:6px 9px;font-size:.82rem}

  /* Слайдеры — убрать лейблы, только значение */
  .slider-wrap{gap:6px}
  .slider-val{min-width:26px;font-size:.75rem}
  .field .slider-wrap{margin-top:0}
  /* Лейбл слайдеров — сжать */
  .field>label{margin-bottom:1px;line-height:1.2}

  /* Прогресс бар */
  .prog-wrap{display:none !important}
  .prog-meta{font-size:.65rem}
  .prog-param{font-size:.62rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}

  /* Кнопки — чуть меньше чем стандарт, но удобные */
  .btn-primary{padding:10px 14px;font-size:.88rem}
  .btn-ghost{padding:8px 10px;font-size:.8rem}
  .action-row{gap:5px}
  /* SW кнопка — скрыть на мобилке (редко нужна) */
  #swStopBtn{display:none !important}

  /* Бесконечный тоггл — скрыт (он всегда on) */
  #infiniteRow{display:none}

  /* Топ-результат: 1 строка */
  #bestSection{display:none !important}
  #validSection{display:block !important}
  #mob-best-row{display:none !important}

  /* Telegram и сохранение — скрыть на мобилке (в настройках десктопа) */
  .sidebar details{display:none}
  .sidebar .div{display:none}

  /* ── ПРАВАЯ ПАНЕЛЬ: занимает остаток экрана ── */
  .right{flex:1;min-height:0;overflow:hidden;display:flex;flex-direction:column}

  /* Top strip — вертикально на мобилке, сам скроллится */
  .top-strip{flex-direction:column;height:auto;max-height:none;flex:1;min-height:0;overflow-y:auto;-webkit-overflow-scrolling:touch;}
  .cycles-col{max-width:100%;border-right:none;border-bottom:1px solid var(--border2);padding:6px 10px;overflow:visible;flex-shrink:0;}
  .cc-strip{flex-wrap:nowrap;overflow-x:auto;-webkit-overflow-scrolling:touch;}
  .log-col{flex:1;min-height:120px;overflow:visible;}
  .log-area{min-height:150px;overflow-y:visible;touch-action:pan-y;}

  /* График — под таблицей, компактнее */
  .chart-area{height:220px;flex:none;}
  #chartFrame{height:220px;min-height:0;}

  /* Циклы — компактная лента */
  .cycles-bar{padding:6px 10px 4px;flex-shrink:0}
  .cycles-label{display:none}
  .cc{width:82px;padding:7px 8px}
  .cc-eq{font-size:.9rem}
  .cc-n{font-size:.55rem}

  /* Мобильные кнопки Топ / Логи */
  #mob-top-toggle{display:flex !important;flex-shrink:0}

  /* На мобиле осветляем тёмный график */
  #chartFrame{filter:brightness(1.35) contrast(0.92);}

  /* Лог */
  .log-area{padding:4px 10px;min-height:80px;}

  /* Таблица топ — обычный блок */
  #top20Wrap{
    display:none;
    position:static;
    max-height:none;
    background:var(--cream);
    z-index:auto;
  }
}
</style></head><body>

<div class="app">

<!-- ── Topbar ── -->
<header class="topbar">
  <div class="topbar-logo">
    <span class="dot-live" id="apidot2"></span>
    WickFill <span style="font-weight:300;color:var(--text3)">Optimizer</span>
    <span style="font-size:.72rem;font-weight:400;color:var(--text3)">v3.106</span>
  </div>
  <div class="topbar-spacer"></div>
  <div class="topbar-meta">
    <span class="pill" id="speedPill" style="display:none">⚡ —</span>
    <span id="statusBadge2"></span>
    <span id="swBadge"></span>
    <button class="icon-btn" onclick="checkApi()">⟳ API</button>
    <span class="pill" id="latencyPill">— мс</span>
    <button class="icon-btn" id="themeBtn" onclick="toggleTheme()" title="Переключить тему">☀</button>
    <button class="icon-btn" id="updateBtn" onclick="updateScript()" title="Скачать последнюю версию скрипта с GitHub">⬇ Download</button>
    <button class="icon-btn" onclick="renameDownload()">✏ Rename</button>
    <button class="icon-btn success" onclick="termuxUpdate()" title="pkill → cp → python screener_pro.py из Downloads">↺ Restart</button>
  </div>
</header>

<!-- ── Main ── -->
<div class="main">

  <!-- ── Sidebar ── -->
  <aside class="sidebar">

    <!-- Settings card -->
    <div class="card">
      <div class="field-inset" style="margin-bottom:6px">
        <label>Символы (через запятую)</label>
        <input type="text" id="wf_symbol" value="BTC" placeholder="BTC, ETH, SOL" style="width:100%">
      </div>
      <div class="field-row" style="margin-bottom:6px">
        <div class="field-inset">
          <label>Таймфрейм</label>
          <select id="wf_tf_sel">
            <option value="5m">5m</option>
            <option value="15m" selected>15m</option>
            <option value="30m">30m</option>
            <option value="1h">1h</option>
            <option value="4h">4h</option>
            <option value="1d">1d</option>
          </select>
        </div>
      </div>
      <div class="field-row" style="margin-bottom:0">
        <div class="field-inset">
          <label>История (дни)</label>
          <input type="number" id="wf_days" min="3" max="90" placeholder="дни" step="1" style="width:100%">
        </div>
        <div class="field-inset">
          <label>Риск %</label>
          <input type="number" id="wf_risk" min="1" max="100" value="10" step="1" style="width:100%">
        </div>
      </div>
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

    <!-- Main action buttons -->
    <button class="btn-primary" id="wfBtn" onclick="startOpt()">
      <span>🔍</span> Запустить оптимизацию
    </button>

    <div class="action-row">
      <button class="btn-ghost red" id="wfStopBtn" style="display:none" onclick="stopOpt()">
        ⏹ Стоп
      </button>
      <button class="btn-ghost" id="swStopBtn" style="display:none" onclick="stopSW()">
        ⏹ SW
      </button>
      <button class="btn-ghost green2" id="chartBtn" style="display:none" onclick="openChart()">
        📊 График
      </button>
      <button class="btn-ghost" onclick="listConfigs()">
        🗂 Конфиги
      </button>

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
          <input type="text" id="al_tg_token" placeholder="123456:AAF..." value="8349574010:AAFXZHork2S_yUB51klIeae4GrDChvdyfMA">
        </div>
        <div class="field">
          <label>Chat ID</label>
          <div class="tg-row">
            <input type="text" id="al_tg_chat" placeholder="123456789" value="181970023">
            <button class="btn-tg-test" id="testMailBtn" onclick="sendTestEmail()">Тест</button>
          </div>
        </div>
        <div class="alert-msg" id="alertStatusMsg"></div>
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
          <div style="display:flex;align-items:center;gap:6px">
            <span id="swStatus2" style="font-size:.65rem;color:var(--text3)"></span>
            <button class="icon-btn" style="font-size:.65rem;padding:3px 7px" onclick="_resetLog()">очистить</button>
          </div>
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
            <th>Сделок</th><th>DD%</th><th>PF</th><th>SL%</th><th>TP%</th><th title="Риск / Стоп-лосс">Плечо×</th>
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
const infiniteMode=true;
function toggleInfinite(){} // режим всегда бесконечный

/* ── API check ── */
function checkApi(){
  const pill=document.getElementById('latencyPill');
  pill.textContent='...';pill.className='pill';
  fetch('/ping').then(r=>r.json()).then(d=>{
    if(d.ok){pill.textContent=d.ms+'мс';pill.className='pill green';}
    else{pill.textContent=d.error||'err';pill.className='pill';}
  }).catch(()=>{pill.textContent='офлайн';pill.className='pill';});
}
checkApi();setInterval(checkApi,60000);

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
      if(!polling){ _loadChartFrame(); document.getElementById('chartBtn').style.display='flex'; }
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
});

function getAlertCfg(){
  const t=document.getElementById('al_tg_token').value.trim();
  const c=document.getElementById('al_tg_chat').value.trim();
  return (t&&c)?{tg_token:t,tg_chat_id:c}:null;
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
    const line3=sl&&tp?`Плечо <b style="color:${levColor}">${levStr}</b> · PF ${pf>=999?'∞':pf.toFixed(1)}`:'Плечо — · PF —';
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
  const s=_symStates[sym]||{};
  // Всегда пробуем загрузить — сервер сам вернёт "не готов" если нет данных
  // Не проверяем chart_updated_at на клиенте — он может быть устаревшим
  const theme=document.documentElement.getAttribute('data-theme')||'light';
  frame.src='/chart?symbol='+encodeURIComponent(sym)+'&t='+Date.now()+'&theme='+theme;
  frame.style.display='block';
  if(ph) ph.style.display='none';
  _lastChartTs[sym]=s.chart_updated_at||0;
}

function startOpt(){
  const rawSym=document.getElementById('wf_symbol').value.trim()||'BTC';
  const sym=_normalizeSymbols(rawSym);
  // Обновляем поле ввода нормализованным значением (без _USDT для читаемости оставляем как есть)
  const tf=document.getElementById('wf_tf_sel').value;
  const days=document.getElementById('wf_days').value;
  const risk=document.getElementById('wf_risk').value;
  const alertCfg=getAlertCfg();
  // Используем seed только если он совпадает с текущим tf (защита от устаревшего seed)
  const _rawSeed=window._loadedSeed||null;
  const seed=(_rawSeed&&_rawSeed.tf&&_rawSeed.tf!==tf)?null:_rawSeed;
  if(_rawSeed&&!seed) console.warn('[seed] Сброшен: tf seed='+_rawSeed.tf+' != выбран='+tf);
  const body=JSON.stringify({wf_symbol:sym,wf_tf:tf,wf_days:days,wf_risk:risk,infinite:infiniteMode,alert_cfg:alertCfg,seed});
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
      document.getElementById('progBar').style.width='0%';
      document.getElementById('progParam').textContent='';
      document.getElementById('chartBtn').style.display='none';
      document.getElementById('swStopBtn').style.display='none';
      document.getElementById('wfBtn').disabled=true;
      document.getElementById('wfStopBtn').style.display='flex';
      document.getElementById('progWrap').style.display='flex';
      const _cf=document.getElementById('chartFrame');
      const _cp=document.getElementById('chartPlaceholder');
      if(_cf){_cf.style.display='none';_cf.src='about:blank';}
      if(_cp){_cp.style.display='flex';}
      startTs=Date.now();
      function scheduleNext(){
        const interval=document.hidden?5000:1500;
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
  document.getElementById('wfBtn').disabled=false;
  document.getElementById('wfStopBtn').style.display='none';
  document.getElementById('swStopBtn').style.display='flex';
  addLogLine('⏹ Остановлен','warn');
}
function stopSW(){
  fetch('/sw_stop').then(()=>{});
  document.getElementById('swStopBtn').style.display='none';
  addLogLine('⏹ Скользящее окно остановлено','warn');
}
function _loadChartFrame(){
  const frame=document.getElementById('chartFrame');
  const ph=document.getElementById('chartPlaceholder');
  if(!frame) return;
  const theme=document.documentElement.getAttribute('data-theme')||'light';
  frame.src='/chart?t='+Date.now()+'&theme='+theme;
  frame.style.display='block';
  if(ph) ph.style.display='none';
}
function openChart(){window.open('/chart','_blank');}
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
  btn.disabled=true; btn.textContent='⏳ Загрузка...';
  fetch('/update_script',{method:'POST'})
    .then(r=>r.json())
    .then(d=>{
      if(d.ok){
        addLogLine('✅ Скрипт обновлён: '+d.path+' ('+d.size_kb+' KB)','ok');
        btn.textContent='✅ Готово';
      } else {
        addLogLine('❌ Ошибка обновления: '+d.msg,'error');
        btn.textContent='❌ Ошибка';
      }
      setTimeout(()=>{btn.disabled=false;btn.textContent='⬇ Download';},3000);
    })
    .catch(e=>{
      addLogLine('❌ Ошибка: '+e,'error');
      btn.disabled=false; btn.textContent='⬇ Download';
    });
}

/* ── Poll ── */
function poll(){
  const useMulti=_symList.length>1;
  const endpoint=useMulti?'/opt_status_all':'/opt_status';
  fetch(endpoint).then(r=>r.json()).then(d=>{
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
        }
      }
      _renderSymCards();
      _renderSymSwitcher();
      // auto-load chart for active sym when timestamp updated
      const activeSt=_symStates[_activeChart]||{};
      const knownTs=_lastChartTs[_activeChart]||0;
      if(activeSt.chart_updated_at>0&&activeSt.chart_updated_at!==knownTs){
        _lastChartTs[_activeChart]=activeSt.chart_updated_at;
        const frame=document.getElementById('chartFrame');
        const ph=document.getElementById('chartPlaceholder');
        const theme=document.documentElement.getAttribute('data-theme')||'light';
        frame.src='/chart?symbol='+encodeURIComponent(_activeChart)+'&t='+Date.now()+'&theme='+theme;
        frame.style.display='block';
        if(ph) ph.style.display='none';
        document.getElementById('chartBtn').style.display='flex';
      }
    }
    const elapsed=Math.round((Date.now()-startTs)/1000);
    document.getElementById('progTime').textContent=elapsed+'с';
    const pct=d.total>0?Math.round(d.progress/d.total*100):0;
    document.getElementById('progBar').style.width=pct+'%';
    const cycleStr=d.infinite?` · Цикл #${d.cycle}`:'';
    document.getElementById('progLabel').textContent=`Круг #${d.pass_num} · ${pct}%${cycleStr}`;
    if(d.current_param) document.getElementById('progParam').textContent='→ '+d.current_param;

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
        sp.textContent='⚡ '+(mins>0?mins+'м ':'')+secs+'с/цикл';
        sp.title='Среднее время одного цикла оптимизации';
      } else {
        sp.style.display='none';
      }
    }
    if(d.sw_running) swb.innerHTML='<span class="pill green">🔄 SW</span>';
    else swb.innerHTML='';
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
    if(d.valid!==undefined) renderValid(d.valid, d.best, d.windows||[], d.min_stable_days??null, d.days||30);
    if(!useMulti&&d.chart_updated_at>0){
      document.getElementById('chartBtn').style.display='flex';
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
    }
  }).catch(()=>{});
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
}

function _setActivity(text){
  let el=document.getElementById('ccActivity');
  if(!el){el=document.createElement('div');el.id='ccActivity';el.className='activity-line';const wl=document.getElementById('wfLog');wl.insertBefore(el,wl.firstChild);}
  el.innerHTML=`<span class="spin" style="font-size:.8rem">⟳</span><span>${text}</span>`;
  el.scrollIntoView({block:'nearest'});
}
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
  if(startM){_setActivity(startM[1].trim()+' — перебор...');return;}
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
    {v:'$'+eq.toFixed(0),l:'Депозит',c:eq>100?'good':eq<100?'bad':''},
    {v:wr.toFixed(1)+'%',l:'Winrate',c:wr>=55?'good':wr<45?'bad':''},
    {v:tr,l:'Сделок',c:''},
    {v:dd.toFixed(1)+'%',l:'Max DD',c:dd<15?'good':dd>30?'bad':''},
    {v:pf===999?'∞':pf.toFixed(2),l:'PF',c:pf>=1.5?'good':'bad'},
    {v:(b.params?.sl_pct??'—')+'%',l:'SL',c:''},
    {v:(b.params?.tp_pct??'—')+'%',l:'TP',c:''},
    {v:b.params?.rsi_len??'—',l:'RSI len',c:''},
  ];
  document.getElementById('bestGrid').innerHTML=stats.map(s=>`<div class="stat-cell"><div class="stat-v ${s.c}">${s.v}</div><div class="stat-l">${s.l}</div></div>`).join('');
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
  if(!v && (!windows||!windows.length) && !minDays){wrap.style.display='none';return;}
  wrap.style.display='block';
  const trainWr=best?.winrate??0;
  const ratio=v&&trainWr>0?(v.winrate/trainWr):null;
  // ok = стабильная только если И валид хорош И хотя бы часть окон работает
  const okWindows=windows?windows.filter(w=>w.ok).length:0;
  const totalWindows=windows?windows.length:0;
  const windowsOk=totalWindows===0||okWindows/totalWindows>=0.4;  // хотя бы 2 из 5
  // Последнее (свежее) окно — первый элемент массива (wi=0 самое свежее)
  const lastWindow=windows&&windows.length>0?windows[0]:null;
  const lastWindowOk=!lastWindow||lastWindow.ok;
  // Стабильная если: валид хороший ИЛИ большинство окон зелёные — НО только если последний период не красный
  const ok=ratio!==null&&(ratio>=0.75||windowsOk&&okWindows>=2)&&lastWindowOk;
  // Деградация: в целом хорошо, но последний период плохой
  const degrading=ratio!==null&&(ratio>=0.75||windowsOk&&okWindows>=2)&&!lastWindowOk;
  const color=ok?'var(--green)':degrading?'var(--yellow)':'var(--red)';
  const bgColor=ok?'var(--green-light)':degrading?'rgba(138,106,26,0.12)':'var(--red-light)';

  // Заголовок: иконка + статус + ключевые цифры в одну строку
  const validWr = v ? v.winrate.toFixed(0)+'%' : '—';
  const validEq = v ? '$'+v.equity.toFixed(0) : '—';
  const validDd = v ? v.max_dd.toFixed(0)+'%' : '—';
  const eqColor = v ? (v.equity>=100?'var(--green)':'var(--red)') : 'var(--text3)';
  const ddColor = v ? (v.max_dd<15?'var(--green)':v.max_dd>25?'var(--red)':'var(--yellow)') : 'var(--text3)';

  let html=`<div style="margin-top:8px;padding:10px 12px;border-radius:12px;border:1.5px solid ${color};background:${bgColor}">`;

  // Строка 1: статус + валид WR vs трейн WR
  html+=`<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
    <span style="color:${color};font-weight:700;font-size:.88rem">${ratio===null?'— Нет данных':ok?'✓ Стабильная':degrading?'⚠ Деградация':'⚠ Нестабильная'}</span>
    <span style="font-size:.72rem;color:var(--text3)">валид <b style="color:${color}">${validWr}</b> / трейн <b style="color:var(--text2)">${trainWr.toFixed(0)}%</b></span>
  </div>`;

  // Строка 2: Депозит · DD · Сделок
  if(v){
    html+=`<div style="display:flex;gap:10px;margin-bottom:10px;font-size:.78rem">
      <span>💰 <b style="color:${eqColor}">${validEq}</b></span>
      <span>📉 DD <b style="color:${ddColor}">${validDd}</b></span>
      <span style="color:var(--text3)">${v.trades} сд · ${v.days}д</span>
    </div>`;
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
      btn.textContent='✓';
      addLogLine('⏳ Перезапуск скрипта...','info');
      setTimeout(()=>location.reload(),3000);
    } else {
      btn.disabled=false;btn.textContent='↺ Обновить';
      addLogLine('⚠ Обновление: '+(d.msg||'Ошибка'),'warn');
    }
  }).catch(()=>{
    btn.textContent='✓';
    addLogLine('⏳ Сервер перезапускается...','info');
    setTimeout(()=>location.reload(),4000);
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
  document.getElementById('themeBtn').textContent=next==='dark'?'🌙':'☀';
  localStorage.setItem('wf_theme',next);
  // Reload chart iframe with new theme
  const frame=document.getElementById('chartFrame');
  if(frame&&frame.style.display!=='none'&&frame.src&&frame.src!=='about:blank'){
    frame.src='/chart?t='+Date.now()+'&theme='+next;
  }
}
document.addEventListener('DOMContentLoaded',function(){
  const btn=document.getElementById('themeBtn');
  const t=document.documentElement.getAttribute('data-theme')||'light';
  if(btn) btn.textContent=t==='dark'?'🌙':'☀';
});

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
                _bg   = "#1e1a17" if req_theme == "dark" else "#fafafa"
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
        elif parsed.path == "/scan_stop":
            import traceback
            print("[STOP] /scan_stop вызван:\n" + "".join(traceback.format_stack()), flush=True)
            _opt_stop_flag.set()
            self._json({"ok":True})
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
            import subprocess, sys, shutil
            script_name=os.path.basename(os.path.abspath(__file__))
            script_path=os.path.abspath(__file__)
            candidate_dirs=["/sdcard/Download", os.path.dirname(script_path)]
            src=next((os.path.join(d,script_name) for d in candidate_dirs if os.path.exists(os.path.join(d,script_name))),None)
            if not src: self._json({"ok":False,"msg":f"'{script_name}' не найден в downloads"}); return
            try:
                # Пишем отдельный sh-скрипт — иначе pkill убивает тот же shell
                # и cp + python никогда не выполняются
                sh=os.path.expanduser("~/wickfill_update.sh")
                with open(sh,"w") as f:
                    f.write("#!/data/data/com.termux/files/usr/bin/bash\n")
                    f.write("termux-wake-lock\n")
                    f.write(f"pkill -f {script_name}\n")
                    f.write("sleep 1\n")
                    f.write(f"cp '{src}' '{script_path}'\n")
                    f.write(f"{sys.executable} '{script_path}'\n")
                os.chmod(sh, 0o755)
                subprocess.Popen(["bash", sh],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    start_new_session=True)
                self._json({"ok":True,"msg":"⏳ Перезапуск..."})
                def _die(): time.sleep(0.4); os._exit(0)
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

        if parsed.path == "/update_script":
            try:
                import urllib.request as _ur
                _raw_url = "https://raw.githubusercontent.com/mambaleylo/wickfill/main/screener_pro.py"
                _headers = {"Authorization": "token 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",
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
            global _opt_thread, _multi_symbols, _active_chart_symbol, _sw_threads, _sw_state
            print(f"{_ts()} [SCAN] infinite={params.get('infinite')} symbol={params.get('wf_symbol')} tf={params.get('wf_tf')}", flush=True)
            if _opt_thread and _opt_thread.is_alive():
                self._json({"ok":False,"msg":"Оптимизация уже запущена. Сначала нажмите Стоп."}); return
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
    print(f"WickFill Optimizer v3.107-perf")
    print(f"  Локально:  http://localhost:{port}")
    print(f"  По сети:   http://{local_ip}:{port}")
    print(f"Остановить: Ctrl+C")
    ReusableHTTPServer(("",port),Handler).serve_forever()


