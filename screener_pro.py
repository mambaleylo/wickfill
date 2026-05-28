#!/usr/bin/env python3
"""
WickFill Optimizer v3.0
- ∞ Бесконечный режим: оптимизация крутится без остановки, рестарт после каждого цикла
- Скользящее окно: каждые N минут (по таймфрейму) добавляет свечу, убирает первую
- Live-алерт: если на новой закрытой свече сигнал по лучшим параметрам — шлёт email
- Динамический график: /chart обновляется автоматически каждые 30с
"""

import json, time, threading, random, math, os
import multiprocessing
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import requests
import smtplib, email.mime.text, email.mime.multipart

GATE_API = "https://api.gateio.ws/api/v4"

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
    "current_param": "", "logs": [], "best": None, "top20": [],
    "started_at": "", "elapsed": 0.0, "error": "",
    "chart_candles": [], "chart_signals": [], "chart_symbol": "", "chart_tf": "",
    "chart_path": "", "chart_updated_at": 0,
    # sliding window
    "sw_running": False, "sw_last_update": 0, "sw_candle_count": 0,
    # live signal alert
    "last_signal_t": 0,   # timestamp последней свечи с сигналом (чтобы не дублировать)
}
opt_lock = threading.Lock()

alert_state = {
    "running": False, "error": "", "last_scan": "",
    "signals": [], "sent": 0,
}
alert_lock = threading.Lock()

# Кеш текущей незакрытой свечи — обновляется фоновым потоком
_live_candle_cache = {}   # {"symbol_tf": {t,o,h,l,c}}
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
                    print(f"[live_updater] данные устарели (last={last_t}, now={now}), перегружаю...", flush=True)
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
                            print(f"[live_updater] перезагружено {len(fresh)} свечей", flush=True)
                    except Exception as e:
                        print(f"[live_updater] ошибка перезагрузки: {e}", flush=True)

                # Обновляем незакрытую свечу
                c = _fetch_current_candle(symbol, tf)
                if c:
                    key = f"{symbol}_{tf}"
                    with _live_candle_lock:
                        _live_candle_cache[key] = c
                    with opt_lock:
                        cc2 = opt_state.get("chart_candles", [])
                        if cc2:
                            if cc2[-1].get("live"):
                                cc2[-1]["o"] = c["open"]
                                cc2[-1]["h"] = c["high"]
                                cc2[-1]["l"] = c["low"]
                                cc2[-1]["c"] = c["close"]
                                cc2[-1]["t"] = c["t"]
                            elif c["t"] > cc2[-1]["t"]:
                                opt_state["chart_candles"] = cc2 + [
                                    {"t":c["t"],"o":c["open"],"h":c["high"],
                                     "l":c["low"],"c":c["close"],"live":True}
                                ]
        except Exception as e:
            print(f"[live_updater] {e}", flush=True)
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
        total=0.0; returns=0.0
        if i<=ret_n+1: return None
        max_look=min(ret_lb, i-ret_n-1)
        for k in range(ret_n+1, max_look+1):
            ki=i-k
            if ki<0: continue
            c=candles_list[ki]; k_rng=c["high"]-c["low"]
            if k_rng<=0: continue
            k_up=(c["high"]-max(c["open"],c["close"]))/k_rng*100
            k_dn=(min(c["open"],c["close"])-c["low"])/k_rng*100
            if is_up_wick and k_up>=ret_sim:
                target=max(c["open"],c["close"]); total+=1
                for j in range(1,ret_n+1):
                    fi=ki+j
                    if fi<n and candles_list[fi]["low"]<=target: returns+=1; break
            elif not is_up_wick and k_dn>=ret_sim:
                target=min(c["open"],c["close"]); total+=1
                for j in range(1,ret_n+1):
                    fi=ki+j
                    if fi<n and candles_list[fi]["high"]>=target: returns+=1; break
        return (returns/total*100) if total>0 else None

    def _count_tested_level(i, level_price, is_up_search):
        wins=0
        if i<3: return wins
        zone_tol=level_price*rep_zone/100.0; max_look=min(rep_lb,i-2)
        for k in range(2, max_look+1):
            ki=i-k
            if ki<0: continue
            c=candles_list[ki]; k_rng=c["high"]-c["low"]
            if k_rng<=0: continue
            k_upw=c["high"]-max(c["open"],c["close"]); k_dnw=min(c["open"],c["close"])-c["low"]
            k_up_pct=k_upw/k_rng*100; k_dn_pct=k_dnw/k_rng*100
            if is_up_search and k_up_pct>=mwp:
                if abs(c["high"]-level_price)<=zone_tol:
                    fi=ki+1
                    if fi<n and candles_list[fi]["close"]<max(c["open"],c["close"]): wins+=1
            elif not is_up_search and k_dn_pct>=mwp:
                if abs(c["low"]-level_price)<=zone_tol:
                    fi=ki+1
                    if fi<n and candles_list[fi]["close"]>min(c["open"],c["close"]): wins+=1
        return wins

    def _count_wick_cluster(i, level_price, is_up_search):
        cnt=0; zone_tol=level_price*clu_pct/100.0; max_look=min(clu_lb,i-1)
        for k in range(1, max_look+1):
            ki=i-k
            if ki<0: continue
            c=candles_list[ki]; k_rng=c["high"]-c["low"]
            if k_rng<=0: continue
            k_upw=c["high"]-max(c["open"],c["close"]); k_dnw=min(c["open"],c["close"])-c["low"]
            if is_up_search and k_upw/k_rng*100>=mwp:
                if abs(c["high"]-level_price)<=zone_tol: cnt+=1
            elif not is_up_search and k_dnw/k_rng*100>=mwp:
                if abs(c["low"]-level_price)<=zone_tol: cnt+=1
        return cnt

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

        prev_hi=max(candles_list[j]["high"] for j in range(max(0,i-ll),i)) if ulf else hi
        prev_lo=min(candles_list[j]["low"]  for j in range(max(0,i-ll),i)) if ulf else lo
        near_hi=(not ulf) or (abs(hi-prev_hi)/prev_hi*100<=ltol if prev_hi>0 else False)
        near_lo=(not ulf) or (abs(lo-prev_lo)/prev_lo*100<=ltol if prev_lo>0 else False)

        if ugf:
            hist_up=[candles_list[j]["high"]-max(candles_list[j]["open"],candles_list[j]["close"]) for j in range(max(0,i-gl),i)]
            hist_dn=[min(candles_list[j]["open"],candles_list[j]["close"])-candles_list[j]["low"] for j in range(max(0,i-gl),i)]
            geo_up=sum(1 for w in hist_up if up_w>w)/len(hist_up)*100 if hist_up else 0
            geo_dn=sum(1 for w in hist_dn if dn_w>w)/len(hist_dn)*100 if hist_dn else 0
            geo_ok_l=geo_up>=gmin; geo_ok_s=geo_dn>=gmin
        else:
            geo_ok_l=geo_ok_s=True

        def _css(is_long):
            wick=dn_w if is_long else up_w; w_pct=dn_w_pct if is_long else up_w_pct
            s1=min(w_pct/mwp*100,100) if mwp>0 else 100
            cp=(cl-lo)/rng*100 if rng>0 else 50
            s2=cp if is_long else 100-cp; s2=max(min(s2,100),0)
            s3=max(min((1-body/wick)*100,100),0) if wick>0 else 0
            hist_rng=[candles_list[j]["high"]-candles_list[j]["low"] for j in range(max(0,i-20),i)]
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
            hist_atr=[atr_series[j] for j in range(max(0,i-q_atr),i) if atr_series[j]>0]
            avg_atr=sum(hist_atr)/len(hist_atr) if hist_atr else atr_now
            ratio=atr_now/avg_atr if avg_atr>0 else 1
            quiet_ok=q_min<=ratio<=q_max
        else:
            quiet_ok=True

        if uswf:
            sw_hi=max((candles_list[j]["high"] for j in range(max(0,i-sw_len),i)), default=hi)
            sw_lo=min((candles_list[j]["low"]  for j in range(max(0,i-sw_len),i)), default=lo)
            sweep_ok_l=hi>=sw_hi*(1-sw_tol/100) and cl<sw_hi
            sweep_ok_s=lo<=sw_lo*(1+sw_tol/100) and cl>sw_lo
        else:
            sweep_ok_l=sweep_ok_s=True

        if umsf and i>=ms_lb*2:
            swing_hi=max(candles_list[j]["high"] for j in range(i-ms_lb,i))
            swing_lo=min(candles_list[j]["low"]  for j in range(i-ms_lb,i))
            prev_s_hi=max(candles_list[j]["high"] for j in range(i-ms_lb*2,i-ms_lb))
            prev_s_lo=min(candles_list[j]["low"]  for j in range(i-ms_lb*2,i-ms_lb))
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
        pnl_ot=t_pos*risk_pct/100*rr_r
        # Не засчитываем незакрытую позицию в статистику — она ещё не завершена
        # Добавляем в pnls только для equity, но не в trades/wins
        equity+=pnl_ot; pnls.append(pnl_ot)
        is_win_ot=pnl_ot>0
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

    if trades<3: fitness=-9999.0
    elif max_dd>=50.0: fitness=-9999.0
    else:
        import math as _math
        net_return=equity-100.0
        # Пол просадки растёт при малом числе сделок — защита от случайных WR=100% на 3 сделках
        min_dd_floor=max(1.0, 15.0/_math.sqrt(max(trades,1)))
        effective_dd=max(max_dd,min_dd_floor)
        calmar=net_return/effective_dd
        # Бонус за абсолютную прибыль (log2): $234→~8, $2500→~17 — выравнивает calmar vs реальные деньги
        profit_bonus=_math.log2(max(net_return,0)+1)*3.0
        if trades<=200: trade_bonus=_math.log(max(trades/8.0,1.0)+1)*4.0
        else: trade_bonus=_math.log(200/8.0+1)*4.0-(trades-200)*0.01
        wr_bonus=max(0.0,wr_val-50.0)*0.08
        pf_val=min(profit_factor,4.0) if profit_factor!=float("inf") else 4.0
        pf_bonus=pf_val*1.5
        dd_penalty=max(0.0,max_dd-15.0)*0.5
        fitness=calmar*4.0+profit_bonus+trade_bonus+wr_bonus+pf_bonus-dd_penalty

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
    top20_list.sort(key=lambda x: -x["fitness"])
    seen=set(); deduped=[]
    for item in top20_list:
        key=round(item["equity"],2)
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
    print(f"[fetch] {symbol} {tf} {days}д — нужно ~{total_needed} свечей...", flush=True)
    while current_from < now:
        pct = int((current_from - since) / max(now - since, 1) * 100)
        print("[fetch] {}% ({} св.)".format(pct, len(all_candles)), end="\r", flush=True)
        try:
            r = requests.get(f"{GATE_API}/futures/usdt/candlesticks",
                params={"contract": symbol, "interval": tf,
                        "from": current_from, "limit": LIMIT}, timeout=15)
            if r.status_code != 200:
                last_http_error = f"HTTP {r.status_code}: {r.text[:200]}"
                print(f"\n[fetch] {last_http_error}", flush=True); break
            data = r.json()
            if not isinstance(data, list):
                last_http_error = f"Неожиданный ответ API: {str(data)[:200]}"
                print(f"\n[fetch] {last_http_error}", flush=True); break
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
            print(f"\n[fetch] err: {e}", flush=True); break
    seen = set(); result = []
    for c in sorted(all_candles, key=lambda x: x["t"]):
        if c["t"] not in seen: seen.add(c["t"]); result.append(c)
    print(f"\n[fetch] Готово: {len(result)} свечей (ожидалось ~{total_needed})", flush=True)
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
def _coordinate_descent_from(start_ind, pmap_fn, olog, t0,
                              top20_global, start_label, max_passes=8,
                              stop_flag=None):
    current = dict(start_ind)
    best_result = pmap_fn([current])[0]
    pass_num = 0

    while True:
        if stop_flag and stop_flag(): break
        pass_num += 1
        keys_shuffled = list(_KEYS); random.shuffle(keys_shuffled)
        olog(f"  {start_label} | Круг #{pass_num} | Депозит: ${best_result['equity']:.2f}", "ok")

        steps_in_pass = sum(len(_GRIDS[k]) for k in keys_shuffled)
        with opt_lock:
            opt_state["pass_num"]=pass_num; opt_state["total"]=steps_in_pass; opt_state["progress"]=0

        step_in_pass=0; improved_in_pass=False

        for param_idx, key in enumerate(keys_shuffled):
            if stop_flag and stop_flag(): break
            label=PARAM_SPACE[key]["label"]; grid=_GRIDS[key]
            with opt_lock:
                opt_state["current_param"]=label; opt_state["generation"]=param_idx+1

            candidates=[{**current, key:val} for val in grid]
            use_key=FILTER_GROUPS.get(key)
            if use_key and current.get(use_key,True):
                candidates.append({**current, use_key:False})

            results=pmap_fn(candidates); results.sort(key=lambda x:-x["fitness"])
            param_best=results[0]; best_val=param_best["params"][key]

            if param_best["fitness"]>best_result["fitness"]:
                delta=param_best["equity"]-best_result["equity"]
                current[key]=best_val; best_result=param_best; improved_in_pass=True
                val_str=("да" if best_val else "нет") if isinstance(best_val,bool) else (f"{best_val:.2f}" if isinstance(best_val,float) else str(best_val))
                olog(f"    ✅ {label}: {val_str} → ${param_best['equity']:.2f} (+{delta:.2f}$) | WR {param_best['winrate']:.1f}% | Сд {param_best['trades']} | DD {param_best['max_dd']:.1f}%","found")

            step_in_pass+=len(grid)+(1 if FILTER_GROUPS.get(key) and current.get(FILTER_GROUPS.get(key),True) else 0)
            with opt_lock:
                opt_state["progress"]=step_in_pass
                opt_state["elapsed"]=round(time.time()-t0,1)

        if stop_flag and stop_flag(): break

        if not improved_in_pass: break
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
const CANDLES={candles_json};
const SIGNALS={signals_json};
const canvas=document.getElementById('c');
const ctx=canvas.getContext('2d');
const wrap=document.getElementById('canvas-wrap');
let viewStart=Math.max(0,CANDLES.length-120),viewLen=Math.min(120,CANDLES.length);
let isDragging=false,dragX=0,dragVS=0,sidebarOpen=true;
function toggleSidebar(){{const sb=document.getElementById('sidebar');sidebarOpen=!sidebarOpen;sb.classList.toggle('hidden',!sidebarOpen);requestAnimationFrame(render);}}
function render(){{
  const dpr=window.devicePixelRatio||1,W=wrap.clientWidth,H=wrap.clientHeight;
  if(!W||!H) return;
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
  // Background fill chart area
  ctx.fillStyle='#1e1a17';ctx.fillRect(0,0,W,H);
  // Time axis separator
  ctx.strokeStyle='rgba(255,255,255,.12)';ctx.lineWidth=1;
  ctx.beginPath();ctx.moveTo(PAD_L,H-PAD_B);ctx.lineTo(W-PAD_R,H-PAD_B);ctx.stroke();
  // Price axis separator
  ctx.beginPath();ctx.moveTo(W-PAD_R,PAD_T);ctx.lineTo(W-PAD_R,H-PAD_B);ctx.stroke();
  ctx.font='10px system-ui';ctx.textAlign='left';
  for(let g=0;g<=7;g++){{
    const price=mn+(mx-mn)*g/7,y=py(price);
    ctx.strokeStyle='rgba(255,255,255,.05)';ctx.lineWidth=1;ctx.beginPath();ctx.moveTo(PAD_L,y);ctx.lineTo(W-PAD_R,y);ctx.stroke();
    ctx.fillStyle='#9a8e83';ctx.fillText(price.toPrecision(6),W-PAD_R+4,y+3);
  }}
  // Only draw TP/SL labels for the active (open) trade
  const activeSig=SIGNALS.find(s=>s.open_end===true&&s.bar_i<end&&s.bar_i>=viewStart-(viewLen*2));
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
    // TP/SL dashed lines and labels ONLY for active open trade
    if(activeSig===s){{
      ctx.setLineDash([4,3]);
      ctx.strokeStyle=isLong?'#3a7d52':'#a03030';ctx.lineWidth=1;ctx.beginPath();ctx.moveTo(x1,py(s.tp));ctx.lineTo(W-PAD_R,py(s.tp));ctx.stroke();
      ctx.strokeStyle='#b0a090';ctx.beginPath();ctx.moveTo(x1,py(s.sl));ctx.lineTo(W-PAD_R,py(s.sl));ctx.stroke();
      ctx.setLineDash([]);
      const tpY=py(s.tp),slY=py(s.sl);
      ctx.font='bold 9px system-ui';ctx.textAlign='left';
      ctx.fillStyle=isLong?'rgba(58,125,82,0.85)':'rgba(160,48,48,0.85)';
      ctx.beginPath();ctx.roundRect(W-PAD_R+1,tpY-7,PAD_R-2,14,3);ctx.fill();
      ctx.fillStyle='#fff';ctx.fillText('TP '+s.tp.toPrecision(5),W-PAD_R+4,tpY+3);
      ctx.fillStyle='rgba(140,120,100,0.75)';
      ctx.beginPath();ctx.roundRect(W-PAD_R+1,slY-7,PAD_R-2,14,3);ctx.fill();
      ctx.fillStyle='#fff';ctx.fillText('SL '+s.sl.toPrecision(5),W-PAD_R+4,slY+3);
      ctx.font='10px system-ui';
    }}
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
  for(const s of SIGNALS){{
    const vi=s.bar_i-viewStart;if(vi<0||vi>=vis.length) continue;
    const x=cx(vi),isLong=s.dir===1;
    const c_sig=vis[vi];
    const arrowSz=Math.max(5,Math.min(7,cw*0.45));
    const arrowOff=Math.max(18,cw*2.2);
    const isOpenEnd=s.open_end===true,isWin=s.win===true;
    ctx.fillStyle=isLong?'#4a7fc1':'#c8902a';
    ctx.strokeStyle=isOpenEnd?'#b0a090':s.win===null?'#b0a090':isWin?'#3a7d52':'#a03030';
    ctx.lineWidth=1.5;ctx.beginPath();
    if(isLong){{const ay=py(c_sig.l)+arrowOff;ctx.moveTo(x,ay-arrowSz);ctx.lineTo(x-arrowSz,ay);ctx.lineTo(x+arrowSz,ay);}}
    else{{const ay=py(c_sig.h)-arrowOff;ctx.moveTo(x,ay+arrowSz);ctx.lineTo(x-arrowSz,ay);ctx.lineTo(x+arrowSz,ay);}}
    ctx.closePath();ctx.fill();ctx.stroke();
    if(!isOpenEnd&&s.exit_bar!==null&&s.win!==null){{
      const exitPrice=s.exit_p??( s.win?s.tp:s.sl);
      const pct=isLong?(exitPrice-s.ep)/s.ep*100:(s.ep-exitPrice)/s.ep*100;
      const lbl=(pct>=0?'+':'')+pct.toFixed(2)+'%';
      const vi_exit=s.exit_bar-viewStart,x_exit=(vi_exit>=0&&vi_exit<vis.length)?cx(vi_exit):x;
      const lx=(x+x_exit)/2;
      const ly=isLong?py(c_sig.l)+arrowOff+arrowSz+16:py(c_sig.h)-arrowOff-arrowSz-16;
      ctx.font=`bold ${{Math.max(9,Math.min(12,cw*1.5))}}px system-ui`;ctx.textAlign='center';
      const tw=ctx.measureText(lbl).width;
      ctx.fillStyle=pct>=0?'rgba(58,125,82,0.9)':'rgba(160,48,48,0.9)';
      ctx.beginPath();ctx.roundRect(lx-tw/2-3,ly-11,tw+6,14,3);ctx.fill();
      ctx.fillStyle='#fff';ctx.fillText(lbl,lx,ly);
    }}
  }}
  ctx.fillStyle='#7a6e63';ctx.font='10px system-ui';ctx.textAlign='center';
  const step=Math.max(1,Math.floor(vis.length/8));
  const isMobile=W<500;
  const mskOffset=3*3600*1000;
  for(let i=0;i<vis.length;i+=step){{
    const t=new Date(vis[i].t*1000+mskOffset);
    let lbl;
    if(isMobile){{
      // On mobile: just HH:MM to avoid overlap
      lbl=t.getUTCHours().toString().padStart(2,'0')+':'+t.getUTCMinutes().toString().padStart(2,'0');
    }}else{{
      lbl=(t.getUTCMonth()+1)+'/'+t.getUTCDate()+' '+t.getUTCHours().toString().padStart(2,'0')+':'+t.getUTCMinutes().toString().padStart(2,'0');
    }}
    ctx.fillText(lbl,cx(i),H-PAD_B+16);
  }}
}}
wrap.addEventListener('wheel',e=>{{e.preventDefault();const delta=e.deltaY>0?1.18:0.84,ratio=(e.offsetX-6)/wrap.clientWidth,pivot=viewStart+ratio*viewLen;viewLen=Math.max(15,Math.min(CANDLES.length,Math.round(viewLen*delta)));viewStart=Math.max(0,Math.min(CANDLES.length-viewLen,Math.round(pivot-ratio*viewLen)));render();}},{{passive:false}});
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
wrap.addEventListener('mousemove',e=>{{
  const W=wrap.clientWidth,vis=CANDLES.slice(viewStart,viewStart+viewLen),cw2=W/vis.length,i=Math.floor(e.offsetX/cw2);
  if(i<0||i>=vis.length){{tip.style.display='none';return;}}
  const c=vis[i],gi=viewStart+i,sig=SIGNALS.find(s=>s.bar_i===gi),d=new Date(c.t*1000);
  const dt=d.toLocaleDateString('ru')+' '+d.toLocaleTimeString('ru',{{hour:'2-digit',minute:'2-digit'}});
  let html=`<b>${{dt}}</b><br>O ${{c.o.toPrecision(6)}} H ${{c.h.toPrecision(6)}}<br>L ${{c.l.toPrecision(6)}} C ${{c.c.toPrecision(6)}}`;
  if(sig){{const dir=sig.dir===1?'🔵 Лонг':'🟡 Шорт',res=sig.open_end?'⛔ не закрыт':sig.win?'✅ TP':'❌ SL';html+=`<br><br>${{dir}} ${{res}}<br>Вход ${{sig.ep.toPrecision(6)}}<br>TP ${{sig.tp.toPrecision(6)}}<br>SL ${{sig.sl.toPrecision(6)}}`;}}
  tip.innerHTML=html;tip.style.display='block';
  const tx=e.offsetX+14;tip.style.left=(tx+tip.offsetWidth>W?tx-tip.offsetWidth-20:tx)+'px';tip.style.top=Math.max(0,e.offsetY-10)+'px';
}});
wrap.addEventListener('mouseleave',()=>tip.style.display='none');
window.addEventListener('resize',render);
// ── Live candle: обновляем незакрытую свечу каждые 5 секунд ──
const LIVE_SYMBOL = '{symbol}';
const LIVE_TF     = '{tf}';

function fetchLiveCandle() {{
  fetch('/live_candle?symbol=' + encodeURIComponent(LIVE_SYMBOL) + '&tf=' + encodeURIComponent(LIVE_TF))
    .then(r => r.json())
    .then(d => {{
      if (!d.ok) return;
      const last = CANDLES[CANDLES.length - 1];
      const atEnd = (viewStart + viewLen >= CANDLES.length); // был ли вид у правого края
      if (last && last.live) {{
        // Обновляем существующую живую свечу
        last.o = d.o; last.h = d.h; last.l = d.l; last.c = d.c; last.t = d.t;
      }} else if (!last || d.t > last.t) {{
        // Удаляем старую live-свечу если есть, добавляем новую
        if (last && last.live) CANDLES.pop();
        CANDLES.push({{t:d.t, o:d.o, h:d.h, l:d.l, c:d.c, live:true}});
        // Если были у правого края — двигаем вид вправо
        if (atEnd) viewStart = Math.max(0, CANDLES.length - viewLen);
      }}
      // Badge
      const badge = document.getElementById('liveBadge');
      if (badge) badge.textContent = '⬤ LIVE  ' + d.c.toPrecision(7);
      render();
    }}).catch(e => console.error('[live_candle]', e));
}}

// Каждые 3 секунды
fetchLiveCandle();
setInterval(fetchLiveCandle, 3000);

// Полный перезапрос страницы раз в 5 минут
setTimeout(() => location.reload(), 300000);

render();
</script></body></html>"""

def _save_chart(candles, signals, best_result, symbol, tf, risk_pct_ui=20.0):
    downloads_dir = os.path.expanduser("~/downloads")
    os.makedirs(downloads_dir, exist_ok=True)
    fpath = os.path.join(downloads_dir, f"wickfill_live_{symbol.replace('_','').lower()}_{tf}.html")
    html = _build_chart_html(candles, signals, best_result, symbol, tf, risk_pct_ui)
    try:
        with open(fpath, "w", encoding="utf-8") as f:
            f.write(html)
        return fpath
    except Exception as e:
        print(f"[chart] err: {e}"); return None

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
            dt = time.strftime("%Y-%m-%d %H:%M", time.gmtime(int(time.time()) + moscow_offset))
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
    """Каждый TF-интервал: загружает последнюю закрытую свечу, добавляет, убирает первую."""
    global _sw_candles, _sw_params, _sw_risk
    interval_sec = TF_SECONDS.get(tf, 3600)

    print(f"[sw] Запущен. Символ={symbol} ТФ={tf} окно={n_candles} свечей интервал={interval_sec}с")

    with opt_lock:
        opt_state["sw_running"] = True

    # Синхронизируем окно со свежими данными при старте
    days_needed = max(1, round(n_candles * interval_sec / 86400) + 1)
    print(f"[sw] Синхронизация свежих свечей ({days_needed}д)...")
    fresh = _fetch_candles(symbol, tf, days_needed)
    if fresh and len(fresh) >= n_candles:
        with opt_lock:
            _sw_candles = fresh[-n_candles:]
        print(f"[sw] Синхронизировано: {len(_sw_candles)} свечей, последняя={time.strftime('%H:%M', time.localtime(_sw_candles[-1]['t']))}")
    else:
        print(f"[sw] Синхронизация не удалась, используем старые данные")

    while True:
        with opt_lock:
            if not opt_state["sw_running"]: break

        # Вычисляем сколько секунд до следующего закрытия свечи по реальному времени
        now = int(time.time())
        # Ближайшее время закрытия = ceil(now / interval_sec) * interval_sec
        next_close = ((now // interval_sec) + 1) * interval_sec
        wait_sec = next_close - now
        # Ждём до закрытия + 5 секунд (чтобы Gate.io успел сформировать свечу)
        sleep_total = wait_sec + 5
        print(f"[sw] Следующая свеча через {sleep_total}с (в {time.strftime('%H:%M:%S', time.localtime(next_close+5))})")

        # Ждём порциями по 5с чтобы можно было остановить
        for _ in range(sleep_total):
            with opt_lock:
                if not opt_state["sw_running"]: break
            time.sleep(1)

        with opt_lock:
            if not opt_state["sw_running"]: break

        # Загружаем последнюю закрытую свечу
        new_c = _fetch_latest_candle(symbol, tf)
        if not new_c:
            print("[sw] Не удалось загрузить новую свечу"); continue

        with opt_lock:
            candles = _sw_candles
            best_p  = dict(_sw_params) if _sw_params else None

        if not candles:
            print("[sw] Свечи ещё не загружены"); continue

        # Проверяем — это действительно новая свеча?
        if new_c["t"] <= candles[-1]["t"]:
            print(f"[sw] Свеча t={new_c['t']} уже есть, пропускаем"); continue

        # Скользящее окно: добавляем новую, убираем первую
        new_candles = candles[1:] + [new_c]

        # Обновляем chart
        if best_p:
            sim = _simulate(new_candles, best_p, 0, _collect=True, risk_pct=risk_pct)
            chart_signals = sim["_signals"] if sim else []
            chart_candles_fmt = [{"t":c["t"],"o":c["open"],"h":c["high"],"l":c["low"],"c":c["close"]} for c in new_candles]
            # Добавляем незакрытую свечу только для отображения
            cur_c2 = _fetch_current_candle(symbol, tf)
            if cur_c2 and cur_c2["t"] > new_candles[-1]["t"]:
                chart_candles_fmt = chart_candles_fmt + [{"t":cur_c2["t"],"o":cur_c2["open"],"h":cur_c2["high"],"l":cur_c2["low"],"c":cur_c2["close"],"live":True}]

            with opt_lock:
                prev_signals_for_close = list(opt_state.get("chart_signals") or [])
                _sw_candles = new_candles
                opt_state["chart_candles"]  = chart_candles_fmt
                opt_state["chart_signals"]  = chart_signals
                opt_state["sw_last_update"] = int(time.time())
                opt_state["sw_candle_count"] = len(new_candles)
                br = opt_state.get("best") or {}

            # Уведомление о закрытии сделки
            if alert_cfg and prev_signals_for_close:
                _check_trade_close(prev_signals_for_close, chart_signals, alert_cfg, symbol, tf)

            chart_path = _save_chart(chart_candles_fmt, chart_signals, br or {"params":best_p,"equity":100,"winrate":0,"max_dd":0,"profit_factor":0,"trades":0}, symbol, tf, risk_pct)
            if chart_path:
                with opt_lock: opt_state["chart_path"] = chart_path; opt_state["chart_updated_at"] = int(time.time())

            print(f"[sw] Свеча добавлена t={new_c['t']} c={new_c['close']:.4g} | всего={len(new_candles)}")

            # Проверка сигнала на новой свече
            if alert_cfg:
                _check_new_candle_signal(new_candles, best_p, risk_pct, alert_cfg)
        else:
            with opt_lock:
                _sw_candles = new_candles

    with opt_lock:
        opt_state["sw_running"] = False
    print("[sw] Остановлен")

# ═══════════════════════════════════════════════════════════════
# OPTIMIZER MAIN LOOP
# ═══════════════════════════════════════════════════════════════
_opt_stop_flag = threading.Event()
_opt_thread = None
_last_fetch_error = None

def _run_one_cycle(candles, days, risk_pct, olog, t0, n_restarts=12,
                   prev_best_params=None, prev_top20=None):
    """Запускает один полный цикл оптимизации. Возвращает (final_result, final_params, top20)."""
    global _sw_params

    n_workers = max(1, os.cpu_count() or 1)
    olog(f"   ProcessPool: {n_workers} процессов (все ядра CPU)")
    _pool = ProcessPoolExecutor(
        max_workers=n_workers,
        initializer=_worker_init,
        initargs=(candles, 0, risk_pct)
    )

    def pmap(candidates):
        if not candidates:
            return []
        chunk = max(1, len(candidates) // (n_workers * 2))
        return list(_pool.map(_worker_evaluate, candidates, chunksize=chunk))

    def stop_flag():
        return _opt_stop_flag.is_set()

    top20_global = list(prev_top20) if prev_top20 else []

    # Фаза 1: многоточечный старт
    if prev_best_params:
        start_points = [prev_best_params] + [_random_individual() for _ in range(n_restarts - 1)]
        olog(f"━━ ФАЗА 1: лучший предыдущего цикла + {n_restarts-1} случайных ━", "ok")
    else:
        start_points = [_default_individual()] + [_random_individual() for _ in range(n_restarts - 1)]
        olog(f"━━ ФАЗА 1: {n_restarts} стартов ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "ok")

    local_bests = []
    for i, start_ind in enumerate(start_points):
        if stop_flag(): break
        label = "Старт #1 (предыдущий лучший)" if (i==0 and prev_best_params) else f"Старт #{i+1}"
        olog(f"── {label} ──", "ok")
        with opt_lock: opt_state["generation"]=i+1
        result, cur, top20_global = _coordinate_descent_from(
            start_ind, pmap, olog, t0, top20_global, label, max_passes=6, stop_flag=stop_flag)
        local_bests.append((result["fitness"], result, cur))
        olog(f"  {label} → ${result['equity']:.2f} WR {result['winrate']:.1f}% DD {result['max_dd']:.1f}%",
             "found" if result["equity"]>100 else "info")
        with opt_lock:
            best_so_far = top20_global[0] if top20_global else result
            opt_state["best"] = best_so_far
            opt_state["top20"] = top20_global
            opt_state["elapsed"] = round(time.time()-t0, 1)
            _sw_params = dict(best_so_far["params"])

    if stop_flag(): _pool.shutdown(wait=False); return None, None, top20_global

    local_bests.sort(key=lambda x: -x[0])
    best_f, best_r1, best_p1 = local_bests[0]

    # Фаза 2: Basin Hopping от лучшей точки — 20 возмущений
    olog(f"━━ ФАЗА 2: Basin Hopping (20 итераций) ━━━━━━━━━━━━━━━━━━━━━━", "ok")
    bh_current=dict(best_p1); bh_best=best_r1; final_result=best_r1; final_params=best_p1
    for bh_i in range(20):
        if stop_flag(): break
        perturbed=dict(bh_current)
        for k in random.sample(_KEYS, max(1, int(len(_KEYS)*0.35))):
            spec=PARAM_SPACE[k]; grid=_GRIDS[k]
            if spec["type"] in ("bool","cat"): perturbed[k]=random.choice(spec["values"])
            else:
                idx=grid.index(bh_current[k]) if bh_current[k] in grid else len(grid)//2
                step=random.randint(1,max(1,len(grid)//4))
                perturbed[k]=grid[min(max(0,idx+random.choice([-step,step])),len(grid)-1)]
        olog(f"  BH {bh_i+1}/20...", "info")
        with opt_lock: opt_state["current_param"]=f"Basin Hopping {bh_i+1}/20"
        bh_r, bh_p, top20_global = _coordinate_descent_from(
            perturbed, pmap, olog, t0, top20_global, f"BH-{bh_i+1}", max_passes=4, stop_flag=stop_flag)
        if bh_r["fitness"] > bh_best["fitness"]:
            bh_best=bh_r; bh_current=bh_p; final_result=bh_r; final_params=bh_p
            olog(f"  ✅ BH {bh_i+1}: ЛУЧШЕ ${bh_r['equity']:.2f}","found")
            with opt_lock: opt_state["best"]=final_result; _sw_params=dict(final_params)

    if top20_global and top20_global[0]["fitness"] > final_result["fitness"]:
        final_result = top20_global[0]
        final_params = dict(final_result["params"])
    _pool.shutdown(wait=False)
    return final_result, final_params, top20_global

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

    with opt_lock:
        # Сохраняем sw_running — не обрываем уже живой тред скользящего окна
        sw_was_running = opt_state.get("sw_running", False)
        opt_state.update({
            "running": True, "done": False, "infinite": infinite,
            "cycle": 0, "progress": 0, "total": 0,
            "generation": 0, "pass_num": 0, "current_param": "",
            "logs": [], "best": None, "top20": [],
            "started_at": time.strftime("%H:%M:%S"),
            "elapsed": 0.0, "error": "",
            "chart_symbol": symbol, "chart_tf": tf,
            "chart_path": "", "chart_updated_at": 0,
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

    olog(f"   {symbol} | {tf} | {days}д | риск {risk_pct:.0f}%")

    # Загрузка свечей
    olog(f"📡 Загрузка свечей...")
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

    cycle = 0
    prev_best_params = None   # лучшие параметры предыдущего цикла
    prev_top20       = []     # накопленный top20 всех циклов

    # Если передан seed из загруженного файла — стартуем с него
    if seed and seed.get("best") and seed["best"].get("params"):
        prev_best_params = dict(seed["best"]["params"])
        prev_top20       = list(seed.get("top20") or [])
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
                olog(f"✅ График готов: {len(sigs_pre)} сигналов", "ok")
        except Exception as e:
            olog(f"⚠ Предварительный график не удался: {e}", "warn")

    while True:
        if _opt_stop_flag.is_set(): break
        cycle += 1
        with opt_lock: opt_state["cycle"] = cycle
        if infinite:
            olog(f"", "info")
            if cycle == 1:
                olog(f"═══ ЦИКЛ #{cycle} — ПЕРВЫЙ ПРОГОН ═══════════════════════════", "ok")
            else:
                prev_eq = prev_top20[0]["equity"] if prev_top20 else 0
                olog(f"═══ ЦИКЛ #{cycle} — ПРОДОЛЖЕНИЕ (лучшее за всё время: ${prev_eq:.2f}) ═══", "ok")

        # Между циклами — проверяем появление новой свечи и бесшовно сдвигаем окно
        if cycle > 1:
            _try_slide_window(symbol, tf, olog)

        # Берём актуальное окно свечей (не перегружаем с сети — SW уже обновляет)
        with opt_lock:
            current_candles = list(_sw_candles)

        final_result, final_params, top20 = _run_one_cycle(
            current_candles, days, risk_pct, olog, t0,
            prev_best_params=prev_best_params if infinite else None,
            prev_top20=prev_top20 if infinite else None)

        if _opt_stop_flag.is_set():
            print(f"[DBG] while-loop: stop_flag сработал на cycle={cycle}", flush=True); break

        print(f"[DBG] cycle={cycle} infinite={infinite} final_result={final_result is not None} stop={_opt_stop_flag.is_set()}", flush=True)
        if final_result:
            elapsed = round(time.time()-t0, 1)

            # Накапливаем top20 между циклами — сначала merge, потом выбираем best
            if infinite:
                merged = list(top20)
                for r in prev_top20:
                    merged = _update_top20(merged, r)
                prev_top20 = merged
            else:
                prev_top20 = top20

            # all_time_best берём из уже merged top20 — гарантия совпадения с таблицей
            all_time_best = prev_top20[0] if prev_top20 else final_result
            prev_best_params = dict(all_time_best["params"])

            if infinite and all_time_best["fitness"] > final_result["fitness"]:
                olog(f"  Цикл #{cycle}: ${final_result['equity']:.2f} — не улучшил рекорд (${all_time_best['equity']:.2f})", "info")
            else:
                olog(f"✅ Цикл #{cycle} готов за {elapsed}с | 🏆 ${all_time_best['equity']:.2f} WR {all_time_best['winrate']:.1f}%", "ok" if cycle==1 else "found")

            all_time_params = dict(all_time_best["params"])
            with opt_lock:
                _sw_params = all_time_params

            # Обновляем chart — показываем сигналы за то же окно что и оптимизация
            with opt_lock:
                current_candles2 = list(_sw_candles)
            # Обрезаем свечи по тому же days_limit что и оптимизатор
            cutoff = time.time() - days * 86400
            chart_candles_window = [c for c in current_candles2 if c.get("t", 0) >= cutoff]
            if len(chart_candles_window) < 10:
                chart_candles_window = current_candles2  # fallback
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
                opt_state["top20"]          = prev_top20
                opt_state["elapsed"]        = elapsed
                opt_state["done"]           = not infinite

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

# ═══════════════════════════════════════════════════════════════
# HTML UI
# ═══════════════════════════════════════════════════════════════
HTML = r"""<!DOCTYPE html>
<html lang="ru"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0,maximum-scale=1.0">
<title>WickFill · Optimizer</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --cream:#f7f3ee;
  --cream2:#ede8e0;
  --cream3:#e2dbd0;
  --sand:#c9bfb0;
  --sand2:#b5a896;
  --warm:#8c7b6b;
  --bark:#4a3f34;
  --text:#1a1310;
  --text2:#504438;
  --text3:#7a6e63;
  --glass:rgba(247,243,238,0.72);
  --glass2:rgba(237,232,224,0.55);
  --blur:saturate(180%) blur(20px);
  --shadow:0 2px 20px rgba(92,79,67,0.10);
  --shadow2:0 8px 40px rgba(92,79,67,0.14);
  --radius:18px;
  --radius-sm:12px;
  --accent:#7c6a58;
  --green:#4a7c59;
  --green-light:#e8f2eb;
  --red:#8b3a3a;
  --red-light:#f5e8e8;
  --blue:#4a6580;
  --blue-light:#e8eef5;
  --yellow:#8a7040;
  --yellow-light:#f5f0e4;
  --border:rgba(92,79,67,0.12);
  --border2:rgba(92,79,67,0.08);
}

html,body{
  height:100%;
  background:var(--cream);
  color:var(--text);
  font-family:'DM Sans',sans-serif;
  font-size:14px;
  overflow:hidden;
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
.field-row{display:grid;grid-template-columns:1fr 1fr;gap:8px}

input[type=text],input[type=password],input[type=number],select{
  padding:8px 11px;
  background:rgba(247,243,238,0.9);
  border:1px solid var(--border);
  border-radius:10px;
  color:var(--text);
  font-size:.85rem;
  font-family:'DM Sans',sans-serif;
  width:100%;
  transition:border-color .18s;
  -webkit-appearance:none;appearance:none;
}
input:focus,select:focus{outline:none;border-color:var(--sand2);background:#fff}
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
  border-radius:50%;background:#fff;
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
  background:#fff;top:2px;left:2px;
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
  background:linear-gradient(135deg,#5c4f43 0%,#7c6a58 100%);
  border:none;border-radius:var(--radius-sm);
  color:#f7f3ee;font-size:.9rem;font-weight:600;
  font-family:'DM Sans',sans-serif;
  cursor:pointer;letter-spacing:-.01em;
  box-shadow:0 2px 12px rgba(92,79,67,.25),inset 0 1px 0 rgba(255,255,255,.12);
  transition:all .18s ease;
  display:flex;align-items:center;justify-content:center;gap:7px;
}
.btn-primary:hover:not(:disabled){
  background:linear-gradient(135deg,#6b5c4e 0%,#8c7a68 100%);
  box-shadow:0 4px 20px rgba(92,79,67,.3);transform:translateY(-1px);
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
  background:rgba(247,243,238,0.9);
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
  max-height:130px;
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
  background:var(--cream2);
}
.chart-placeholder{
  flex:1;display:flex;align-items:center;justify-content:center;
  flex-direction:column;gap:8px;
  color:var(--text3);font-size:.78rem;
}
#chartFrame{
  width:100%;height:100%;flex:1;border:none;display:none;min-height:0;
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
.cc.pos{border-color:rgba(74,124,89,.3);background:rgba(232,242,235,.5)}
.cc.neg{border-color:rgba(139,58,58,.3);background:rgba(245,232,232,.4)}
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
tbody tr:hover td{background:rgba(247,243,238,.7)}
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
  background:rgba(247,243,238,.9);border:1px solid var(--border2);
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

/* Save/load row */
.save-row{display:flex;gap:7px}
.save-row .btn-ghost{flex:1;font-size:.78rem;padding:7px 10px}
.save-status{font-size:.68rem;color:var(--text3);padding:2px 0}
.save-status.ok{color:var(--green)}
.save-status.err{color:var(--red)}

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
    overflow:visible;
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
  .prog-wrap{gap:3px}
  .prog-meta{font-size:.65rem}
  .prog-param{font-size:.62rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}

  /* Кнопки — чуть меньше чем стандарт, но удобные */
  .btn-primary{padding:10px 14px;font-size:.88rem}
  .btn-ghost{padding:8px 10px;font-size:.8rem}
  .action-row{gap:5px}
  /* SW кнопка — скрыть на мобилке (редко нужна) */
  #swStopBtn{display:none !important}

  /* Save row */
  .save-row .btn-ghost{padding:7px 8px;font-size:.75rem}

  /* Бесконечный тоггл — скрыт (он всегда on) */
  #infiniteRow{display:none}

  /* Топ-результат: 1 строка */
  #bestSection{display:none !important}
  #mob-best-row{display:flex !important}

  /* Telegram и сохранение — скрыть на мобилке (в настройках десктопа) */
  .sidebar details{display:none}
  .sidebar .div{display:none}
  .save-row{display:none}
  #saveLoadStatus{display:none !important}
  /* Мобильные кнопки сохранить/загрузить */
  #mob-save-row{display:flex !important}

  /* ── ПРАВАЯ ПАНЕЛЬ: занимает остаток экрана ── */
  .right{flex:1;min-height:0;overflow-y:auto;display:flex;flex-direction:column}

  /* Top strip — вертикально на мобилке */
  .top-strip{flex-direction:column;height:auto;flex-shrink:0;}
  .cycles-col{max-width:100%;border-right:none;border-bottom:1px solid var(--border2);padding:6px 10px;overflow:visible;}
  .cc-strip{flex-wrap:nowrap;overflow-x:auto;}
  .log-col{max-height:70px;}

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
  .log-area{padding:4px 10px;}

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
    <span style="font-size:.72rem;font-weight:400;color:var(--text3)">v3.0</span>
  </div>
  <div class="topbar-spacer"></div>
  <div class="topbar-meta">
    <span class="pill" id="latencyPill">— мс</span>
    <span id="statusBadge2"></span>
    <span id="swBadge"></span>
    <button class="icon-btn" onclick="checkApi()">⟳ API</button>
    <button class="icon-btn success" onclick="termuxUpdate()">↑ Update</button>
    <button class="icon-btn" onclick="renameDownload()">✏ Fix</button>
    <button class="icon-btn danger" onclick="deleteDownload()">✕</button>
  </div>
</header>

<!-- ── Main ── -->
<div class="main">

  <!-- ── Sidebar ── -->
  <aside class="sidebar">

    <!-- Settings card -->
    <div class="card">
      <div class="card-title">Настройки</div>

      <div class="field-row" style="margin-bottom:10px">
        <div class="field">
          <label>Символ</label>
          <input type="text" id="wf_symbol" value="BTC_USDT">
        </div>
        <div class="field">
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
        <div class="field">
          <label>История (дни)</label>
          <input type="number" id="wf_days" min="3" max="90" value="3" step="1" style="width:100%">
        </div>
        <div class="field">
          <label>Риск %</label>
          <input type="number" id="wf_risk" min="1" max="100" value="20" step="1" style="width:100%">
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
    </div>

    <!-- Save / load -->
    <div class="save-row">
      <button class="btn-ghost" onclick="saveResult()">💾 Сохранить</button>
      <button class="btn-ghost" onclick="loadResult()">📂 Загрузить</button>
    </div>
    <div class="save-status" id="saveLoadStatus" style="display:none"></div>

    <!-- Мобильная строка топ-результата (1 строка, видна только на телефоне) -->
    <div id="mob-best-row" style="display:none;align-items:center;gap:8px;flex-wrap:wrap;padding:6px 2px;border-radius:10px;background:var(--glass2);border:1px solid var(--border2)">
      <span id="mob-eq" style="font-weight:700;font-family:'DM Mono',monospace;font-size:1rem;color:var(--green);padding:0 8px">—</span>
      <span id="mob-wr" style="font-size:.78rem;color:var(--text2)">WR —</span>
      <span id="mob-dd" style="font-size:.78rem;color:var(--text2)">DD —</span>
      <span id="mob-tr" style="font-size:.78rem;color:var(--text3)">— сд</span>
      <span id="mob-sl" style="font-size:.78rem;color:var(--text3)">SL —</span>
      <span id="mob-tp" style="font-size:.78rem;color:var(--text3)">TP —</span>
      <span style="flex:1"></span>
      <span style="font-size:.72rem;cursor:pointer;color:var(--text3);padding:0 8px" onclick="toggleParams()">⚙</span>
    </div>

    <!-- Мобильные кнопки Сохранить / Загрузить -->
    <div id="mob-save-row" style="display:none;gap:7px">
      <button class="btn-ghost" style="flex:1;font-size:.78rem;padding:7px 10px" onclick="saveResult()">💾 Сохранить</button>
      <button class="btn-ghost" style="flex:1;font-size:.78rem;padding:7px 10px" onclick="loadResult()">📂 Загрузить</button>
    </div>

    <!-- Best result (desktop) -->
    <div id="bestSection" style="display:none">
      <div class="div"></div>
      <div class="card-title" style="margin-bottom:8px">Лучший результат</div>
      <div class="stats-grid" id="bestGrid"></div>
      <div id="bestParamsWrap" style="display:none;margin-top:8px">
        <div class="params-toggle" onclick="toggleParams()">› Параметры стратегии</div>
        <div class="params-box" id="bestParams"></div>
      </div>
    </div>

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
          <span class="cycles-label">Циклы</span>
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
            <th>Сделок</th><th>DD%</th><th>PF</th><th>SL%</th><th>TP%</th>
          </tr>
        </thead>
        <tbody id="top20Body"></tbody>
      </table>
    </div>

    <!-- Chart — fills remaining space -->
    <div class="chart-area">
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
let polling=null, startTs=0, lastLogCount=0, chartOpened=false, lastChartTs=0;
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
function _slStatus(msg,ok){
  const el=document.getElementById('saveLoadStatus');
  if(!el)return;el.style.display='block';
  el.className='save-status '+(ok?'ok':'err');el.textContent=msg;
}
function saveResult(){
  const best=window._lastBest,top20=window._lastTop20;
  const sym=document.getElementById('wf_symbol').value.trim()||'BTC_USDT';
  const tf=document.getElementById('wf_tf_sel').value;
  if(!best){_slStatus('Нет результата',false);return;}
  const data={best,top20:top20||[],symbol:sym,tf,saved_at:new Date().toLocaleString()};
  const blob=new Blob([JSON.stringify(data,null,2)],{type:'application/json'});
  const url=URL.createObjectURL(blob);
  const a=document.createElement('a');a.href=url;a.download=`wickfill_${sym}_${tf}.json`;a.click();
  URL.revokeObjectURL(url);_slStatus('✓ Скачан файл',true);
}
function loadResult(){
  const input=document.createElement('input');input.type='file';input.accept='.json';
  input.onchange=function(e){
    const file=e.target.files[0];if(!file)return;
    const reader=new FileReader();
    reader.onload=function(ev){
      try{
        const d=JSON.parse(ev.target.result);
        if(!d.best){_slStatus('Неверный формат',false);return;}
        window._loadedSeed={best:d.best,top20:d.top20};
        if(d.best) renderBest(d.best,d.top20||[]);
        _slStatus(`✓ $${d.best?.equity?.toFixed(0)} WR${d.best?.winrate?.toFixed(0)}%`,true);
      }catch(err){_slStatus('Ошибка: '+err,false);}
    };reader.readAsText(file);
  };input.click();
}

/* ── Start / Stop ── */
function startOpt(){
  const sym=document.getElementById('wf_symbol').value.trim()||'BTC_USDT';
  const tf=document.getElementById('wf_tf_sel').value;
  const days=document.getElementById('wf_days').value;
  const risk=document.getElementById('wf_risk').value;
  const alertCfg=getAlertCfg();
  const seed=window._loadedSeed||null;
  const body=JSON.stringify({wf_symbol:sym,wf_tf:tf,wf_days:days,wf_risk:risk,infinite:infiniteMode,alert_cfg:alertCfg,seed});
  fetch('/scan',{method:'POST',headers:{'Content-Type':'application/json'},body})
    .then(r=>r.json()).then(d=>{
      if(!d.ok){addLogLine('[!!] '+(d.msg||'Ошибка'),'error');return;}
      lastLogCount=0;chartOpened=false;lastChartTs=0;
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
  frame.src='/chart?t='+Date.now();
  frame.style.display='block';
  if(ph) ph.style.display='none';
}
function openChart(){window.open('/chart','_blank');}

/* ── Poll ── */
function poll(){
  fetch('/opt_status').then(r=>r.json()).then(d=>{
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
    if(d.running&&d.infinite) badge.innerHTML='<span class="pill blue pulse">∞ бесконечный</span>';
    else badge.innerHTML='';
    if(d.sw_running) swb.innerHTML='<span class="pill green">🔄 SW</span>';
    else swb.innerHTML='';
    if(d.sw_running&&!d.running) document.getElementById('swStopBtn').style.display='flex';
    if(!d.sw_running) document.getElementById('swStopBtn').style.display='none';

    const logs=d.logs||[];
    if(logs.length>lastLogCount){
      for(let i=lastLogCount;i<logs.length;i++) logLine(logs[i].msg,logs[i].level);
      lastLogCount=logs.length;
    }
    if(d.best&&d.best.equity!==undefined){window._lastBest=d.best;window._lastTop20=d.top20||[];renderBest(d.best);}
    if(d.top20&&d.top20.length) renderTop20(d.top20);
    if(d.chart_path){
      document.getElementById('chartBtn').style.display='flex';
      if(d.chart_updated_at>0&&d.chart_updated_at!==lastChartTs){
        lastChartTs=d.chart_updated_at;
        _loadChartFrame();
      }
    }
    if(d.done&&!d.running&&!d.infinite){
      clearTimeout(polling);polling=null;
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
  lastLogCount=0; _cc={}; _ccPrevEq=null; _startBuf=null;
}

function addLogLine(msg,level){
  const el=document.createElement('div');
  el.className='log-line '+(level||'info');
  el.textContent=msg;
  document.getElementById('wfLog').appendChild(el);
  el.scrollIntoView({block:'nearest'});
}

function _setActivity(text){
  let el=document.getElementById('ccActivity');
  if(!el){el=document.createElement('div');el.id='ccActivity';el.className='activity-line';document.getElementById('wfLog').appendChild(el);}
  el.innerHTML=`<span class="spin" style="font-size:.8rem">⟳</span><span>${text}</span>`;
  el.scrollIntoView({block:'nearest'});
}
function _clearActivity(){const el=document.getElementById('ccActivity');if(el)el.remove();}

function _cycleCard(n,eq,wr,dd,elapsed,done){
  const isPos=eq>100;
  const delta=(_ccPrevEq!==null)?(eq-_ccPrevEq):null;
  const allEqs=Object.values(_cc).map(c=>parseFloat(c.dataset.eq||'100'));
  allEqs.push(eq);
  const maxEq=Math.max(...allEqs),minEq=Math.min(100,...allEqs),range=maxEq-minEq||1;
  const barPct=Math.min(100,Math.max(3,((eq-minEq)/range)*100));
  let card=_cc[n];
  if(!card){
    card=document.createElement('div');card.dataset.n=n;
    _cc[n]=card;const strip=document.getElementById('ccStrip');strip.insertBefore(card,strip.firstChild);
  }
  card.dataset.eq=eq;
  card.className='cc '+(done?(isPos?'pos':'neg'):'running');
  const dStr=delta===null?'':(delta>=0?'↑ +':'↓ ')+Math.abs(delta).toFixed(0)+'$';
  const dCls=delta===null?'flat':delta>=0?'pos':'neg';
  const eqCls=done?(isPos?'pos':'neg'):'run';
  card.innerHTML=
    `<div class="cc-n">Цикл ${n}</div>`+
    `<div class="cc-eq ${eqCls}">$${eq.toFixed(0)}</div>`+
    (delta!==null?`<div class="cc-d ${dCls}">${dStr}</div>`:`<div class="cc-d flat">—</div>`)+
    `<div class="cc-m">WR ${wr.toFixed(0)}%`+(dd>0?` · DD ${dd.toFixed(0)}%`:'')+`</div>`+
    (elapsed?`<div class="cc-m">${elapsed}с</div>`:'')+
    `<div class="cc-bar ${isPos?'':'neg'}" style="width:${barPct}%"></div>`;
  if(done) _ccPrevEq=eq;
}

function logLine(msg,level){
  if(!msg||!msg.trim()) return;
  if(/WickFill Optimizer|загрузка свечей|загружено \d+/i.test(msg)){
    addLogLine(msg.replace(/^[📡🔄⟳✅⏹\s]+/,''),level||'info');return;
  }
  const cycleM=msg.match(/═+\s*ЦИКЛ\s*#(\d+)/i);
  if(cycleM){_startBuf=null;_cycleCard(parseInt(cycleM[1]),100,0,0,null,false);_setActivity('Цикл '+cycleM[1]+' — оптимизация...');return;}
  const startM=msg.match(/──\s*(Старт\s*#(\d+)[^─]*?)\s*──/);
  if(startM){_setActivity(startM[1].trim()+' — перебор...');return;}
  const passM=msg.match(/Круг\s*#(\d+)\s*\|\s*Депозит:\s*\$([\d.]+)/);
  if(passM){_setActivity('Круг #'+passM[1]+' · $'+passM[2]);return;}
  const foundM=msg.match(/✅\s*.+?→\s*\$([\d.]+)\s*\(\+?([-\d.]+)\$\)\s*\|\s*WR\s*([\d.]+)%\s*\|\s*Сд\s*(\d+)\s*\|\s*DD\s*([\d.]+)%/);
  if(foundM){
    const eq=parseFloat(foundM[1]),wr=parseFloat(foundM[3]),dd=parseFloat(foundM[5]);
    if(!_startBuf||eq>_startBuf.eq)_startBuf={eq,wr,dd};
    const ns=Object.keys(_cc);
    if(ns.length){const lastN=parseInt(ns[ns.length-1]);if(!_cc[lastN].classList.contains('pos')&&!_cc[lastN].classList.contains('neg'))_cycleCard(lastN,eq,wr,dd,null,false);}
    return;
  }
  const endM=msg.match(/Старт\s*#\d+[^→]*→\s*\$([\d.]+)\s+WR\s*([\d.]+)%\s+DD\s*([\d.]+)%/);
  if(endM){const eq=parseFloat(endM[1]),wr=parseFloat(endM[2]),dd=parseFloat(endM[3]);if(!_startBuf||eq>_startBuf.eq)_startBuf={eq,wr,dd};return;}
  const doneM=msg.match(/✅\s*Цикл\s*#(\d+)\s*готов\s*за\s*(\d+)с\s*\|\s*🏆\s*\$([\d.]+)\s+WR\s+([\d.]+)%/);
  if(doneM){
    _clearActivity();
    _cycleCard(parseInt(doneM[1]),parseFloat(doneM[3]),parseFloat(doneM[4]),_startBuf?.dd||0,doneM[2],true);
    _startBuf=null;return;
  }
  if(/остановлен|остановлено/i.test(msg)){_clearActivity();addLogLine('⏹ '+msg.replace(/^[⏹\s]+/,''),'warn');return;}
  if(level==='error') addLogLine(msg,'error');
}

function renderBest(b){
  document.getElementById('bestSection').style.display='block';
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

function renderTop20(list){
  document.getElementById('top20Wrap').style.display='block';
  const top=list.slice(0,1);
  document.getElementById('top20Body').innerHTML=top.map((r)=>{
    const eq=(r.equity??100).toFixed(0),wr=(r.winrate??0).toFixed(1),dd=(r.max_dd??0).toFixed(1);
    const pf=r.profit_factor===999?'∞':(r.profit_factor??0).toFixed(2);
    const sl=r.params?.sl_pct??'—',tp=r.params?.tp_pct??'—';
    return `<tr><td>$${eq}</td><td>${wr}</td><td>${r.trades??0}</td>
      <td style="color:${parseFloat(dd)>25?'var(--red)':'inherit'}">${dd}</td>
      <td style="color:${parseFloat(pf)>=1.5?'var(--green)':'inherit'}">${pf}</td>
      <td>${sl}</td><td>${tp}</td></tr>`;
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
  fetch('/reset_running').then(()=>setTimeout(()=>location.reload(),500));
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
                    "best":           opt_state["best"],
                    "top20":          opt_state["top20"],
                    "elapsed":        opt_state["elapsed"],
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
        elif parsed.path in ("/chart", "/chart_download"):
            with opt_lock:
                chart_candles = list(opt_state.get("chart_candles", []))
                chart_signals = list(opt_state.get("chart_signals", []))
                chart_symbol  = opt_state.get("chart_symbol", "")
                chart_tf      = opt_state.get("chart_tf", "")
                chart_best    = opt_state.get("best", None)
                chart_path    = opt_state.get("chart_path", "")
            if not chart_best or not chart_candles:
                self.send_response(200)
                self.send_header("Content-Type","text/html;charset=utf-8"); self.end_headers()
                self.wfile.write("<html><body style='background:#0d1117;color:#e6edf3;font-family:system-ui;padding:40px'><h2>⏳ График ещё не готов</h2><p style='color:#8b949e;margin-top:10px'>Запустите оптимизацию и подождите первого цикла.</p><script>setTimeout(()=>location.reload(),5000)</script></body></html>".encode())
                return
            try:
                data = _build_chart_html(chart_candles, chart_signals, chart_best, chart_symbol, chart_tf).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type","text/html;charset=utf-8")
                self.send_header("Content-Length",str(len(data)))
                self.send_header("Cache-Control","no-store")
                if parsed.path=="/chart_download" and chart_path:
                    self.send_header("Content-Disposition",f'attachment;filename="{os.path.basename(chart_path)}"')
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
            if c:
                self._json({"ok": True, "t": c["t"], "o": c["open"],
                            "h": c["high"], "l": c["low"], "c": c["close"]})
            else:
                self._json({"ok": False, "msg": "нет данных в кеше"})
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
            candidate_dirs=[os.path.expanduser("~/downloads"),os.path.expanduser("~/Download"),
                "/sdcard/Download","/sdcard/Downloads","/storage/emulated/0/Download",
                "/storage/emulated/0/Downloads",os.path.expanduser("~/Downloads")]
            deleted=[]
            _pat=_re.compile(r'^screener_pro\s*\(\d+\)\.py$')
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
            candidate_dirs = [os.path.expanduser("~/downloads"), os.path.expanduser("~/Download"),
                "/sdcard/Download", "/sdcard/Downloads",
                "/storage/emulated/0/Download", "/storage/emulated/0/Downloads",
                os.path.expanduser("~/Downloads")]
            _pat2 = _re.compile(r'^screener_pro\s*\(\d+\)\.py$')
            renamed = False
            msg = ""
            for d in candidate_dirs:
                if not os.path.isdir(d): continue
                matches = [f for f in os.listdir(d) if _pat2.match(f)]
                if not matches: continue
                src = os.path.join(d, sorted(matches)[-1])
                dst = os.path.join(d, script_name)
                try:
                    if os.path.exists(dst): os.remove(dst)
                    os.rename(src, dst)
                    renamed = True
                    msg = f"Переименован: {os.path.basename(src)} → {script_name}"
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
            candidate_dirs=[os.path.expanduser("~/downloads"),os.path.expanduser("~/Download"),
                "/sdcard/Download","/sdcard/Downloads","/storage/emulated/0/Download",
                "/storage/emulated/0/Downloads",os.path.expanduser("~/Downloads")]
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
            fname=f"wickfill_{symbol.replace('/','_')}_{tf}.json"
            fpath=os.path.join(os.path.dirname(os.path.abspath(__file__)),fname)
            if not os.path.exists(fpath):
                self._json({"ok":False,"msg":f"Файл не найден: {fname}"}); return
            try:
                with open(fpath,"r",encoding="utf-8") as f: data=json.load(f)
                self._json({"ok":True,"best":data.get("best"),"top20":data.get("top20",[]),"saved_at":data.get("saved_at","")})
            except Exception as e: self._json({"ok":False,"msg":str(e)})
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
            if not best: self._json({"ok":False,"msg":"Нет данных"}); return
            fname=f"wickfill_{symbol.replace('/','_')}_{tf}.json"
            fpath=os.path.join(os.path.dirname(os.path.abspath(__file__)),fname)
            try:
                with open(fpath,"w",encoding="utf-8") as f:
                    json.dump({"best":best,"top20":top20,"symbol":symbol,"tf":tf,"saved_at":time.strftime("%Y-%m-%d %H:%M:%S")},f,ensure_ascii=False,indent=2)
                self._json({"ok":True,"file":fname})
            except Exception as e: self._json({"ok":False,"msg":str(e)})
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

        if parsed.path == "/scan":
            try: params=json.loads(body)
            except: self._json({"ok":False,"msg":"bad JSON"}); return
            global _opt_thread
            print(f"[SCAN] infinite={params.get('infinite')} symbol={params.get('wf_symbol')} tf={params.get('wf_tf')}", flush=True)
            # Если тред жив — не перезапускаем, чтобы не сбрасывать циклы
            if _opt_thread and _opt_thread.is_alive():
                self._json({"ok":False,"msg":"Оптимизация уже запущена. Сначала нажмите Стоп."}); return
            _opt_thread = threading.Thread(target=run_optimizer_safe, args=(params,), daemon=True)
            _opt_thread.start()
            self._json({"ok":True})
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
    print(f"WickFill Optimizer v3.0")
    print(f"  Локально:  http://localhost:{port}")
    print(f"  По сети:   http://{local_ip}:{port}")
    print(f"Остановить: Ctrl+C")
    ReusableHTTPServer(("",port),Handler).serve_forever()
