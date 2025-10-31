from __future__ import annotations

# Flask 相关导入已注释（FastAPI 项目不需要）
# from flask import Blueprint, request, jsonify
# calculate_bp = Blueprint("calculate", __name__)

import json
import numpy as np
from scipy import stats
import requests
import datetime as dt
from collections import defaultdict


# ======================= 公共工具 =======================

def _fetch_rows(metric_api_address, JWT, datas_key: str):
    """从数据池取回 rows"""
    if not datas_key:
        return []
    url = f"{metric_api_address}/api/v1/copilot/datas/{datas_key}"
    headers = {"Authorization": JWT}
    try:
        r = requests.get(url, headers=headers, timeout=20)
        j = r.json()
        data_str = (j.get("payload") or {}).get("datas", "[]")
        return json.loads(data_str)
    except Exception:
        return []

def _parse_date(s: str):
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except Exception:
            pass
    try:
        return dt.datetime.fromisoformat(s.replace("Z","").replace("T"," ")).date()
    except Exception:
        return None

def _infer_period_and_value(rows, target_col: str, date_col: str):
    """
    返回 (value, period_label)
    - 有 date_col：按最大日期行取该行 target_col
    - 否则：取第一条有数值的 target_col；若多行无法判定则取均值
    """
    if not rows:
        return None, None

    # 有日期——按最大日期那行
    if date_col and isinstance(rows[0], dict) and date_col in rows[0]:
        best = None
        for r in rows:
            d = r.get(date_col)
            if not isinstance(d, str):
                continue
            dd = _parse_date(d)
            if dd is None:
                continue
            if best is None or dd > best[0]:
                best = (dd, r)
        if best:
            v = best[1].get(target_col)
            try:
                v = float(v)
                label = best[0].strftime("%Y-%m")
                return v, label
            except Exception:
                pass

    vals = []
    for r in rows:
        try:
            vals.append(float(r.get(target_col)))
        except Exception:
            continue
    if not vals:
        return None, None
    return float(np.mean(vals)), None

def _rows_to_array(rows, target_col: str):
    arr = []
    for r in rows or []:
        try:
            arr.append(float(r.get(target_col)))
        except Exception:
            pass
    return arr

def _ensure_number(x):
    try:
        return float(x)
    except Exception:
        return None


# ======================= 基础统计 =======================

def mean(arr):                 return {"result": float(np.mean(arr))}
def median(arr):               return {"result": float(np.median(arr))}
def standard_deviation(arr):   return {"result": float(np.std(arr))}
def skewness(arr):             return {"result": float(stats.skew(arr, bias=False))}
def kurtosis(arr):             return {"result": float(stats.kurtosis(arr, bias=False))}

def calculate_quartiles(arr):
    return {"Q1": float(np.percentile(arr, 25)),
            "Q2": float(np.percentile(arr, 50)),
            "Q3": float(np.percentile(arr, 75))}

def calculate_percentiles(arr, percentiles=None):
    percentiles = percentiles or [10, 25, 50, 75, 90]
    return {f"{int(p)}th": float(np.percentile(arr, p)) for p in percentiles}

def analyze_distribution(arr):
    arr = np.array(arr)
    stats_result = {
        "mean":    float(np.mean(arr)),
        "median":  float(np.median(arr)),
        "std_dev": float(np.std(arr)),
        "skewness":float(stats.skew(arr)),
        "kurtosis":float(stats.kurtosis(arr)),
    }
    hist, bins = np.histogram(arr, bins="auto")
    q1 = float(np.percentile(arr, 25))
    md = float(np.median(arr))
    q3 = float(np.percentile(arr, 75))
    iqr = q3 - q1
    min_val = float(np.min(arr[arr >= q1 - 1.5 * iqr])) if len(arr)>0 else None
    max_val = float(np.max(arr[arr <= q3 + 1.5 * iqr])) if len(arr)>0 else None
    return {
        "stats": stats_result,
        "histogram": {"type":"histogram","data":hist.tolist(),"bin_edges":bins.tolist()},
        "boxplot": {"type":"boxplot","min":min_val,"q1":q1,"median":md,"q3":q3,"max":max_val}
    }

def detect_outliers(arr):
    if not arr:
        return {"result": []}
    z = np.abs(stats.zscore(arr))
    out = [{"value": float(arr[i]), "outlier_degree": float(z[i])} for i in range(len(arr)) if z[i] > 3]
    return {"result": out}


# ======================= 趋势（需要完整行数据） =======================

def analyze_trend_rows(rows, target_col, date_col='生产日期'):
    if not rows:
        return {"error":"empty data"}
    seq = []
    for r in rows:
        d = r.get(date_col)
        v = r.get(target_col)
        dd = _parse_date(str(d)) if isinstance(d, str) else None
        if dd is not None:
            try:
                vv = float(v)
                seq.append((dd, vv))
            except Exception:
                pass
    if len(seq) < 2:
        return {"error":"not enough points"}
    seq.sort(key=lambda t: t[0])
    y = np.array([v for _, v in seq])
    x = np.arange(len(y))
    slope, intercept = np.polyfit(x, y, 1)
    yhat = slope * x + intercept
    y_mean = np.mean(y)
    ss_tot = np.sum((y - y_mean) ** 2)
    ss_res = np.sum((y - yhat) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot != 0 else 0.0
    return {
        "stats": {
            "slope": float(slope),
            "intercept": float(intercept),
            "r_squared": float(r2),
            "latest_value": float(y[-1]),
            "max_value": float(np.max(y)),
            "min_value": float(np.min(y)),
            "mean": float(np.mean(y)),
            "std": float(np.std(y)),
        },
        "plot_data": {
            "type": "trend",
            "dates": [d.strftime("%Y-%m-%d") for d, _ in seq],
            "values": [float(v) for _, v in seq],
            "trendline": [float(v) for v in yhat],
            "r_squared": float(r2),
        }
    }


# ======================= 对比类（YoY/MoM/QoQ/WoW/DoD） =======================

def compare_core(base_rows, cmp_rows, target_col, date_col):
    base_v, base_period = _infer_period_and_value(base_rows, target_col, date_col)
    cmp_v,  cmp_period  = _infer_period_and_value(cmp_rows,  target_col, date_col)
    base_v = _ensure_number(base_v)
    cmp_v  = _ensure_number(cmp_v)
    if base_v is None or cmp_v is None:
        return {"error":"value not found"}, None, None
    delta = cmp_v - base_v
    pct = None
    if base_v == 0:
        pct = None
    else:
        pct = (delta / base_v) * 100.0
    return {
        "base_value": base_v,
        "compare_value": cmp_v,
        "delta": float(delta),
        "delta_pct": None if pct is None else float(pct),
        "base_period": base_period,
        "compare_period": cmp_period
    }, base_period, cmp_period

def run_compare(metric_api_address, JWT, data):
    target_col = data.get("target_column")
    date_col   = data.get("date_column") or "生产日期"
    # base
    base_rows = data.get("rows") or _fetch_rows(metric_api_address, JWT, data.get("datasKey",""))
    # compare
    cmp_info  = data.get("compare") or {}
    cmp_rows  = cmp_info.get("rows") or _fetch_rows(metric_api_address, JWT, cmp_info.get("datasKey",""))
    result, bp, cp = compare_core(base_rows, cmp_rows, target_col, date_col)
    return {"result": result}


# ======================= 分组与过滤工具（新增） =======================

def _apply_filter(rows, filter_obj):
    """
    过滤 rows；支持的操作符：
      eq, ne, gt, ge, lt, le, in, nin, contains, startswith, endswith
    结构示例：
      {"k2": {"eq": "v1"}, "k1": {"gt": 3}}
    """
    if not rows or not isinstance(filter_obj, dict) or not filter_obj:
        return rows

    def ok(row):
        for col, cond in filter_obj.items():
            v = row.get(col, None)
            if not isinstance(cond, dict):
                # 兼容 {"col": value} → eq
                if v != cond:
                    return False
                continue
            for op, rhs in cond.items():
                if op == "eq" and v != rhs: return False
                if op == "ne" and v == rhs: return False
                if op == "gt":
                    try:
                        if not (float(v) > float(rhs)): return False
                    except Exception: return False
                if op == "ge":
                    try:
                        if not (float(v) >= float(rhs)): return False
                    except Exception: return False
                if op == "lt":
                    try:
                        if not (float(v) < float(rhs)): return False
                    except Exception: return False
                if op == "le":
                    try:
                        if not (float(v) <= float(rhs)): return False
                    except Exception: return False
                if op == "in":
                    try:
                        if v not in rhs: return False
                    except Exception: return False
                if op == "nin":
                    try:
                        if v in rhs: return False
                    except Exception: return False
                if op == "contains":
                    try:
                        if str(rhs) not in str(v): return False
                    except Exception: return False
                if op == "startswith":
                    try:
                        if not str(v).startswith(str(rhs)): return False
                    except Exception: return False
                if op == "endswith":
                    try:
                        if not str(v).endswith(str(rhs)): return False
                    except Exception: return False
        return True

    return [r for r in rows if ok(r)]

def _group_key(row, group_by):
    if not group_by: return ()
    return tuple(row.get(k) for k in group_by)

def _agg_one_group(rows, aggregations):
    """
    rows: 属于一个组的 list[dict]
    aggregations: [{"col":"k1","op":"max"}, ...]
    支持 op: sum, mean/avg, max, min, count, median, std
    返回: dict，如 {"k1_max": 5, "k3_sum": 12, "count": 7}
    """
    out = {}
    for agg in aggregations or []:
        col = agg.get("col")
        op  = (agg.get("op") or "").lower()
        if op == "count":
            out["count"] = len(rows)
            continue
        # 取数值列
        vals = []
        for r in rows:
            try:
                if col in r and r[col] is not None:
                    vals.append(float(r[col]))
            except Exception:
                pass
        key = f"{col}_{op}" if col else op
        if not vals:
            out[key] = None
            continue
        if op in {"sum"}:
            out[key] = float(np.sum(vals))
        elif op in {"mean","avg"}:
            out[key] = float(np.mean(vals))
        elif op == "max":
            out[key] = float(np.max(vals))
        elif op == "min":
            out[key] = float(np.min(vals))
        elif op == "median":
            out[key] = float(np.median(vals))
        elif op == "std":
            out[key] = float(np.std(vals))
        else:
            # 未知 op → 返回 None
            out[key] = None
    # 如果 aggregations 里没有 count，又给个基础 count
    if "count" not in out:
        out["count"] = len(rows)
    return out

def _sort_and_limit(items, order_by, limit_):
    """
    items: list[dict]
    order_by: [{"col":"k1_max","order":"desc"}]
    limit_: int
    """
    if order_by and isinstance(order_by, list):
        # 多列排序，从后往前应用
        for rule in reversed(order_by):
            col  = rule.get("col")
            o    = (rule.get("order") or "desc").lower()
            rev  = (o != "asc")
            items.sort(key=lambda d: (d.get(col) is None, d.get(col)), reverse=rev)
    if isinstance(limit_, int) and limit_ > 0:
        items = items[:limit_]
    return items


# ======================= 分组类算法实现（新增） =======================

def groupby_agg_rows(rows, group_by, aggregations, order_by=None, limit_=None, filter_obj=None):
    rows = _apply_filter(rows, filter_obj)
    groups = defaultdict(list)
    for r in rows or []:
        groups[_group_key(r, group_by)].append(r)
    result = []
    for gkey, grows in groups.items():
        gdict = {group_by[i]: gkey[i] for i in range(len(group_by or []))}
        agg   = _agg_one_group(grows, aggregations)
        gdict.update(agg)
        result.append(gdict)
    result = _sort_and_limit(result, order_by, limit_)
    return {"result": result}

def topn_agg_rows(rows, group_by, aggregations, order_by=None, limit_=None, filter_obj=None):
    """
    语义：先 groupby 聚合，再按 order_by 进行全局排序并取前 N（TopN 组）。
    与 groupby_agg 的唯一区别：强调 TopN 取法，便于上游语义区分。
    """
    return groupby_agg_rows(rows, group_by, aggregations, order_by, limit_, filter_obj)

def group_trend_rows_full(rows, group_by, target_col, date_col="生产日期", order_by=None, limit_=None, filter_obj=None):
    """
    每个组单独做趋势拟合（与 analyze_trend_rows 同逻辑），返回每组的 slope / r2 / latest 等统计。
    order_by/limit_ 作用在这些“组级统计”上（例如按 slope desc 取前 5 个上升最快的组）。
    """
    rows = _apply_filter(rows, filter_obj)
    buckets = defaultdict(list)
    for r in rows or []:
        d = r.get(date_col)
        dd = _parse_date(str(d)) if isinstance(d, str) else None
        if dd is None:
            continue
        try:
            v = float(r.get(target_col))
        except Exception:
            continue
        buckets[_group_key(r, group_by)].append((dd, v))

    items = []
    for gkey, seq in buckets.items():
        if len(seq) < 2:
            continue
        seq.sort(key=lambda t: t[0])
        y = np.array([v for _, v in seq])
        x = np.arange(len(y))
        slope, intercept = np.polyfit(x, y, 1)
        yhat = slope * x + intercept
        y_mean = np.mean(y)
        ss_tot = np.sum((y - y_mean) ** 2)
        ss_res = np.sum((y - yhat) ** 2)
        r2 = 1 - ss_res / ss_tot if ss_tot != 0 else 0.0

        gdict = {group_by[i]: gkey[i] for i in range(len(group_by or []))}
        stats_block = {
            "slope": float(slope),
            "intercept": float(intercept),
            "r_squared": float(r2),
            "latest_value": float(y[-1]),
            "max_value": float(np.max(y)),
            "min_value": float(np.min(y)),
            "mean": float(np.mean(y)),
            "std": float(np.std(y)),
        }
        plot_block = {
            "type": "trend",
            "dates": [d.strftime("%Y-%m-%d") for d, _ in seq],
            "values": [float(v) for _, v in seq],
            "trendline": [float(v) for v in yhat],
            "r_squared": float(r2),
        }
        items.append({"group": gdict, "stats": stats_block, "plot_data": plot_block})

    # 支持按 "stats.slope" 等点号路径排序
    if order_by and isinstance(order_by, list):
        for rule in reversed(order_by):
            col  = rule.get("col")
            o    = (rule.get("order") or "desc").lower()
            rev  = (o != "asc")
            def _get_val(d):
                if not col:
                    return None
                if "." in col:
                    a, b = col.split(".", 1)
                    base = d.get(a) or {}
                    return base.get(b)
                return d.get(col)
            items.sort(key=lambda d: (_get_val(d) is None, _get_val(d)), reverse=rev)

    if isinstance(limit_, int) and limit_ > 0:
        items = items[:limit_]

    return {"result": items}

# Flask 路由已注释（FastAPI 项目使用 api/search_api.py 中的路由）
# @calculate_bp.route("/api/comprehensive", methods=["POST"])
# def comprehensive_api():
#     try:
#         req = request.get_json(force=True)
#         metric_api_address = req.get("metric_api_address")
#         JWT = req.get("JWT")
#         data = req.get("data") or {}
#         if not all([metric_api_address, JWT, data]):
#             return jsonify({"error": "missing required fields: metric_api_address, JWT, data"}), 400
#
#         out = comprehensive_analysis(metric_api_address, JWT, data)
#         return jsonify(out)
#     except Exception as e:
#         import traceback
#         return jsonify({"error": f"internal error: {str(e)}", "trace": traceback.format_exc()}), 500

def comprehensive_analysis(metric_api_address, JWT, data):
    """
    对多个 target_columns 执行全套分析。
    支持：基础统计、分布、异常值、趋势、对比、分组聚合、分组趋势。
    """
    # 获取主数据
    rows = data.get("rows") or _fetch_rows(metric_api_address, JWT, data.get("datasKey", ""))
    if not rows:
        return {"error": "no data rows"}

    target_columns = data.get("target_columns") or []
    if not target_columns:
        # 自动推断数值列（可选增强）
        sample = rows[0]
        target_columns = [k for k, v in sample.items() if isinstance(v, (int, float)) or (isinstance(v, str) and _ensure_number(v) is not None)]
        if not target_columns:
            return {"error": "no target_columns specified and cannot auto-infer"}

    date_column = data.get("date_column") or "生产日期"
    group_by = data.get("group_by") or []
    compare_info = data.get("compare")  # 用于同比环比

    # 获取对比数据（如果存在）
    cmp_rows = None
    if compare_info:
        cmp_rows = compare_info.get("rows") or _fetch_rows(metric_api_address, JWT, compare_info.get("datasKey", ""))

    result = {}

    for col in target_columns:
        col_result = {}

        # 1. 基础统计 & 分布 & 异常值（基于数值数组）
        arr = _rows_to_array(rows, col)
        if arr:
            col_result["basic_stats"] = {
                "mean": float(np.mean(arr)),
                "median": float(np.median(arr)),
                "std": float(np.std(arr)),
                "skewness": float(stats.skew(arr, bias=False)),
                "kurtosis": float(stats.kurtosis(arr, bias=False)),
                "min": float(np.min(arr)),
                "max": float(np.max(arr)),
                "count": len(arr)
            }
            col_result["quartiles"] = calculate_quartiles(arr)
            col_result["distribution"] = analyze_distribution(arr)
            col_result["outliers"] = detect_outliers(arr)
        else:
            col_result["error"] = "no numeric data for this column"

        # 2. 时间序列趋势（如果 date_column 存在且有效）
        if date_column and any(_parse_date(str(r.get(date_column))) for r in rows if r.get(date_column) is not None):
            trend_res = analyze_trend_rows(rows, col, date_column)
            if "error" not in trend_res:
                col_result["trend"] = trend_res

        # 4. 分组聚合（如果 group_by 非空）
        if group_by:
            agg_res = groupby_agg_rows(
                rows=rows,
                group_by=group_by,
                aggregations=[
                    {"col": col, "op": "sum"},
                    {"col": col, "op": "mean"},
                    {"col": col, "op": "max"},
                    {"col": col, "op": "min"},
                    {"col": col, "op": "std"},
                    {"col": col, "op": "median"}
                ],
                filter_obj=data.get("filter_obj"),
                order_by=None,
                limit_=None
            )
            if "result" in agg_res:
                col_result["groupby_agg"] = agg_res["result"]

        # 5. 分组趋势（如果同时有 group_by 和 date_column）
        if group_by and date_column and any(_parse_date(str(r.get(date_column))) for r in rows if r.get(date_column) is not None):
            group_trend_res = group_trend_rows_full(
                rows=rows,
                group_by=group_by,
                target_col=col,
                date_col=date_column,
                filter_obj=data.get("filter_obj"),
                order_by=None,
                limit_=None
            )
            if "result" in group_trend_res and group_trend_res["result"]:
                col_result["group_trend"] = group_trend_res["result"]

        result[col] = col_result

    return {"comprehensive_result": result}


# ======================= 统一入口 =======================

def unified_api(metric_api_address, JWT, data, func_name, func_param=None):
    """
    func_name 支持：
      - mean/median/standard_deviation/skewness/kurtosis/calculate_quartiles/calculate_percentiles
      - analyze_distribution/detect_outliers
      - analyze_trend（使用 rows + date_column）
      - yoy/mom/qoq/wow/dod/compare（对比类，方向：对比期相对于基期，Δ% 以基期为分母
      - groupby_agg / topn_agg / group_trend（分组类，新增）
    """
    f = (func_name or "").lower()

    # 对比类
    if f in {"yoy","mom","qoq","wow","dod","compare"}:
        return run_compare(metric_api_address, JWT, data)

    # === 分组类：groupby_agg / topn_agg / group_trend ===
    if f in {"groupby_agg", "topn_agg", "group_trend"}:
        rows = data.get("rows") or _fetch_rows(metric_api_address, JWT, data.get("datasKey",""))
        group_by     = data.get("group_by") or []
        aggregations = data.get("aggregations") or []
        order_by     = data.get("order_by") or []
        limit_       = data.get("limit")
        filter_obj   = data.get("filter_obj") or {}
        date_col     = data.get("date_column") or "生产日期"
        target_col   = data.get("target_column")  # group_trend 需要

        if f == "groupby_agg":
            return groupby_agg_rows(rows, group_by, aggregations, order_by, limit_, filter_obj)
        if f == "topn_agg":
            return topn_agg_rows(rows, group_by, aggregations, order_by, limit_, filter_obj)
        if f == "group_trend":
            if not target_col:
                return {"error":"missing target_column for group_trend"}
            return group_trend_rows_full(rows, group_by, target_col, date_col, order_by, limit_, filter_obj)

    # 趋势：用行数据
    if f == "analyze_trend":
        rows = data.get("rows") or _fetch_rows(metric_api_address, JWT, data.get("datasKey",""))
        return analyze_trend_rows(rows, data.get("target_column"), data.get("date_column") or "生产日期")

    # 其他统计：转为数值数组
    rows = data.get("rows") or _fetch_rows(metric_api_address, JWT, data.get("datasKey",""))
    arr = _rows_to_array(rows, data.get("target_column"))
    if not arr:
        return {"error":"no numeric data"}
    if f == "mean":  return mean(arr)
    if f == "median":return median(arr)
    if f == "standard_deviation": return standard_deviation(arr)
    if f == "skewness": return skewness(arr)
    if f == "kurtosis": return kurtosis(arr)
    if f == "calculate_quartiles": return calculate_quartiles(arr)
    if f == "calculate_percentiles":
        ps = (func_param or {}).get("percentiles")
        return calculate_percentiles(arr, ps)
    if f == "analyze_distribution": return analyze_distribution(arr)
    if f == "detect_outliers":     return detect_outliers(arr)

    return {"error": f"unsupported func_name: {func_name}"}

def _group_sort_rows(rows, group_by, sort_by_col=None, sort_order="desc", limit_per_group=None):
    """
    对 rows 按 group_by 分组，并在每组内按 sort_by_col 排序。
    
    Args:
        rows: 原始数据列表
        group_by: 分组字段列表
        sort_by_col: 排序字段（若为 None，则按原始顺序）
        sort_order: "asc" 或 "desc"
        limit_per_group: 每组最多返回多少条（None 表示不限）
    
    Returns:
        {"result": [{"group": {...}, "rows": [...]}, ...]}
    """
    if not rows or not group_by:
        return {"result": []}
    
    buckets = defaultdict(list)
    for r in rows:
        key = _group_key(r, group_by)
        buckets[key].append(r)
    
    result = []
    reverse = (sort_order != "asc")
    
    for gkey, group_rows in buckets.items():
        # 构建 group 信息
        group_dict = {group_by[i]: gkey[i] for i in range(len(group_by))}
        
        # 按字段排序
        if sort_by_col:
            def safe_key(row):
                val = row.get(sort_by_col)
                try:
                    return float(val)
                except Exception:
                    return str(val) if val is not None else ""
            group_rows.sort(key=safe_key, reverse=reverse)
        
        # 截断
        if limit_per_group and isinstance(limit_per_group, int) and limit_per_group > 0:
            group_rows = group_rows[:limit_per_group]
        
        result.append({
            "group": group_dict,
            "rows": group_rows
        })
    
    return {"result": result}
