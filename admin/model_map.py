"""平台级模型映射：下游可见名 ↔ 上游真实 model_id。

## 解决什么

1. **每平台可指定映射**：同一模型在不同平台叫法不同（`deepseek-v4.1-flash` /
   `dfmodel` / `DeepSeek-V4.1-Flash`），`model_alias` 的自动归一只够覆盖常见
   写法；这里允许人为钉住「下游名 → 上游 model_id」，路由时优先采用。
2. **下游看不到上游真实模型名**：映射表反向查即得脱敏名，见 `redactions()`。
   `/v1/*` 中间件（`admin.unified_errors`）用它把响应体 / SSE 里出现的上游
   id 换回下游名。

## 存储

`system_settings` 一条记录（key=`platform_model_map`），JSON：

    {"hide_upstream": true,
     "platforms": {"<落库平台>": {"<下游名>": "<上游 model_id>"}}}

`hide_upstream` 默认 true —— 「不暴露上游模型名」是默认行为；排查问题时可
在控制台临时关掉，把上游原文放回来。
"""
from __future__ import annotations

import json
import threading
import time

from admin.models import SystemSetting

#: system_settings 里的键
KEY = "platform_model_map"

#: 进程内脱敏表缓存时长（秒）。选号/转发路径上一个请求会查多次，不能每次读库。
_CACHE_TTL = 20.0

_lock = threading.Lock()
_cache: dict = {"ts": 0.0, "value": {}, "enabled": True}

_DEFAULT = {"hide_upstream": True, "platforms": {}}


# ---------------------------------------------------------------------------
# 归一
# ---------------------------------------------------------------------------
def _text(value) -> str:
    return str(value or "").strip()


def normalize_platform(value) -> str:
    """把平台写法归一到**落库平台**（`trae-cn` -> `trae`）。"""
    raw = _text(value).lower()
    if not raw:
        return ""
    try:
        from admin.platform import registry

        spec = registry.get(raw)
        storage = (spec.storage() if spec is not None else "") or ""
        if storage:
            return storage.strip().lower()
    except Exception:  # noqa: BLE001 - 注册表未就绪时退回原值
        pass
    return raw


def _truthy(value, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return default
    return text not in ("0", "false", "off", "no", "n")


def normalize(data) -> dict:
    """把任意输入洗成规范结构（去空、去重、平台归一）。"""
    src = data if isinstance(data, dict) else {}
    out = {"hide_upstream": _truthy(src.get("hide_upstream"), True),
           "platforms": {}}
    platforms = src.get("platforms")
    if not isinstance(platforms, dict):
        return out
    for pid, mapping in platforms.items():
        platform = normalize_platform(pid)
        if not platform or not isinstance(mapping, dict):
            continue
        clean: dict[str, str] = {}
        for downstream, upstream in mapping.items():
            down, up = _text(downstream), _text(upstream)
            if down and up:
                clean[down] = up
        if clean:
            out["platforms"][platform] = clean
    return out


# ---------------------------------------------------------------------------
# 存取
# ---------------------------------------------------------------------------
def load(db) -> dict:
    row = (db.query(SystemSetting).populate_existing()
           .filter(SystemSetting.key == KEY).first())
    if row is None or not (row.value or "").strip():
        return normalize(_DEFAULT)
    try:
        return normalize(json.loads(row.value))
    except (ValueError, TypeError):
        return normalize(_DEFAULT)


def save(db, data) -> dict:
    payload = normalize(data)
    from admin.routing_policy import save_value

    save_value(db, KEY, json.dumps(payload, ensure_ascii=False))
    db.commit()
    invalidate()
    return payload


def invalidate() -> None:
    with _lock:
        _cache.update({"ts": 0.0, "value": {}, "enabled": True})


# ---------------------------------------------------------------------------
# 路由侧查询
# ---------------------------------------------------------------------------
def upstream_for(db, platform: str, model: str) -> str:
    """下游名 -> 该平台的上游 model_id；没有映射返回空串。"""
    name = _text(model)
    storage = normalize_platform(platform)
    if not name or not storage:
        return ""
    mapping = load(db)["platforms"].get(storage) or {}
    if name in mapping:
        return mapping[name]
    # 下游名大小写与配置不一致时也认（客户端常把展示名当模型名传）
    lowered = name.lower()
    for down, up in mapping.items():
        if down.lower() == lowered:
            return up
    return ""


def downstream_for(db, platform: str, upstream_id: str) -> str:
    """上游 model_id -> 下游可见名；没有映射返回空串。"""
    target = _text(upstream_id)
    storage = normalize_platform(platform)
    if not target or not storage:
        return ""
    mapping = load(db)["platforms"].get(storage) or {}
    for down, up in mapping.items():
        if up == target:
            return down
    lowered = target.lower()
    for down, up in mapping.items():
        if up.lower() == lowered:
            return down
    return ""


def redaction_pairs(db) -> dict[str, str]:
    """{上游 id 小写: 下游名}，跨平台合并（冲突先到先得）。"""
    data = load(db)
    if not data.get("hide_upstream", True):
        return {}
    out: dict[str, str] = {}
    for mapping in data["platforms"].values():
        for down, up in mapping.items():
            out.setdefault(up.strip().lower(), down)
    return out


def redactions(force: bool = False) -> dict[str, str]:
    """带缓存的脱敏表（中间件在事件循环里调用，不能每次读库）。"""
    now = time.time()
    if not force and (now - float(_cache.get("ts") or 0.0)) < _CACHE_TTL:
        return dict(_cache.get("value") or {})
    pairs: dict[str, str] = {}
    try:
        from admin.db import SessionLocal

        with SessionLocal() as db:
            pairs = redaction_pairs(db)
    except Exception:  # noqa: BLE001 - 读不到就沿用上次结果（脱敏不能中断转发）
        return dict(_cache.get("value") or {})
    with _lock:
        _cache.update({"ts": now, "value": pairs})
    return dict(pairs)


def redact(model: str) -> str:
    """把单个上游模型 id 换成下游名（无映射则原样返回）。"""
    name = _text(model)
    if not name:
        return name
    pairs = _cache.get("value") or {}
    return pairs.get(name.lower(), name)
