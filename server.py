#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rpg-mcp-server 对话式适配网关 (dialogue gateway)
================================================

做两件事，且只做这两件事：

1. 传输层：把 `npx -y rpg-mcp-server` 这个 stdio MCP 服务器包成一个公网
   streamable_http MCP 端点（POST /mcp），不开子进程池、不做持久化，
   所有 HTTP 请求共用同一个 rpg-mcp-server 子进程（它的状态本来就只活在
   这一个进程的内存里，多进程反而会互相看不到局面）。

2. 交互层：原服务是给「有 UI 的客户端」写的 ——
   - promptUserActions 返回 content[0] 是一大坨 text/html 的 resource
     （约 7KB 的 <button> 界面），对话式智能体没有渲染位；
   - 它靠用户在网页上「点击按钮」再 postMessage 回 selectAction；
   - updateGame(isGameOver) 也塞一坨 game-over 的 HTML。
   本网关把 resource 类内容剥掉，换成一段纯文本的「场景 + 变化 + 选项清单」，
   并记住每个 gameId 当前的真实选项列表；随后当智能体调 selectAction 时，
   允许 selectedOption 传「2」这类序号或选项前缀，网关按记忆里的原文补全，
   使「用户用文字回答 → 映射回 selectAction」这条链路在协议层就成立。

对外只暴露 1 个工具 `rpg`（action 分派到上游那 7 个），描述与参数都已中文化。

环境变量：
  RPG_SERVER_CMD  上游 stdio 命令，默认 "npx -y rpg-mcp-server"
  PORT            监听端口，默认 8000
  MCP_PATH        HTTP 路径，默认 /mcp
  RPG_DEBUG       置 1 打印每次转发的收发日志到 stderr
仅用 Python 标准库（目标机无 pip / PEP668，避免任何三方依赖）。
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DEBUG = os.environ.get("RPG_DEBUG") == "1"
PORT = int(os.environ.get("PORT", "8000"))
MCP_PATH = os.environ.get("MCP_PATH", "/mcp")
SERVER_CMD = os.environ.get("RPG_SERVER_CMD", "npx -y rpg-mcp-server")
REQ_TIMEOUT = float(os.environ.get("RPG_REQ_TIMEOUT", "90"))
# 响应形态：json（默认，最省事）或 sse（把同一个响应包成 text/event-stream 单帧）。
# 有的平台只认 SSE，两种都支持就不用改代码试错。
RESP_MODE = os.environ.get("RPG_RESPONSE_MODE", "json").lower()


def log(*a):
    print("[gateway]", *a, file=sys.stderr, flush=True)


# --------------------------------------------------------------------------
# 1. stdio 上游进程
# --------------------------------------------------------------------------
class Upstream:
    """长驻一个 rpg-mcp-server 子进程，按 id 匹配响应。"""

    def __init__(self, command: str):
        self.command = command
        self.proc = subprocess.Popen(
            ["/bin/sh", "-c", command],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, bufsize=1,
        )
        self.lock = threading.Lock()
        self.pending: dict[int, dict] = {}
        self.cv = threading.Condition()
        self._rid = 0
        self._last_init = None
        threading.Thread(target=self._read_stdout, daemon=True).start()
        threading.Thread(target=self._read_stderr, daemon=True).start()

    def _read_stdout(self):
        for line in self.proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except Exception:
                continue
            mid = msg.get("id")
            if mid is None:
                continue
            with self.cv:
                self.pending[mid] = msg
                self.cv.notify_all()

    def _read_stderr(self):
        for line in self.proc.stderr:
            if DEBUG:
                print("[upstream]", line.rstrip(), file=sys.stderr, flush=True)

    def _write(self, obj):
        with self.lock:
            self.proc.stdin.write(json.dumps(obj, ensure_ascii=False) + "\n")
            self.proc.stdin.flush()

    def request(self, method, params=None, timeout=REQ_TIMEOUT):
        self._rid += 1
        rid = self._rid
        msg = {"jsonrpc": "2.0", "id": rid, "method": method}
        if params is not None:
            msg["params"] = params
        self._write(msg)
        deadline = time.time() + timeout
        with self.cv:
            while rid not in self.pending:
                remain = deadline - time.time()
                if remain <= 0:
                    return {"jsonrpc": "2.0", "id": rid,
                            "error": {"code": -32000, "message": "upstream timeout"}}
                self.cv.wait(min(remain, 0.5))
            return self.pending.pop(rid)

    def notify(self, method, params=None):
        msg = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            msg["params"] = params
        self._write(msg)

    def healthy(self):
        return self.proc.poll() is None


UP = Upstream(SERVER_CMD)

# --------------------------------------------------------------------------
# 2. 交互层改写：选项记忆 + promptUserActions / selectAction / game over 文本化
# --------------------------------------------------------------------------
class DialogueState:
    """记住每个 gameId 当前真实的选项原文与已发生的选择次数。"""

    def __init__(self):
        self.lock = threading.Lock()
        self.options: dict[str, list[str]] = {}
        self.choices: dict[str, int] = {}
        self.mapping_hits: dict[str, int] = {}

    def remember_options(self, game_id, options):
        if not isinstance(options, list) or not options:
            return
        with self.lock:
            self.options[game_id] = [str(o) for o in options]

    def current_options(self, game_id):
        with self.lock:
            return list(self.options.get(game_id, []))

    def note_choice(self, game_id):
        with self.lock:
            self.choices[game_id] = self.choices.get(game_id, 0) + 1
            return self.choices[game_id]

    def choice_count(self, game_id):
        with self.lock:
            return self.choices.get(game_id, 0)

    def note_hit(self, kind):
        with self.lock:
            self.mapping_hits[kind] = self.mapping_hits.get(kind, 0) + 1

    def hits(self):
        with self.lock:
            return dict(self.mapping_hits)


DS = DialogueState()
NUM_RE = re.compile(r"(?:^|\D)([1-9]\d?)(?:\D|$)")


def _norm(s: str) -> str:
    return re.sub(r"[\s，。、,.!！?？\"'“”‘’()（）\[\]【】:：;；-]", "", str(s or "")).lower()


def resolve_selection(game_id: str, selected_option, selected_index):
    """把「用户/智能体给的松散输入」映射回 promptUserActions 的选项原文。

    返回 (option_text, index, how)
      how ∈ exact / index_only / index_from_text / prefix / fuzzy / passthrough
    """
    opts = DS.current_options(game_id)
    if not opts:
        return (selected_option, selected_index, "passthrough")
    # 1) 精确匹配
    if isinstance(selected_option, str):
        for i, o in enumerate(opts):
            if o == selected_option:
                return (o, i, "exact")
    # 2) 只给了序号
    idx = None
    if isinstance(selected_index, bool):
        idx = None
    elif isinstance(selected_index, int):
        idx = selected_index
    elif isinstance(selected_index, str) and selected_index.strip().isdigit():
        idx = int(selected_index.strip())
    if selected_option in (None, "", []) and idx is not None and 0 <= idx < len(opts):
        return (opts[idx], idx, "index_only")
    # 3) 从文本里抠出序号（"2" / "第2个" / "选 2"）
    if isinstance(selected_option, str):
        s = selected_option.strip()
        if s.isdigit() and 0 < int(s) <= len(opts):
            return (opts[int(s) - 1], int(s) - 1, "index_from_text")
        m = NUM_RE.search(s)
        if m and len(s) <= 6 and 0 < int(m.group(1)) <= len(opts):
            n = int(m.group(1))
            return (opts[n - 1], n - 1, "index_from_text")
    # 4) 前缀 / 子串 / 归一化模糊匹配
    if isinstance(selected_option, str) and selected_option.strip():
        sn = _norm(selected_option)
        cands = []
        for i, o in enumerate(opts):
            on = _norm(o)
            if on.startswith(sn) or sn.startswith(on[:max(4, len(sn))]):
                cands.append((i, o))
        if len(cands) == 1:
            return (cands[0][1], cands[0][0], "prefix")
        cands = [(i, o) for i, o in enumerate(opts) if sn and (sn in _norm(o) or _norm(o) in sn)]
        if len(cands) == 1:
            return (cands[0][1], cands[0][0], "fuzzy")
    # 5) 兜底：有合法序号就用序号
    if idx is not None and 0 <= idx < len(opts):
        return (opts[idx], idx, "index_only")
    return (selected_option, selected_index, "passthrough")


def _plain_choices_block(game_id, story, deltas, options, note_extra=""):
    """给对话式智能体的选项文本块（1-based，人读友好，同时给出原文）。"""
    lines = []
    lines.append("━" * 34)
    lines.append("【界面已转为文字】")
    if story:
        lines.append("▍当前场景：%s" % story)
    if deltas:
        lines.append("▍最近变化：")
        for d in deltas:
            lines.append("   ⚡ %s" % d.get("description", d.get("field", "")))
    lines.append("▍请玩家回复序号选择（回复 1-%d）：" % len(options))
    for i, o in enumerate(options, 1):
        lines.append("   %d) %s" % (i, o))
    lines.append("▍把玩家的回复转成 selectAction 时：selectedIndex = 序号-1，"
                 "selectedOption 必须用上面括号外的完整原文（网关也接受只给序号）。")
    lines.append("━" * 34)
    if note_extra:
        lines.append(note_extra)
    return "\n".join(lines)


def _plain_gameover_block(game_id, reason):
    return "\n".join([
        "━" * 34,
        "【界面已转为文字】",
        "☠️ 游戏结束：%s" % (reason or "（服务端未提供原因）"),
        "▍想再来一局就回复「重开」，智能体将调用 selectRestart 并用新的初始状态 createGame。",
        "━" * 34,
    ])


def _extract_deltas(html: str):
    """从被丢弃的 HTML 里把「最近变化」救出来转成文本（唯一的信息来源）。"""
    out = []
    for m in re.finditer(r'<div class="delta-item">\s*(.*?)\s*</div>', html or "", re.S):
        s = re.sub(r"<[^>]+>", "", m.group(1))
        s = s.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">") \
             .replace("&quot;", '"').replace("&#39;", "'").strip()
        s = s.lstrip("⚡ ").strip()
        if s:
            out.append({"description": s})
    return out


def rewrite_call_result(name, arguments, response):
    """把 tools/call 的响应改成对话友好形态。返回新 response。"""
    result = response.get("result")
    if not isinstance(result, dict):
        return response
    # 先记忆选项（用请求参数，最可靠）
    game_id = (arguments or {}).get("gameId") or ""
    if name == "promptUserActions":
        DS.remember_options(game_id, (arguments or {}).get("options"))
    # 松散 selectAction 归一化（真的改写了发给上游的参数，见 call_tool 里调用点）
    items = result.get("content")
    if not isinstance(items, list):
        return response
    text_items = [i for i in items if i.get("type") == "text"]
    other_items = [i for i in items if i.get("type") not in ("text", "resource")]
    resource_items = [i for i in items if i.get("type") == "resource"]

    new_items = list(text_items)
    if resource_items and not result.get("isError"):
        uri = ""
        html = ""
        try:
            uri = resource_items[0].get("resource", {}).get("uri", "")
            html = resource_items[0].get("resource", {}).get("text", "") or ""
        except Exception:
            uri = ""
        if name == "promptUserActions":
            opts = DS.current_options(game_id) or (arguments or {}).get("options") or []
            story = ""
            for t in text_items:
                m = re.search(r'Situation: "(.*?)"', t.get("text", ""), re.S)
                if m:
                    story = m.group(1)
                    break
            deltas = _extract_deltas(html)
            new_items.append({"type": "text", "text": _plain_choices_block(
                game_id, story, deltas, opts)})
        elif name == "updateGame" and uri.endswith("/game-over"):
            reason = (arguments or {}).get("gameOverReason", "")
            new_items.append({"type": "text", "text": _plain_gameover_block(game_id, reason)})
        else:
            new_items.append({"type": "text", "text":
                              "【界面已转为文字】（原响应含 text/html 资源 %s，无渲染位的客户端可忽略）" % uri})
    new_items.extend(other_items)
    if not new_items:
        new_items = [{"type": "text", "text": "（上游返回了空内容）"}]

    # selectRestart 的历史条数上游永远是 0（读错了字段），按网关自己的统计补一行
    if name == "selectRestart":
        real = DS.choice_count(game_id)
        new_items.append({"type": "text", "text":
                          "【网关校正】本局玩家真实选择次数：%d（上游报的 Total decisions made 读的是 "
                          "gameHistory，实际存在 _gameHistory，所以恒为 0）。" % real})

    new = dict(response)
    new["result"] = dict(result)
    new["result"]["content"] = new_items
    return new


# --------------------------------------------------------------------------
# 2.5 schema 规范化：百工平台只接受 JSON Schema 的保守子集
#
# 实测结论（对比平台【已成功导入】的另一个 MCP）：
#   - schema 顶层有 "title"      → 平台接受
#   - 属性里有 "anyOf"           → 平台接受
#   - 属性缺 "description"       → 平台接受
#   - 属性 "type" 写成【联合类型数组】（如 ["string","number","object",...]）
#                                → 平台导入失败：建插件返回 502002 获取数据失败
# 所以这里统一把 type 收敛成单个字符串，并去掉 examples 这类非标准键。
# 选择 string 作为主类型同时也是百工约定：平台把 object/array 参数以 JSON 字符串注入。
# --------------------------------------------------------------------------
_PRIMARY_TYPE = ("string", "object", "array", "number", "integer", "boolean", "null")


def clean_prop(p):
    """把属性 schema 规范化成保守形状。"""
    if not isinstance(p, dict):
        return p
    out = dict(p)
    out.pop("examples", None)
    t = out.get("type")
    if isinstance(t, list):
        chosen = next((c for c in _PRIMARY_TYPE if c in t), None)
        if chosen:
            out["type"] = chosen
        else:
            out.pop("type", None)
        others = [x for x in t if x != chosen]
        if others:
            note = "（此参数也可为 %s）" % "/".join(others)
            if note not in (out.get("description") or ""):
                out["description"] = (out.get("description") or "") + note
    return out


def sanitize_tools_list(resp):
    """重写 tools/list 响应，把每个工具的入参 schema 收敛成平台可接受的形状。"""
    if not isinstance(resp, dict):
        return resp
    result = resp.get("result")
    if not isinstance(result, dict):
        return resp
    tools = result.get("tools")
    if not isinstance(tools, list):
        return resp
    new_tools = []
    for t in tools:
        if not isinstance(t, dict):
            new_tools.append(t)
            continue
        nt = dict(t)
        sch = nt.get("inputSchema")
        if isinstance(sch, dict):
            ns = dict(sch)
            props = ns.get("properties")
            if isinstance(props, dict):
                ns["properties"] = {k: clean_prop(v) for k, v in props.items()}
            nt["inputSchema"] = ns
        new_tools.append(nt)
    out = dict(resp)
    out["result"] = dict(result)
    out["result"]["tools"] = new_tools
    return out


# --------------------------------------------------------------------------
# 2.6 工具合并：7 个 MCP 工具 → 1 个 action 分派工具
#
# 为什么：平台给智能体的工具预算有限，7 个工具会挤占模型的选择准确率。
# 对外只暴露 1 个 `rpg`，内部按 action 路由到原上游工具；旧工具名仍接受，
# 已建好的插件不会因此失效。
# --------------------------------------------------------------------------
MERGED_NAME = "rpg"
MERGED_ACTIONS = ["createGame", "getGame", "progressStory", "promptUserActions",
                  "selectAction", "updateGame", "selectRestart"]
MERGED_PARAMS = ("gameId", "initialStateInJson", "progress", "options", "selectedOption",
                 "selectedIndex", "fieldSelector", "value", "restart", "restartReason")
MERGED_REQUIRED = {
    "createGame": ["initialStateInJson"],
    "getGame": ["gameId"],
    "progressStory": ["gameId", "progress"],
    "promptUserActions": ["gameId", "options"],
    "selectAction": ["gameId"],  # 另需 selectedOption 或 selectedIndex 之一
    "updateGame": ["gameId", "fieldSelector", "value"],
    "selectRestart": ["gameId"],
}
MERGED_HINT = {
    "createGame": "initialStateInJson 传初始局面 JSON，如 {'title':'失落的遗迹','characters':[{'name':'冒险者','level':1,'hp':100,'mp':50,'strength':10,'agility':9,'intelligence':8}],'world':{'location':'新手村','time':'清晨','weather':'晴朗'}}",
    "getGame": "gameId 就是 createGame 返回的那个 id",
    "progressStory": "progress 是这一段要推进的剧情叙述",
    "promptUserActions": "options 是你自己拟的 2~4 个行动选项（字符串数组），要正负结果混合",
    "selectAction": "selectedOption 给玩家选的行动，序号（如「2」）或选项原文都行",
    "updateGame": "fieldSelector 是字段路径如 characters[0].hp，value 是新值",
    "selectRestart": "gameId 就是上一步那个 id",
}

RPG_TOOL_SCHEMA = {
    "type": "object",
    "properties": {
        "action": {"type": "string",
                   "description": "要执行的操作，取值：createGame（开新局）/ getGame（读局面）/ "
                                  "progressStory（推进剧情）/ promptUserActions（生成可选行动）/ "
                                  "selectAction（玩家选定行动）/ updateGame（修改局面字段）/ selectRestart（重开）"},
        "gameId": {"type": "string",
                   "description": "局面 ID。除 createGame 外都需要，就是上一步返回的那个 id，原样回传不要改动"},
        "initialStateInJson": {"type": "string",
                               "description": "仅 createGame：初始局面 JSON。含 title（局名）、"
                                              "characters（角色数组，每人 name/level/hp/mp/strength/agility/intelligence）、"
                                              "world（location/time/weather）"},
        "progress": {"type": "string", "description": "仅 progressStory：这一段要推进的剧情叙述"},
        "options": {"type": "array",
                    "items": {"type": "string"},
                    "description": "仅 promptUserActions：你自己拟的 2~4 个行动选项。要正负结果混合——"
                                   "有稳妥的也有冒险的，各选项后果要不同。例：['谨慎上前搭话（可能得情报，也可能被骗）',"
                                   "'先发制人攻击（有风险但可能一击定胜负）','绕路另找入口（更安全但耗时）']"},
        "selectedOption": {"type": "string",
                           "description": "仅 selectAction：玩家选定的行动。可直接给序号（如「2」「第2个」）或选项原文"},
        "selectedIndex": {"type": "integer",
                          "description": "仅 selectAction：选项序号，可省略；给了也会被校正成真实序号"},
        "fieldSelector": {"type": "string", "description": "仅 updateGame：要修改的字段路径，如 characters[0].hp"},
        "value": {"type": "string", "description": "仅 updateGame：新值（数字/字符串/对象都写这里）"},
    },
    "required": ["action"],
}

RPG_TOOL_DESC = (
    "文字跑团引擎，规则判定全在服务端（骰子、战斗、物品、关系、局面持久化），你负责叙事与拟选项。"
    "用 action 选择操作：createGame 开新局 → progressStory 推进剧情 → "
    "promptUserActions（你拟 2~4 个选项放进 options）→ 等玩家选 → selectAction 提交玩家的选择 → "
    "updateGame 施加后果 → 再 progressStory 推进。getGame 复读局面，selectRestart 重开。"
    "三个要点：① 每一步都要把上一步返回的 gameId 原样回传，否则局面会丢；"
    "② 摆给玩家的选项就用你传给 promptUserActions 的原文，不要下次改写；"
    "③ 玩家用生活语言回答（如「第2个」）时，把序号或原文放进 selectedOption 即可。"
)


def merge_tools_list(resp):
    """把上游那 7 个工具替换成 1 个 action 分派工具（描述与参数均中文化）。"""
    if not isinstance(resp, dict) or not isinstance(resp.get("result"), dict):
        return resp
    out = dict(resp)
    out["result"] = dict(resp["result"])
    out["result"]["tools"] = [{
        "name": MERGED_NAME,
        "description": RPG_TOOL_DESC,
        "inputSchema": RPG_TOOL_SCHEMA,
    }]
    return out


def dispatch_merged(arguments):
    """拆解合并工具入参 → (上游工具名, 上游入参)；不合法时返回 (None, None, 中文错误)。"""
    args = dict(arguments or {})
    action = args.pop("action", None)
    if not isinstance(action, str) or action not in MERGED_ACTIONS:
        return None, None, ("action 必须是以下之一：%s。当前收到：%r"
                            % (" / ".join(MERGED_ACTIONS), action))
    keep = {}
    for k, v in args.items():
        # 只放行已知参数，并丢掉空串 / None（模型常把用不到的参数填空串）
        if k in MERGED_PARAMS and v is not None and v != "":
            keep[k] = v
    missing = [p for p in MERGED_REQUIRED.get(action, []) if p not in keep]
    if action == "selectAction" and not ("selectedOption" in keep or "selectedIndex" in keep):
        missing.append("selectedOption（或 selectedIndex）")
    if missing:
        return None, None, ("action=%s 缺少必填参数：%s。%s"
                            % (action, "、".join(missing), MERGED_HINT.get(action, "")))
    return action, keep, None


def coerce_options(v):
    """options 必须是字符串数组。模型实际会给：真数组 / JSON 串 / 换行分隔的纯文本，都归一化。"""
    if isinstance(v, list):
        return [str(x) for x in v if str(x).strip()]
    if isinstance(v, str):
        s = v.strip()
        if s.startswith("["):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    return [str(x) for x in arr if str(x).strip()]
            except Exception:
                pass
        parts = [p.strip(" \t-•*·") for p in re.split(r"[\n;；]+", s)]
        parts = [p for p in parts if p]
        if len(parts) >= 2:
            return parts
        return [s] if s else []
    return v


# --------------------------------------------------------------------------
# 3. 业务分发（HTTP 层与 stdio 过滤层共用同一套逻辑）
# --------------------------------------------------------------------------
def process_message(msg):
    """返回 (http_status, response_obj_or_None)。response 为 None 表示空体（notification）。"""
    is_notification = "id" not in msg or msg.get("id") is None
    method = msg.get("method", "")
    params = msg.get("params") or {}
    if is_notification:
        if method in ("notifications/initialized", "notifications/cancelled",
                      "notifications/progress", "notifications/roots/list_changed"):
            UP.notify(method, params if params else None)
        return (202, None)
    if method == "initialize":
        return (200, UP.request(method, params))
    if method == "ping":
        return (200, {"jsonrpc": "2.0", "id": msg["id"], "result": {}})
    if method == "tools/list":
        # schema 必须收敛成平台可接受的保守形状，否则建插件会 502002；
        # 然后再合并成单个 action 分派工具。
        return (200, merge_tools_list(sanitize_tools_list(UP.request(method, params or {}))))
    if method == "tools/call":
        name = params.get("name")
        arguments = params.get("arguments") or {}
        if name == MERGED_NAME:
            name, arguments, err = dispatch_merged(arguments)
            if err:
                return (200, {"jsonrpc": "2.0", "id": msg["id"], "result": {
                    "content": [{"type": "text", "text": err}], "isError": True}})
        sent_args = dict(arguments)
        how = "n/a"
        if name == "selectAction":
            opt, idx, how = resolve_selection(arguments.get("gameId", ""),
                                              arguments.get("selectedOption"),
                                              arguments.get("selectedIndex"))
            sent_args["selectedOption"] = opt
            if isinstance(idx, int):
                sent_args["selectedIndex"] = idx
            DS.note_hit("selectAction_rewritten" if sent_args != arguments else "selectAction_exact")
        # 参数还原：schema 里我们把 value / initialStateInJson 声明为 string
        # （百工约定会把 object/array 以 JSON 字符串注入），这里还原成真实类型再给上游。
        for _k in ("value", "initialStateInJson", "options"):
            if _k in sent_args and isinstance(sent_args[_k], str):
                try:
                    sent_args[_k] = json.loads(sent_args[_k])
                except Exception:
                    pass  # 不是合法 JSON 就按普通字符串用
        if "options" in sent_args:
            sent_args["options"] = coerce_options(sent_args["options"])
        resp = UP.request(method, {"name": name, "arguments": sent_args})
        if name == "selectAction" and not (resp.get("result") or {}).get("isError"):
            DS.note_choice(arguments.get("gameId", ""))
        rewritten = rewrite_call_result(name, sent_args, resp)
        if DEBUG:
            log("tools/call %s map=%s args=%s" % (
                name, how, json.dumps(sent_args, ensure_ascii=False)[:200]))
        return (200, rewritten)
    if method in ("resources/list", "prompts/list", "completion/complete",
                  "resources/templates/list", "logging/setLevel"):
        return (200, {"jsonrpc": "2.0", "id": msg["id"],
                      "error": {"code": -32601, "message": "method not found: %s" % method}})
    return (200, UP.request(method, params))


# --------------------------------------------------------------------------
# 4. HTTP 层：streamable_http
# --------------------------------------------------------------------------
# --------------------------------------------------------------------------
# 请求记录（诊断用：看清平台握手时到底发了什么、断在哪一步）
# GET /__debug/requests 可读回最近 40 条
# --------------------------------------------------------------------------
REQ_LOG = []
_REQ_LOCK = threading.Lock()


def record_req(kind, path, headers, body):
    try:
        with _REQ_LOCK:
            REQ_LOG.append({
                "t": time.strftime("%H:%M:%S"),
                "kind": kind,
                "path": path,
                "headers": {k: v for k, v in headers.items()
                            if k.lower() in ("content-type", "accept", "mcp-session-id",
                                             "user-agent", "origin", "content-length")},
                "body": (body or "")[:700],
            })
            del REQ_LOG[:-40]
    except Exception:
        pass


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "rpg-dialogue-gateway/1.0"

    # ---- 工具 ----
    def _send(self, code, body: bytes, ctype="application/json", extra=None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Access-Control-Expose-Headers", "Mcp-Session-Id")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _json(self, code, obj, extra=None):
        payload = json.dumps(obj, ensure_ascii=False)
        if RESP_MODE == "sse" and code == 200:
            body = ("event: message\ndata: %s\n\n" % payload).encode("utf-8")
            self._send(code, body, "text/event-stream", extra)
            return
        self._send(code, payload.encode("utf-8"), "application/json", extra)

    def log_message(self, fmt, *args):
        if DEBUG:
            print("[http]", fmt % args, file=sys.stderr, flush=True)

    def _path_ok(self):
        return self.path.split("?")[0].rstrip("/") in (MCP_PATH.rstrip("/"), "")

    # ---- 方法 ----
    def do_OPTIONS(self):
        self._send(204, b"")

    def do_GET(self):
        p = self.path.split("?")[0]
        if p in ("/__debug/requests", "/_dbg"):
            self._json(200, {"count": len(REQ_LOG), "requests": REQ_LOG})
            return
        if p in ("/healthz", "/health", "/"):
            self._send(200, b"rpg-dialogue-gateway ok\n", "text/plain; charset=utf-8")
            return
        # 本网关不做服务端推送，按 spec 返回 405
        self._send(405, b'{"error":"GET not supported (no server-initiated SSE)"}',
                   "application/json")

    def do_DELETE(self):
        self._send(200, b'{"ok":true}', "application/json")

    def do_POST(self):
        if not self._path_ok():
            self._json(404, {"error": "unknown path %s" % self.path})
            return
        try:
            n = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            n = 0
        raw = self.rfile.read(n) if n else b""
        record_req("POST", self.path, self.headers, raw.decode("utf-8", "replace"))
        try:
            msg = json.loads(raw.decode("utf-8"))
        except Exception:
            self._json(400, {"jsonrpc": "2.0", "id": None,
                             "error": {"code": -32700, "message": "parse error"}})
            return

        is_notification = "id" not in msg or msg.get("id") is None
        method = msg.get("method", "")
        extra = {}
        if method == "initialize":
            # 签发会话 id；不强制校验，方便平台用任意 header 复用同一实例
            extra["Mcp-Session-Id"] = uuid.uuid4().hex
        status, resp = process_message(msg)
        if resp is None:
            self._send(status, b"")
            return
        self._json(status, resp, extra)


def run_stdio_shim():
    """stdio -> stdio 过滤层：给「用 supergateway 桥接」的部署用。
    读 stdin 行分隔 JSON-RPC，转发给上游并把响应改写后写回 stdout。
    这样 supergateway 只负责传输，对话化改写仍由本文件完成。"""
    log("stdio-shim 模式启动，上游命令：%s" % SERVER_CMD)
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except Exception:
            continue
        try:
            status, resp = process_message(msg)
        except Exception as e:  # 任何异常都要变成 JSON-RPC 错误，不能吞掉
            if "id" in msg and msg["id"] is not None:
                sys.stdout.write(json.dumps(
                    {"jsonrpc": "2.0", "id": msg["id"],
                     "error": {"code": -32603, "message": "gateway error: %s" % e}},
                    ensure_ascii=False) + "\n")
                sys.stdout.flush()
            continue
        if resp is None:
            continue
        sys.stdout.write(json.dumps(resp, ensure_ascii=False) + "\n")
        sys.stdout.flush()


def keep_awake():
    """Render 免费实例闲置约 15 分钟会休眠，冷启动实测 20~60 秒（热态 <1 秒）。
    每 10 分钟自请求一次 /healthz，让实例保持热态。
    只在托管环境开启（RENDER / KEEP_AWAKE 环境变量），本地跑不受影响。"""
    import urllib.request
    base = (os.environ.get("RENDER_EXTERNAL_URL") or "").rstrip("/")
    if not base:
        log("keep-awake: 未设置 RENDER_EXTERNAL_URL，跳过")
        return
    target = base + "/healthz"
    while True:
        time.sleep(600)
        try:
            urllib.request.urlopen(target, timeout=60).read()
            log("keep-awake ok: %s" % target)
        except Exception as e:
            log("keep-awake failed: %s: %s" % (type(e).__name__, e))


def main():
    if len(sys.argv) > 1 and sys.argv[1] in ("stdio-shim", "stdio", "shim"):
        run_stdio_shim()
        return
    if not UP.healthy():
        log("上游进程未起来，命令：%s" % SERVER_CMD)
    log("上游命令: %s" % SERVER_CMD)
    log("监听 0.0.0.0:%d  path=%s" % (PORT, MCP_PATH))
    log("对外工具: %s（action 分派 %d 个上游工具）" % (MERGED_NAME, len(MERGED_ACTIONS)))
    if os.environ.get("RENDER") or os.environ.get("KEEP_AWAKE"):
        threading.Thread(target=keep_awake, daemon=True).start()
        log("keep-awake 线程已启动（每 10 分钟）")
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    srv.daemon_threads = True
    srv.serve_forever()


if __name__ == "__main__":
    main()
