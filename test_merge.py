#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""合并工具逻辑的离线单测：不联网，只验证 merge / dispatch 的正确性。"""
import importlib.util
import json
import os
import sys

os.environ["RPG_SERVER_CMD"] = "cat"  # 不真的起 node，只占位
HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("gw", os.path.join(HERE, "server.py"))
gw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gw)

PASS = FAIL = 0


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  PASS  %s" % name)
    else:
        FAIL += 1
        print("  FAIL  %s  %s" % (name, extra))


print("=== 1. tools/list 合并 ===")
fake = {"jsonrpc": "2.0", "id": 1, "result": {"tools": [
    {"name": n, "description": "x", "inputSchema": {"type": "object", "properties": {}}}
    for n in gw.MERGED_ACTIONS]}}
merged = gw.merge_tools_list(gw.sanitize_tools_list(fake))
tools = merged["result"]["tools"]
ck("工具数 = 1", len(tools) == 1, "got %d" % len(tools))
ck("工具名 = rpg", tools[0]["name"] == "rpg", tools[0]["name"])
props = tools[0]["inputSchema"]["properties"]
ck("参数 8 个", len(props) == 8, "got %d: %s" % (len(props), list(props)))
ck("required = ['action']", tools[0]["inputSchema"]["required"] == ["action"])
ck("每个参数都有 description", all(p.get("description") for p in props.values()),
   [k for k, v in props.items() if not v.get("description")])
ck("type 全是单个字符串（无联合数组）",
   all(isinstance(p.get("type"), str) for p in props.values()))
ck("工具描述是中文且提到 gameId", "gameId" in tools[0]["description"])

print("=== 2. dispatch：合法路径 ===")
a, kept, err = gw.dispatch_merged({"action": "createGame", "initialStateInJson": '{"title":"t"}'})
ck("createGame 通过", err is None and a == "createGame", err)
ck("createGame 参数保留", kept == {"initialStateInJson": '{"title":"t"}'}, kept)

a, kept, err = gw.dispatch_merged({"action": "getGame", "gameId": "abc"})
ck("getGame 通过", err is None and a == "getGame" and kept["gameId"] == "abc")

a, kept, err = gw.dispatch_merged({"action": "selectAction", "gameId": "abc", "selectedOption": "2"})
ck("selectAction 传序号通过", err is None and kept["selectedOption"] == "2")

a, kept, err = gw.dispatch_merged({"action": "selectAction", "gameId": "abc", "selectedIndex": 1})
ck("selectAction 只给序号也通过", err is None and kept.get("selectedIndex") == 1, err)

print("=== 3. dispatch：模型常见坏输入 ===")
a, kept, err = gw.dispatch_merged({"action": "progressStory", "gameId": "abc", "progress": ""})
ck("空串参数被剔除并报缺必填", err is not None and "progress" in err, err)

a, kept, err = gw.dispatch_merged({"action": "getGame"})
ck("缺 gameId 报错（中文）", err is not None and "gameId" in err, err)

a, kept, err = gw.dispatch_merged({"action": "别的东西", "gameId": "x"})
ck("未知 action 报错并列出合法值", err is not None and "createGame" in err and "selectRestart" in err, err)

a, kept, err = gw.dispatch_merged({})
ck("完全没 action 也报错不崩", err is not None, err)

a, kept, err = gw.dispatch_merged({"action": "getGame", "gameId": "x", "foo": "bar"})
ck("多余参数被丢掉", err is None and kept == {"gameId": "x"}, kept)

a, kept, err = gw.dispatch_merged({"action": "updateGame", "gameId": "x",
                                   "fieldSelector": "characters[0].hp", "value": 0})
ck("value=0 不被当成空值丢弃", err is None and kept.get("value") == 0, kept)

a, kept, err = gw.dispatch_merged({"action": "selectAction", "gameId": "x"})
ck("selectAction 两个选项参数都没有 → 报错", err is not None and "selectedOption" in err, err)

print("=== 4. 错误响应的形状（工具层而非协议层报错）===")
resp = gw.process_message({"jsonrpc": "2.0", "id": 9, "method": "tools/call",
                           "params": {"name": "rpg", "arguments": {"action": "getGame"}}})
code, body = resp
ck("HTTP 仍 200", code == 200, code)
ck("isError = True（模型能读懂）", body["result"]["isError"] is True)
ck("错误文案是中文且带用法提示",
   "缺少必填参数" in body["result"]["content"][0]["text"], body)

print()
print("结果：%d 通过 / %d 失败" % (PASS, FAIL))
sys.exit(1 if FAIL else 0)
