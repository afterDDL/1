#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""对「真实抓下来的线上输出」跑本地化，断言模型不会再看到英文样板或已不存在的工具名。

数据源：/Users/admin/rpg-mcp-work/merged_outputs.json（capture_outputs.py 抓的线上真货）
"""
import importlib.util
import json
import os
import re
import sys

os.environ["RPG_SERVER_CMD"] = "cat"
HERE = "/Users/admin/Projects/baigong-plugins/rpg-deploy"
spec = importlib.util.spec_from_file_location("gw", os.path.join(HERE, "server.py"))
gw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gw)

SRC = "/Users/admin/rpg-mcp-work/merged_outputs.json"
OUTDIR = "/Users/admin/rpg-mcp-work/localized"
os.makedirs(OUTDIR, exist_ok=True)
data = json.load(open(SRC, encoding="utf-8"))

PASS = FAIL = 0
BAD_PHRASES = [
    "Completed Successfully", "Call the '", "Game Context:", "Next Step:",
    "What Happened:", "Additional Notes:", "💡 Workflow:", "Reason:",
    "Game state modified", "Initialized game world", "Narrative advanced",
    "Waiting for user input", "Interactive UI generated", "PAUSED:",
    "Determine consequences", "Calculate new value", "Describe the opening",
    "MIX positive AND negative", "risk/reward tradeoffs", "Not started",
    "Ready to start fresh", "Total decisions made",
]


def ck(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  PASS  %s" % name)
    else:
        FAIL += 1
        print("  FAIL  %s   %s" % (name, str(extra)[:400]))


CJK = re.compile(r"[\u4e00-\u9fff]")


def english_leaks(text):
    """只把「出现在纯英文行里」的样板算残留：中文句子中引用英文标签是允许的。"""
    leaks = []
    for line in text.splitlines():
        if CJK.search(line):
            continue
        for p in BAD_PHRASES:
            if p in line:
                leaks.append((p, line.strip()[:80]))
    return leaks


print("=== 逐 action 检查本地化结果 ===")
for k, v in data.items():
    loc = gw.localize_text(v["text"])
    with open(os.path.join(OUTDIR, k + ".txt"), "w", encoding="utf-8") as f:
        f.write(loc)
    left = english_leaks(loc)
    ck("%-22s 无残留英文样板" % k, not left, "残留: %s" % left)
    ck("%-22s 无已不存在的工具名" % k, "Call the '" not in loc)

    # 纯英文行只能剩「参数名: 值」这类必须保留的行
    stray = [l.strip() for l in loc.splitlines()
             if l.strip() and not CJK.search(l)
             and not re.match(r"^- (gameId|progress|options|fieldSelector|value|selectedOption|selectedIndex|initialStateInJson|action):", l.strip())
             and not re.match(r"^[─━▍\[\]0-9a-f\-\s]+$", l.strip())]
    ck("%-22s 纯英文行已清干净" % k, not stray, "剩余: %s" % stray[:3])

print("\n=== 关键内容抽查 ===")
cg = gw.localize_text(data["createGame"]["text"])
ck("局号标签已中文化", "- 局号:" in cg, cg[:200])
ck("UUID 未被破坏", bool(re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-", cg)))
ck("下一步指引改成 rpg(action=…)", 'rpg(action="progressStory")' in cg)
ck("参数名保留（模型要照着填）", "- gameId:" in cg)
ck("执行成功是中文字样", "执行成功" in cg)
ck("流程行已中文化", "💡 流程：" in cg and "Step 1/5" in cg)

pp = gw.localize_text(data["progressStory"]["text"])
ck("options 指引不再是一串英文长句",
   "Create 2-4 meaningful options that:" not in pp and "你自己拟 2~4 个行动选项" in pp)
ck("options 数组里的英文要求逐条中文化",
   "贴合当前处境" in pp and "有稳妥的也有冒险的" in pp, pp[pp.find("options:"):][:250])
ck("没有把正常英文句子改坏（全局 ' to ' 替换的坑）",
   "变为 the current situation" not in pp and not re.search(r"[A-Za-z] 变为", pp),
   re.findall(r".{0,30}变为.{0,30}", pp)[:3])

print("\n=== 原因句与其余英文句子 ===")
for k, v in data.items():
    loc = gw.localize_text(v["text"])
    reasons = [l.strip() for l in loc.splitlines() if l.strip().startswith("原因：")]
    bad = [r for r in reasons if not re.search(r"[\u4e00-\u9fff]", r[3:])]
    ck("%-22s 原因句已中文化" % k, not bad, bad[:2])

pua = gw.localize_text(data["promptUserActions"]["text"])
ck("中文选项块原样保留（没被二次翻译）", "【界面已转为文字】" in pua and "请玩家回复序号" in pua)
ck("选项块不再教模型算 selectedIndex（网关自己会算）",
   "selectedIndex = 序号-1" not in pua, pua[-260:])
ck("选项清单标签与内容相符", "- 选项清单:" in pua or "[0]" not in pua)
ck("等待玩家输入已中文化", "等待玩家输入" in pua)

sa = gw.localize_text(data["selectAction"]["text"])
ck("玩家选择已中文化", "玩家选择:" in sa)
ck("下一步指向 rpg(updateGame)", 'rpg(action="updateGame")' in sa)

ug = gw.localize_text(data["updateGame"]["text"])
ck("变更描述中文化", "局面已修改。变更：" in ug and "角色：减少" in ug, ug[ug.find("📝"):][:160])
ck("字段/新值标签中文化", "- 已修改字段:" in ug and "- 新值:" in ug)

sr = gw.localize_text(data["selectRestart"]["text"])
ck("重开流程中文化", "重开流程：" in sr)
ck("新局指引指向 rpg(createGame)", 'rpg(action="createGame")' in sr)

gf = gw.localize_text(data["getGame"]["text"])
ck("只读说明中文化", "这是只读操作" in gf)

print("\n=== 报错路径 ===")
err = gw.localize_text(data["getGame_notfound"]["text"])
ck("报错 JSON 变成一句中文", err.startswith("❌ 调用失败") and "不存在" in err, err[:160])
ck("报错里说明了怎么办", "createGame 重开一局" in err, err[:160])

print("\n=== 幂等性（重复跑不会二次损坏）===")
once = gw.localize_text(data["createGame"]["text"])
twice = gw.localize_text(once)
ck("两次结果一致", once == twice)

print("\n=== 上游换文案时的兜底 ===")
fake = "🎯 Next Step:\nCall the 'someNewTool' tool with these parameters:\n- x: 1"
fk = gw.localize_text(fake)
ck("未知工具名也被改成 rpg(action=…)", "Call the '" not in fk and 'rpg(action="someNewTool")' in fk, fk)

print("\n本地化结果已存到 %s/" % OUTDIR)
print("结果：%d 通过 / %d 失败" % (PASS, FAIL))
sys.exit(1 if FAIL else 0)
