# -*- coding: utf-8 -*-
"""
从抖音爬取的评论数据中分析并筛选有教育培训需求的潜在用户。
用法: python tools/analyze_edu_users.py
"""

import json
import os
import re
from collections import defaultdict
from datetime import datetime

# ==================== 配置 ====================
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "douyin", "jsonl")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "douyin")

# 需求信号关键词 —— 评论中包含这些词的用户更可能是潜在客户
DEMAND_KEYWORDS = {
    "书法培训": [
        "想学", "想练", "怎么学", "怎么练", "如何学", "如何练", "如何入门",
        "零基础", "0基础", "新手", "小白", "初学", "入门",
        "哪里学", "在哪学", "哪里报", "在哪报", "报班", "培训班", "辅导班",
        "推荐", "求推荐", "有推荐", "老师推荐",
        "多少钱", "费用", "学费", "价格", "收费",
        "几岁", "多大", "孩子", "小孩", "儿童", "少儿", "女儿", "儿子",
        "成人", "大人", "上班族",
        "网课", "线上", "线下", "课程",
        "第一天", "刚开始", "刚入门", "准备开始",
        "有救吗", "还来得及", "来不来得及", "晚不晚",
        "字帖", "用什么笔", "什么纸", "买什么",
        "请教", "指点", "指导", "带带我",
    ],
    "语文辅导": [
        "语文", "作文", "阅读理解", "古诗", "文言文", "拼音",
        "语文成绩", "语文差", "语文不好", "提高语文",
    ],
    "数学辅导": [
        "数学", "奥数", "几何", "代数", "算术",
        "数学成绩", "数学差", "数学不好", "提高数学", "数学补习",
    ],
    "英语辅导": [
        "英语", "口语", "听力", "单词", "语法", "音标",
        "英语成绩", "英语差", "英语不好", "提高英语", "英语补习",
    ],
    "综合教育": [
        "补课", "补习", "辅导", "家教", "培训",
        "小学", "初中", "高中", "中考", "高考",
        "成绩", "提分", "提高成绩", "考试",
    ],
}

# 排除的用户类型（内容创作者/老师/机构，而非潜在客户）
CREATOR_KEYWORDS = [
    "书法课", "教写字", "书法教", "培训机构", "书法工作室",
    "辅导机构", "教育机构", "补习班",
]


def load_jsonl(filepath):
    """读取JSONL文件"""
    items = []
    if not os.path.exists(filepath):
        return items
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    items.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return items


def is_creator(nickname, user_signature):
    """判断用户是否为内容创作者/机构（非潜在客户）"""
    text = f"{nickname or ''} {user_signature or ''}"
    for kw in CREATOR_KEYWORDS:
        if kw in text:
            return True
    return False


def match_demand(comment_text):
    """匹配评论中的需求信号，返回匹配到的分类和关键词"""
    if not comment_text:
        return []
    matches = []
    for category, keywords in DEMAND_KEYWORDS.items():
        for kw in keywords:
            if kw in comment_text:
                matches.append((category, kw))
                break  # 每个分类只取第一个匹配
    return matches


def analyze_comments(comments):
    """分析评论数据，提取潜在教育培训客户"""
    # user_id -> 用户信息 + 评论列表
    user_map = defaultdict(lambda: {
        "user_id": "",
        "sec_uid": "",
        "nickname": "",
        "avatar": "",
        "ip_location": "",
        "short_user_id": "",
        "user_unique_id": "",
        "comments": [],
        "demand_categories": set(),
        "demand_keywords": set(),
        "total_likes": 0,
    })

    for comment in comments:
        content = comment.get("content", "")
        if not content or len(content.strip()) < 2:
            continue

        matches = match_demand(content)
        if not matches:
            continue

        user_id = comment.get("user_id", "")
        nickname = comment.get("nickname", "")
        user_signature = comment.get("user_signature", "")

        # 排除创作者/机构账号
        if is_creator(nickname, user_signature):
            continue

        user = user_map[user_id]
        user["user_id"] = user_id
        user["sec_uid"] = comment.get("sec_uid", "")
        user["nickname"] = nickname
        user["avatar"] = comment.get("avatar", "")
        user["ip_location"] = comment.get("ip_location", "")
        user["short_user_id"] = comment.get("short_user_id", "")
        user["user_unique_id"] = comment.get("user_unique_id", "")
        user["total_likes"] += comment.get("like_count", 0)

        for cat, kw in matches:
            user["demand_categories"].add(cat)
            user["demand_keywords"].add(kw)

        user["comments"].append({
            "content": content,
            "aweme_id": comment.get("aweme_id", ""),
            "create_time": comment.get("create_time", ""),
            "ip_location": comment.get("ip_location", ""),
            "like_count": comment.get("like_count", 0),
            "matched_categories": [cat for cat, _ in matches],
        })

    return user_map


def format_timestamp(ts):
    """格式化时间戳"""
    if not ts:
        return "未知"
    try:
        return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M")
    except (ValueError, TypeError, OSError):
        return str(ts)


def generate_report(user_map, contents):
    """生成分析报告"""
    users = sorted(
        user_map.values(),
        key=lambda u: (len(u["demand_categories"]), len(u["comments"]), u["total_likes"]),
        reverse=True,
    )

    # 统计
    category_count = defaultdict(int)
    location_count = defaultdict(int)
    for user in users:
        for cat in user["demand_categories"]:
            category_count[cat] += 1
        loc = user["ip_location"]
        if loc:
            location_count[loc] += 1

    lines = []
    lines.append("=" * 70)
    lines.append("抖音教育培训潜在客户分析报告")
    lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 70)

    lines.append(f"\n## 数据概览")
    lines.append(f"- 分析视频/帖子数: {len(contents)}")
    lines.append(f"- 筛选出潜在客户数: {len(users)}")

    lines.append(f"\n## 需求分类统计")
    for cat, count in sorted(category_count.items(), key=lambda x: -x[1]):
        lines.append(f"  - {cat}: {count} 人")

    lines.append(f"\n## 地区分布 (Top 15)")
    for loc, count in sorted(location_count.items(), key=lambda x: -x[1])[:15]:
        lines.append(f"  - {loc}: {count} 人")

    lines.append(f"\n{'=' * 70}")
    lines.append("## 潜在客户详情")
    lines.append("=" * 70)

    for i, user in enumerate(users, 1):
        douyin_url = f"https://www.douyin.com/user/{user['sec_uid']}" if user["sec_uid"] else "N/A"
        lines.append(f"\n--- 用户 #{i} ---")
        lines.append(f"  昵称: {user['nickname']}")
        lines.append(f"  用户ID: {user['user_id']}")
        lines.append(f"  抖音号: {user['user_unique_id'] or user['short_user_id'] or 'N/A'}")
        lines.append(f"  主页: {douyin_url}")
        lines.append(f"  IP地区: {user['ip_location'] or '未知'}")
        lines.append(f"  需求分类: {', '.join(sorted(user['demand_categories']))}")
        lines.append(f"  需求关键词: {', '.join(sorted(user['demand_keywords']))}")
        lines.append(f"  相关评论 ({len(user['comments'])} 条):")
        for c in user["comments"]:
            time_str = format_timestamp(c["create_time"])
            lines.append(f"    [{time_str}] {c['content'][:120]}")
            lines.append(f"      -> 视频: https://www.douyin.com/video/{c['aweme_id']}")

    return "\n".join(lines), users


def export_to_jsonl(users, output_path):
    """导出潜在客户数据为JSONL"""
    with open(output_path, "w", encoding="utf-8") as f:
        for user in users:
            export_item = {
                "user_id": user["user_id"],
                "sec_uid": user["sec_uid"],
                "nickname": user["nickname"],
                "douyin_id": user["user_unique_id"] or user["short_user_id"] or "",
                "douyin_url": f"https://www.douyin.com/user/{user['sec_uid']}" if user["sec_uid"] else "",
                "ip_location": user["ip_location"],
                "demand_categories": sorted(user["demand_categories"]),
                "demand_keywords": sorted(user["demand_keywords"]),
                "comment_count": len(user["comments"]),
                "total_likes": user["total_likes"],
                "comments": user["comments"],
            }
            f.write(json.dumps(export_item, ensure_ascii=False) + "\n")


def main():
    # 查找所有JSONL数据文件
    comments_files = sorted(
        [f for f in os.listdir(DATA_DIR) if f.startswith("search_comments_") and f.endswith(".jsonl")]
    )
    contents_files = sorted(
        [f for f in os.listdir(DATA_DIR) if f.startswith("search_contents_") and f.endswith(".jsonl")]
    )

    if not comments_files:
        print("[ERROR] 未找到评论数据文件，请先运行爬虫获取数据。")
        return

    # 加载所有数据
    all_comments = []
    all_contents = []
    for f in comments_files:
        filepath = os.path.join(DATA_DIR, f)
        data = load_jsonl(filepath)
        all_comments.extend(data)
        print(f"[INFO] 加载评论文件: {f} ({len(data)} 条)")

    for f in contents_files:
        filepath = os.path.join(DATA_DIR, f)
        data = load_jsonl(filepath)
        all_contents.extend(data)
        print(f"[INFO] 加载内容文件: {f} ({len(data)} 条)")

    print(f"\n[INFO] 共加载 {len(all_comments)} 条评论, {len(all_contents)} 条视频/帖子")

    # 分析
    user_map = analyze_comments(all_comments)
    print(f"[INFO] 筛选出 {len(user_map)} 个潜在教育培训客户")

    # 生成报告
    report, users = generate_report(user_map, all_contents)

    # 保存报告
    report_path = os.path.join(OUTPUT_DIR, "edu_potential_users_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"\n[INFO] 分析报告已保存: {report_path}")

    # 导出JSONL
    export_path = os.path.join(OUTPUT_DIR, "edu_potential_users.jsonl")
    export_to_jsonl(users, export_path)
    print(f"[INFO] 用户数据已导出: {export_path}")

    # 打印报告摘要
    print("\n" + report)


if __name__ == "__main__":
    main()
