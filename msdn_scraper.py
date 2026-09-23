#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
msdn.itellyou.cn 数据爬虫 / scraper.

抓取 https://msdn.itellyou.cn/ 上全部微软原版软件/系统镜像的元数据，
包括分类、语言、文件名、发布时间、ed2k 下载地址、SHA1、文件大小，
并导出为 CSV 和 JSON。

站点是一个 SPA，数据通过下面的 REST 接口按层级返回：

    GET  /                       -> 首页 HTML，含 data-menuid（8 个顶级大类 id）
    POST /Category/Index  {id}   -> 该大类下的小分类（产品）列表
    POST /Category/GetLang {id}  -> {status, result:[{id, lang}, ...]}   语言列表
    POST /Category/GetList {id, lang, filter}
                                 -> {status, result:[{id, name, url, post, ...}]} 文件列表
    POST /Category/GetProduct {id}
                                 -> {status, result:{FileName, DownLoad, PostDateString, SHA1, size, ...}}

所有 POST 请求都需要 Referer 头，否则会被拒绝。

用法:
    pip install -r requirements.txt
    python msdn_scraper.py                    # 抓取全部，输出到 ./output/
    python msdn_scraper.py --workers 16       # 提高并发（默认 8）
    python msdn_scraper.py --delay 0.1        # 每个详情请求之间的间隔（秒）
    python msdn_scraper.py --out mydir        # 自定义输出目录
    python msdn_scraper.py --no-detail        # 跳过 GetProduct（更快，但无 SHA1/大小）
    python msdn_scraper.py --selftest         # 用内置的假数据离线自测解析逻辑

注意: 运行环境必须能访问外网 (msdn.itellyou.cn)。
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

_IMPORT_ERROR: Optional[BaseException] = None
try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except Exception as _exc:  # noqa: BLE001 - capture the REAL reason, don't hide it
    requests = None  # type: ignore
    _IMPORT_ERROR = _exc

ROOT_URL = "https://msdn.itellyou.cn"

# 首页解析失败时使用的稳定顶级大类 id（来自站点，长期未变）。
FALLBACK_TOP_CATEGORIES = [
    "7ab5f0cb-7607-4bbe-9e88-50716dc43de6",  # 操作系统
    "36d3766e-0efb-491e-961b-d1a419e06c68",  # 服务器
    "051d75ee-ff53-43fe-80e9-bac5c10fc0fb",  # 应用程序
    "fcf12b78-0662-4dd4-9a82-72040db91c9e",  # 开发人员工具
    "5d6967f0-b58d-4385-8769-b886bfc2b78c",  # 设计人员工具
    "aff8a80f-2dee-4bba-80ec-611ac56d3849",  # 企业解决方案
    "23958de6-bedb-4998-825c-aa3d1e00d097",  # MSDN 技术资源库
    "95c4acfd-d1a6-41fe-b14d-a6816973d2aa",  # 工具和资源
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": ROOT_URL + "/",
    "X-Requested-With": "XMLHttpRequest",
}

MENUID_RE = re.compile(r'data-menuid="([0-9a-fA-F-]{36})"')


# --------------------------------------------------------------------------- #
# 数据模型
# --------------------------------------------------------------------------- #
@dataclass
class Record:
    """一条软件/镜像记录。"""
    category: str = ""        # 大分类, e.g. 操作系统
    subcategory: str = ""     # 小分类, e.g. Windows 10
    language: str = ""        # 语言, e.g. 中文 - 简体
    name: str = ""            # 名字
    updated: str = ""         # 更新时间
    download: str = ""        # 下载地址 (ed2k / magnet / http)
    sha1: str = ""            # SHA1
    size: str = ""            # 大小
    file_name: str = ""       # 文件名
    product_id: str = ""      # GetProduct 的 id

    # CSV 列顺序（中文表头，与社区备份格式兼容）。
    CSV_HEADER = ["大分类", "小分类", "语言", "名字", "更新时间", "下载地址", "SHA1", "大小", "文件名", "产品ID"]

    def as_row(self) -> List[str]:
        return [
            self.category, self.subcategory, self.language, self.name,
            self.updated, self.download, self.sha1, self.size,
            self.file_name, self.product_id,
        ]


# --------------------------------------------------------------------------- #
# 抓取器
# --------------------------------------------------------------------------- #
class MsdnScraper:
    def __init__(self, workers: int = 8, delay: float = 0.05,
                 fetch_detail: bool = True, timeout: int = 30):
        if requests is None:
            raise RuntimeError(
                "导入 requests / urllib3 相关依赖失败。\n"
                f"真实错误: {type(_IMPORT_ERROR).__name__}: {_IMPORT_ERROR}\n"
                "若 requests 已安装仍报此错，通常是 urllib3 版本不兼容 "
                "(requests 2.31 需要 urllib3<2)。请尝试:\n"
                '    pip install "urllib3<2" "requests>=2.31,<3"\n'
                "或直接: pip install -r requirements.txt --upgrade"
            )
        self.workers = workers
        self.delay = delay
        self.fetch_detail = fetch_detail
        self.timeout = timeout
        self.session = self._build_session()
        self._lock = threading.Lock()
        self._done = 0

    @staticmethod
    def _build_session() -> "requests.Session":
        s = requests.Session()
        s.headers.update(HEADERS)
        retry = Retry(
            total=5, connect=5, read=5, backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"]),
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        return s

    # ---- 低层请求 ---------------------------------------------------------- #
    def _post_json(self, path: str, payload: Dict[str, Any]) -> Optional[dict]:
        url = ROOT_URL + path
        try:
            r = self.session.post(url, data=payload, timeout=self.timeout)
            r.raise_for_status()
            return r.json()
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(f"[warn] POST {path} {payload} 失败: {exc}\n")
            return None

    # ---- 各接口 ------------------------------------------------------------ #
    def get_top_categories(self) -> List[str]:
        """从首页解析 8 个顶级大类 id，失败则用回退列表。"""
        try:
            r = self.session.get(ROOT_URL + "/", timeout=self.timeout)
            r.raise_for_status()
            ids = list(dict.fromkeys(MENUID_RE.findall(r.text)))
            if ids:
                return ids
            sys.stderr.write("[warn] 首页未解析到 data-menuid，使用回退大类列表。\n")
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(f"[warn] 获取首页失败 ({exc})，使用回退大类列表。\n")
        return list(FALLBACK_TOP_CATEGORIES)

    def get_index(self, cat_id: str) -> List[dict]:
        """POST /Category/Index -> 小分类列表。返回 [{id, name}, ...]。"""
        data = self._post_json("/Category/Index", {"id": cat_id})
        return parse_index(data)

    def get_langs(self, sub_id: str) -> List[dict]:
        """POST /Category/GetLang -> [{id, lang}, ...]。"""
        data = self._post_json("/Category/GetLang", {"id": sub_id})
        return parse_result_list(data)

    def get_list(self, sub_id: str, lang_id: str) -> List[dict]:
        """POST /Category/GetList -> 文件列表 [{id, name, url, post, ...}]。"""
        data = self._post_json(
            "/Category/GetList",
            {"id": sub_id, "lang": lang_id, "filter": "true"},
        )
        return parse_result_list(data)

    def get_product(self, product_id: str) -> Optional[dict]:
        """POST /Category/GetProduct -> 详情 dict。"""
        data = self._post_json("/Category/GetProduct", {"id": product_id})
        if isinstance(data, dict) and data.get("status") and isinstance(data.get("result"), dict):
            return data["result"]
        return None

    # ---- 编排 -------------------------------------------------------------- #
    def scrape(self) -> List[Record]:
        top_ids = self.get_top_categories()
        sys.stderr.write(f"[info] 顶级大类 {len(top_ids)} 个。\n")

        # 先把层级走到「文件列表」这一层，收集所有条目。
        pending: List[Record] = []
        for cat_id in top_ids:
            subs = self.get_index(cat_id)
            cat_name = _first_nonempty(_names_from(subs)) or cat_id
            for sub in subs:
                sub_id = sub.get("id", "")
                sub_name = sub.get("name", "")
                if not sub_id:
                    continue
                langs = self.get_langs(sub_id)
                for lang in langs:
                    lang_id = lang.get("id", "")
                    lang_name = lang.get("lang", "") or lang.get("name", "")
                    if not lang_id:
                        continue
                    files = self.get_list(sub_id, lang_id)
                    for f in files:
                        pending.append(
                            build_record_from_list_item(
                                f, category=cat_name, subcategory=sub_name,
                                language=lang_name,
                            )
                        )
            sys.stderr.write(f"[info] 大类 {cat_name!r} 完成，累计条目 {len(pending)}。\n")

        # 需要详情则并发抓取 GetProduct 补全 SHA1/大小/下载地址。
        if self.fetch_detail and pending:
            self._enrich_details(pending)
        return pending

    def _enrich_details(self, records: List[Record]) -> None:
        total = len(records)
        sys.stderr.write(f"[info] 抓取 {total} 条产品详情 (workers={self.workers})…\n")

        def work(rec: Record) -> None:
            if self.delay:
                time.sleep(self.delay)
            detail = self.get_product(rec.product_id) if rec.product_id else None
            if detail:
                apply_product_detail(rec, detail)
            with self._lock:
                self._done += 1
                if self._done % 100 == 0 or self._done == total:
                    sys.stderr.write(f"[info]   详情进度 {self._done}/{total}\n")

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = [pool.submit(work, r) for r in records]
            for _ in as_completed(futures):
                pass


# --------------------------------------------------------------------------- #
# 纯解析函数（可离线单测，不依赖网络）
# --------------------------------------------------------------------------- #
def parse_index(data: Any) -> List[dict]:
    """/Category/Index 的返回可能是 list，或 {status,result:[...]}。统一成 list。"""
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        if isinstance(data.get("result"), list):
            return [x for x in data["result"] if isinstance(x, dict)]
    return []


def parse_result_list(data: Any) -> List[dict]:
    """解析 {status:true, result:[...]} 结构。"""
    if isinstance(data, dict) and data.get("status") and isinstance(data.get("result"), list):
        return [x for x in data["result"] if isinstance(x, dict)]
    return []


def _epoch_to_date(post: Any) -> str:
    """把类似 '/Date(952214400000)/' 或纯毫秒时间戳转成 YYYY-MM-DD。"""
    if post is None:
        return ""
    m = re.search(r"(\d{10,})", str(post))
    if not m:
        return ""
    ts = int(m.group(1)) / 1000.0
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    except (OverflowError, OSError, ValueError):
        return ""


def build_record_from_list_item(item: dict, category: str, subcategory: str,
                                 language: str) -> Record:
    return Record(
        category=category,
        subcategory=subcategory,
        language=language,
        name=str(item.get("name", "")),
        download=str(item.get("url", "") or ""),
        updated=_epoch_to_date(item.get("post")),
        product_id=str(item.get("id", "")),
    )


def apply_product_detail(rec: Record, detail: dict) -> None:
    """用 GetProduct 的详情补全一条记录。"""
    dl = detail.get("DownLoad") or detail.get("Download") or rec.download
    rec.download = str(dl or "")
    rec.sha1 = str(detail.get("SHA1", "") or "")
    rec.size = str(detail.get("size", "") or detail.get("Size", "") or "")
    rec.file_name = str(detail.get("FileName", "") or "")
    date = detail.get("PostDateString") or _epoch_to_date(detail.get("PostDate"))
    if date:
        rec.updated = str(date)
    if not rec.name:
        rec.name = str(detail.get("Name", "") or "")


def _names_from(subs: List[dict]) -> List[str]:
    return [s.get("category", "") or s.get("cat", "") for s in subs]


def _first_nonempty(items: List[str]) -> str:
    for it in items:
        if it:
            return it
    return ""


# --------------------------------------------------------------------------- #
# 输出
# --------------------------------------------------------------------------- #
def write_csv(records: List[Record], path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.writer(fh)
        writer.writerow(Record.CSV_HEADER)
        for r in records:
            writer.writerow(r.as_row())


def write_json(records: List[Record], path: str) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump([asdict(r) for r in records], fh, ensure_ascii=False, indent=2)


# --------------------------------------------------------------------------- #
# 自测（离线，用假数据验证解析/输出逻辑）
# --------------------------------------------------------------------------- #
def _selftest() -> int:
    print("running offline self-test…")

    # parse_result_list
    lang_payload = {"status": True, "result": [
        {"id": "lang-1", "lang": "中文 - 简体"},
        {"id": "lang-2", "lang": "英语"},
    ]}
    langs = parse_result_list(lang_payload)
    assert langs == lang_payload["result"], langs
    assert parse_result_list({"status": False, "result": []}) == []
    assert parse_result_list(None) == []

    # parse_index (both shapes)
    assert parse_index([{"id": "a", "name": "Windows 10"}]) == [{"id": "a", "name": "Windows 10"}]
    assert parse_index({"result": [{"id": "b"}]}) == [{"id": "b"}]
    assert parse_index({}) == []

    # date parsing
    assert _epoch_to_date("/Date(952214400000)/") == "2000-03-05"
    assert _epoch_to_date(952214400000) == "2000-03-05"
    assert _epoch_to_date(None) == ""
    assert _epoch_to_date("garbage") == ""

    # build + enrich
    item = {
        "id": "prod-1",
        "name": "Windows 10 (multi-edition), Version 22H2 (x64) - DVD (Chinese-Simplified)",
        "url": "ed2k://|file|zh-cn_win10.iso|6000000000|ABCDEF|/",
        "post": "/Date(1664582400000)/",
    }
    rec = build_record_from_list_item(item, "操作系统", "Windows 10", "中文 - 简体")
    assert rec.product_id == "prod-1"
    assert rec.updated == "2022-10-01", rec.updated
    assert rec.download.startswith("ed2k://")

    detail = {
        "FileName": "zh-cn_win10.iso",
        "DownLoad": "ed2k://|file|zh-cn_win10.iso|6000000000|ABCDEF|/",
        "SHA1": "1234567890ABCDEF1234567890ABCDEF12345678",
        "size": "5.59GB",
        "PostDateString": "2022-10-01",
    }
    apply_product_detail(rec, detail)
    assert rec.sha1 == "1234567890ABCDEF1234567890ABCDEF12345678"
    assert rec.size == "5.59GB"
    assert rec.file_name == "zh-cn_win10.iso"

    # output round-trip
    tmp_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_selftest_tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    csv_path = os.path.join(tmp_dir, "t.csv")
    json_path = os.path.join(tmp_dir, "t.json")
    write_csv([rec], csv_path)
    write_json([rec], json_path)
    with open(csv_path, encoding="utf-8-sig") as fh:
        rows = list(csv.reader(fh))
    assert rows[0] == Record.CSV_HEADER
    assert rows[1][0] == "操作系统" and rows[1][6] == detail["SHA1"]
    with open(json_path, encoding="utf-8") as fh:
        loaded = json.load(fh)
    assert loaded[0]["sha1"] == detail["SHA1"]
    # cleanup
    os.remove(csv_path)
    os.remove(json_path)
    os.rmdir(tmp_dir)

    print("OK: all self-tests passed.")
    return 0


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="msdn.itellyou.cn 数据爬虫")
    parser.add_argument("--out", default="output", help="输出目录 (默认: output)")
    parser.add_argument("--workers", type=int, default=8, help="详情抓取并发数 (默认: 8)")
    parser.add_argument("--delay", type=float, default=0.05,
                        help="每个详情请求前的等待秒数 (默认: 0.05)")
    parser.add_argument("--no-detail", action="store_true",
                        help="跳过 GetProduct，不抓 SHA1/大小 (更快)")
    parser.add_argument("--timeout", type=int, default=30, help="请求超时秒数")
    parser.add_argument("--selftest", action="store_true",
                        help="离线自测解析逻辑后退出，不联网")
    args = parser.parse_args(argv)

    if args.selftest:
        return _selftest()

    scraper = MsdnScraper(
        workers=args.workers, delay=args.delay,
        fetch_detail=not args.no_detail, timeout=args.timeout,
    )
    started = time.time()
    records = scraper.scrape()
    elapsed = time.time() - started

    if not records:
        sys.stderr.write(
            "[error] 未抓到任何数据。请确认运行环境能访问 https://msdn.itellyou.cn/ 。\n"
        )
        return 1

    os.makedirs(args.out, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d")
    csv_path = os.path.join(args.out, f"msdn_itellyou_{stamp}.csv")
    json_path = os.path.join(args.out, f"msdn_itellyou_{stamp}.json")
    write_csv(records, csv_path)
    write_json(records, json_path)

    sys.stderr.write(
        f"[done] 共 {len(records)} 条，用时 {elapsed:.1f}s\n"
        f"        CSV : {csv_path}\n"
        f"        JSON: {json_path}\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
