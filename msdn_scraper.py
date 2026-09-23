#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
msdn.itellyou.cn 数据爬虫 / scraper.

抓取 https://msdn.itellyou.cn/ 上全部微软原版软件/系统镜像的元数据，
包括分类、语言、文件名、发布时间、ed2k 下载地址、SHA1、文件大小，
并导出为 CSV 和 JSON。

站点是一个 SPA，数据通过下面的 REST 接口按层级返回。改版后：
  * 接口前缀是 /Index/（旧版是 /Category/，现已 404）；
  * 每个 POST 都需要 CSRF token：先 GET 首页，从 HTML 里取 data-token，
    再作为 X-CSRF-TOKEN 请求头发送（同时带上首页返回的 Cookie）；
  * 详情字段名是小写（filename / download / sha1 / size）。

    GET  /                          -> 首页 HTML，含 data-token=<csrf>
    POST /Index/GetCategory {id}    -> 某顶级大类下的分类(产品)列表: [{id, name}, ...]
    POST /Index/GetLang     {id}    -> {result:[{id, lang}, ...]}   语言列表
    POST /Index/GetList {id, lang, filter}
                                    -> {result:[{id, name, ...}, ...]} 文件列表
    POST /Index/GetProduct  {id}    -> {result:{filename, download, sha1, size, ...}}

用法:
    python -m pip install -r requirements.txt
    python msdn_scraper.py                    # 抓取全部，输出到 ./output/
    python msdn_scraper.py --workers 16       # 提高并发（默认 8）
    python msdn_scraper.py --delay 0.1        # 每个详情请求之间的间隔（秒）
    python msdn_scraper.py --out mydir        # 自定义输出目录
    python msdn_scraper.py --no-detail        # 跳过 GetProduct（更快，但无 SHA1/大小）
    python msdn_scraper.py --selftest         # 用内置的假数据离线自测解析逻辑
    python msdn_scraper.py --debug            # 打印每个请求的原始响应片段，便于排查接口变更

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
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

_IMPORT_ERROR: Optional[BaseException] = None
try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except Exception as _exc:  # noqa: BLE001 - capture the REAL reason, don't hide it
    requests = None  # type: ignore
    _IMPORT_ERROR = _exc

ROOT_URL = "https://msdn.itellyou.cn"

# 改版后的接口（/Index/ 前缀）。
EP_CATEGORY = "/Index/GetCategory"
EP_LANG = "/Index/GetLang"
EP_LIST = "/Index/GetList"
EP_PRODUCT = "/Index/GetProduct"

# 8 个顶级大类（id 长期稳定）。GetCategory 以这些 id 为入口，返回其下的分类列表。
TOP_CATEGORIES: List[Tuple[str, str]] = [
    ("7ab5f0cb-7607-4bbe-9e88-50716dc43de6", "操作系统"),
    ("36d3766e-0efb-491e-961b-d1a419e06c68", "服务器"),
    ("051d75ee-ff53-43fe-80e9-bac5c10fc0fb", "应用程序"),
    ("fcf12b78-0662-4dd4-9a82-72040db91c9e", "开发人员工具"),
    ("5d6967f0-b58d-4385-8769-b886bfc2b78c", "设计人员工具"),
    ("aff8a80f-2dee-4bba-80ec-611ac56d3849", "企业解决方案"),
    ("23958de6-bedb-4998-825c-aa3d1e00d097", "MSDN 技术资源库"),
    ("95c4acfd-d1a6-41fe-b14d-a6816973d2aa", "工具和资源"),
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": ROOT_URL + "/",
    "X-Requested-With": "XMLHttpRequest",
}

# 首页里的 CSRF token，形如 data-token=xxxx 或 data-token="xxxx"。
TOKEN_RE = re.compile(r'data-token=["\']?([\w-]+)')


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
                 fetch_detail: bool = True, timeout: int = 30, debug: bool = False):
        if requests is None:
            raise RuntimeError(
                "导入 requests / urllib3 相关依赖失败。\n"
                f"真实错误: {type(_IMPORT_ERROR).__name__}: {_IMPORT_ERROR}\n"
                "请用运行脚本的同一个解释器安装依赖:\n"
                "    python -m pip install -r requirements.txt\n"
                "若 requests 已装仍报错，通常是版本不兼容，可尝试:\n"
                '    python -m pip install "urllib3<2" "charset-normalizer<3.4" "requests>=2.28"'
            )
        self.workers = workers
        self.delay = delay
        self.fetch_detail = fetch_detail
        self.timeout = timeout
        self.debug = debug
        self.session = self._build_session()
        self._lock = threading.Lock()
        self._done = 0
        self._init_token()

    @staticmethod
    def _build_session() -> "requests.Session":
        s = requests.Session()
        s.headers.update(HEADERS)
        retry = Retry(
            total=6, connect=6, read=6, backoff_factor=1.0,
            # 含 Cloudflare 52x（522=源站连接超时，站点繁忙时常见）。
            status_forcelist=(429, 500, 502, 503, 504, 520, 521, 522, 523, 524),
            allowed_methods=frozenset(["GET", "POST"]),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        return s

    def _init_token(self) -> None:
        """GET 首页拿 Cookie，并从 HTML 里解析 CSRF token 写入请求头。"""
        try:
            r = self.session.get(ROOT_URL + "/", timeout=self.timeout)
            r.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                f"访问首页失败: {exc}\n请确认本机能打开 https://msdn.itellyou.cn/ 。"
            ) from exc
        m = TOKEN_RE.search(r.text)
        if not m:
            raise RuntimeError(
                "未能从首页解析出 CSRF token (data-token)。\n"
                "站点结构可能又变了。请用 --debug 运行，或在浏览器 F12 → Network 里\n"
                "查看首页 HTML 中的 token 字段名，然后把它发给我以便更新脚本。"
            )
        token = m.group(1)
        self.session.headers["X-CSRF-TOKEN"] = token
        if self.debug:
            sys.stderr.write(f"[debug] CSRF token = {token}\n")

    # ---- 低层请求 ---------------------------------------------------------- #
    def _post_json(self, path: str, payload: Dict[str, Any]) -> Optional[Any]:
        url = ROOT_URL + path
        if self.debug:
            sys.stderr.write(f"[debug] -> POST {path} {payload}\n")
            sys.stderr.flush()
        t0 = time.time()
        try:
            r = self.session.post(url, data=payload, timeout=self.timeout)
            r.raise_for_status()
            data = r.json()
            if self.debug:
                dt = time.time() - t0
                snippet = json.dumps(data, ensure_ascii=False)[:300]
                sys.stderr.write(f"[debug]    <- {r.status_code} ({dt:.1f}s) {snippet}\n")
            return data
        except Exception as exc:  # noqa: BLE001
            dt = time.time() - t0
            sys.stderr.write(f"[warn] POST {path} {payload} 失败 ({dt:.1f}s): {exc}\n")
            return None

    # ---- 各接口 ------------------------------------------------------------ #
    def get_categories(self, base_id: str) -> List[dict]:
        """POST /Index/GetCategory -> 该顶级大类下的分类列表 [{id, name}, ...]。"""
        return parse_category_list(self._post_json(EP_CATEGORY, {"id": base_id}))

    def get_langs(self, cat_id: str) -> List[dict]:
        """POST /Index/GetLang -> [{id, lang}, ...]。"""
        return parse_result_list(self._post_json(EP_LANG, {"id": cat_id}))

    def get_list(self, cat_id: str, lang_id: str) -> List[dict]:
        """POST /Index/GetList -> 文件列表 [{id, name, ...}]。"""
        data = self._post_json(EP_LIST, {"id": cat_id, "lang": lang_id, "filter": "true"})
        return parse_result_list(data)

    def get_product(self, product_id: str) -> Optional[dict]:
        """POST /Index/GetProduct -> 详情 dict。"""
        data = self._post_json(EP_PRODUCT, {"id": product_id})
        if isinstance(data, dict) and isinstance(data.get("result"), dict):
            return data["result"]
        return None

    # ---- 编排 -------------------------------------------------------------- #
    def scrape(self) -> List[Record]:
        sys.stderr.write(f"[info] 顶级大类 {len(TOP_CATEGORIES)} 个。\n")
        pending: List[Record] = []
        for base_id, base_name in TOP_CATEGORIES:
            cats = self.get_categories(base_id)
            sys.stderr.write(f"[info] 大类 {base_name!r}: {len(cats)} 个子分类。\n")
            sys.stderr.flush()
            for idx, cat in enumerate(cats, 1):
                sub_id = str(cat.get("id", ""))
                sub_name = str(cat.get("name", ""))
                if not sub_id:
                    continue
                before = len(pending)
                for lang in self.get_langs(sub_id):
                    lang_id = str(lang.get("id", ""))
                    lang_name = str(lang.get("lang", "") or lang.get("name", ""))
                    if not lang_id:
                        continue
                    for f in self.get_list(sub_id, lang_id):
                        pending.append(
                            build_record_from_list_item(
                                f, category=base_name, subcategory=sub_name,
                                language=lang_name,
                            )
                        )
                sys.stderr.write(
                    f"[info]   [{base_name} {idx}/{len(cats)}] {sub_name} "
                    f"(+{len(pending) - before} 条，累计 {len(pending)})\n"
                )
                sys.stderr.flush()
            sys.stderr.write(f"[info] 大类 {base_name!r} 完成，累计条目 {len(pending)}。\n")

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
def parse_category_list(data: Any) -> List[dict]:
    """GetCategory 可能返回裸 list，或 {result:[...]}。统一成 list。"""
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict) and isinstance(data.get("result"), list):
        return [x for x in data["result"] if isinstance(x, dict)]
    return []


def parse_result_list(data: Any) -> List[dict]:
    """解析 {result:[...]} 结构（新版可能没有 status 字段，故不强制要求）。"""
    if isinstance(data, dict) and isinstance(data.get("result"), list):
        return [x for x in data["result"] if isinstance(x, dict)]
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
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


def _pick(d: dict, *keys: str) -> str:
    """按顺序取第一个非空字段（大小写字段名兼容）。"""
    for k in keys:
        v = d.get(k)
        if v:
            return str(v)
    return ""


def build_record_from_list_item(item: dict, category: str, subcategory: str,
                                 language: str) -> Record:
    updated = _pick(item, "PostDateString", "updatetime", "time")
    if not updated:
        updated = _epoch_to_date(item.get("post") or item.get("PostDate"))
    return Record(
        category=category,
        subcategory=subcategory,
        language=language,
        name=_pick(item, "name", "Name"),
        download=_pick(item, "download", "url", "DownLoad", "Download"),
        updated=updated,
        product_id=_pick(item, "id", "Id", "ID"),
    )


def apply_product_detail(rec: Record, detail: dict) -> None:
    """用 GetProduct 的详情补全一条记录（新版字段为小写）。"""
    dl = _pick(detail, "download", "DownLoad", "Download")
    if dl:
        rec.download = dl
    rec.sha1 = _pick(detail, "sha1", "SHA1") or rec.sha1
    rec.size = _pick(detail, "size", "Size") or rec.size
    rec.file_name = _pick(detail, "filename", "FileName") or rec.file_name
    date = _pick(detail, "PostDateString", "updatetime", "time")
    if not date:
        date = _epoch_to_date(detail.get("post") or detail.get("PostDate"))
    if date:
        rec.updated = date
    if not rec.name:
        rec.name = _pick(detail, "name", "Name")


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

    # CSRF token 正则：兼容带引号/不带引号。
    assert TOKEN_RE.search('foo data-token=abc-123_XY bar').group(1) == "abc-123_XY"
    assert TOKEN_RE.search('<div data-token="tok-9">').group(1) == "tok-9"

    # GetCategory: 裸 list 与 {result:[...]} 两种形态。
    assert parse_category_list([{"id": "a", "name": "Windows 10"}]) == [{"id": "a", "name": "Windows 10"}]
    assert parse_category_list({"result": [{"id": "b"}]}) == [{"id": "b"}]
    assert parse_category_list({}) == []

    # GetLang / GetList: 有无 status 都要能解析。
    assert parse_result_list({"result": [{"id": "l1", "lang": "中文 - 简体"}]}) == [{"id": "l1", "lang": "中文 - 简体"}]
    assert parse_result_list({"status": True, "result": [{"id": "x"}]}) == [{"id": "x"}]
    assert parse_result_list(None) == []

    # 时间戳解析
    assert _epoch_to_date("/Date(952214400000)/") == "2000-03-05"
    assert _epoch_to_date(952214400000) == "2000-03-05"
    assert _epoch_to_date(None) == ""

    # 列表项 -> 记录
    item = {"id": "prod-1", "name": "Windows 10 22H2 (x64) (Chinese-Simplified)"}
    rec = build_record_from_list_item(item, "操作系统", "Windows 10", "中文 - 简体")
    assert rec.product_id == "prod-1"
    assert rec.category == "操作系统" and rec.subcategory == "Windows 10"

    # 详情补全（新版小写字段）
    detail = {
        "filename": "zh-cn_win10.iso",
        "download": "ed2k://|file|zh-cn_win10.iso|6000000000|ABCDEF|/",
        "sha1": "1234567890ABCDEF1234567890ABCDEF12345678",
        "size": "5.59GB",
    }
    apply_product_detail(rec, detail)
    assert rec.sha1 == detail["sha1"]
    assert rec.size == "5.59GB"
    assert rec.file_name == "zh-cn_win10.iso"
    assert rec.download.startswith("ed2k://")

    # 详情补全（旧版大写字段也要兼容）
    rec2 = build_record_from_list_item({"id": "p2", "name": "X"}, "服务器", "SBS", "英语")
    apply_product_detail(rec2, {"FileName": "x.iso", "DownLoad": "ed2k://x", "SHA1": "AABB", "size": "1GB"})
    assert rec2.file_name == "x.iso" and rec2.sha1 == "AABB"

    # 输出 round-trip
    tmp_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_selftest_tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    csv_path = os.path.join(tmp_dir, "t.csv")
    json_path = os.path.join(tmp_dir, "t.json")
    write_csv([rec], csv_path)
    write_json([rec], json_path)
    with open(csv_path, encoding="utf-8-sig") as fh:
        rows = list(csv.reader(fh))
    assert rows[0] == Record.CSV_HEADER
    assert rows[1][0] == "操作系统" and rows[1][6] == detail["sha1"]
    with open(json_path, encoding="utf-8") as fh:
        loaded = json.load(fh)
    assert loaded[0]["sha1"] == detail["sha1"]
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
    parser.add_argument("--debug", action="store_true", help="打印每个请求的原始响应片段")
    parser.add_argument("--selftest", action="store_true",
                        help="离线自测解析逻辑后退出，不联网")
    args = parser.parse_args(argv)

    if args.selftest:
        return _selftest()

    scraper = MsdnScraper(
        workers=args.workers, delay=args.delay,
        fetch_detail=not args.no_detail, timeout=args.timeout, debug=args.debug,
    )
    started = time.time()
    records = scraper.scrape()
    elapsed = time.time() - started

    if not records:
        sys.stderr.write(
            "[error] 未抓到任何数据。用 --debug 重跑查看接口返回，"
            "若接口又变了请把 --debug 输出发我。\n"
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
