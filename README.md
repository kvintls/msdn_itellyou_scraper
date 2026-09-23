# msdn.itellyou.cn 数据爬虫

抓取 [https://msdn.itellyou.cn/](https://msdn.itellyou.cn/) 上全部微软原版
软件 / 系统镜像的元数据，导出为 **CSV** 和 **JSON**。

每条记录包含：大分类、小分类、语言、名字、更新时间、**ed2k 下载地址**、**SHA1**、文件大小、文件名、产品 ID。

---

## ⚠️ 关于运行环境的重要说明

这个爬虫是在一个**无法访问外网**的沙箱里编写的，因此**我无法在这里直接把线上数据抓下来**：

- 沙箱内直连 `msdn.itellyou.cn` 失败（连接被网络策略拦截）。
- 站点对非浏览器客户端返回 `403`。

所以交付物分两部分：

1. **`msdn_scraper.py`** —— 一个完整、可直接运行的爬虫。**请在你自己能上网的电脑上运行**，即可抓到最新的完整数据。爬虫的解析与输出逻辑已通过离线自测验证（见下文）。
2. **`data/msdn_backup_snapshot.csv`** —— 一份社区历史备份快照（约 2146 条记录），来源于开源备份仓库 [hanxi/backup.msdn.itellyou.cn](https://github.com/hanxi/backup.msdn.itellyou.cn)。**它可能已过时**（不含最近几年的 Windows 11 等新版本），仅作为即时可用的参考数据。想要最新数据请运行爬虫。

---

## 安装

**务必用 `python -m pip`** —— 这样能保证把依赖装进 `python` 这个解释器
(而不是另一个同名但不同路径的 Python，否则会出现「pip 说装好了、python 却找不到」的问题)：

```bash
python -m pip install -r requirements.txt
```

> 如果 `python` 命令不存在，把上面和下面所有命令里的 `python` 换成 `python3`。
> 想确认有没有装串：`where python`（Windows）/ `which python`（Mac/Linux），
> 对比它和 `pip` 的路径是否一致。

## 使用

```bash
# 抓取全部数据，输出到 ./output/msdn_itellyou_YYYYMMDD.{csv,json}
python msdn_scraper.py

# 常用参数
python msdn_scraper.py --workers 16     # 详情抓取并发数（默认 8）
python msdn_scraper.py --delay 0.1      # 每个详情请求前的等待秒数（礼貌抓取，默认 0.05）
python msdn_scraper.py --out mydir      # 自定义输出目录
python msdn_scraper.py --no-detail      # 跳过 GetProduct，速度快但没有 SHA1 / 大小
python msdn_scraper.py --timeout 60     # 请求超时（秒）

# 离线自测（不联网，验证解析/输出逻辑）
python msdn_scraper.py --selftest
```

## 输出示例

CSV（表头，UTF-8 with BOM，Excel 可直接打开）：

```
大分类,小分类,语言,名字,更新时间,下载地址,SHA1,大小,文件名,产品ID
操作系统,Windows 10,中文 - 简体,"Windows 10 ...",2022-10-01,ed2k://|file|...,<SHA1>,5.59GB,zh-cn_win10.iso,<uuid>
```

JSON 为对象数组，字段：`category, subcategory, language, name, updated, download, sha1, size, file_name, product_id`。

---

## 工作原理

站点是单页应用（SPA），数据通过下面的 REST 接口按层级返回。所有 `POST`
请求都必须带 `Referer: https://msdn.itellyou.cn/` 头，否则被拒。

| 步骤 | 请求 | 说明 |
|------|------|------|
| 1 | `GET  /` | 从 HTML 中用 `data-menuid="..."` 解析 8 个顶级大类 id（解析失败时使用内置的稳定回退 id 列表） |
| 2 | `POST /Category/Index`  `{id}` | 某大类下的小分类（产品）列表 |
| 3 | `POST /Category/GetLang` `{id}` | `{status, result:[{id, lang}]}` 语言列表 |
| 4 | `POST /Category/GetList` `{id, lang, filter}` | `{status, result:[{id, name, url, post}]}` 文件列表 |
| 5 | `POST /Category/GetProduct` `{id}` | `{status, result:{FileName, DownLoad, SHA1, size, PostDateString}}` 详情 |

爬虫特性：会话复用 + 自动重试/退避（429/5xx）、详情请求线程池并发、
可调节的请求间隔（礼貌抓取）、进度输出、CSV + JSON 双输出。

## 合规提示

请遵守站点的使用条款与 `robots.txt`，控制并发与频率（保留默认 `--delay`
即可），仅用于个人获取微软原版镜像等正当用途。
