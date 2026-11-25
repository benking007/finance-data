"""Human-readable reference metadata for the application's read API."""

from __future__ import annotations

from urllib.parse import quote


API_TITLE = "金融数据库 API"
API_VERSION = "1.0"
API_KEY_HEADER = "X-API-Key"
MAX_QUERY_YEARS = 25


def dataset_paths(directory: dict) -> dict[str, list[str]]:
    """Map each registered dataset to its place in the official catalog."""
    paths = {}
    for entry in directory["entries"]:
        for dataset_id in entry.get("profile_ids", ()):
            paths.setdefault(dataset_id, entry["path"])
    return paths


def dataset_entry(profile: dict, path: list[str] | None, *, columns=None, csv_allowed=False, show_source=False) -> dict:
    def dimension(name):
        field = profile.get(name)
        return field if columns is None or field in columns else None

    result = {
        "id": profile["id"],
        "title": profile["title"],
        "category": profile["category"],
        "path": path or [profile["category"], profile["title"]],
        "description": profile.get("description") or "",
        "mode": profile.get("mode"),
        "code_field": dimension("code_field"),
        "date_field": dimension("date_field"),
        "can_export": csv_allowed,
    }
    source = profile.get("doc_url") or profile.get("source_url")
    if show_source and source:
        result["source_url"] = source
    return result


def _parameter(name, description, *, kind="string", location="query", required=False, **constraints):
    return {"name": name, "location": location, "type": kind, "required": required, "description": description, **constraints}


def _field(name, kind, description):
    return {"name": name, "type": kind, "description": description}


def _operation(id, title, path, description, parameters, fields, example, **extra):
    return {"id": id, "title": title, "method": "GET", "path": path, "description": description,
            "parameters": parameters, "response_fields": fields, "response_example": example, **extra}


def _generic_dataset_parameter(example):
    return _parameter("dataset_id", "已授权的数据集 ID，可从数据集列表获取。", location="path", required=True, example=example)


def _dataset_url(dataset):
    dataset_id = quote(dataset["id"], safe="") if dataset else "{dataset_id}"
    return f"/v1/datasets/{dataset_id}"


def _coverage_fields(*, local):
    fields = [
        _field("id", "string", "数据集 ID。"),
        _field("title", "string", "数据集名称。"),
        _field("category", "string", "所属一级分类。"),
        _field("description", "string", "数据范围或含义。"),
        _field("mode", "string", "采集方式，例如 history、snapshot 或 file。"),
        _field("history_start", "string / null", "已知的上游历史起点。"),
        _field("code_field", "string / null", "可按 code 筛选的字段。"),
        _field("date_field", "string / null", "可按 start 和 end 筛选的字段。"),
        _field("row_count", "integer", "当前目标库或发布库的记录数。"),
        _field("queryable", "boolean", "数据集是否提供记录查询。"),
        _field("min_date", "string / null", "已入库记录的最早日期。"),
        _field("max_date", "string / null", "已入库记录的最新日期。"),
        _field("last_sync", "string / null", "最近一次成功同步的时间。"),
        _field("can_export", "boolean", "当前密钥是否具有 CSV 权限。"),
        _field("complete_partitions", "integer", "已完成的分片数。"),
        _field("pending_partitions", "integer", "待处理的分片数。"),
    ]
    if local:
        fields.append(_field("source_url", "string / null", "上游来源文档。"))
    return fields


def _dataset_example(dataset, csv_allowed):
    return {"id": dataset["id"], "title": dataset["title"], "category": dataset["category"],
            "code_field": dataset.get("code_field"), "date_field": dataset.get("date_field"),
            "row_count": 0, "can_export": csv_allowed}


def _definition_operation(dataset, preferred, fields=(), *, local, csv_allowed):
    entry = dataset or preferred
    example = _dataset_example(entry, csv_allowed) | {"fields": list(fields[:3]), "key": []}
    coverage = _coverage_fields(local=local)
    coverage.extend((
        _field("fields", "array<object>", "当前可读取字段的名称、类型、单位和说明。"),
        _field("key", "array<string>", "可见的业务键字段，可能为空。"),
    ))
    return _operation("definition", "数据集定义", _dataset_url(dataset),
                      "读取一个数据集的字段字典和入库覆盖范围。", [_generic_dataset_parameter(entry["id"])], coverage, example)


def _rows_example(entry, fields):
    example = {}
    for field in fields:
        name, kind = field["name"], (field.get("type") or "").lower()
        if name == entry.get("date_field"):
            example[name] = "20261124"
        elif entry["id"] == "daily" and name == entry.get("code_field"):
            example[name] = "600000.SH"
        elif kind.startswith(("float", "double", "decimal")):
            example[name] = "10.2500"
        elif kind.startswith("int"):
            example[name] = 100
        else:
            example[name] = None
    rows = [example] if example else []
    return {"dataset_id": entry["id"], "rows": rows, "fields": list(example), "next_cursor": None, "page_count": len(rows)}


def _rows_operation(dataset, preferred, fields=(), *, csv_allowed, code_supported=True, date_supported=True):
    entry = dataset or preferred
    params = [_generic_dataset_parameter(entry["id"])]
    show_code = bool(dataset.get("code_field")) if dataset else code_supported
    show_date = bool(dataset.get("date_field")) if dataset else date_supported
    if show_code:
        scope = f"按 {dataset['code_field']} 精确匹配。" if dataset else "按数据集的 code_field 精确匹配。仅支持定义了代码字段的数据集。"
        params.append(_parameter("code", f"{scope} 最长 40 字符，可用字母、数字、下划线、点和连字符。",
                                 example="600000.SH" if entry["id"] == "daily" else None))
    if show_date:
        scope = f"按 {dataset['date_field']} 筛选。" if dataset else "仅支持定义了日期字段的数据集。"
        formats = "支持 YYYY-MM-DD、YYYYMMDD、YYYYMM 和 YYYYQn，月和季度按首日解析。"
        params.extend((
            _parameter("start", f"含起点。{scope}{formats}", example="2026-11-01"),
            _parameter("end", f"含终点。{scope}{formats} 两端同时填写时跨度上限为 {MAX_QUERY_YEARS} 年。", example="2026-11-25"),
        ))
    params.extend((
        _parameter("fields", "以逗号分隔的字段名，最长 4000 字符。字段须在当前数据集的字段字典中，省略时返回可见字段。", example=",".join(item["name"] for item in fields[:3]) or None),
        _parameter("cursor", "把上一页返回的 next_cursor 原样传入，并保持数据集、代码和日期筛选一致。首屏省略，最长 1000 字符。"),
        _parameter("limit", "每页记录数，范围 1 至 1000。", kind="integer", default=100, example=5),
        _parameter("format", "json 返回分页对象，csv 下载本页并要求密钥具有 CSV 权限。分页游标通过 JSON 响应获取。" if csv_allowed else "当前访问权限提供 JSON 查询。",
                   default="json", options=["json", "csv"] if csv_allowed else ["json"]),
    ))
    columns = [item["name"] for item in fields[:3]]
    example = _rows_example(entry, fields[:3])
    example_query = {"limit": "5"}
    if entry["id"] == "daily" and dataset:
        if dataset.get("code_field"):
            example_query["code"] = "600000.SH"
        if dataset.get("date_field"):
            example_query.update(start="2026-11-01", end="2026-11-25")
    if columns:
        example_query["fields"] = ",".join(columns)
    return _operation("rows", "读取数据记录", _dataset_url(dataset) + "/rows",
                      "查询当前数据库内的记录。有日期的记录按日期倒序排列，保留精度的十进制值可能以 JSON 字符串返回。",
                      params, [
                          _field("dataset_id", "string", "数据集 ID。"),
                          _field("rows", "array<object>", "本页记录。字段见数据集的字段字典。"),
                          _field("fields", "array<string>", "本次查询的字段列表。"),
                          _field("next_cursor", "string / null", "下一页的游标。null 表示已到末页。"),
                          _field("page_count", "integer", "本页实际记录数。"),
                      ], example, example_query=example_query)


def _partitions_operation(dataset, preferred):
    path = _dataset_url(dataset) + "/partitions"
    return _operation("partitions", "读取采集分片", path, "管理员可以查看本地最近的采集分片及其写入状态。",
                      [_generic_dataset_parameter((dataset or preferred)["id"]),
                       _parameter("limit", "最多返回 1 至 100 条最新分片。", kind="integer", default=50)],
                      [_field("partitions", "array<object>", "分片列表，包含 partition_id、params、status、source_count、row_count 和记录时间。")],
                      {"partitions": []})


def _source_catalog_operation():
    return _operation("source-catalog", "来源目录与覆盖", "/v1/source-catalog",
                      "管理员可以查看 Tushare 目录、层级、采集适配状态和分类覆盖。", [],
                      [_field("entries", "array<object>", "官方条目，包含名称、接口名、分类路径和对应数据集。"),
                       _field("tree", "array<object>", "按一级和子分类分组的目录树。"),
                       _field("coverage", "object", "各分类的条目数和采集覆盖。")],
                      {"entries": [], "tree": [], "coverage": {}})


def reference_index(datasets, *, auth_required, rate_limit, local, admin, csv_allowed):
    preferred = next((entry for entry in datasets if entry["id"] == "daily"), None)
    preferred = preferred or next(iter(datasets), {"id": "dataset_id", "title": "数据集", "category": "分类"})
    catalog = _operation("datasets", "列出数据集", "/v1/datasets", "列出当前访问权限内的数据集、字段维度和入库覆盖。", [],
                         [_field("datasets", "array<object>", "数据集定义摘要与覆盖范围。")], {"datasets": [_dataset_example(preferred, csv_allowed)]})
    groups = [
        {"id": "catalog", "title": "目录与定义", "description": "查找数据集 ID、字段和覆盖范围。",
         "operations": [catalog, _definition_operation(None, preferred, local=local, csv_allowed=csv_allowed)]},
        {"id": "query", "title": "数据查询", "description": "按字段、时间范围和游标读取入库记录。",
         "operations": [_rows_operation(None, preferred, csv_allowed=csv_allowed,
                                        code_supported=any(entry.get("code_field") for entry in datasets),
                                        date_supported=any(entry.get("date_field") for entry in datasets))]},
    ]
    if admin:
        groups.append({"id": "management", "title": "本地管理", "description": "查看上游目录与本地采集状态。",
                       "operations": [_source_catalog_operation(), _partitions_operation(None, preferred)]})
    errors = [
        {"status": 401, "description": "缺少有效的查询 API 密钥，或提供的密钥无效。"},
        {"status": 403, "description": "当前密钥没有对应的管理或 CSV 权限，或请求超出本地访问范围。"
         if local else "当前密钥没有 CSV 权限。"},
        {"status": 404, "description": "数据集不存在，或当前密钥未获授权。"},
        {"status": 422, "description": "查询参数、日期范围、字段或分页游标无效。"},
        {"status": 503, "description": "目标数据库尚未连接，或暂时无法查询。"},
    ]
    if rate_limit is not None:
        errors.insert(-1, {"status": 429, "description": "达到当前密钥的分钟请求上限，等待 Retry-After 指定的秒数后重试。"})
    return {
        "title": API_TITLE,
        "version": API_VERSION,
        "authentication": {"required": auth_required, "header": API_KEY_HEADER,
                           "description": "通过请求头传入本服务的查询 API 密钥。" if auth_required else "本机访问已开放，无需查询密钥。"},
        "limits": {"requests_per_minute": rate_limit},
        "groups": groups,
        "datasets": datasets,
        "errors": errors,
    }


def reference_dataset(dataset, fields, *, local, admin):
    operations = [
        _definition_operation(dataset, dataset, fields, local=local, csv_allowed=dataset["can_export"]),
        _rows_operation(dataset, dataset, fields, csv_allowed=dataset["can_export"]),
    ]
    if admin:
        operations.append(_partitions_operation(dataset, dataset))
    return dataset | {"fields": fields, "operations": operations}
