import os
import argparse
import pprint
import traceback
import mimetypes
from collections import namedtuple
from pathlib import Path
from typing import Optional, BinaryIO, Dict, Any

import uvicorn
from fastapi import FastAPI, Request, Response, Header
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from pydantic import BaseModel, Field
from pydantic.fields import FieldInfo
from starlette.routing import compile_path


class TagValue(BaseModel):
    id: str = Field("?")
    label: str = Field("?")


class TagValueType:
    ns = None
    name = None
    icon = "question"
    title = "Field"
    description = "a tag value"
    ContextActionHandler = None
    SearchHandler = None
    MatchHandler = None
    LoadHandler = None
    entries = None

    @classmethod
    def to_field(cls, title=None, description=None):
        return DTypeField(
            ns=cls.ns,
            name=cls.name,
            title=title or cls.title,
            description=description or cls.description,
        )

    @classmethod
    def to_context_action(cls, info=None):
        return {
            "for": {"ns": cls.ns, "name": cls.name},
            "handler": cls.ContextActionHandler,
            "info": info,
        }

    @classmethod
    def to_type_def(cls):
        r = {"icon": cls.icon}

        if cls.entries is not None:
            r["entries"] = cls.entries

        if cls.LoadHandler:
            r["loadEntriesHandlerId"] = cls.LoadHandler.__name__

        if cls.SearchHandler:
            r["searchHandlerId"] = cls.SearchHandler.__name__

        if cls.MatchHandler:
            r["matchHandlerId"] = cls.MatchHandler.__name__

        return r

    @classmethod
    def to_data_tag(cls, key, label):
        return ["tv", [cls.ns, cls.name, key, label]]


class ContextActionReq(BaseModel):
    value: Optional[TagValue] = Field(description="the selected value")
    info: Dict[str, Any] = Field(
        {}, title="Info", description="the info that was clicked"
    )

    async def handler(self, info):
        return {"name": None, "args": {}}

    class Config:
        extra = "allow"


class TagValueSearchReq(BaseModel):
    query: str = Field(
        "",
        title="Query",
        description="The substring to match",
    )

    async def handler(self, info):
        return {
            "info": None,
            "entries": [],
        }


class TagValueMatchReq(BaseModel):
    value: str = Field("", title="Value", description="The substring to search")

    async def handler(self, info):
        return {"entry": None}


class TagValueLoadReq(BaseModel):
    async def handler(self, info):
        return {"info": None, "entries": []}


class PathPattern:
    def __init__(self, path_regex, path_format, param_convertors):
        self.path_regex = path_regex
        self.path_format = path_format
        self.param_convertors = param_convertors

    @classmethod
    def from_path(cls, path):
        path_regex, path_format, param_convertors = compile_path(path)
        return cls(path_regex, path_format, param_convertors)

    def match(self, path):
        match = self.path_regex.match(path)
        if match:
            matched_params = match.groupdict()
            for key, value in matched_params.items():
                matched_params[key] = self.param_convertors[key].convert(value)

            return matched_params
        else:
            return None


class PathHandlers:
    def __init__(self, handlers):
        self.handlers = handlers

    @classmethod
    def from_dict(cls, d):
        handlers = [(PathPattern.from_path(path), fn) for (path, fn) in d.items()]
        return cls(handlers)

    def handle(self, path, *args, **kwargs):
        for path_pattern, fn in self.handlers:
            params = path_pattern.match(path)
            if params is not None:
                return path_pattern, fn(params, *args, **kwargs)

        return None, None


class DTypeField(FieldInfo):
    def __init__(
        self,
        name,
        description,
        title=None,
        ns=None,
        default="?",
        default_factory=None,
        annotation=str,
    ):
        super().__init__(
            default=default,
            default_factory=default_factory,
            title=title,
            description=description,
            annotation=annotation,
        )
        self.dtype_ns = ns
        self.dtype_name = name


def send_bytes_range_requests(
    file_obj: BinaryIO, start: int, end: int, chunk_size: int = 10_000
):
    with file_obj as f:
        f.seek(start)
        while (pos := f.tell()) <= end:
            read_size = min(chunk_size, end + 1 - pos)
            yield f.read(read_size)


def get_mime_type(file_path):
    mime_type, encoding = mimetypes.guess_type(file_path)

    if mime_type is None:
        mime_type = "application/octet-stream"

    return mime_type


def serve_static_file(
    file_path: str, request: Request, range: Optional[str] = Header(None)
):
    try:
        file_size = os.path.getsize(file_path)
    except FileNotFoundError:
        return Response(status_code=404, content="File not found")

    if range:
        # Example range header: bytes=0-100
        range_val = range.split("=")[-1]
        start_str, end_str = range_val.split("-")

        # Parse start and end bytes
        start = int(start_str) if start_str else 0
        end = int(end_str) if end_str else file_size - 1

        # Ensure range values are valid
        if start >= file_size or end >= file_size or start > end:
            return Response(
                status_code=416, headers={"Content-Range": f"bytes */{file_size}"}
            )

        content_length = end - start + 1
        content_range = f"bytes {start}-{end}/{file_size}"
        content_type = get_mime_type(file_path)
        headers = {
            "Content-Type": content_type,
            "Content-Length": str(content_length),
            "Content-Range": content_range,
            "Accept-Ranges": "bytes",
        }

        print("serving range", headers, file_path)

        return StreamingResponse(
            send_bytes_range_requests(open(file_path, mode="rb"), start, end),
            status_code=206,
            headers=headers,
        )

    # Serve the entire file if no range is specified
    return FileResponse(file_path, headers={"Accept-Ranges": "bytes"})


def parse_int_or(v, default=0):
    try:
        float_value = float(v)
        return int(round(float_value))
    except ValueError:
        return default


def get_schema_fields(model):
    json_schema = model.schema()
    props = json_schema.get("properties", {})
    return props


def fn_from_model(model, ns):
    fields = get_schema_fields(model)
    Info = model.Info
    title = Info.title

    ui_prefix = getattr(Info, "ui_prefix", title)
    ui_args = {}

    for key, field in model.model_fields.items():
        if isinstance(field, DTypeField):
            ui_args[key] = {
                "from": key,
                "prefix": field.title or key,
                "dtypeNs": field.dtype_ns or ns,
                "dtypeName": field.dtype_name,
            }
        else:
            ui_args[key] = field.title or key

    context_actions_0 = getattr(Info, "context_actions", [])

    context_actions = []
    for action_0 in context_actions_0:
        action = dict(**action_0)
        handler = action.get("handler")
        if not isinstance(handler, str):
            action["handler"] = handler_to_name(handler)

        context_actions.append(action)

    schema = {
        "title": title,
        "schema": {"fields": fields},
        "ui": {
            "prefix": ui_prefix,
            "args": ui_args,
            "manualUpdate": getattr(Info, "manual_update", False),
        },
        "examples": getattr(Info, "examples", [f"Show {title}"]),
        "contextActions": context_actions,
    }

    return schema


def res_error(code, reason, info=None):
    return {"ok": False, "code": code, "reason": reason, "info": info}


def handler_to_name(item):
    if isinstance(item, str):
        return item
    else:
        return getattr(item, "__name__")


def handlers_to_names(items):
    if items:
        return [handler_to_name(item) for item in items]

    return items


def tag_values_to_data(d):
    if not d:
        return None

    r = {}

    for cls in d:
        r[cls.name] = cls.to_type_def()

    return r


def get_handlers_from_tag_values(items):
    r = []

    if not items:
        return r

    for cls in items:
        search_handler = cls.SearchHandler
        if search_handler:
            r.append(search_handler)

        match_handler = cls.MatchHandler
        if match_handler:
            r.append(match_handler)

        handler = cls.LoadHandler
        if handler:
            r.append(handler)

    return r


def get_handlers_from_functions(items):
    r = []

    if not items:
        return r

    for model in items:
        r.append(model)
        Info = getattr(model, "Info")
        if not Info:
            continue

        for action in getattr(Info, "context_actions", []):
            handler = action.get("handler")
            if handler:
                r.append(handler)

    return r


def default_before_parse_obj(model_class, args):
    Info = getattr(model_class, "Info", None)
    if Info:
        default_args = getattr(Info, "default_args", {})

        for key, val in default_args.items():
            if args.get(key) is None:
                args[key] = val if not callable(val) else val()

    return args


def fill_model_defaults(model_class, args):
    fn = getattr(model_class, "before_parse_obj", default_before_parse_obj)
    return fn(model_class, args)


class Handlers:
    def __init__(self):
        self.by_name = {}

    def add_from_dict(self, d):
        for name, handler in d.items():
            self.add_handler_with_name(name, handler)

    def add_from_list(self, lst):
        for handler in lst:
            self.add_handler(handler)

    def add_handler_with_name(self, name, handler):
        if name in self.by_name and self.by_name[name] is not handler:
            print("overriding handler", name, handler)
        else:
            self.by_name[name] = handler

    def add_handler(self, handler):
        self.add_handler_with_name(handler_to_name(handler), handler)

    async def handle(self, op_name, raw_args, info):
        model_class = self.by_name.get(op_name)
        if model_class:
            model_handler = model_class.handler

            try:
                print("BEFORE", raw_args)
                new_raw_args = fill_model_defaults(model_class, raw_args)
                print("AFTER", new_raw_args)
                model_instance = model_class.parse_obj(new_raw_args)
                return await model_handler(model_instance, info)
            except Exception as err:
                print(err)
                print(traceback.format_exc())
                print(
                    "bad args format for model",
                    model_class.__name__,
                    raw_args,
                    model_class,
                )
                res = {"ok": False, "reason": "BadFormat"}
                return return_json(res)
        else:
            return res_error(
                "OpNameNotFound", "Op Name Not Found", {"op_name": op_name}
            )


def get_handlers_from_init_info(data):
    handlers = Handlers()
    tag_values = data.get("tagValues")
    functions = data.get("tools")

    handlers.add_from_list(get_handlers_from_tag_values(tag_values))
    handlers.add_from_list(get_handlers_from_functions(functions))
    handlers.add_from_list(data.get("handlers", []))

    return handlers


def init_info_to_data(data):
    r = dict(**data)

    tag_values = r.get("tagValues")
    handlers = r.get("handlers")
    functions = r.get("tools")
    ns = r.get("ns", None)

    if tag_values:
        r["tagValues"] = tag_values_to_data(tag_values)

    if handlers:
        r["handlers"] = [handler_to_name(handler) for handler in handlers]

    if functions:
        r["tools"] = {handler_to_name(fn): fn_from_model(fn, ns) for fn in functions}

    return r


def return_json(res):
    try:
        return JSONResponse(content=jsonable_encoder(res), status_code=200)
    except Exception as err:
        print("Error encoding response", err, res)
        return JSONResponse(content=jsonable_encoder({}), status_code=500)


def maybe_dict_to_named_tuple(v, name):
    if isinstance(v, dict):
        keys = list(v.keys())
        C = namedtuple(name, keys)
        return C(**v)
    else:
        return v


def make_server(info, serve_resource=None):
    state = maybe_dict_to_named_tuple(info.get("state"), "HandlerState")
    init_info = init_info_to_data(info)
    handlers = get_handlers_from_init_info(info)

    handler_name_set = set(handlers.by_name.keys()).union(
        set(init_info.get("handlers", []))
    )

    if handler_name_set:
        init_info["handlers"] = sorted(list(handler_name_set))

    app = FastAPI()

    @app.post("/")
    async def root_post(request: Request):
        body = await request.json()

        if not isinstance(body, dict):
            return res_error("BadRequestBody", "Bad Request Body")

        action = body.get("action", None)
        if action == "info":
            return {
                "title": init_info.get("title"),
                "ns": init_info.get("ns"),
                "dynEnums": init_info.get("dynEnums"),
                "tagValues": init_info.get("tagValues"),
                "tools": init_info.get("tools"),
                "handlers": init_info.get("handlers"),
            }
        elif action == "request":
            op_name = body.get("opName", None)
            req_info = body.get("info", {})

            if op_name:
                pprint.pprint(body)
                res = await handlers.handle(op_name, req_info, state)
                return return_json(res)
            else:
                return res_error("NoOpName", "No Op Name", {"op_name": op_name})
        else:
            return res_error("UnknownAction", "Unknown Action", {"action": action})

    @app.get("/resource/{resource_path:path}")
    async def resource_get(request: Request):
        if serve_resource:
            res = await serve_resource(request)
            if res:
                return res

        return Response(status_code=404, content="Not Found")

    return app


def join_safe_base_path(base_dir, rest):
    base_path = Path(base_dir).resolve()
    final_path = (base_path / rest).resolve()

    assert str(final_path).startswith(str(base_path)), f"unsafe path for {rest}"
    return final_path


def make_base_cli_parser(description="Start the server with specified host and port"):
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Hostname or IP address to bind the server to. Default is '127.0.0.1'.",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=8888,
        help="Port number to bind the server to. Default is 8888.",
    )

    return parser


def make_run_info(ns, title, state=None, tools=None, handlers=None, tag_values=None):
    return {
        "ns": ns,
        "title": title,
        "state": state,
        "tools": tools if tools is not None else [],
        "handlers": handlers if handlers is not None else [],
        "tagValues": tag_values if tag_values is not None else [],
    }


def table_col_info(id, label, visible=True):
    return dict(id=id, label=label, visible=visible)


def run(info, serve_resource=None, host="127.0.0.1", port=8888):
    print(f"Starting server on {host}:{port}")
    app = make_server(info, serve_resource)
    uvicorn.run(app, host=host, port=port)
