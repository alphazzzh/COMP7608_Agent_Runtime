"""读取严格的服务端 TOML 配置，不启动服务或接入外部适配器。

配置只在本地解析与校验：密钥必须显式给出，或从指定环境变量读取。
所有对外配置错误使用固定文字，避免 TOML、Pydantic 或系统异常回显凭据。
适配器 options 保留 JSON 值，后续每次运行再独立复制并进入资源上下文。
"""

from __future__ import annotations

import os
import re
import tomllib
from pathlib import Path
from typing import Annotated
from urllib.parse import urlsplit

from pydantic import Field, SecretStr, ValidationError, field_validator, model_validator

from . import models as m
from .adapters.planner import _ALLOWED_PARAMETERS, JsonPlanner

_Identifier = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")]
_Label = Annotated[str, Field(min_length=1, max_length=200)]
_FACTORY = re.compile(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*\Z", re.ASCII)
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
# 仅用于安全的迁移诊断，不把退役字段重新加入 RunConfig，也不静默忽略它们。
# 名称由代码固定，不能把任意用户键名或配置值拼入错误信息。
_REMOVED_RUN_FIELDS = frozenset({"max_feedback_rounds", "fixed_extract_max_chars"})


class ConfigError(ValueError):
    """不含用户配置值的启动或适配器接入错误。"""


class LLMSettings(m.Model):
    """所有真实模型模块共享的连接设置；密钥序列化时自动遮盖。"""

    base_url: Annotated[str, Field(min_length=1)]
    api_key: SecretStr
    model_id: Annotated[str, Field(min_length=1)]
    model_params: m.JsonObject = Field(default_factory=lambda: {"temperature": 0})

    @field_validator("base_url")
    @classmethod
    def validate_endpoint(cls, value: str) -> str:
        """仅接受无凭据、查询参数和片段的绝对 HTTP API 根地址。"""
        parts = urlsplit(value)
        if (
            parts.scheme not in {"http", "https"}
            or not parts.hostname
            or parts.username is not None
            or parts.password is not None
            or "?" in value
            or "#" in value
            or any(character.isspace() or ord(character) < 32 for character in value)
        ):
            raise ValueError("invalid model endpoint")
        # 访问 port 属性也会拒绝无效端口，校验期间不会创建 HTTP 客户端。
        if parts.port is not None and not 1 <= parts.port <= 65535:
            raise ValueError("invalid model endpoint")
        return value.rstrip("/")

    @field_validator("api_key")
    @classmethod
    def validate_key(cls, value: SecretStr) -> SecretStr:
        raw = value.get_secret_value()
        if not raw.strip() or any(character in raw for character in "\r\n"):
            raise ValueError("invalid model credential")
        return value

    @field_validator("model_id")
    @classmethod
    def validate_model_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("invalid model identity")
        return value

    @field_validator("model_params")
    @classmethod
    def validate_model_params(cls, value: m.JsonObject) -> m.JsonObject:
        # 与实际规划器使用同一组限制，拒绝 stream/messages/model 等保护字段。
        if set(value) - _ALLOWED_PARAMETERS or not JsonPlanner._valid_params(value):
            raise ValueError("invalid model parameters")
        return value


class ServerSettings(m.Model):
    """Web 监听与本地审计目录；目录在读取配置时解析但不创建。"""

    port: Annotated[int, Field(ge=1, le=65535)] = 8765
    data_dir: Path = Path("runs/workspace")
    max_active_runs: Annotated[int, Field(ge=1)] = 2

    @field_validator("data_dir", mode="before")
    @classmethod
    def validate_data_dir(cls, value: object) -> Path:
        if isinstance(value, Path):
            return value
        if isinstance(value, str) and value.strip() and "\x00" not in value:
            return Path(value)
        raise ValueError("invalid data directory")


class ProfileSettings(m.Model):
    """人工声明的公开数据池身份，不从连接 options 推断数据源。"""

    profile_id: _Identifier
    title: _Label
    dataset_id: _Identifier
    dataset_name: _Label
    revision: _Label
    description: Annotated[str, Field(max_length=2000)] = ""
    suggested_questions: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=2000)]], Field(max_length=8)
    ] = Field(default_factory=list)


class AdapterSettings(m.Model):
    """可信宿主的导入目标与私有 JSON 参数；不允许 HTTP 调用方指定。"""

    factory: str
    options: m.JsonObject = Field(default_factory=dict, repr=False)

    @field_validator("factory")
    @classmethod
    def validate_factory(cls, value: str) -> str:
        if not _FACTORY.fullmatch(value):
            raise ValueError("invalid adapter factory")
        return value


class AdapterBindings(m.Model):
    data: AdapterSettings
    content: AdapterSettings


class AppConfig(m.Model):
    """校验完成的配置；run 中模型字段始终来自 llm 的单一声明。"""

    llm: LLMSettings
    server: ServerSettings = Field(default_factory=ServerSettings)
    profile: ProfileSettings
    adapters: AdapterBindings
    run: m.RunConfig = Field(default_factory=m.RunConfig)

    @model_validator(mode="before")
    @classmethod
    def reject_duplicate_models(cls, value: object) -> object:
        if isinstance(value, dict):
            run = value.get("run")
            if isinstance(run, dict) and {"model_id", "model_params"} & run.keys():
                raise ValueError("model settings must be declared once")
        return value

    @model_validator(mode="after")
    def bind_run_model(self) -> AppConfig:
        # 深复制模型参数，防止单次运行通过嵌套 dict 修改 LLM 原始配置。
        self.run = self.run.model_copy(
            deep=True,
            update={
                "model_id": self.llm.model_id,
                "model_params": self.llm.model_copy(deep=True).model_params,
            },
        )
        return self


def _validation_error_message(error: ValidationError) -> str:
    """只显示代码定义的字段路径，绝不显示用户键名、输入值或错误上下文。"""
    known = {(section,) for section in AppConfig.model_fields}
    for section, model in (
        ("llm", LLMSettings),
        ("server", ServerSettings),
        ("profile", ProfileSettings),
        ("run", m.RunConfig),
    ):
        known.update((section, name) for name in model.model_fields)
    for role in ("data", "content"):
        known.add(("adapters", role))
        known.update(("adapters", role, name) for name in AdapterSettings.model_fields)
    known.update(("run", name) for name in _REMOVED_RUN_FIELDS)
    paths = set()
    removed = set()
    for issue in error.errors(include_url=False, include_context=False, include_input=False):
        location = issue["loc"]
        if len(location) >= 2 and location[0] == "run" and location[1] in _REMOVED_RUN_FIELDS:
            removed.add("run." + location[1])
        matches = [path for path in known if location[: len(path)] == path]
        if matches:
            paths.add(".".join(max(matches, key=len)))
    suffix = " Check: " + ", ".join(sorted(paths)) + "." if paths else ""
    if removed:
        suffix += " Remove retired settings from [run]: " + ", ".join(sorted(removed)) + "."
    return "Application configuration failed strict validation." + suffix


def load_config(path: str | Path) -> AppConfig:
    """解析 TOML 并读取一次显式密钥；不导入工厂、不创建网络客户端。"""
    try:
        config_path = Path(path).expanduser().resolve()
        with config_path.open("rb") as source:
            raw = tomllib.load(source)
    except (OSError, ValueError, TypeError, tomllib.TOMLDecodeError):
        raise ConfigError("Configuration file could not be read as valid TOML.") from None

    llm = raw.get("llm")
    if not isinstance(llm, dict) or ("api_key" in llm) == ("api_key_env" in llm):
        raise ConfigError("Specify exactly one model credential source.") from None
    if "api_key_env" in llm:
        variable = llm.pop("api_key_env")
        if not isinstance(variable, str) or not _ENV_NAME.fullmatch(variable):
            raise ConfigError("The model credential environment setting is invalid.") from None
        key = os.environ.get(variable)
        if not key or not key.strip():
            raise ConfigError("The configured model credential environment variable is empty.")
        llm["api_key"] = key
    try:
        config = AppConfig.model_validate(raw)
        directory = config.server.data_dir.expanduser()
        if not directory.is_absolute():
            directory = config_path.parent / directory
        config.server.data_dir = directory.resolve()
        return config
    except ValidationError as error:
        raise ConfigError(_validation_error_message(error)) from None
    except (ValueError, TypeError, OSError):
        # 不转发 Pydantic.errors()、TOML 行内容或文件系统异常，
        # 它们可能带有密钥、URL、options 中的密码或任意未知字段名。
        raise ConfigError("Application configuration failed strict validation.") from None
