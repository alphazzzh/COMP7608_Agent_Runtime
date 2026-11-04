"""测试配置文本与临时文件构造器，不读取私有配置或真实凭据。

只使用显式测试端口、临时目录和内存传输；测试场景不代表真实模型质量。
"""

from __future__ import annotations

from pathlib import Path


def config_text(*, credential='api_key = "private-test-key"', extra=""):
    """只生成测试配置；凭据不对应任何真实提供方。"""
    return f"""[llm]
base_url = "https://planner.example/v1"
{credential}
model_id = "configured-model"
model_params = {{ temperature = 0.25, seed = 7 }}

[profile]
profile_id = "project-main"
title = "Research data"
dataset_id = "research"
dataset_name = "Research pool"
revision = "v1"

[adapters.data]
factory = "configured_test_adapters:data"
options = {{ nested = {{ password = "private-adapter-option" }}, rows = [1, 2] }}

[adapters.content]
factory = "configured_test_adapters:content"
options = {{ index = "main" }}
{extra}
"""


def write_config(tmp_path: Path, text: str | None = None):
    path = tmp_path / "application.toml"
    path.write_text(config_text() if text is None else text, encoding="utf-8")
    return path


def write_settings(tmp_path):
    """有效连接设置只使用测试域名；工厂在 CLI 边界替换，不会进入。"""
    path = tmp_path / "config.toml"
    path.write_text(
        """[llm]
base_url = "https://configured.example/v1"
api_key = "cli-private-key"
model_id = "cli-model"
[profile]
profile_id = "cli-project"
title = "CLI project"
dataset_id = "cli-data"
dataset_name = "CLI data"
revision = "v1"
[adapters.data]
factory = "team.data:create_port"
[adapters.content]
factory = "team.content:create_port"
[server]
port = 8768
data_dir = "records"
max_active_runs = 3
""",
        encoding="utf-8",
    )
    return path
