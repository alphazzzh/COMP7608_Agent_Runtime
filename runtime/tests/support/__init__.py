"""仅供测试使用的合成端口、请求构造器与故障注入支持。

测试模块按职责归档后，共享支持代码只依赖本包和生产模块，不导入测试用例。
源码资源统一从这里定位，避免用例目录深度再次变化时破坏子解释器或 JS 回归。
"""

from pathlib import Path

RUNTIME_ROOT = Path(__file__).resolve().parents[2]
