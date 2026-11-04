"""核验适用状态的确定性身份；不读取截断上下文或运行尝试元数据。

核验结果只说明其实际检查的完整问题、答案与证据在当前验收政策下的判定。
收据不复制整份答案或证据，仅保留业务输入摘要、有效核验和原调用身份。
修订使用控制器当前已接纳答案，答案变化时清除旧收据。
SQLite恢复会用已保存的严格请求/响应重新构造同一收据，无独立检查点或模型调用。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from .. import models as m
from ..validation import canonical_json


def verification_fingerprint(
    *,
    question: str,
    answer: m.AnswerRecord,
    evidence: list[m.EvidenceRecord],
    mode: m.RunMode,
    require_verification: bool,
) -> str:
    """绑定完整verify业务输入和验收政策，保持证据列表的实际传入顺序。

    完整EvidenceRecord含可信引用身份、来源、定位、payload及warnings；不只比较ID。
    不加入当前时间、attempt ID、审计顺序、预算余量或调用超时，以免恢复错误失效。
    """
    return hashlib.sha256(
        canonical_json(
            {
                "question": question,
                "answer": answer.model_dump(mode="json"),
                "evidence": [item.model_dump(mode="json") for item in evidence],
                "policy": {"mode": mode, "require_verification": require_verification},
            }
        )
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class VerificationReceipt:
    """控制器私有收据；结果已通过端口和引用验证后才能建立。"""

    input_fingerprint: str
    result: m.Verification
    call_id: str
