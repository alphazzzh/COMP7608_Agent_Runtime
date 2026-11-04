"""为单个项目执行过程生成可移植且经过转义的 HTML 快照。

导出文档不含脚本或远程资源，只包含工作台中可见的同一份结果、
注册配置、证据与审计记录。
"""

from html import escape


def html_report(view):
    """将用户和适配器的每个值转义后，再嵌入独立 HTML 文档。"""

    def text(value):
        return escape(str(value), quote=True)

    result = view.get("result") or {}
    answer = result.get("answer") or {}
    profile = view.get("profile") or {}
    environment = "项目任务 · 运行记录"
    identity = profile.get("title") or view.get("profile_id") or "未记录配置"
    modules = " · ".join(
        f"{name}: {value['label']} ({value['status']})"
        for name, value in profile.get("modules", {}).items()
    )
    boundary = "此报告保存任务结果和配置身份。请结合来源、模块接入状态与独立评估判断答案质量。"
    evidence = (
        "".join(
            f"<article><h3>{text(item['source_id'])}</h3><p>{text(item['payload'])}</p><small>{text(item['evidence_id'])}</small></article>"
            for item in view["evidence"]
        )
        or "<p>暂无证据。</p>"
    )
    events = "".join(
        f"<tr><td>{text(item['attempt_id'])}</td><td>{item['seq']}</td><td>{text(item['event_type'])}</td><td>{text(item.get('status') or '')}</td><td>{'历史回放' if item['detail'].get('replayed') else ''}</td></tr>"
        for item in view["events"]
    )
    return f"""<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Agent 运行报告</title>
<style>body{{font:16px/1.7 system-ui,sans-serif;color:#192a2a;background:#f5f6f2;margin:0}}main{{max-width:980px;margin:auto;padding:40px 24px}}h1{{font-size:32px}}article,section{{background:white;padding:20px;margin:16px 0;border:1px solid #dce3dc;border-radius:12px}}p{{white-space:pre-wrap;overflow-wrap:anywhere}}small{{color:#65746e;overflow-wrap:anywhere}}table{{width:100%;font-size:12px;border-collapse:collapse}}td,th{{padding:8px;text-align:left;border-bottom:1px solid #dce3dc;overflow-wrap:anywhere}}.badge{{color:#17655a}}</style>
<main><span class="badge">{text(environment)}</span><h1>Agent 运行报告</h1><p>{text(view["question"])}</p>
<small>运行 {text(view["run_id"])} · 配置 {text(identity)} · 状态 {text(view["status"])} · {text(view["created_at"])}</small>
<section><h2>执行配置</h2><p>数据池：{text(profile.get("dataset_name", "未记录"))}\n版本：{text(profile.get("revision", "未记录"))}\n模型：{text(profile.get("model_id", "未记录"))}\n{text(modules)}</p></section>
<section><h2>答案</h2><p>{text(answer.get("text", "本次运行尚未产生最终答案。"))}</p><small>引用：{text(", ".join(answer.get("citations", [])))}</small></section>
<section><h2>累计执行</h2><p>工具尝试 {view["metrics"]["tools"]} · 证据 {view["metrics"]["evidence"]} · 模型请求 {view["metrics"]["model_calls"]} · 执行段 {view["metrics"]["attempts"]}</p></section>
<h2>来源证据</h2>{evidence}<section><h2>审计时间线</h2><table><thead><tr><th>执行段</th><th>序号</th><th>事件</th><th>状态</th><th>说明</th></tr></thead><tbody>{events}</tbody></table></section>
<p>{text(boundary)}</p></main></html>"""
