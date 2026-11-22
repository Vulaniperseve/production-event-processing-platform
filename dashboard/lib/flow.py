"""
HTML for the live pipeline diagram (two lanes, 8 stages).

Pure CSS animation, no JavaScript: a running stage pulses and data dots
flow along the connector leading into it. The page re-renders this every
few seconds with fresh statuses from Airflow.
"""

from html import escape

from dashboard.lib.pipeline_status import (
    BLOCKED,
    FAILED,
    QUEUED,
    RETRYING,
    RUNNING,
    SKIPPED,
    SUCCESS,
    WAITING,
)

STATUS_STYLE = {
    WAITING: ("Waiting", "st-wait", "○"),
    QUEUED: ("Queued", "st-queue", "◔"),
    RUNNING: ("Running", "st-run", ""),
    RETRYING: ("Retrying", "st-retry", "↻"),
    SUCCESS: ("Done", "st-ok", "✓"),
    SKIPPED: ("Skipped", "st-skip", "↷"),
    FAILED: ("Failed", "st-fail", "✕"),
    BLOCKED: ("Blocked", "st-block", "–"),
}

CSS = """
<style>
.pf{font-family:inherit;color:#EEEDFE;--ok:#5DCAA5;--okbg:rgba(29,158,117,.18);--run:#AFA9EC;--runbg:rgba(127,119,221,.28);
 --fail:#F09595;--failbg:rgba(226,75,74,.18);--mut:#A9A5D6;--line:rgba(127,119,221,.30);--card:rgba(38,33,92,.62);
 --soft:rgba(20,18,43,.45);--warn:#FAC775;--warnbg:rgba(239,159,39,.16);--chip:rgba(127,119,221,.16)}
.pf-lane{border:1px solid var(--line);border-radius:16px;padding:14px 16px 16px;background:var(--soft);margin-bottom:10px;
 backdrop-filter:blur(10px)}
.pf-lane-h{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;gap:8px;flex-wrap:wrap}
.pf-lane-t{font-weight:600;font-size:14px;color:#fff}
.pf-lane-s{font-size:12px;color:var(--mut)}
.pf-row{display:grid;grid-template-columns:minmax(0,1fr) 34px minmax(0,1fr) 34px minmax(0,1fr) 34px minmax(0,1fr);align-items:stretch}
.pf-card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:12px 12px 10px;position:relative;
 transition:border-color .4s,box-shadow .4s,transform .2s;min-height:150px;display:flex;flex-direction:column}
.pf-card:hover{transform:translateY(-2px)}
.pf-top{display:flex;align-items:center;gap:8px;margin-bottom:8px}
.pf-ico{width:34px;height:34px;border-radius:10px;display:flex;align-items:center;justify-content:center;flex:none;
 font-family:'Material Symbols Rounded';font-size:20px;line-height:1;position:relative}
.pf-num{position:absolute;right:-6px;bottom:-6px;width:17px;height:17px;border-radius:50%;display:flex;align-items:center;
 justify-content:center;font-size:10px;font-weight:700;font-family:inherit;background:#26215C;color:#CECBF6;border:1px solid var(--line)}
.pf-name{font-weight:600;font-size:14px;line-height:1.25;margin-bottom:2px;color:#fff}
.pf-sub{font-size:12px;color:var(--mut);margin-bottom:8px;line-height:1.35}
.pf-metric{font-size:13px;font-weight:500;margin-top:auto;color:#E1F5EE}
.pf-note{font-size:11.5px;color:var(--mut);margin-top:2px}
.pf-pill{margin-left:auto;font-size:11px;font-weight:600;padding:2px 8px;border-radius:999px;display:flex;gap:4px;align-items:center;white-space:nowrap}
.pf-bar{height:4px;border-radius:2px;background:rgba(255,255,255,.08);margin-top:6px;overflow:hidden}
.pf-bar>i{display:block;height:100%;background:linear-gradient(90deg,#7F77DD,#5DCAA5);transition:width .6s}
.st-wait .pf-pill,.st-queue .pf-pill{background:var(--chip);color:var(--mut)}
.st-run{border-color:#7F77DD;box-shadow:0 0 0 3px var(--runbg),0 0 26px rgba(127,119,221,.45);animation:pf-glow 1.6s ease-in-out infinite}
.st-run .pf-pill{background:var(--runbg);color:#fff}
.st-retry{border-color:#EF9F27}.st-retry .pf-pill{background:var(--warnbg);color:var(--warn)}
.st-ok .pf-pill{background:var(--okbg);color:var(--ok)}.st-ok .pf-num{background:#0F6E56;color:#E1F5EE;border-color:#1D9E75}
.st-skip .pf-pill{background:var(--chip);color:var(--mut)}.st-skip{border-style:dashed}
.st-fail{border-color:var(--fail);box-shadow:0 0 0 3px var(--failbg)}.st-fail .pf-pill{background:var(--failbg);color:var(--fail)}
.st-fail .pf-num{background:#A32D2D;color:#fff}
.st-wait,.st-block{opacity:.72}.st-block .pf-pill{background:var(--chip);color:var(--mut)}
.pf-spin{width:10px;height:10px;border:2px solid var(--runbg);border-top-color:#fff;border-radius:50%;animation:pf-rot .8s linear infinite}
.pf-link{position:relative;display:flex;align-items:center;justify-content:center}
.pf-link::before{content:"";position:absolute;left:4px;right:4px;top:50%;height:2px;background:var(--line)}
.pf-link.done::before{background:var(--ok);box-shadow:0 0 8px rgba(93,202,165,.6)}
.pf-link.flow::before{background:repeating-linear-gradient(90deg,#AFA9EC 0 6px,transparent 6px 12px);
 background-size:24px 2px;animation:pf-flow .6s linear infinite}
.pf-link::after{content:"";position:absolute;right:2px;top:calc(50% - 4px);border:4px solid transparent;border-left:6px solid var(--line)}
.pf-link.done::after{border-left-color:var(--ok)}.pf-link.flow::after{border-left-color:#AFA9EC}
.pf-q{display:inline-block;margin-top:6px;font-size:11px;padding:2px 7px;border-radius:6px;background:var(--warnbg);color:var(--warn)}
.pf-q.zero{background:var(--chip);color:var(--mut)}
.pf-asset{display:flex;align-items:center;justify-content:center;gap:10px;margin:2px 0 12px;font-size:12px;color:var(--mut)}
.pf-asset b{font-weight:600;color:#CECBF6}
.pf-asset .v{width:2px;height:22px;background:var(--line)}
.pf-asset.flow .v{background:repeating-linear-gradient(180deg,#AFA9EC 0 5px,transparent 5px 10px);background-size:2px 20px;animation:pf-vflow .6s linear infinite}
.pf-asset.done .v{background:var(--ok);box-shadow:0 0 8px rgba(93,202,165,.6)}
@keyframes pf-flow{from{background-position:0 0}to{background-position:24px 0}}
@keyframes pf-vflow{from{background-position:0 0}to{background-position:0 20px}}
@keyframes pf-rot{to{transform:rotate(360deg)}}
@keyframes pf-glow{0%,100%{box-shadow:0 0 0 3px var(--runbg),0 0 18px rgba(127,119,221,.35)}
 50%{box-shadow:0 0 0 6px var(--runbg),0 0 32px rgba(127,119,221,.6)}}
@media (prefers-reduced-motion:reduce){.st-run,.pf-link.flow::before,.pf-asset.flow .v{animation:none}}
@media (max-width:900px){.pf-row{grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:10px}.pf-link{display:none}}
</style>
"""

# Icon (Material Symbols name) and tile colours per stage. Colour follows the
# kind of work: teal = data in, pink = quality, blue = storage, amber =
# analytics; the lake layers use their own metals.
STAGE_ICONS = {
    "extract": ("cloud_download", "#0F6E56", "#E1F5EE"),
    "validate": ("fact_check", "#993556", "#FBEAF0"),
    "load": ("database", "#185FA5", "#E6F1FB"),
    "analytics": ("insights", "#854F0B", "#FAEEDA"),
    "bronze": ("inventory_2", "#8A4B22", "#F5C4B3"),
    "silver": ("layers", "#5F5E5A", "#F1EFE8"),
    "gold": ("workspace_premium", "#B7861A", "#FFF6D6"),
    "publish": ("cloud_upload", "#3C3489", "#CECBF6"),
}


def _fmt_duration(seconds):
    if seconds is None:
        return ""
    seconds = int(round(seconds))
    return f"{seconds // 60}m {seconds % 60:02d}s" if seconds >= 60 else f"{seconds}s"


def _int(n):
    return f"{int(n):,}" if n is not None else "–"


def stage_metric(key, status, numbers, lake):
    """The one-line result shown on a finished stage."""
    if status not in (SUCCESS, SKIPPED):
        return ""
    numbers = numbers or {}
    lake = lake or {}
    if key == "extract":
        return f"{_int(numbers.get('api_responses'))} API calls · {_int(numbers.get('records_received'))} records"
    if key == "validate":
        return f"{_int(numbers.get('valid'))} valid records"
    if key == "load":
        new = numbers.get("new_events")
        old = numbers.get("already_present")
        if old:
            return f"{_int(new)} new · {_int(old)} already stored"
        return f"{_int(new)} new events loaded"
    if key == "analytics":
        return "6 views refreshed"
    if status == SKIPPED:
        return ""
    if key == "bronze":
        return f"{_int(lake.get('bronze', {}).get('files'))} raw files in lake"
    if key == "silver":
        s = lake.get("silver", {})
        return f"{_int(s.get('rows'))} rows · {_int(s.get('partitions'))} partitions"
    if key == "gold":
        return f"{_int(lake.get('gold', {}).get('rows'))} rows · 3 tables"
    if key == "publish":
        return "Lake synced to S3"
    return ""


def _card(index, stage, numbers, lake):
    text, css, icon = STATUS_STYLE.get(stage.status, STATUS_STYLE[WAITING])
    icon_html = '<span class="pf-spin"></span>' if stage.status == RUNNING else escape(icon)
    duration = _fmt_duration(stage.duration_s)
    pill = f"{icon_html}{escape(text)}" + (f" · {duration}" if duration and stage.status != RUNNING else "")

    progress = ""
    if stage.total > 1 and stage.status in (RUNNING, RETRYING, QUEUED):
        pct = int(100 * stage.done / stage.total)
        progress = (f'<div class="pf-note">{stage.done} of {stage.total} symbols done</div>'
                    f'<div class="pf-bar"><i style="width:{pct}%"></i></div>')

    quarantine = ""
    if stage.key == "validate" and stage.status == SUCCESS and numbers:
        q = numbers.get("quarantined", 0)
        quarantine = f'<span class="pf-q{" zero" if q == 0 else ""}">{_int(q)} quarantined</span>'

    glyph, bg, fg = STAGE_ICONS.get(stage.key, ("radio_button_unchecked", "#3C3489", "#CECBF6"))
    metric = stage_metric(stage.key, stage.status, numbers, lake)
    note = f'<div class="pf-note">{escape(stage.note)}</div>' if stage.note else ""
    return (
        f'<div class="pf-card {css}">'
        f'<div class="pf-top"><div class="pf-ico" style="background:{bg};color:{fg}">{glyph}'
        f'<div class="pf-num">{index}</div></div><div class="pf-pill">{pill}</div></div>'
        f'<div class="pf-name">{escape(stage.label)}</div>'
        f'<div class="pf-sub">{escape(stage.subtitle)}</div>'
        f'{progress}<div class="pf-metric">{escape(metric)}</div>{quarantine}{note}</div>'
    )


def _link(before, after):
    if after.status in (RUNNING, RETRYING, QUEUED) and before.status in (SUCCESS, SKIPPED):
        return '<div class="pf-link flow"></div>'
    if before.status in (SUCCESS, SKIPPED) and after.status in (SUCCESS, SKIPPED, FAILED):
        return '<div class="pf-link done"></div>'
    return '<div class="pf-link"></div>'


def _lane(title, subtitle, stages, offset, numbers, lake):
    parts = []
    for i, stage in enumerate(stages):
        if i:
            parts.append(_link(stages[i - 1], stage))
        parts.append(_card(offset + i + 1, stage, numbers, lake))
    return (
        f'<div class="pf-lane"><div class="pf-lane-h"><div class="pf-lane-t">{escape(title)}</div>'
        f'<div class="pf-lane-s">{escape(subtitle)}</div></div><div class="pf-row">{"".join(parts)}</div></div>'
    )


def render_flow(view, numbers=None, lake=None):
    ingest, lakehouse = view.stages[:4], view.stages[4:]

    ingest_run = view.ingest_run or {}
    lake_run = view.lake_run or {}
    ingest_sub = f"DAG production_event_pipeline · run {ingest_run.get('dag_run_id', '–')}"
    lake_sub = (f"DAG market_data_lakehouse · run {lake_run.get('dag_run_id')}"
                if lake_run else "DAG market_data_lakehouse · starts automatically")

    analytics, bronze = ingest[-1], lakehouse[0]
    if view.lake_pending or (analytics.status == SUCCESS and bronze.status in (QUEUED, RUNNING)):
        asset_css = "flow"
    elif analytics.status == SUCCESS and bronze.status in (SUCCESS, SKIPPED, FAILED):
        asset_css = "done"
    else:
        asset_css = ""
    asset = (f'<div class="pf-asset {asset_css}"><span class="v"></span>'
             f'<span><b>Asset event</b> market_events_staging → triggers the lake DAG</span>'
             f'<span class="v"></span></div>')

    return (
        CSS + '<div class="pf">'
        + _lane("Ingestion → warehouse", ingest_sub, ingest, 0, numbers, lake)
        + asset
        + _lane("Data lake (Spark)", lake_sub, lakehouse, 4, numbers, lake)
        + "</div>"
    )
