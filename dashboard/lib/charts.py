"""
Plotly figures for the dashboard, with one consistent style.

Colors follow the entity, never its rank: each symbol keeps its color on
every chart, so AAPL is always the same blue.
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go

SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
INK, MUTED, GRID, BASE = "#1f2329", "#6b6f76", "#eceef1", "#c3c2b7"
POS, NEG = "#1a7f4b", "#c93636"


def symbol_colors(symbols):
    return {s: SERIES[i % len(SERIES)] for i, s in enumerate(sorted(symbols))}


def _style(fig, height=320, legend=True, yformat=None):
    fig.update_layout(
        height=height,
        margin=dict(l=8, r=8, t=8, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=INK, size=12),
        hovermode="x unified",
        showlegend=legend,
        legend=dict(orientation="h", yanchor="bottom", y=1.0, xanchor="left", x=0,
                    font=dict(color=MUTED, size=12), bgcolor="rgba(0,0,0,0)"),
        hoverlabel=dict(bgcolor="white", font_size=12),
    )
    fig.update_xaxes(showgrid=False, linecolor=BASE, tickfont=dict(color=MUTED))
    fig.update_yaxes(gridcolor=GRID, zeroline=False, tickfont=dict(color=MUTED), tickformat=yformat)
    return fig


def price_chart(df, color):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["trade_date"], y=df["close_price"], name="Close",
                             line=dict(color=color, width=2)))
    fig.add_trace(go.Scatter(x=df["trade_date"], y=df["sma_5"], name="5-day average",
                             line=dict(color="#eb6834", width=1.5, dash="dash")))
    fig.add_trace(go.Scatter(x=df["trade_date"], y=df["sma_20"], name="20-day average",
                             line=dict(color="#1baf7a", width=1.5, dash="dot")))
    fig.update_traces(hovertemplate="%{y:$,.2f}")
    return _style(fig, 340, yformat="$,.0f")


def drawdown_chart(df, color):
    fig = go.Figure(go.Scatter(
        x=df["trade_date"], y=df["drawdown"], name="Drawdown", fill="tozeroy",
        line=dict(color=NEG, width=1.5), fillcolor="rgba(201,54,54,0.12)",
        hovertemplate="%{y:.1%} below peak",
    ))
    return _style(fig, 180, legend=False, yformat=".0%")


def returns_chart(df):
    r = df.dropna(subset=["daily_return"])
    fig = go.Figure(go.Bar(
        x=r["trade_date"], y=r["daily_return"],
        marker_color=np.where(r["daily_return"] >= 0, POS, NEG),
        hovertemplate="%{y:+.2%}", name="Daily return",
    ))
    return _style(fig, 180, legend=False, yformat=".0%")


def rebased_chart(frames, colors):
    """All symbols rebased to 100 on their first day: fair comparison of performance."""
    fig = go.Figure()
    for symbol, df in frames.items():
        if df.empty:
            continue
        base = df["close_price"].iloc[0]
        fig.add_trace(go.Scatter(
            x=df["trade_date"], y=100 * df["close_price"] / base, name=symbol,
            line=dict(color=colors[symbol], width=2), hovertemplate=f"{symbol} %{{y:.1f}}",
        ))
    fig.add_hline(y=100, line=dict(color=BASE, width=1, dash="dot"))
    return _style(fig, 300)


def monthly_heatmap(monthly):
    m = monthly.dropna(subset=["monthly_return"]).copy()
    m["symbol"] = m["symbol"].astype(str)
    pivot = m.pivot(index="symbol", columns="month", values="monthly_return").sort_index()
    text = pivot.map(lambda v: "" if pd.isna(v) else f"{v:+.1%}")
    limit = float(np.nanmax(np.abs(pivot.values))) if pivot.size else 0.1
    fig = go.Figure(go.Heatmap(
        z=pivot.values, x=list(pivot.columns), y=list(pivot.index), text=text.values,
        texttemplate="%{text}", colorscale=[[0, "#c93636"], [0.5, "#f0efec"], [1, "#2a78d6"]],
        zmin=-limit, zmax=limit, showscale=False, xgap=3, ygap=3,
        hovertemplate="%{y} %{x}: %{z:+.2%}<extra></extra>",
    ))
    fig = _style(fig, 60 + 46 * len(pivot.index), legend=False)
    fig.update_layout(hovermode="closest")
    fig.update_yaxes(showgrid=False, autorange="reversed")
    return fig


def correlation_heatmap(corr):
    c = corr.copy()
    c["symbol_a"], c["symbol_b"] = c["symbol_a"].astype(str), c["symbol_b"].astype(str)
    symbols = sorted(set(c["symbol_a"]) | set(c["symbol_b"]))
    matrix = pd.DataFrame(np.eye(len(symbols)), index=symbols, columns=symbols)
    for _, row in c.iterrows():
        matrix.loc[row["symbol_a"], row["symbol_b"]] = row["return_correlation"]
        matrix.loc[row["symbol_b"], row["symbol_a"]] = row["return_correlation"]
    fig = go.Figure(go.Heatmap(
        z=matrix.values, x=symbols, y=symbols, text=matrix.map(lambda v: f"{v:.2f}").values,
        texttemplate="%{text}", colorscale=[[0, "#c93636"], [0.5, "#f0efec"], [1, "#2a78d6"]],
        zmin=-1, zmax=1, showscale=False, xgap=3, ygap=3,
        hovertemplate="%{y} vs %{x}: %{z:.3f}<extra></extra>",
    ))
    fig = _style(fig, 80 + 56 * len(symbols), legend=False)
    fig.update_layout(hovermode="closest")
    fig.update_yaxes(showgrid=False, autorange="reversed")
    return fig


def health_chart(health):
    daily = health.groupby("run_date", as_index=False)[
        ["new_events_loaded", "records_quarantined", "records_received"]].sum()
    daily["already_stored"] = (daily["records_received"] - daily["records_quarantined"]
                               - daily["new_events_loaded"]).clip(lower=0)
    daily = daily.sort_values("run_date")
    daily["run_date"] = pd.to_datetime(daily["run_date"]).dt.strftime("%d %b")   # run days only
    fig = go.Figure()
    for col, name, color in [("new_events_loaded", "New events loaded", POS),
                             ("already_stored", "Already stored (idempotent skip)", "#b4b2a9"),
                             ("records_quarantined", "Quarantined", "#eda100")]:
        fig.add_trace(go.Bar(x=daily["run_date"], y=daily[col], name=name, marker_color=color,
                             hovertemplate="%{y:,}"))
    fig.update_layout(barmode="stack", bargap=0.45)
    fig = _style(fig, 300)
    fig.update_xaxes(type="category")
    return fig
