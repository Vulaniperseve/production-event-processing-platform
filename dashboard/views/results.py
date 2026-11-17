"""Market results page: what the data says."""

import pandas as pd
import streamlit as st

from dashboard.lib import charts
from dashboard.lib import resources as res

st.title("Market results")
st.caption(
    "Prices, trends and risk per symbol from the PostgreSQL analytics layer (SQL window "
    "functions), plus monthly returns and correlations from the Spark gold layer."
)

try:
    summary = res.symbol_summary()
except Exception as exc:
    st.error(f"PostgreSQL isn't reachable: {exc}")
    st.stop()

if summary.empty:
    st.info("No data yet. Run the pipeline from the Pipeline page.", icon=":material/info:")
    st.stop()

symbols = list(summary["symbol"])
colors = charts.symbol_colors(symbols)


def pct(v, signed=True):
    return "–" if v is None or pd.isna(v) else (f"{v:+.2%}" if signed else f"{v:.2%}")


# ------------------------------------------------------------ KPI tiles
st.subheader("Performance over the period")
cols = st.columns(len(symbols))
for col, (_, row) in zip(cols, summary.iterrows()):
    with col:
        st.metric(
            f"{row['symbol']} · {int(row['trading_days'])} trading days",
            f"${row['last_close']:,.2f}",
            delta=pct(row["period_return"]),
            help=f"From {row['first_date']} (${row['first_close']:,.2f}) to {row['last_date']}",
        )
        st.caption(
            f"Volatility {pct(row['annualized_volatility'], False)} a year · "
            f"max drawdown {pct(row['max_drawdown'])}"
        )

frames = {s: res.price_indicators(s) for s in symbols}
st.plotly_chart(charts.rebased_chart(frames, colors), width="stretch",
                config={"displayModeBar": False})
st.caption("Every symbol rebased to 100 on its first day, so different price levels compare fairly.")

# -------------------------------------------------------- one symbol
st.divider()
st.subheader("Symbol detail")
symbol = st.segmented_control("Symbol", symbols, default=symbols[0], label_visibility="collapsed") or symbols[0]
df = frames[symbol]
row = summary.set_index("symbol").loc[symbol]

a, b, c, d, e = st.columns(5)
a.metric("Period return", pct(row["period_return"]))
b.metric("Annualized volatility", pct(row["annualized_volatility"], False))
c.metric("Max drawdown", pct(row["max_drawdown"]))
d.metric("Best day", pct(row["best_day_return"]), help=f"On {row['best_day']}")
e.metric("Worst day", pct(row["worst_day_return"]), help=f"On {row['worst_day']}")

st.markdown("**Close price with 5- and 20-day moving averages**")
st.plotly_chart(charts.price_chart(df, colors[symbol]), width="stretch",
                config={"displayModeBar": False})

left, right = st.columns(2)
with left:
    st.markdown("**Drawdown** · how far below its running peak")
    st.plotly_chart(charts.drawdown_chart(df, colors[symbol]), width="stretch",
                    config={"displayModeBar": False})
with right:
    st.markdown("**Daily returns**")
    st.plotly_chart(charts.returns_chart(df), width="stretch",
                    config={"displayModeBar": False})

# ------------------------------------------------------------- gold
st.divider()
st.subheader("From the Spark gold layer")
monthly, corr = res.gold("monthly_summary"), res.gold("return_correlation")
if monthly.empty:
    st.info("The data lake hasn't been built yet. It builds after the next pipeline run.")
else:
    left, right = st.columns([3, 2])
    with left:
        st.markdown("**Monthly returns** · `gold/monthly_summary` (Spark SQL)")
        st.plotly_chart(charts.monthly_heatmap(monthly), width="stretch",
                        config={"displayModeBar": False})
    with right:
        st.markdown("**Daily return correlation** · `gold/return_correlation`")
        if corr.empty:
            st.caption("Needs at least two symbols.")
        else:
            st.plotly_chart(charts.correlation_heatmap(corr), width="stretch",
                            config={"displayModeBar": False})
    st.caption(
        "Correlation near 0 means the stocks move independently, which is useful for "
        "diversification. 1 means they move together."
    )
