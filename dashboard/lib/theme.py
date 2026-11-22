"""
"Midnight aurora" look for the dashboard: CSS injected once per page.

Streamlit's own theme (.streamlit/config.toml) sets the base colours. This adds
what the config can't: a slowly drifting aurora behind the app, translucent
"glass" cards, a glowing primary button and coloured titles.

Performance: the aurora is one fixed layer animated with `transform` only,
which the browser hands to the GPU, so it doesn't repaint the page or cost the
CPU anything noticeable. People who set "reduce motion" in their OS get a
still background.
"""

# One source of truth for colours, shared with flow.py and charts.py.
PALETTE = {
    "bg": "#14122B",
    "panel": "rgba(34, 29, 71, 0.62)",
    "panel_solid": "#221D47",
    "line": "rgba(127, 119, 221, 0.28)",
    "line_strong": "#534AB7",
    "ink": "#EEEDFE",
    "muted": "#A9A5D6",
    "accent": "#7F77DD",
    "accent_soft": "#AFA9EC",
    "teal": "#5DCAA5",
    "pink": "#ED93B1",
    "amber": "#FAC775",
    "blue": "#85B7EB",
    "coral": "#F0997B",
    "red": "#F09595",
}

CSS = """
<style>
.stApp {background: #14122B;}
.stApp::before {
  content: ""; position: fixed; inset: -25%; z-index: 0; pointer-events: none;
  background:
    radial-gradient(38% 32% at 22% 18%, rgba(83, 74, 183, 0.55), transparent 70%),
    radial-gradient(34% 30% at 82% 78%, rgba(15, 110, 86, 0.45), transparent 70%),
    radial-gradient(28% 26% at 70% 12%, rgba(153, 53, 86, 0.30), transparent 70%),
    radial-gradient(30% 28% at 12% 88%, rgba(24, 95, 165, 0.30), transparent 70%);
  filter: blur(30px);
  will-change: transform;
  animation: aurora-drift 26s ease-in-out infinite alternate;
}
@keyframes aurora-drift {
  0%   {transform: translate3d(0, 0, 0) rotate(0deg) scale(1);}
  50%  {transform: translate3d(4%, -3%, 0) rotate(8deg) scale(1.06);}
  100% {transform: translate3d(-4%, 3%, 0) rotate(-6deg) scale(1.02);}
}
@media (prefers-reduced-motion: reduce) {.stApp::before {animation: none;}}

[data-testid="stAppViewContainer"], [data-testid="stHeader"] {background: transparent;}
[data-testid="stAppViewContainer"] > * {position: relative; z-index: 1;}
[data-testid="stSidebar"] {
  background: rgba(20, 18, 43, 0.72); backdrop-filter: blur(14px);
  border-right: 1px solid rgba(127, 119, 221, 0.28);
}
.block-container {padding-top: 2.2rem; max-width: 1400px;}

h1 {
  font-weight: 700; letter-spacing: -0.02em;
  background: linear-gradient(90deg, #FFFFFF 0%, #CECBF6 45%, #5DCAA5 100%);
  -webkit-background-clip: text; background-clip: text; color: transparent !important;
}
h2, h3 {color: #EEEDFE;}

/* Glass cards: bordered containers, metrics, expanders, tables */
[data-testid="stVerticalBlockBorderWrapper"]:has(> div > [data-testid="stVerticalBlock"]) ,
[data-testid="stMetric"], [data-testid="stExpander"] details {
  background: rgba(34, 29, 71, 0.55); backdrop-filter: blur(10px);
  border: 1px solid rgba(127, 119, 221, 0.28) !important; border-radius: 14px;
}
[data-testid="stMetric"] {padding: 12px 14px;}
[data-testid="stMetricLabel"] p {font-size: 13px; color: #A9A5D6;}
[data-testid="stMetricValue"] {color: #FFFFFF;}

/* Primary button: purple with a soft glow */
button[data-testid="stBaseButton-primary"] {
  background: linear-gradient(135deg, #7F77DD, #534AB7); border: none;
  box-shadow: 0 0 0 1px rgba(175, 169, 236, 0.35), 0 6px 22px rgba(127, 119, 221, 0.45);
  transition: transform .15s, box-shadow .2s;
}
button[data-testid="stBaseButton-primary"]:hover {
  transform: translateY(-1px);
  box-shadow: 0 0 0 1px rgba(175, 169, 236, 0.6), 0 8px 28px rgba(127, 119, 221, 0.65);
}

/* Sidebar navigation: coloured icons, highlighted current page */
[data-testid="stSidebarNav"] a {border-radius: 10px;}
[data-testid="stSidebarNav"] a[aria-current="page"] {background: rgba(83, 74, 183, 0.55);}
[data-testid="stSidebarNav"] li:nth-child(1) [data-testid="stIconMaterial"] {color: #AFA9EC;}
[data-testid="stSidebarNav"] li:nth-child(2) [data-testid="stIconMaterial"] {color: #5DCAA5;}
[data-testid="stSidebarNav"] li:nth-child(3) [data-testid="stIconMaterial"] {color: #ED93B1;}
[data-testid="stSidebarNav"] li:nth-child(4) [data-testid="stIconMaterial"] {color: #FAC775;}
[data-testid="stSidebar"] [data-testid="stPageLink"] [data-testid="stIconMaterial"] {color: #85B7EB;}

/* The live view refreshes every few seconds: don't fade it while it does. */
[data-stale="true"] {opacity: 1 !important; transition: none !important;}
</style>
"""

BRAND = """
<div style="display:flex;align-items:center;gap:10px;margin:2px 0 4px">
  <div style="width:34px;height:34px;border-radius:10px;display:flex;align-items:center;justify-content:center;
              background:linear-gradient(135deg,#7F77DD,#0F6E56);box-shadow:0 4px 16px rgba(127,119,221,.45);
              font-family:'Material Symbols Rounded';font-size:20px;color:#fff">bolt</div>
  <div>
    <div style="font-weight:700;font-size:16px;color:#EEEDFE;line-height:1.2">Event Processing Platform</div>
    <div style="font-size:12px;color:#A9A5D6">API → PostgreSQL → Spark lake</div>
  </div>
</div>
"""
