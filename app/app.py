"""
AI/BI Genie Resume — Interactive Resume as a Databricks App

A visual resume dashboard + conversational Genie Q&A where recruiters
can ask natural-language questions about your career and get instant,
SQL-backed answers from your structured resume data.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import os
import time
import base64
from pathlib import Path
from datetime import datetime, timedelta

HAS_DATABRICKS_SDK = False
try:
    from databricks.sdk import WorkspaceClient
    HAS_DATABRICKS_SDK = True
except ImportError:
    pass

from fpdf import FPDF

def _html(content):
    """Render HTML content using st.html (Streamlit 1.33+) with fallback."""
    if hasattr(st, "html"):
        st.html(content)
    else:
        _html(content)

# ────────────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────────────

def _get_config(key, default=""):
    """Read config from Streamlit secrets (cloud), env vars (Databricks App), or default."""
    try:
        return st.secrets["databricks"][key]
    except Exception:
        pass
    return os.getenv(key.upper(), default)

CATALOG = _get_config("catalog", "workspace")
SCHEMA = _get_config("schema", "career_profile")
WAREHOUSE_ID = _get_config("warehouse_id", "")
GENIE_SPACE_ID = _get_config("genie_space_id", "")
def _find_data_file():
    """Locate resume_data.json in both local dev and deployed Databricks App layouts."""
    candidates = [
        Path(__file__).parent / "config" / "resume_data.json",
        Path(__file__).parent.parent / "config" / "resume_data.json",
    ]
    for p in candidates:
        if p.exists():
            return p
    return candidates[0]

DATA_FILE = _find_data_file()

COLORS = {
    "primary": "#1B3A4B",
    "secondary": "#065A82",
    "accent": "#1C7C54",
    "highlight": "#F4A261",
    "bg_card": "#FFFFFF",
    "text": "#212529",
    "muted": "#6C757D",
}

PROFICIENCY_ORDER = {"Expert": 3, "Advanced": 2, "Intermediate": 1}
CATEGORY_COLORS = {
    "Technical": "#065A82",
    "Leadership": "#1C7C54",
    "Business": "#F4A261",
}

# ────────────────────────────────────────────────────────────────
# Page config & styling
# ────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Krish Kilaru — Databricks Solutions Architect | AI Resume",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="collapsed",
)

if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = False

if st.session_state.dark_mode:
    _html("""<style>
        .stApp, .main, [data-testid="stAppViewContainer"] { background-color: #0E1117 !important; color: #FAFAFA !important; }
        .block-container { color: #FAFAFA !important; }
        div[data-testid="stMetric"] { background: #1E2530 !important; border-color: #2D3748 !important; }
        div[data-testid="stMetric"] label { color: #A0AEC0 !important; }
        div[data-testid="stMetric"] [data-testid="stMetricValue"] { color: #E2E8F0 !important; }
        .section-header { color: #63B3ED !important; border-color: #3182CE !important; }
        .exp-card { background: #1E2530 !important; border-color: #2D3748 !important; }
        .exp-card h3 { color: #E2E8F0 !important; }
        .exp-card .highlight { color: #CBD5E0 !important; }
        .info-card { background: #1E2530 !important; border-color: #2D3748 !important; color: #CBD5E0 !important; }
        .sk-card { background: #1E2530 !important; }
        .sk-card .sk-name { color: #CBD5E0 !important; }
        .sk-bar-bg { background: #2D3748 !important; }
        p, span, div { color: inherit; }
        [data-testid="stTabs"] [data-baseweb="tab-list"] { background: linear-gradient(135deg,#1a1a2e,#16213e) !important; }
        [data-testid="stTabs"] button[role="tab"] { color: #A0AEC0 !important; }
        [data-testid="stTabs"] button[role="tab"][aria-selected="true"] { color: #fff !important; }
    </style>""")

_html("""
<style>
    /* Hide default Streamlit chrome for cleaner look */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Main container */
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 1rem;
        max-width: 1200px;
    }

    /* Metric cards */
    div[data-testid="stMetric"] {
        background: #FFFFFF;
        border: 1px solid #E9ECEF;
        border-radius: 12px;
        padding: 16px 20px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }
    div[data-testid="stMetric"] label {
        color: #6C757D;
        font-size: 0.85rem;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #1B3A4B;
        font-size: 1.8rem;
        font-weight: 700;
    }

    /* Section headers */
    .section-header {
        color: #1B3A4B;
        font-size: 1.3rem;
        font-weight: 700;
        margin-top: 1.5rem;
        margin-bottom: 0.5rem;
        padding-bottom: 0.3rem;
        border-bottom: 3px solid #065A82;
        display: inline-block;
    }

    /* Experience cards */
    .exp-card {
        background: #FFFFFF;
        border: 1px solid #E9ECEF;
        border-left: 4px solid #065A82;
        border-radius: 8px;
        padding: 20px 24px;
        margin-bottom: 16px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    }
    .exp-card h4 {
        color: #1B3A4B;
        margin: 0 0 4px 0;
        font-size: 1.25rem;
    }
    .exp-card .subtitle {
        color: #065A82;
        font-weight: 600;
        font-size: 1.1rem;
    }
    .exp-card .meta {
        color: #6C757D;
        font-size: 0.95rem;
        margin-bottom: 12px;
    }
    .exp-card .highlight {
        padding: 6px 0;
        font-size: 1.0rem;
        color: #212529;
    }
    .exp-card .badge {
        display: inline-block;
        font-size: 0.82rem;
        font-weight: 600;
        padding: 2px 8px;
        border-radius: 12px;
        margin-right: 4px;
    }
    .badge-technical { background: #E3F0FF; color: #065A82; }
    .badge-leadership { background: #E3F8EE; color: #1C7C54; }
    .badge-business { background: #FFF3E0; color: #E76F00; }

    /* Profile header */
    .profile-header {
        background: linear-gradient(135deg, #1B3A4B 0%, #065A82 100%);
        color: white;
        padding: 32px 40px;
        border-radius: 16px;
        margin-bottom: 24px;
    }
    .profile-header h1 {
        margin: 0;
        font-size: 2rem;
        font-weight: 800;
        letter-spacing: -0.5px;
    }
    .profile-header .headline {
        font-size: 0.95rem;
        opacity: 0.9;
        margin-top: 6px;
        font-weight: 400;
    }
    .profile-header .location {
        font-size: 0.88rem;
        opacity: 0.8;
        margin-top: 8px;
    }
    .profile-header-inner {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 24px;
    }
    .profile-photo {
        width: 130px;
        height: 130px;
        border-radius: 50%;
        object-fit: cover;
        border: 3px solid rgba(255,255,255,0.5);
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
        flex-shrink: 0;
    }
    .profile-info-col {
        flex: 1;
        min-width: 0;
    }
    .cert-images-col {
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        gap: 10px;
        flex-shrink: 0;
    }
    .cert-badge-img {
        height: 140px;
        width: auto;
        border-radius: 6px;
        transition: transform 0.2s ease;
        cursor: pointer;
        filter: drop-shadow(0 2px 8px rgba(0,0,0,0.35));
    }
    .cert-badge-img:hover {
        transform: scale(1.1);
    }
    .cert-text-row {
        display: flex;
        flex-wrap: wrap;
        gap: 6px;
        margin-top: 12px;
    }
    .cert-badge {
        background: rgba(255,255,255,0.15);
        border: 1px solid rgba(255,255,255,0.3);
        color: #fff;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.72rem;
        font-weight: 500;
        white-space: nowrap;
        backdrop-filter: blur(4px);
    }
    .cert-badge:hover {
        background: rgba(255,255,255,0.28);
    }
    /* Client logo grid */
    .clients-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
        gap: 16px;
        margin-top: 8px;
    }
    .client-card {
        background: #fff;
        border: 1px solid #E8EDF1;
        border-radius: 12px;
        padding: 16px 10px 12px;
        display: flex;
        flex-direction: column;
        align-items: center;
        text-align: center;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .client-card:hover {
        transform: translateY(-3px);
        box-shadow: 0 6px 18px rgba(0,0,0,0.1);
    }
    .client-logo {
        height: 48px;
        width: auto;
        max-width: 100px;
        object-fit: contain;
        margin-bottom: 10px;
    }
    .client-logo-fallback {
        height: 48px;
        width: 48px;
        border-radius: 50%;
        background: #1B3A4B;
        color: #fff;
        font-size: 1.3rem;
        font-weight: 700;
        display: flex;
        align-items: center;
        justify-content: center;
        margin-bottom: 10px;
    }
    .client-name {
        font-size: 0.82rem;
        font-weight: 600;
        color: #1B3A4B;
        margin-bottom: 6px;
        line-height: 1.2;
    }
    .client-domain {
        font-size: 0.68rem;
        color: #fff;
        padding: 2px 10px;
        border-radius: 12px;
        font-weight: 500;
    }
    .profile-header .links {
        display: flex;
        align-items: center;
        flex-wrap: wrap;
        gap: 4px;
    }
    .profile-header .links a, .profile-header .links span {
        color: #B8D4E3;
        text-decoration: none;
        font-size: 0.82rem;
        white-space: nowrap;
    }
    .profile-header .links .sep {
        color: rgba(255,255,255,0.3);
        font-size: 0.75rem;
    }
    .profile-header .links img.social-icon {
        width: 18px;
        height: 18px;
        vertical-align: middle;
        filter: brightness(0) invert(0.8);
        transition: filter 0.2s;
    }
    .profile-header .links a:hover img.social-icon {
        filter: brightness(0) invert(1);
    }
    .profile-header .links a.calendly-btn {
        background: #fff;
        color: #065A82;
        font-weight: 700;
        padding: 6px 16px;
        border-radius: 20px;
        font-size: 0.82rem;
        transition: all 0.25s ease;
        box-shadow: 0 2px 8px rgba(0,0,0,0.15);
    }
    .profile-header .links a.calendly-btn:hover {
        background: #E8F5E9;
        color: #1C7C54;
        box-shadow: 0 4px 14px rgba(0,0,0,0.2);
        transform: translateY(-1px);
    }

    /* Info cards */
    .info-card {
        background: #FFFFFF;
        border: 1px solid #E9ECEF;
        border-radius: 10px;
        padding: 18px 22px;
        margin-bottom: 12px;
        box-shadow: 0 1px 2px rgba(0,0,0,0.04);
    }
    .info-card h5 {
        color: #1B3A4B;
        margin: 0 0 4px 0;
        font-size: 0.95rem;
    }
    .info-card .detail {
        color: #6C757D;
        font-size: 0.85rem;
    }

    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 12px;
        background: linear-gradient(135deg, #F0F7FA 0%, #E8F5E9 100%);
        padding: 8px 12px;
        border-radius: 14px;
        border: 1px solid #DEE2E6;
        justify-content: center;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 12px 32px;
        font-weight: 700;
        font-size: 1.05rem;
        border-radius: 10px;
        color: #1B3A4B;
        background: transparent;
        border: none;
        transition: all 0.25s ease;
    }
    .stTabs [data-baseweb="tab"]:hover {
        background: rgba(6, 90, 130, 0.08);
        color: #065A82;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #065A82, #1C7C54) !important;
        color: #fff !important;
        box-shadow: 0 4px 12px rgba(6, 90, 130, 0.3);
        border-radius: 10px;
    }
    .stTabs [data-baseweb="tab-highlight"] {
        display: none;
    }
    .stTabs [data-baseweb="tab-border"] {
        display: none;
    }

    /* Chat enhancements */
    .suggested-q {
        display: inline-block;
        border: 1px solid #DEE2E6;
        border-radius: 20px;
        padding: 6px 14px;
        margin: 4px;
        font-size: 0.82rem;
        color: #065A82;
        cursor: pointer;
        background: #F8F9FA;
    }
    .genie-banner {
        background: linear-gradient(135deg, #065A82, #1C7C54);
        color: white;
        padding: 20px 28px;
        border-radius: 12px;
        margin-bottom: 20px;
    }
    .genie-banner h3 { margin: 0 0 6px 0; }
    .genie-banner p { margin: 0; opacity: 0.85; font-size: 0.9rem; }

    /* ─── Mobile Responsive ─── */
    @media (max-width: 768px) {
        .block-container {
            padding-top: 0.5rem;
            padding-left: 0.8rem;
            padding-right: 0.8rem;
        }
        .profile-header {
            padding: 18px 16px;
            border-radius: 10px;
            margin-bottom: 14px;
        }
        .profile-header-inner {
            flex-direction: column;
            align-items: center;
            gap: 12px;
        }
        .profile-header h1 {
            font-size: 1.4rem;
            text-align: center;
        }
        .profile-header .headline {
            font-size: 0.78rem;
            text-align: center;
        }
        .profile-header .location {
            font-size: 0.75rem;
            text-align: center;
        }
        .profile-photo {
            width: 80px;
            height: 80px;
        }
        .profile-info-col {
            width: 100%;
            text-align: center;
        }
        .cert-images-col {
            justify-content: center;
            width: 100%;
        }
        .cert-badge-img {
            height: 60px;
        }
        .profile-header .links {
            justify-content: center;
            gap: 3px;
            text-align: center;
        }
        .profile-header .links a,
        .profile-header .links span {
            font-size: 0.7rem;
        }
        .profile-header .links br {
            display: none;
        }
        .profile-header .links img.social-icon {
            width: 14px;
            height: 14px;
        }

        .section-header {
            font-size: 1rem;
            margin-top: 1rem;
        }

        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {
            gap: 4px;
            padding: 4px 6px;
            flex-wrap: wrap;
        }
        .stTabs [data-baseweb="tab"] {
            padding: 8px 10px;
            font-size: 0.72rem;
        }

        /* Experience cards */
        .exp-card {
            padding: 14px;
        }
        .exp-card h3 {
            font-size: 0.95rem;
        }
        .exp-card .highlight {
            font-size: 0.8rem;
        }

        /* Skills grid */
        .sk-grid {
            grid-template-columns: 1fr !important;
        }
        .sk-card {
            margin-bottom: 8px;
        }
        .sk-name {
            font-size: 0.72rem !important;
        }
        .sk-val {
            font-size: 0.58rem !important;
            min-width: 60px !important;
        }

        /* Clients grid */
        .clients-grid {
            grid-template-columns: repeat(auto-fill, minmax(100px, 1fr));
            gap: 8px;
        }
        .client-card {
            padding: 10px 6px 8px;
        }
        .client-name {
            font-size: 0.7rem;
        }
        .client-logo, .client-logo-fallback {
            height: 36px;
            width: 36px;
        }

        /* Genie chat */
        .genie-banner {
            padding: 14px 16px;
        }
        .genie-banner h3 {
            font-size: 0.9rem;
        }
        .genie-banner p {
            font-size: 0.75rem;
        }
    }

    @media (max-width: 480px) {
        .profile-header h1 {
            font-size: 1.15rem;
        }
        .profile-header .headline {
            font-size: 0.7rem;
        }
        .cert-badge-img {
            height: 45px;
        }
        .stTabs [data-baseweb="tab"] {
            padding: 6px 8px;
            font-size: 0.65rem;
        }
    }
</style>
""")


# ────────────────────────────────────────────────────────────────
# Data Loading — Databricks SQL with JSON fallback
# ────────────────────────────────────────────────────────────────

@st.cache_resource
def get_workspace_client():
    if not HAS_DATABRICKS_SDK:
        st.session_state["_wsc_error"] = "databricks-sdk not installed"
        return None
    try:
        host = _get_config("host", "")
        token = _get_config("token", "")
        if host and token:
            return WorkspaceClient(host=host, token=token)
        return WorkspaceClient()
    except Exception as e:
        st.session_state["_wsc_error"] = str(e)
        return None


@st.cache_data(ttl=60)
def load_resume_json():
    if DATA_FILE.exists():
        with open(DATA_FILE) as f:
            return json.load(f)
    return None


def query_sql(sql_statement):
    """Execute SQL via Databricks Statement Execution API."""
    w = get_workspace_client()
    if not w or not WAREHOUSE_ID:
        return None

    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            catalog=CATALOG,
            schema=SCHEMA,
            statement=sql_statement,
            wait_timeout="30s",
        )
        if result.status and result.status.state and result.status.state.value == "SUCCEEDED":
            columns = [c.name for c in result.manifest.schema.columns]
            rows = result.result.data_array if result.result else []
            return pd.DataFrame(rows, columns=columns)
    except Exception:
        pass
    return None


def _json_to_df(data, table):
    """Convert resume JSON section to DataFrame matching table schema."""
    raw = data.get(table, [])
    if isinstance(raw, dict):
        raw = [raw]

    if table == "work_highlights":
        rows = []
        hid = 1
        for i, exp in enumerate(data.get("work_experience", []), 1):
            for h in exp.get("highlights", []):
                rows.append({
                    "highlight_id": hid, "experience_id": i,
                    "company": exp["company"], "title": exp.get("title_at_employer", exp.get("title", "")),
                    "highlight": h["description"], "category": h["category"],
                    "impact_metric": h["impact_metric"],
                })
                hid += 1
        return pd.DataFrame(rows)

    if table == "work_experience":
        rows = []
        for i, exp in enumerate(raw, 1):
            start = datetime.strptime(exp["start_date"], "%Y-%m-%d")
            end = datetime.now() if not exp.get("end_date") else datetime.strptime(exp["end_date"], "%Y-%m-%d")
            rows.append({
                "experience_id": i, "company": exp["company"],
                "title": exp.get("title_at_employer", exp.get("title", "")),
                "role_at_customer": exp.get("role_at_customer", ""),
                "location": exp["location"],
                "employment_type": exp.get("employment_type", "Full-time"),
                "start_date": exp["start_date"],
                "end_date": exp.get("end_date") or "Present",
                "is_current_role": exp.get("is_current", False),
                "industry": exp.get("industry", ""),
                "team_size_managed": exp.get("team_size_managed", 0),
                "description": exp.get("description", ""),
                "duration_months": (end.year - start.year) * 12 + (end.month - start.month),
            })
        return pd.DataFrame(rows)

    if table == "career_timeline":
        rows = []
        tid = 1
        for exp in data.get("work_experience", []):
            rows.append({
                "timeline_id": tid, "event_type": "Work",
                "title": exp.get("title_at_employer", exp.get("title", "")), "organization": exp["company"],
                "start_date": exp["start_date"],
                "end_date": exp.get("end_date") or "Present",
                "is_current": exp.get("is_current", False),
                "location": exp.get("location"), "category": exp.get("industry", ""),
            })
            tid += 1
        for edu in data.get("education", []):
            rows.append({
                "timeline_id": tid, "event_type": "Education",
                "title": f"{edu['degree']} in {edu['field_of_study']}",
                "organization": edu["institution"],
                "start_date": edu["start_date"], "end_date": edu["end_date"],
                "is_current": False, "location": None, "category": "Academia",
            })
            tid += 1
        return pd.DataFrame(rows)

    if table == "skills":
        rows = []
        for i, s in enumerate(raw, 1):
            rows.append({
                "skill_id": i, "skill_name": s["skill_name"],
                "category": s["category"], "proficiency_level": s["proficiency"],
                "years_of_experience": s["years_used"],
            })
        return pd.DataFrame(rows)

    if table == "certifications":
        rows = []
        for i, c in enumerate(raw, 1):
            active = True
            if c.get("expiry_date"):
                active = c["expiry_date"] >= datetime.now().strftime("%Y-%m-%d")
            rows.append({
                "certification_id": i, "certification_name": c["name"],
                "issuing_organization": c["issuing_organization"],
                "issue_date": c["issue_date"],
                "expiry_date": c.get("expiry_date"),
                "is_active": active,
            })
        return pd.DataFrame(rows)

    if table == "clients":
        rows = []
        for i, c in enumerate(raw, 1):
            rows.append({
                "client_id": i, "client_name": c["name"],
                "domain": c["domain"], "logo_url": c.get("logo", ""),
            })
        return pd.DataFrame(rows)

    if table == "projects":
        rows = []
        for i, p in enumerate(raw, 1):
            rows.append({
                "project_id": i, "project_name": p["name"],
                "description": p["description"], "role": p["role"],
                "client": p.get("client", ""),
                "technologies_used": p["technologies"],
                "start_date": p["start_date"],
                "end_date": p.get("end_date") or "Present",
                "is_current": p.get("is_current", False),
                "impact": p["impact"],
                "url": p.get("url") or "",
            })
        return pd.DataFrame(rows)

    if table == "education":
        rows = []
        for i, e in enumerate(raw, 1):
            rows.append({
                "education_id": i, "institution": e["institution"],
                "degree": e["degree"], "field_of_study": e["field_of_study"],
                "start_date": e["start_date"], "end_date": e["end_date"],
                "gpa": e.get("gpa"), "honors": e.get("honors", ""),
                "relevant_coursework": e.get("relevant_coursework", ""),
            })
        return pd.DataFrame(rows)

    if table == "publications":
        rows = []
        for i, p in enumerate(raw, 1):
            rows.append({
                "publication_id": i, "title": p["title"],
                "publisher": p["publisher"], "publication_date": p["date"],
                "publication_type": p["type"], "url": p.get("url"),
            })
        return pd.DataFrame(rows)

    return pd.DataFrame(raw if isinstance(raw, list) else [raw])


@st.cache_data(ttl=60)
def load_table(table_name):
    """Load a table: try Databricks first, fall back to local JSON."""
    df = query_sql(f"SELECT * FROM {table_name}")
    if df is not None and not df.empty:
        return df
    data = load_resume_json()
    if data:
        return _json_to_df(data, table_name)
    return pd.DataFrame()


# ────────────────────────────────────────────────────────────────
# Genie Q&A Engine
# ────────────────────────────────────────────────────────────────

# Intent patterns: (keywords, intent_key)
_INTENT_PATTERNS = [
    (["tell me about", "who is", "summary", "overview", "introduce", "about this candidate", "about yourself"], "profile"),
    (["current role", "current job", "currently", "right now", "present role", "working now", "current position"], "current_role"),
    (["top skill", "best skill", "strongest", "expert skill", "main skill", "key skill"], "top_skills"),
    (["technical skill", "tech skill", "programming", "technology", "technologies", "tech stack", "tools"], "technical_skills"),
    (["all skill", "skill", "what skills", "list skill", "proficien"], "all_skills"),
    (["experience with", "how many years", "years of experience with", "how long", "worked with"], "skill_lookup"),
    (["career progression", "career journey", "career timeline", "career path", "career history", "progression", "timeline"], "timeline"),
    (["work experience", "job history", "employment", "work history", "where has krish worked", "where have they worked", "previous job", "past role"], "work_history"),
    (["achievement", "accomplish", "impact", "highlight", "result", "deliver"], "achievements"),
    (["leadership", "manage", "team", "led", "lead", "mentor", "direct report"], "leadership"),
    (["education", "degree", "university", "school", "college", "academic", "studied", "gpa", "major"], "education"),
    (["certification", "certified", "credential", "certificate"], "certifications"),
    (["project", "built", "designed", "architected", "implemented"], "projects"),
    (["publication", "talk", "blog", "article", "publish", "conference", "wrote", "written", "speak"], "publications"),
    (["industry", "industries", "sector", "domain", "worked in"], "industries"),
    (["databricks", "spark", "delta", "unity catalog", "delta live", "lakehouse"], "databricks_skills"),
    (["cloud", "aws", "azure", "gcp", "google cloud"], "cloud_skills"),
    (["python", "sql", "scala", "java", "programming language", "language", "code"], "programming"),
    (["salary", "compensation", "pay", "money"], "not_available"),
    (["contact", "email", "phone", "linkedin", "github", "reach", "hire"], "contact"),
    (["relocate", "remote", "hybrid", "on-site", "work model", "location", "where"], "work_preferences"),
    (["cost saving", "saved", "revenue", "business impact", "roi", "dollar", "\\$"], "business_impact"),
    (["real-time", "streaming", "kafka", "real time", "event"], "streaming"),
    (["ml", "machine learning", "mlflow", "model", "ai ", "artificial intelligence", "feature store"], "ml_skills"),
]

# ── Recruiter FAQ: keyword triggers → (answer, suggested_sql) ──
_RECRUITER_FAQ = [
    {
        "keywords": ["why should we hire", "why hire", "why krish", "why is krish a good fit", "why this candidate", "what makes krish special", "what sets krish apart", "differentiator", "differentiate", "standout", "stand out", "unique about"],
        "answer": (
            "**Why Krish?** Here are the top reasons:\n\n"
            "1. **Proven Deal Winner** — Delivered a live Databricks AI/BI Genie demo that won the Capital Group engagement against Accenture, KPMG, Capgemini, and Infosys.\n"
            "2. **19 Years of Data Engineering** — Deep expertise across Databricks, AWS, Hadoop, Spark, and the full modern data stack.\n"
            "3. **10 Databricks Certifications** — Including Data Engineer Professional, ML Associate, and GenAI Engineer Associate.\n"
            "4. **Built a CoE from Scratch** — Established a Databricks Center of Excellence at Lumenalta, trained 50+ engineers, and achieved Silver Partner status.\n"
            "5. **Pre-Sales & Architecture** — 6+ years translating complex technical capabilities into business outcomes for executive stakeholders.\n"
            "6. **Published Thought Leader** — Author of whitepapers and technical articles on Genie, RAG, dbt vs Databricks, and more.\n"
            "7. **Community Builder** — Admin of the First Coast Databricks User Group (Jacksonville, FL).\n"
            "8. **Enterprise Client Portfolio** — TD Bank, Moody's, J&J, Capital Group, Rocket Mortgage, Wells Fargo, BCBS, and more.\n"
            "9. **AWS re:Invent** — Serverless architecture for Moody's featured at AWS re:Invent."
        ),
    },
    {
        "keywords": ["biggest achievement", "greatest accomplishment", "proudest moment", "best accomplishment", "most impressive", "biggest win"],
        "answer": (
            "**Krish's biggest achievement:**\n\n"
            "Delivered a live Databricks AI/BI Genie demo during the Capital Group pre-sales engagement that **won the deal against Accenture, KPMG, Capgemini, and Infosys** — four of the largest consulting firms in the world. "
            "The POC with the Quants team was then converted into a full enterprise Databricks onboarding project for Capital Group's Enterprise Data Office.\n\n"
            "Other notable highlights:\n"
            "- Built a Databricks Center of Excellence from zero, certifying 50+ engineers\n"
            "- Serverless architecture for Moody's **featured at AWS re:Invent**\n"
            "- Achieved **3x throughput improvement** for J&J's Spark pipelines\n"
            "- Delivered **40% query time reduction** for Guardian Life\n"
            "- Led Essent Mortgage's 15-year legacy migration: **50% faster processing, 35% cost savings**"
        ),
    },
    {
        "keywords": ["notice period", "availability", "when can krish start", "when can he start", "start date", "available to start", "how soon", "when available", "joining date"],
        "answer": (
            "For Krish's current availability and notice period, please reach out directly:\n\n"
            "- 📧 Email: thedatabrickster@gmail.com\n"
            "- 📱 Phone: 63 63 62 62 63\n"
            "- 📅 Book a meeting: https://calendly.com/thedatabrickster\n\n"
            "Krish is currently working as **Associate Director, Data & AI** at **Lumenalta** (since July 2024)."
        ),
    },
    {
        "keywords": ["visa", "work authorization", "work permit", "sponsorship", "green card", "h1b", "h-1b", "h1-b", "immigration", "authorized to work", "eligible to work", "legally"],
        "answer": (
            "**Work Authorization:** Canadian Citizen, Green Card EAD (GC expected May 2026).\n\n"
            "Krish is authorized to work in the United States and does not require visa sponsorship."
        ),
    },
    {
        "keywords": ["willing to relocate", "open to relocation", "relocate", "move to", "based out of", "work from"],
        "answer": (
            "**Location:** Jacksonville, FL\n\n"
            "**Willing to relocate:** Yes\n\n"
            "**Preferred work model:** Hybrid\n\n"
            "Krish is open to relocation and flexible on work arrangements."
        ),
    },
    {
        "keywords": ["how many years databricks", "databricks experience", "years in databricks", "long with databricks", "databricks expertise"],
        "answer": (
            "Krish has **6+ years of hands-on Databricks experience** across multiple enterprise clients:\n\n"
            "- **Capital Group** — AI/BI Dashboards, Genie, Quant model migration\n"
            "- **TD Bank** — Glue-to-Databricks migration, Unity Catalog, Ingestion/ETL frameworks\n"
            "- **Guardian Life** — Databricks environment optimization, 40% query time reduction\n"
            "- **J&J** — Spark pipeline optimization, 3x throughput improvement\n"
            "- **FIS Global** — Terraform-based Databricks infrastructure provisioning\n\n"
            "He holds **4 Databricks certifications**: Data Engineer Associate, Data Engineer Professional, ML Associate, and GenAI Engineer Associate, plus 6 additional Databricks accreditations."
        ),
    },
    {
        "keywords": ["what clients", "which clients", "which companies", "client list", "client portfolio", "who has krish worked", "companies worked", "customers served"],
        "answer": (
            "Krish has served **20+ enterprise clients** across Financial Services, Insurance, Healthcare, Manufacturing, Government, and Technology:\n\n"
            "**Databricks Engagements:** Capital Group, TD Bank, Guardian Life, J&J, FIS Global, BondCliQ\n\n"
            "**AWS Engagements:** Moody's (architecture featured at AWS re:Invent), Essent Mortgage, Rocket Mortgage\n\n"
            "**Other Enterprise Clients:** BCBS IL, Nissan North America, Wells Fargo, Horace Mann, "
            "Illinois Board of Education, Illinois Office of Comptroller, CareFusion, Caterpillar, Henkel, Microsoft, SuperValu\n\n"
            "**Technology Partners:** Databricks, AWS, Hortonworks"
        ),
    },
    {
        "keywords": ["pre-sales", "presales", "pre sales", "sales engineer", "demo experience", "poc experience", "customer facing", "client facing"],
        "answer": (
            "**Yes!** Krish has **6+ years of pre-sales and customer-facing experience:**\n\n"
            "- Delivered a live **Databricks AI/BI Genie demo** that won the Capital Group deal against 4 major competitors\n"
            "- Delivers pre-sales demos on **Genie, AI/BI Dashboards, Databricks Apps, and AgentBricks**\n"
            "- Executes **POC delivery** — converted the Capital Group POC into a full enterprise project\n"
            "- Conducts **technical workshops and client enablement** sessions\n"
            "- Skilled in **stakeholder & executive communication**, translating technical capabilities into business outcomes\n"
            "- Experience with **GTM & territory planning** alongside Account Executives\n"
            "- **GSI & partner collaboration** (Databricks Silver Partner achievement)"
        ),
    },
    {
        "keywords": ["team size", "how many people", "direct reports", "manage a team", "team management", "managed team", "people manager", "management experience"],
        "answer": (
            "Krish currently manages a team of **8** at Lumenalta as Associate Director, Data & AI.\n\n"
            "**Leadership highlights:**\n"
            "- Built a **Databricks Center of Excellence** from scratch with training programs and delivery frameworks\n"
            "- Drove **50+ Databricks certifications** and produced **2 Databricks MVPs**\n"
            "- Championed the Databricks partnership to achieve **Silver Partner** status\n"
            "- 8+ years of team leadership and mentoring experience"
        ),
    },
    {
        "keywords": ["what industries", "industry experience", "sectors", "domains", "verticals"],
        "answer": (
            "Krish has experience across **8+ industries:**\n\n"
            "- 🏦 **Financial Services** — Capital Group, TD Bank, Wells Fargo, Moody's, FIS Global, BondCliQ\n"
            "- 🛡️ **Insurance** — Guardian Life, BCBS IL, Essent Mortgage, Horace Mann\n"
            "- 🏥 **Healthcare** — J&J, CareFusion\n"
            "- 🏭 **Manufacturing** — Nissan North America, Caterpillar\n"
            "- 🏛️ **Government** — Illinois Office of Comptroller, Illinois Board of Education\n"
            "- 🏠 **Mortgage** — Rocket Mortgage, Essent Mortgage, Wells Fargo\n"
            "- 🛒 **Retail** — SuperValu, Henkel\n"
            "- 💻 **Technology** — Microsoft, Databricks, AWS"
        ),
    },
    {
        "keywords": ["genie experience", "ai/bi genie", "aibi genie", "genie demo", "natural language", "genie space"],
        "answer": (
            "Krish is one of the foremost practitioners of **Databricks AI/BI Genie:**\n\n"
            "- **Won Capital Group** engagement with a live Genie demo against Accenture, KPMG, Capgemini, and Infosys\n"
            "- Implementing **Genie Spaces** for power users in Capital Group's Enterprise Data Office\n"
            "- Delivers pre-sales demos on Genie to win new client engagements\n"
            "- Published a **2-part comprehensive guide** on AI/BI Genie covering basics, advanced features, security, privacy, LLMs, architecture internals, and workflows\n"
            "- Built this very resume website with an **Abu chatbot powered by Databricks Genie** 🐒\n"
            "- Admin of the **First Coast Databricks User Group**, actively demoing Genie to the community"
        ),
    },
    {
        "keywords": ["unity catalog experience", "unity catalog", "data governance experience", "governance"],
        "answer": (
            "Krish has **3+ years of Unity Catalog and data governance expertise:**\n\n"
            "- **TD Bank** — Onboarded Unity Catalog for the Data Governance team, establishing centralized access controls, lineage tracking, and audit policies\n"
            "- **Capital Group** — Implementing Unity Catalog as part of enterprise Databricks platform architecture\n"
            "- Expert in data lineage, access control, governance, and auditing\n"
            "- Published thought leadership on governance best practices"
        ),
    },
    {
        "keywords": ["conference", "speaking", "speaker", "presented at", "reinvent", "re:invent", "public speaking", "talk"],
        "answer": (
            "Krish's serverless data architecture for Moody's Analytics was **featured at AWS re:Invent**:\n\n"
            "- 🎤 **AWS re:Invent** — Serverless architecture using Lambda, Glue, Step Functions & DynamoDB\n"
            "  ▶️ Watch: https://www.youtube.com/watch?v=tyM3OHT_0M8\n\n"
            "Krish also regularly delivers:\n"
            "- Pre-sales demos and workshops on Databricks Genie, AI/BI Dashboards, and AgentBricks\n"
            "- Technical enablement sessions for enterprise clients\n"
            "- Community presentations as Admin of the First Coast Databricks User Group"
        ),
    },
    {
        "keywords": ["compensation", "salary", "pay", "ctc", "rate", "billing", "hourly", "expected salary", "salary expectation"],
        "answer": "Compensation details are not included in this resume. Please reach out directly to discuss:\n\n- 📧 thedatabrickster@gmail.com\n- 📅 Book a meeting: https://calendly.com/thedatabrickster",
    },
    {
        "keywords": ["contract or full", "full-time or contract", "employment type", "open to contract", "w2 or c2c", "w2", "c2c", "1099", "contract to hire"],
        "answer": (
            "Krish is currently employed **full-time** as Associate Director, Data & AI at Lumenalta.\n\n"
            "For questions about engagement preferences (full-time, contract, etc.), please reach out directly:\n\n"
            "- 📧 thedatabrickster@gmail.com\n"
            "- 📅 Book a meeting: https://calendly.com/thedatabrickster"
        ),
    },
    {
        "keywords": ["strengths", "strong suit", "what is krish good at", "core competenc", "key strengths", "best at"],
        "answer": (
            "**Krish's core strengths:**\n\n"
            "1. **Databricks Platform Mastery** — 10 certifications, 6+ years hands-on across engineering, analytics, ML, and governance\n"
            "2. **Pre-Sales & Deal Winning** — Won Capital Group over 4 global SI competitors with a live Genie demo\n"
            "3. **Solution Architecture** — End-to-end design of lakehouse, serverless, and streaming architectures\n"
            "4. **Client Delivery** — 20+ enterprise clients across 8+ industries with measurable impact\n"
            "5. **Leadership & CoE Building** — Built a Databricks CoE, trained 50+ engineers, achieved Silver Partner\n"
            "6. **Technical Communication** — Published author, community leader, work featured at AWS re:Invent\n"
            "7. **Full Stack Data** — ETL/ELT, Spark, Delta Lake, Unity Catalog, MLflow, GenAI, RAG, Genie"
        ),
    },
    {
        "keywords": ["coe", "center of excellence", "centre of excellence", "practice build"],
        "answer": (
            "**Yes!** Krish built a **Databricks Center of Excellence from scratch** at Lumenalta:\n\n"
            "- Created training programs, best practices, and delivery frameworks\n"
            "- Drove company-wide certification initiative: **50+ certified employees** and **2 Databricks MVPs**\n"
            "- Championed the Databricks partnership: elevated Lumenalta from no partnership to **Silver Partner** status\n"
            "- Established reusable accelerators for pre-sales demos, POC delivery, and client enablement"
        ),
    },
    {
        "keywords": ["aws experience", "amazon web services", "cloud experience aws"],
        "answer": (
            "Krish has **8+ years of AWS experience** across multiple enterprise clients:\n\n"
            "- **Moody's** — Designed fully serverless architecture using Lambda, Glue, Step Functions, DynamoDB. **Featured at AWS re:Invent**\n"
            "- **Essent Mortgage** — Led 15-year legacy migration to AWS (50% faster processing, 35% cost savings)\n"
            "- **Rocket Mortgage** — Migrated Hortonworks to AWS Glue, drove org-wide Glue adoption\n"
            "- **TD Bank** — AWS Glue-to-Databricks migration\n\n"
            "Expert in: AWS Glue, Lambda, Step Functions, EMR, Redshift, S3, DynamoDB, and Databricks on AWS."
        ),
    },
    {
        "keywords": ["download resume", "pdf resume", "resume pdf", "resume document", "send resume", "resume copy"],
        "answer": (
            "This interactive website **is** Krish's resume! It includes all the information a traditional PDF would have, plus:\n\n"
            "- 🐒 **Abu chatbot** — Ask any question about Krish's background (powered by Databricks Genie)\n"
            "- 📊 **Interactive dashboard** — Career timeline, skills visualization, client grid, and more\n"
            "- 📅 **Book a meeting** — https://calendly.com/thedatabrickster\n\n"
            "For a direct conversation, reach out at thedatabrickster@gmail.com."
        ),
    },
    {
        "keywords": ["fit for solutions architect", "good for sa role", "solutions architect role", "sa position", "architect role"],
        "answer": (
            "**Krish is an excellent fit for a Solutions Architect role.** Here's why:\n\n"
            "- **6+ years in customer-facing pre-sales and consulting** — designing, demoing, and delivering solutions\n"
            "- **Won enterprise deals** against Accenture, KPMG, Capgemini, and Infosys through live technical demos\n"
            "- **End-to-end architecture** — Lakehouse, serverless, streaming, Delta Lake, Unity Catalog, AI/BI\n"
            "- **POC to production** — Converted Capital Group POC into full enterprise Databricks onboarding\n"
            "- **10 Databricks certifications** including Data Engineer Professional\n"
            "- **Work featured at AWS re:Invent** — serverless architecture for Moody's Analytics\n"
            "- **Published thought leader** with whitepapers and technical articles\n"
            "- **CoE builder** — built Databricks practice, trained 50+ engineers"
        ),
    },
]


def _check_recruiter_faq(question):
    q = question.lower().strip()
    for faq in _RECRUITER_FAQ:
        for kw in faq["keywords"]:
            if kw in q:
                return {"text": faq["answer"], "sql": None, "df": None,
                        "conversation_id": "faq", "status": "COMPLETED"}
    return None


def _detect_intent(question):
    q = question.lower().strip()
    for keywords, intent in _INTENT_PATTERNS:
        for kw in keywords:
            if kw in q:
                return intent, kw
    return "general", None


def _extract_skill_name(question):
    """Try to extract a specific technology/skill name from the question."""
    data = load_resume_json()
    if not data:
        return None
    q = question.lower()
    for s in data.get("skills", []):
        if s["skill_name"].lower() in q:
            return s["skill_name"]
    tech_aliases = {
        "spark": "Apache Spark", "k8s": "Kubernetes", "dbt": "dbt",
        "dlt": "Delta Live Tables", "uc": "Unity Catalog",
    }
    for alias, name in tech_aliases.items():
        if alias in q:
            return name
    return None


def _genie_ask_api(question, conversation_id=None):
    """Call Databricks Genie via REST API (avoids SDK version issues)."""
    import requests as _req

    host = _get_config("host", "")
    token = _get_config("token", "")
    if not host or not token or not GENIE_SPACE_ID:
        return None

    if conversation_id == "local":
        conversation_id = None

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    base = f"{host.rstrip('/')}/api/2.0/genie/spaces/{GENIE_SPACE_ID}"

    try:
        if conversation_id:
            r = _req.post(f"{base}/conversations/{conversation_id}/messages",
                          headers=headers, json={"content": question}, timeout=30)
            r.raise_for_status()
            d = r.json()
            conv_id = conversation_id
            msg_id = d.get("message_id") or d.get("id")
        else:
            r = _req.post(f"{base}/start-conversation",
                          headers=headers, json={"content": question}, timeout=30)
            r.raise_for_status()
            d = r.json()
            conv_id = d.get("conversation_id") or d.get("conversation", {}).get("id")
            msg_id = d.get("message_id") or d.get("message", {}).get("id")

        if not conv_id or not msg_id:
            st.session_state["_genie_api_error"] = f"Missing IDs: conv={conv_id}, msg={msg_id}"
            return None

        msg_url = f"{base}/conversations/{conv_id}/messages/{msg_id}"
        msg_data = None
        for _ in range(60):
            time.sleep(1)
            r2 = _req.get(msg_url, headers=headers, timeout=30)
            r2.raise_for_status()
            msg_data = r2.json()
            status = msg_data.get("status", "")
            if status in ("COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED",
                          "EXECUTING_QUERY", "ASKING_AI"):
                if status in ("COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"):
                    break
                has_text = any("text" in a for a in msg_data.get("attachments", []))
                if has_text:
                    break

        answer_text = ""
        sql_query = None
        result_df = None

        for att in msg_data.get("attachments", []):
            if "text" in att:
                answer_text += att["text"].get("content", "") + "\n"
            if "query" in att:
                sql_query = att["query"].get("query", "")
                row_count = att["query"].get("query_result_metadata", {}).get("row_count", 0)
                if row_count > 0:
                    try:
                        qr_url = f"{msg_url}/query-result"
                        qr = _req.get(qr_url, headers=headers, timeout=30)
                        qr.raise_for_status()
                        qr_data = qr.json()
                        sr = qr_data.get("statement_response", {})
                        cols = [c["name"] for c in sr.get("manifest", {}).get("schema", {}).get("columns", [])]
                        rows = sr.get("result", {}).get("data_array", [])
                        if cols and rows:
                            result_df = pd.DataFrame(rows, columns=cols)
                    except Exception:
                        pass

        if not answer_text:
            answer_text = msg_data.get("content", "")

        return {
            "text": answer_text.strip(), "sql": sql_query,
            "df": result_df, "conversation_id": conv_id,
            "status": msg_data.get("status", "COMPLETED"),
        }
    except Exception as e:
        st.session_state["_genie_api_error"] = f"{type(e).__name__}: {e}"
        return None


_GREETING_WORDS = {"hi", "hello", "hey", "howdy", "hola", "greetings", "sup", "yo",
                    "what's up", "whats up", "good morning", "good afternoon", "good evening"}
_SMALLTALK_PATTERNS = {"how are you", "how's it going", "how do you do", "what's going on",
                       "how are things", "how r u", "wassup", "how is it going", "nice to meet you",
                       "who are you", "what are you", "what can you do", "help", "thank you",
                       "thanks", "bye", "goodbye", "see you", "take care"}

def _is_greeting(question):
    q = question.strip().lower().rstrip("!.,? ")
    return q in _GREETING_WORDS

def _is_smalltalk(question):
    q = question.strip().lower().rstrip("!.,? ")
    if q in _SMALLTALK_PATTERNS:
        return True
    if any(q.startswith(p) for p in _SMALLTALK_PATTERNS):
        return True
    if any(p in q for p in ["how are", "how r u", "how do you", "who are you",
                             "what are you", "what can you", "nice to meet",
                             "thank", "bye", "goodbye", "see you", "take care"]):
        return True
    return False

def _smalltalk_response(question):
    q = question.strip().lower()
    if any(w in q for w in ["how are you", "how's it going", "how do you do", "how r u"]):
        text = (
            "I'm doing great, thanks for asking! 🐒✨ Just swinging through Krish's career data, "
            "polishing those 19+ years of experience until they shine brighter than the Cave of Wonders.\n\n"
            "But enough about me — I'm here for *you*! What would you like to know about Krish? "
            "Skills, experience, certifications, projects — you name it, I'll fetch it! 🪄"
        )
    elif any(w in q for w in ["who are you", "what are you", "what can you do"]):
        text = (
            "I'm **Abu** 🐒 — Krish Kilaru's AI Career Assistant, powered by the **Databricks AI/BI Genie** 🧞!\n\n"
            "I can answer questions about Krish's:\n"
            "- 💼 **Work experience** across 12+ enterprise clients\n"
            "- 🛠 **Technical skills** (Databricks, Spark, AWS, and more)\n"
            "- 🎓 **Education** & **Certifications** (10 and counting!)\n"
            "- 📝 **Publications** & thought leadership\n"
            "- 🏆 **Key achievements** and impact metrics\n\n"
            "Ask away — your wish is my command! 🪄"
        )
    elif any(w in q for w in ["thank", "thanks"]):
        text = (
            "You're welcome! 🐒 Happy to help! If you have more questions about Krish's experience, "
            "I'm always here — unlike a regular resume, I never get filed away in a drawer. 😄"
        )
    elif any(w in q for w in ["bye", "goodbye", "see you", "take care"]):
        text = (
            "See you later! 👋 Remember, Krish is always open to connecting — "
            "reach out on [LinkedIn](https://www.linkedin.com/in/brickster/) anytime.\n\n"
            "As the Genie would say: *\"You ain't never had a friend like me!\"* 🧞✨"
        )
    elif "help" in q:
        text = (
            "Sure thing! Here's what you can ask me:\n\n"
            "🔹 **\"What are Krish's top skills?\"**\n"
            "🔹 **\"Tell me about his work at Capital Group\"**\n"
            "🔹 **\"What certifications does he hold?\"**\n"
            "🔹 **\"What companies has he worked with?\"**\n"
            "🔹 **\"What's his education background?\"**\n"
            "🔹 **\"Show me his publications\"**\n\n"
            "Just type a question and I'll dig through the data for you! 🐒🔍"
        )
    else:
        text = (
            "Hey! I'm **Abu** 🐒 — Krish Kilaru's AI Career Assistant, powered by **Databricks AI/BI Genie** 🧞.\n\n"
            "You can ask me anything about Krish's career! For example:\n\n"
            "- \"What are his top technical skills?\"\n"
            "- \"Which clients has he worked with?\"\n"
            "- \"Tell me about his Databricks experience\"\n"
            "- \"What certifications does he hold?\"\n\n"
            "Go ahead, your wish is my command! 🪄"
        )
    return {
        "text": text, "sql": None, "df": None,
        "conversation_id": None, "status": "COMPLETED", "source": "greeting",
    }

def _get_time_greeting():
    """Return time-of-day greeting based on server time (US Eastern approx)."""
    hour = datetime.now().hour
    if 5 <= hour < 12:
        return "Good morning"
    if 12 <= hour < 17:
        return "Good afternoon"
    if 17 <= hour < 21:
        return "Good evening"
    return "Hey there, night owl"


def _greeting_response():
    time_greet = _get_time_greeting()
    return {
        "text": (
            f"**{time_greet}!** I'm **Abu** 🐒 — Krish's trusty sidekick, here to guide you through "
            "the cave of wonders that is his career!\n\n"
            "I'm powered by the **Databricks AI/BI Genie** 🧞 (yes, *that* Genie — phenomenal "
            "cosmic powers, itty-bitty SQL warehouse), and I have access to Krish's entire "
            "career profile — 19+ years of data engineering, Databricks wizardry, and enough "
            "certifications to fill Aladdin's treasure room.\n\n"
            "**Rub the lamp and ask me things like:**\n"
            "- \"What are Krish's top skills?\"\n"
            "- \"Tell me about his Databricks experience\"\n"
            "- \"Which companies has he worked with?\"\n"
            "- \"What certifications does he hold?\"\n"
            "- \"What did he do at Capital Group?\"\n\n"
            "Go ahead — your wish is my command! (Well, up to three... just kidding, ask as many as you want.) 🪄"
        ),
        "sql": None, "df": None, "conversation_id": None,
        "status": "COMPLETED", "source": "greeting",
    }

def genie_ask(question, conversation_id=None):
    """Handle greetings/smalltalk locally, then try Genie API, then fall back to local Q&A."""
    if _is_greeting(question):
        return _greeting_response()
    if _is_smalltalk(question):
        return _smalltalk_response(question)

    faq_result = _check_recruiter_faq(question)
    if faq_result:
        faq_result["source"] = "faq"
        return faq_result

    api_result = _genie_ask_api(question, conversation_id)
    if api_result and api_result.get("text"):
        genie_text = api_result["text"].lower()
        genie_unhelpful = [
            "unrelated to", "cannot answer", "not related",
            "cannot find", "no information", "not available",
            "i'm here to help you analyze", "please let me know what",
            "please ask a question about", "available tables",
            "don't have", "do not have", "not in the data",
        ]
        if any(phrase in genie_text for phrase in genie_unhelpful):
            faq_fallback = _check_recruiter_faq(question)
            if faq_fallback:
                faq_fallback["source"] = "faq"
                return faq_fallback
            local_result = _genie_ask_local(question)
            if local_result and local_result.get("text"):
                local_result["source"] = "local"
                return local_result
            return _smalltalk_response(question)
        api_result["source"] = "genie"
        return api_result

    local_result = _genie_ask_local(question)
    local_result["source"] = "local"
    return local_result


def _genie_ask_local(question):
    """Local Genie-style Q&A: detect intent, generate SQL, query DataFrames."""
    data = load_resume_json()
    if not data:
        return {"text": "No resume data found.", "sql": None, "df": None,
                "conversation_id": "local", "status": "ERROR"}

    intent, matched_kw = _detect_intent(question)
    profile = data.get("profile", {})
    name = profile.get("full_name", "this candidate")

    # Load DataFrames
    skills_df = load_table("skills")
    work_df = load_table("work_experience")
    highlights_df = load_table("work_highlights")
    edu_df = load_table("education")
    certs_df = load_table("certifications")
    projects_df = load_table("projects")
    pubs_df = load_table("publications")
    timeline_df = load_table("career_timeline")

    text = ""
    sql = ""
    df = None

    if intent == "profile":
        text = (f"**{name}** — {profile.get('headline', '')}\n\n"
                f"{profile.get('summary', '')}\n\n"
                f"📍 {profile.get('location_city', '')}, {profile.get('location_state', '')} · "
                f"{profile.get('years_of_experience', '')} years of experience · "
                f"Preferred: {profile.get('preferred_work_model', '')} · "
                f"{profile.get('work_authorization', '')}")
        sql = "SELECT full_name, headline, summary, location_city, location_state,\n       years_of_experience, preferred_work_model, work_authorization\nFROM profile;"

    elif intent == "current_role":
        if not work_df.empty:
            current = work_df[work_df["is_current_role"].astype(str).str.lower().isin(["true", "1"])]
            if not current.empty:
                r = current.iloc[0]
                text = (f"**{name}** is currently working as **{r['title']}** at **{r['company']}** "
                        f"in {r['location']}.\n\n*{r.get('description', '')}*")
                if not highlights_df.empty:
                    ch = highlights_df[highlights_df["experience_id"].astype(str) == str(r["experience_id"])]
                    if not ch.empty:
                        text += "\n\n**Key achievements in this role:**"
                        df = ch[["highlight", "category", "impact_metric"]].rename(
                            columns={"highlight": "Achievement", "category": "Category", "impact_metric": "Impact"})
                sql = ("SELECT w.company, w.title, w.location, w.start_date, w.description,\n"
                       "       h.highlight, h.category, h.impact_metric\n"
                       "FROM work_experience w\n"
                       "LEFT JOIN work_highlights h ON w.experience_id = h.experience_id\n"
                       "WHERE w.is_current_role = true;")
            else:
                text = "No current role found in the data."
        else:
            text = "No work experience data available."

    elif intent == "top_skills":
        if not skills_df.empty:
            s = skills_df.copy()
            s["years_of_experience"] = pd.to_numeric(s["years_of_experience"], errors="coerce")
            top = s[s["proficiency_level"] == "Expert"].sort_values("years_of_experience", ascending=False)
            if top.empty:
                top = s.sort_values("years_of_experience", ascending=False).head(10)
            text = f"Here are **{name}**'s top skills at **Expert** proficiency level:"
            df = top[["skill_name", "category", "proficiency_level", "years_of_experience"]].rename(
                columns={"skill_name": "Skill", "category": "Category",
                         "proficiency_level": "Proficiency", "years_of_experience": "Years"})
            sql = ("SELECT skill_name, category, proficiency_level, years_of_experience\n"
                   "FROM skills\n"
                   "WHERE proficiency_level = 'Expert'\n"
                   "ORDER BY years_of_experience DESC;")

    elif intent == "technical_skills":
        if not skills_df.empty:
            s = skills_df.copy()
            s["years_of_experience"] = pd.to_numeric(s["years_of_experience"], errors="coerce")
            tech = s[s["category"] != "Soft Skills"].sort_values("years_of_experience", ascending=False)
            text = f"Here are all **technical skills** for {name}:"
            df = tech[["skill_name", "category", "proficiency_level", "years_of_experience"]].rename(
                columns={"skill_name": "Skill", "category": "Category",
                         "proficiency_level": "Proficiency", "years_of_experience": "Years"})
            sql = ("SELECT skill_name, category, proficiency_level, years_of_experience\n"
                   "FROM skills\n"
                   "WHERE category != 'Soft Skills'\n"
                   "ORDER BY years_of_experience DESC;")

    elif intent == "all_skills":
        if not skills_df.empty:
            s = skills_df.copy()
            s["years_of_experience"] = pd.to_numeric(s["years_of_experience"], errors="coerce")
            text = f"{name} has **{len(s)} skills** across {s['category'].nunique()} categories:"
            df = s[["skill_name", "category", "proficiency_level", "years_of_experience"]].rename(
                columns={"skill_name": "Skill", "category": "Category",
                         "proficiency_level": "Proficiency", "years_of_experience": "Years"})
            sql = "SELECT skill_name, category, proficiency_level, years_of_experience\nFROM skills\nORDER BY years_of_experience DESC;"

    elif intent == "skill_lookup":
        skill_name = _extract_skill_name(question)
        if skill_name and not skills_df.empty:
            match = skills_df[skills_df["skill_name"].str.lower() == skill_name.lower()]
            if not match.empty:
                r = match.iloc[0]
                text = (f"{name} has **{r['years_of_experience']} years** of experience with "
                        f"**{r['skill_name']}** at **{r['proficiency_level']}** level.\n\n"
                        f"Category: {r['category']}")
                df = match[["skill_name", "category", "proficiency_level", "years_of_experience"]].rename(
                    columns={"skill_name": "Skill", "category": "Category",
                             "proficiency_level": "Proficiency", "years_of_experience": "Years"})
                sql = f"SELECT skill_name, category, proficiency_level, years_of_experience\nFROM skills\nWHERE LOWER(skill_name) = LOWER('{skill_name}');"
            else:
                text = f"No specific data found for '{skill_name}'. Here are all skills:"
                df = skills_df[["skill_name", "category", "proficiency_level", "years_of_experience"]].rename(
                    columns={"skill_name": "Skill", "category": "Category",
                             "proficiency_level": "Proficiency", "years_of_experience": "Years"})
                sql = "SELECT * FROM skills ORDER BY years_of_experience DESC;"
        else:
            text = "Here are all skills with years of experience:"
            if not skills_df.empty:
                s = skills_df.copy()
                s["years_of_experience"] = pd.to_numeric(s["years_of_experience"], errors="coerce")
                df = s.sort_values("years_of_experience", ascending=False)[
                    ["skill_name", "category", "proficiency_level", "years_of_experience"]].rename(
                    columns={"skill_name": "Skill", "category": "Category",
                             "proficiency_level": "Proficiency", "years_of_experience": "Years"})
            sql = "SELECT skill_name, category, proficiency_level, years_of_experience\nFROM skills\nORDER BY years_of_experience DESC;"

    elif intent == "timeline":
        if not timeline_df.empty:
            text = f"Here is **{name}**'s career timeline from earliest to most recent:"
            df = timeline_df.sort_values("start_date")[
                ["event_type", "title", "organization", "start_date", "end_date"]].rename(
                columns={"event_type": "Type", "title": "Title", "organization": "Organization",
                         "start_date": "Start", "end_date": "End"})
            sql = ("SELECT event_type, title, organization, start_date, end_date\n"
                   "FROM career_timeline\n"
                   "ORDER BY start_date;")

    elif intent == "work_history":
        if not work_df.empty:
            text = f"{name} has worked at **{len(work_df)} companies**:"
            df = work_df[["company", "title", "location", "start_date", "end_date", "industry", "duration_months"]].rename(
                columns={"company": "Company", "title": "Title", "location": "Location",
                         "start_date": "Start", "end_date": "End", "industry": "Industry",
                         "duration_months": "Months"})
            sql = ("SELECT company, title, location, start_date, end_date, industry, duration_months\n"
                   "FROM work_experience\n"
                   "ORDER BY start_date DESC;")

    elif intent == "achievements":
        if not highlights_df.empty:
            text = f"Here are **{name}**'s key achievements and impact metrics:"
            df = highlights_df[["company", "title", "highlight", "category", "impact_metric"]].rename(
                columns={"company": "Company", "title": "Role", "highlight": "Achievement",
                         "category": "Category", "impact_metric": "Impact"})
            sql = ("SELECT h.company, h.title AS role, h.highlight, h.category, h.impact_metric\n"
                   "FROM work_highlights h\n"
                   "ORDER BY h.experience_id, h.highlight_id;")

    elif intent == "leadership":
        text_parts = []
        if not work_df.empty:
            mgr = work_df[work_df["team_size_managed"].astype(int) > 0]
            if not mgr.empty:
                for _, r in mgr.iterrows():
                    text_parts.append(f"- **{r['title']}** at {r['company']}: managed a team of **{r['team_size_managed']}**")
        if not highlights_df.empty:
            lead_h = highlights_df[highlights_df["category"] == "Leadership"]
            if not lead_h.empty:
                text = f"Yes! {name} has leadership experience:\n\n" + "\n".join(text_parts) + "\n\n**Leadership achievements:**"
                df = lead_h[["company", "highlight", "impact_metric"]].rename(
                    columns={"company": "Company", "highlight": "Achievement", "impact_metric": "Impact"})
            else:
                text = f"{name}'s management experience:\n\n" + "\n".join(text_parts) if text_parts else "No explicit leadership data found."
        else:
            text = "\n".join(text_parts) if text_parts else "No leadership data found."
        sql = ("SELECT w.company, w.title, w.team_size_managed,\n"
               "       h.highlight, h.impact_metric\n"
               "FROM work_experience w\n"
               "LEFT JOIN work_highlights h ON w.experience_id = h.experience_id\n"
               "WHERE w.team_size_managed > 0 OR h.category = 'Leadership';")

    elif intent == "education":
        if not edu_df.empty:
            text = f"**{name}**'s educational background:"
            df = edu_df[["institution", "degree", "field_of_study", "gpa", "honors", "end_date"]].rename(
                columns={"institution": "Institution", "degree": "Degree", "field_of_study": "Field",
                         "gpa": "GPA", "honors": "Honors", "end_date": "Graduated"})
            sql = ("SELECT institution, degree, field_of_study, gpa, honors, end_date AS graduated\n"
                   "FROM education\n"
                   "ORDER BY end_date DESC;")

    elif intent == "certifications":
        if not certs_df.empty:
            active = certs_df[certs_df["is_active"].astype(str).str.lower().isin(["true", "1"])]
            n_active = len(active)
            text = f"{name} holds **{len(certs_df)} certifications** ({n_active} currently active):"
            df = certs_df[["certification_name", "issuing_organization", "issue_date", "expiry_date", "is_active"]].rename(
                columns={"certification_name": "Certification", "issuing_organization": "Issuer",
                         "issue_date": "Issued", "expiry_date": "Expires", "is_active": "Active"})
            sql = ("SELECT certification_name, issuing_organization, issue_date, expiry_date,\n"
                   "       CASE WHEN is_active THEN 'Active' ELSE 'Expired' END AS status\n"
                   "FROM certifications\n"
                   "ORDER BY issue_date DESC;")

    elif intent == "projects":
        if not projects_df.empty:
            text = f"{name} has worked on **{len(projects_df)} notable projects**:"
            df = projects_df[["project_name", "role", "technologies_used", "impact"]].rename(
                columns={"project_name": "Project", "role": "Role",
                         "technologies_used": "Technologies", "impact": "Impact"})
            sql = ("SELECT project_name, role, technologies_used, impact\n"
                   "FROM projects\n"
                   "ORDER BY start_date DESC;")

    elif intent == "publications":
        if not pubs_df.empty:
            text = f"{name} has **{len(pubs_df)} publications/talks**:"
            df = pubs_df[["title", "publisher", "publication_type", "publication_date"]].rename(
                columns={"title": "Title", "publisher": "Publisher",
                         "publication_type": "Type", "publication_date": "Date"})
            sql = ("SELECT title, publisher, publication_type, publication_date\n"
                   "FROM publications\n"
                   "ORDER BY publication_date DESC;")

    elif intent == "industries":
        if not work_df.empty:
            w = work_df.copy()
            w["duration_months"] = pd.to_numeric(w["duration_months"], errors="coerce")
            ind = w.groupby("industry").agg(
                total_months=("duration_months", "sum"),
                roles=("title", "count")
            ).reset_index()
            ind["years"] = (ind["total_months"] / 12).round(1)
            text = f"{name} has experience across **{len(ind)} industries**:"
            df = ind[["industry", "years", "roles"]].rename(
                columns={"industry": "Industry", "years": "Years", "roles": "Roles"})
            sql = ("SELECT industry,\n"
                   "       ROUND(SUM(duration_months) / 12.0, 1) AS years,\n"
                   "       COUNT(*) AS roles\n"
                   "FROM work_experience\n"
                   "GROUP BY industry\n"
                   "ORDER BY years DESC;")

    elif intent == "databricks_skills":
        db_keywords = ["databricks", "spark", "delta", "unity catalog", "delta live", "lakehouse", "mlflow"]
        if not skills_df.empty:
            mask = skills_df["skill_name"].str.lower().apply(lambda x: any(k in x.lower() for k in db_keywords))
            db_skills = skills_df[mask]
            if not db_skills.empty:
                text = f"{name}'s **Databricks ecosystem** expertise:"
                df = db_skills[["skill_name", "proficiency_level", "years_of_experience"]].rename(
                    columns={"skill_name": "Skill", "proficiency_level": "Proficiency",
                             "years_of_experience": "Years"})
            else:
                text = "No Databricks-specific skills found."
        if not highlights_df.empty:
            db_h = highlights_df[highlights_df["highlight"].str.lower().apply(
                lambda x: any(k in x for k in db_keywords))]
            if not db_h.empty:
                text += "\n\n**Related achievements:**"
                df2 = db_h[["company", "highlight", "impact_metric"]].rename(
                    columns={"company": "Company", "highlight": "Achievement", "impact_metric": "Impact"})
                if df is not None:
                    text += "\n\n**Skills:**"
                df = df2 if df is None else df
        sql = ("SELECT skill_name, proficiency_level, years_of_experience\n"
               "FROM skills\n"
               "WHERE LOWER(skill_name) LIKE '%databricks%'\n"
               "   OR LOWER(skill_name) LIKE '%spark%'\n"
               "   OR LOWER(skill_name) LIKE '%delta%'\n"
               "   OR LOWER(skill_name) LIKE '%unity catalog%'\n"
               "   OR LOWER(skill_name) LIKE '%mlflow%';")

    elif intent == "cloud_skills":
        cloud_kw = ["aws", "azure", "gcp", "google cloud", "cloud"]
        if not skills_df.empty:
            mask = skills_df["skill_name"].str.lower().apply(lambda x: any(k in x.lower() for k in cloud_kw))
            cloud = skills_df[mask]
            if not cloud.empty:
                text = f"{name}'s **cloud platform** experience:"
                df = cloud[["skill_name", "proficiency_level", "years_of_experience"]].rename(
                    columns={"skill_name": "Platform", "proficiency_level": "Proficiency",
                             "years_of_experience": "Years"})
            else:
                text = "No cloud-specific skills found."
        sql = ("SELECT skill_name, proficiency_level, years_of_experience\n"
               "FROM skills\n"
               "WHERE category = 'Cloud Platform'\n"
               "ORDER BY years_of_experience DESC;")

    elif intent == "programming":
        if not skills_df.empty:
            prog = skills_df[skills_df["category"] == "Programming"]
            if not prog.empty:
                text = f"{name}'s **programming languages**:"
                df = prog[["skill_name", "proficiency_level", "years_of_experience"]].rename(
                    columns={"skill_name": "Language", "proficiency_level": "Proficiency",
                             "years_of_experience": "Years"})
            else:
                text = "No programming languages found in the skills data."
        sql = ("SELECT skill_name, proficiency_level, years_of_experience\n"
               "FROM skills\n"
               "WHERE category = 'Programming'\n"
               "ORDER BY years_of_experience DESC;")

    elif intent == "contact":
        text = (f"**Contact {name}:**\n\n"
                f"- 📧 Email: {profile.get('email', 'N/A')}\n"
                f"- 📱 Phone: {profile.get('phone', 'N/A')}\n"
                f"- 🔗 LinkedIn: {profile.get('linkedin_url', 'N/A')}\n"
                f"- 💻 GitHub: {profile.get('github_url', 'N/A')}\n"
                f"- 🌐 Website: {profile.get('website_url', 'N/A')}")
        sql = "SELECT email, phone, linkedin_url, github_url, website_url\nFROM profile;"

    elif intent == "work_preferences":
        text = (f"**{name}**'s work preferences:\n\n"
                f"- 📍 Location: {profile.get('location_city', '')}, {profile.get('location_state', '')}\n"
                f"- 🏠 Preferred model: **{profile.get('preferred_work_model', 'N/A')}**\n"
                f"- ✈️ Willing to relocate: **{'Yes' if profile.get('willing_to_relocate') else 'No'}**\n"
                f"- 🛂 Work authorization: **{profile.get('work_authorization', 'N/A')}**")
        sql = ("SELECT location_city, location_state, preferred_work_model,\n"
               "       willing_to_relocate, work_authorization\n"
               "FROM profile;")

    elif intent == "business_impact":
        if not highlights_df.empty:
            biz = highlights_df[highlights_df["category"] == "Business"]
            if not biz.empty:
                text = f"{name}'s **business impact** achievements:"
                df = biz[["company", "title", "highlight", "impact_metric"]].rename(
                    columns={"company": "Company", "title": "Role",
                             "highlight": "Achievement", "impact_metric": "Impact"})
            else:
                text = "No business-category achievements found. Showing all achievements:"
                df = highlights_df[["company", "highlight", "category", "impact_metric"]].rename(
                    columns={"company": "Company", "highlight": "Achievement",
                             "category": "Category", "impact_metric": "Impact"})
        sql = ("SELECT company, title, highlight, impact_metric\n"
               "FROM work_highlights\n"
               "WHERE category = 'Business'\n"
               "ORDER BY experience_id;")

    elif intent == "streaming":
        results = []
        if not skills_df.empty:
            stream = skills_df[skills_df["skill_name"].str.lower().apply(
                lambda x: any(k in x for k in ["kafka", "streaming", "spark"]))]
            if not stream.empty:
                results.append(("Skills", stream[["skill_name", "proficiency_level", "years_of_experience"]].rename(
                    columns={"skill_name": "Skill", "proficiency_level": "Proficiency", "years_of_experience": "Years"})))
        if not highlights_df.empty:
            sh = highlights_df[highlights_df["highlight"].str.lower().apply(
                lambda x: any(k in x for k in ["streaming", "real-time", "real time", "kafka", "event"]))]
            if not sh.empty:
                results.append(("Achievements", sh[["company", "highlight", "impact_metric"]].rename(
                    columns={"company": "Company", "highlight": "Achievement", "impact_metric": "Impact"})))
        text = f"{name}'s **real-time/streaming** experience:"
        if results:
            df = results[0][1]
        sql = ("SELECT s.skill_name, s.proficiency_level, s.years_of_experience\n"
               "FROM skills s\n"
               "WHERE LOWER(s.skill_name) LIKE '%kafka%'\n"
               "   OR LOWER(s.skill_name) LIKE '%streaming%'\n"
               "   OR LOWER(s.skill_name) LIKE '%spark%';")

    elif intent == "ml_skills":
        if not skills_df.empty:
            ml = skills_df[skills_df["category"] == "AI/ML"]
            if not ml.empty:
                text = f"{name}'s **AI/ML** skills:"
                df = ml[["skill_name", "proficiency_level", "years_of_experience"]].rename(
                    columns={"skill_name": "Skill", "proficiency_level": "Proficiency",
                             "years_of_experience": "Years"})
            else:
                text = "No AI/ML skills found."
        sql = ("SELECT skill_name, proficiency_level, years_of_experience\n"
               "FROM skills\n"
               "WHERE category = 'AI/ML'\n"
               "ORDER BY years_of_experience DESC;")

    elif intent == "not_available":
        text = "Sorry, salary and compensation information is not included in this resume data."
        sql = ""

    else:
        # General fallback: search across all text fields
        q_lower = question.lower()
        found_items = []

        if not highlights_df.empty:
            mask = highlights_df["highlight"].str.lower().str.contains(q_lower, na=False)
            matches = highlights_df[mask]
            if not matches.empty:
                found_items.append(("Matching achievements", matches[["company", "highlight", "impact_metric"]].rename(
                    columns={"company": "Company", "highlight": "Achievement", "impact_metric": "Impact"})))

        if not skills_df.empty:
            mask = skills_df["skill_name"].str.lower().str.contains(q_lower, na=False)
            matches = skills_df[mask]
            if not matches.empty:
                found_items.append(("Matching skills", matches[["skill_name", "proficiency_level", "years_of_experience"]].rename(
                    columns={"skill_name": "Skill", "proficiency_level": "Proficiency", "years_of_experience": "Years"})))

        if not projects_df.empty:
            mask = (projects_df["project_name"].str.lower().str.contains(q_lower, na=False) |
                    projects_df["technologies_used"].str.lower().str.contains(q_lower, na=False) |
                    projects_df["description"].str.lower().str.contains(q_lower, na=False))
            matches = projects_df[mask]
            if not matches.empty:
                found_items.append(("Matching projects", matches[["project_name", "role", "impact"]].rename(
                    columns={"project_name": "Project", "role": "Role", "impact": "Impact"})))

        if found_items:
            text = f"Here's what I found related to your question:"
            df = found_items[0][1]
        else:
            text = (f"I couldn't find specific information matching that question. "
                    f"Try asking about **skills**, **experience**, **education**, "
                    f"**certifications**, **projects**, or **achievements**.\n\n"
                    f"Example: *\"What are Krish's top skills?\"* or *\"Tell me about Krish's Databricks experience\"*")
        sql = f"-- Full-text search across resume tables\n-- Query: '{question}'"

    return {"text": text, "sql": sql, "df": df,
            "conversation_id": "local", "status": "COMPLETED"}


# ────────────────────────────────────────────────────────────────
# Dashboard Components
# ────────────────────────────────────────────────────────────────

def render_profile_header(profile_df, certs_df=None):
    if profile_df.empty:
        return
    p = profile_df.iloc[0]
    name = p.get("full_name", "Your Name")
    headline = p.get("headline", "")
    city = p.get("location_city", "")
    state = p.get("location_state", "")
    location = f"{city}, {state}" if city else ""
    linkedin = p.get("linkedin_url", "")
    github = p.get("github_url", "")
    email = p.get("email", "")

    calendly = p.get("calendly_url", "")
    website = p.get("website_url", "")

    phone = p.get("phone", "")

    usergroup = p.get("usergroup_url", "")

    sep = '<span class="sep">|</span>'
    nav_parts = []
    if linkedin:
        nav_parts.append(f'<a href="{linkedin}" target="_blank"><img src="https://cdn-icons-png.flaticon.com/512/174/174857.png" class="social-icon" alt="LinkedIn" title="LinkedIn"></a>')
    if github:
        nav_parts.append(f'<a href="{github}" target="_blank"><img src="https://cdn-icons-png.flaticon.com/512/25/25231.png" class="social-icon" alt="GitHub" title="GitHub"></a>')
    if usergroup:
        nav_parts.append(f'<a href="{usergroup}" target="_blank">🧱 First Coast Databricks User Group</a>')
    nav_html = f' {sep} '.join(nav_parts)

    contact_parts = []
    if email:
        contact_parts.append(f'<a href="mailto:{email}">📧 {email}</a>')
    if phone:
        contact_parts.append(f'<span>📱 {phone}</span>')
    contact_html = f' {sep} '.join(contact_parts)

    links_html = nav_html
    if contact_html:
        links_html += f'<br>{contact_html}'

    photo_html = ""
    photo_path = Path(__file__).parent.parent / "images" / "KrishImage.png"
    if photo_path.exists():
        b64 = base64.b64encode(photo_path.read_bytes()).decode()
        photo_html = f'<img src="data:image/png;base64,{b64}" alt="{name}" class="profile-photo" />'

    CERT_BADGES = [
        ("Databricks Certified Data Engineer Associate", "https://www.databricks.com/sites/default/files/2025-10/associate-badge-de.png?v=1761149691"),
        ("Databricks Certified Data Engineer Professional", "https://www.databricks.com/sites/default/files/2025-10/professional-badge-de.png?v=1761143167"),
        ("Databricks Certified Machine Learning Associate", "https://www.databricks.com/sites/default/files/2025-10/Associate-badge-ML.png?v=1761077024"),
        ("Databricks Certified Generative AI Engineer Associate", "https://www.databricks.com/sites/default/files/2025-10/associate-badge-gen-ai.png?v=1761153880"),
    ]

    image_badges_html = ""
    for cert_name, img_url in CERT_BADGES:
        image_badges_html += f'<img src="{img_url}" alt="{cert_name}" title="{cert_name}" class="cert-badge-img" />'

    text_badges_html = ""

    right_col = ""
    if image_badges_html:
        right_col = f'<div class="cert-images-col">{image_badges_html}</div>'

    text_row = ""
    if text_badges_html:
        text_row = f'<div class="cert-text-row">{text_badges_html}</div>'

    _html(f"""
    <div class="profile-header">
        <div class="profile-header-inner">
            {photo_html}
            <div class="profile-info-col">
                <h1>{name}</h1>
                <div class="headline">{headline.replace(' | Tech Pre-Sales', '<br>Tech Pre-Sales | Design | Architecture') if 'Design' not in headline else headline.replace(' | Tech Pre-Sales', '<br>Tech Pre-Sales')}</div>
                <div class="location">📍 {location}</div>
                <div class="links" style="margin-top:10px;">{links_html}</div>
                {text_row}
            </div>
            {right_col}
        </div>
    </div>
    """)


def render_metrics(profile_df, work_df, skills_df, certs_df, clients_df=None):
    yrs = int(profile_df.iloc[0].get("years_of_experience", 0)) if not profile_df.empty else 0
    total_certs = len(certs_df) if not certs_df.empty else 0
    total_skills = len(skills_df) if not skills_df.empty else 0
    expert_skills = 0
    prof_col = "proficiency_level" if "proficiency_level" in skills_df.columns else "proficiency"
    if not skills_df.empty and prof_col in skills_df.columns:
        expert_skills = len(skills_df[skills_df[prof_col] == "Expert"])
    clients_count = len(clients_df) if clients_df is not None and not clients_df.empty else 0

    metrics = [
        ("Years Experience", yrs, "#1B6B93"),
        ("Clients Served", clients_count, "#E24A33"),
        ("Total Skills", total_skills, "#2E8B57"),
        ("Expert Skills", expert_skills, "#7B2D8E"),
        ("Certifications", total_certs, "#D4A017"),
    ]

    dark = st.session_state.get('dark_mode', False)
    bg = '#1E2530' if dark else '#fff'
    border_c = '#2D3748' if dark else '#E9ECEF'
    lbl_c = '#A0AEC0' if dark else '#6C757D'

    cards = ""
    for label, value, color in metrics:
        cards += f"""<div class="mc">
            <div class="ml" style="color:{lbl_c};">{label}</div>
            <div class="mv" style="color:{color};">{value}</div>
        </div>"""

    _html(f"""
    <style>
        .mg {{
            display: grid;
            grid-template-columns: repeat(5, 1fr);
            gap: 10px;
            margin-bottom: 8px;
        }}
        .mc {{
            background: {bg};
            border: 1px solid {border_c};
            border-radius: 12px;
            padding: 14px 10px;
            text-align: center;
            box-shadow: 0 1px 3px rgba(0,0,0,0.06);
        }}
        .ml {{ font-size: 0.78rem; margin-bottom: 4px; }}
        .mv {{ font-size: 1.7rem; font-weight: 700; }}
        @media (max-width: 768px) {{
            .mg {{ grid-template-columns: repeat(3, 1fr); }}
            .ml {{ font-size: 0.65rem; }}
            .mv {{ font-size: 1.2rem; }}
            .mc {{ padding: 10px 6px; border-radius: 8px; }}
        }}
        @media (max-width: 400px) {{
            .mg {{ grid-template-columns: repeat(2, 1fr); }}
        }}
    </style>
    <div class="mg">{cards}</div>
    """)


def render_summary(profile_df):
    if profile_df.empty:
        return
    summary = profile_df.iloc[0].get("summary", "")
    if summary:
        _html('<div class="section-header">Professional Summary</div>')
        paragraphs = summary.replace("\\n\\n", "\n\n").split("\n\n")
        body = "".join(f"<p style='font-size:0.95rem; line-height:1.7; color:#333; text-align:justify; margin:0 0 10px;'>{p.strip()}</p>" for p in paragraphs if p.strip())
        _html(body)


def render_career_timeline(timeline_df):
    if timeline_df.empty:
        return
    _html('<div class="section-header">Career Timeline</div>')

    df = timeline_df.copy()
    today = datetime.now()
    today_str = today.strftime("%Y-%m-%d")
    df["end_calc"] = df["end_date"].apply(lambda x: today_str if x in ("Present", None, "") else x)
    df["start_dt"] = pd.to_datetime(df["start_date"])
    df["end_dt"] = pd.to_datetime(df["end_calc"])
    df["months"] = ((df["end_dt"] - df["start_dt"]).dt.days / 30.44).round().astype(int)
    df["duration_label"] = df["months"].apply(
        lambda m: f"{m // 12}y {m % 12}m" if m >= 12 and m % 12 else (f"{m // 12}y" if m >= 12 else f"{m}m")
    )
    df["is_current"] = df["is_current"].astype(str).str.lower().isin(["true", "1"])
    df = df[df["event_type"] != "Education"]
    df = df.sort_values("start_dt", ascending=True)

    earliest = df["start_dt"].min()
    latest = df["end_dt"].max()
    total_days = max((latest - earliest).days, 1)

    palette = ["#065A82", "#E76F00", "#7B2D8E", "#1C7C54", "#C62828", "#00838F"]
    orgs = df["organization"].unique().tolist()
    org_colors = {org: palette[i % len(palette)] for i, org in enumerate(orgs)}

    import math
    durations = [(row["end_dt"] - row["start_dt"]).days for _, row in df.iterrows()]
    sqrt_durations = [math.sqrt(d) for d in durations]
    total_sqrt = sum(sqrt_durations)
    widths = [(s / total_sqrt) * 100 for s in sqrt_durations]

    cards_html = ""
    cursor_pct = 0.0
    for (_, row), width_pct in zip(df.iterrows(), widths):
        is_work = row["event_type"] == "Work"
        is_current = row["is_current"]
        icon = "💼" if is_work else "🎓"
        color = org_colors[row["organization"]]
        start_fmt = row["start_dt"].strftime("%Y")
        end_fmt = "Now" if is_current else row["end_dt"].strftime("%Y")

        left_pct = cursor_pct
        cursor_pct += width_pct

        pulse_css = "animation:tl-pulse 2s infinite;" if is_current else ""
        border = f"border:2px solid #F4A261;" if is_current else f"border:1px solid {color};"

        cards_html += f'''
        <div style="position:absolute;left:{left_pct}%;width:{width_pct}%;top:0;bottom:0;padding:0 1px;box-sizing:border-box;">
            <div style="height:100%;background:{color};border-radius:8px;padding:8px 8px 6px;
                        box-shadow:0 2px 8px rgba(0,0,0,0.12);{border}
                        display:flex;flex-direction:column;justify-content:space-between;overflow:hidden;
                        position:relative;cursor:default;"
                 title="{row['title']}&#10;{row['organization']}&#10;{row['start_dt'].strftime('%b %Y')} — {'Present' if is_current else row['end_dt'].strftime('%b %Y')}&#10;Duration: {row['duration_label']}">
                <div style="overflow:hidden;flex:1;min-height:0;">
                    <div style="color:#fff;font-weight:700;font-size:0.73rem;line-height:1.3;
                                word-wrap:break-word;overflow-wrap:break-word;">
                        {icon} {row["title"]}
                    </div>
                    <div style="color:rgba(255,255,255,0.8);font-size:0.68rem;margin-top:2px;line-height:1.25;
                                word-wrap:break-word;overflow-wrap:break-word;">
                        {row["organization"]}
                    </div>
                </div>
                <div style="display:flex;justify-content:space-between;align-items:flex-end;margin-top:4px;flex-shrink:0;">
                    <span style="color:rgba(255,255,255,0.65);font-size:0.62rem;">{start_fmt}–{end_fmt}</span>
                    <span style="background:rgba(255,255,255,0.2);color:#fff;font-size:0.6rem;font-weight:700;
                                 padding:1px 4px;border-radius:4px;">{row["duration_label"]}</span>
                </div>
                {f'<div style="position:absolute;top:3px;right:3px;width:7px;height:7px;border-radius:50%;background:#4CAF50;{pulse_css}"></div>' if is_current else ''}
            </div>
        </div>'''

    year_markers = ""
    cum = 0.0
    for (_, row), w in zip(df.iterrows(), widths):
        start_y = row["start_dt"].strftime("%Y")
        year_markers += f'<div style="position:absolute;left:{cum}%;top:-16px;font-size:0.68rem;color:#999;font-weight:600;">{start_y}</div>'
        cum += w
    last_row = df.iloc[-1]
    end_label = "Now" if last_row["is_current"] else last_row["end_dt"].strftime("%Y")
    year_markers += f'<div style="position:absolute;right:0;top:-16px;font-size:0.68rem;color:#999;font-weight:600;">{end_label}</div>'

    work_count = len(df[df["event_type"] == "Work"])
    work_yrs = df[df["event_type"] == "Work"]["months"].sum() // 12

    legend_items = ""
    for org, color in org_colors.items():
        legend_items += (
            f'<div style="display:flex;align-items:center;gap:4px;font-size:0.78rem;">'
            f'<span style="display:inline-block;width:10px;height:10px;background:{color};border-radius:2px;flex-shrink:0;"></span>'
            f'<span style="color:#555;white-space:nowrap;">{org}</span></div>'
        )

    _html(f'''
    <style>
        @keyframes tl-pulse {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.4; }}
        }}
    </style>
    <div style="display:flex;gap:12px;margin-bottom:20px;flex-wrap:wrap;align-items:center;">
        {legend_items}
        <div style="margin-left:auto;font-size:0.8rem;color:#888;">
            {work_yrs}+ years across {work_count} roles
        </div>
    </div>
    <div style="position:relative;height:150px;margin:24px 0 20px;padding:0 2px;">
        {year_markers}
        {cards_html}
    </div>
    ''')


def _skill_rating(prof, years):
    """Map proficiency + years to a 1-10 rating."""
    y = float(years) if years else 0
    if prof == "Expert":
        if y >= 10:
            return 10
        if y >= 5:
            return 9
        return 8
    if prof == "Advanced":
        if y >= 5:
            return 7
        if y >= 3:
            return 6
        return 5
    return 4


def render_skills_charts(skills_df):
    if skills_df.empty:
        return
    _html('<div class="section-header">Skills & Expertise</div>')

    df = skills_df.copy()
    if "years_of_experience" in df.columns:
        df["years_of_experience"] = pd.to_numeric(df["years_of_experience"], errors="coerce")
    yrs_col = "years_of_experience" if "years_of_experience" in df.columns else "years_used"
    prof_col = "proficiency_level" if "proficiency_level" in df.columns else "proficiency"

    df["rating"] = df.apply(lambda r: _skill_rating(r.get(prof_col, ""), r.get(yrs_col, 0)), axis=1)

    cat_order = [
        "1. Data Engineering & Pipelines",
        "2. Lakehouse & Data Platform",
        "3. SQL, Analytics & BI",
        "4. AI / Machine Learning",
        "5. Generative AI & Agents",
        "6. Data Governance & Catalog",
        "7. Apps, Interfaces & Access",
        "8. Databases & New Storage",
        "9. Cloud & Infrastructure",
        "10. DevOps & Deployment",
    ]
    cat_color_map = {
        "1. Data Engineering & Pipelines":  "#1B6B93",
        "2. Lakehouse & Data Platform":     "#E24A33",
        "3. SQL, Analytics & BI":           "#2E8B57",
        "4. AI / Machine Learning":         "#7B2D8E",
        "5. Generative AI & Agents":        "#E91E63",
        "6. Data Governance & Catalog":     "#D4A017",
        "7. Apps, Interfaces & Access":     "#4682B4",
        "8. Databases & New Storage":       "#FF6347",
        "9. Cloud & Infrastructure":        "#FF9800",
        "10. DevOps & Deployment":          "#607D8B",
    }
    cat_icons = {
        "1. Data Engineering & Pipelines":  "⚙️",
        "2. Lakehouse & Data Platform":     "🏠",
        "3. SQL, Analytics & BI":           "📊",
        "4. AI / Machine Learning":         "🤖",
        "5. Generative AI & Agents":        "🔥",
        "6. Data Governance & Catalog":     "🔒",
        "7. Apps, Interfaces & Access":     "📱",
        "8. Databases & New Storage":       "💾",
        "9. Cloud & Infrastructure":        "☁️",
        "10. DevOps & Deployment":          "🚀",
    }

    chart_df = df[df["category"].isin(cat_order)].copy()
    if chart_df.empty:
        return

    cards_html = ""
    for cat in cat_order:
        cat_df = chart_df[chart_df["category"] == cat].sort_values("rating", ascending=False)
        if cat_df.empty:
            continue
        color = cat_color_map[cat]
        icon = cat_icons[cat]
        label = cat.split(". ", 1)[1]

        skills_rows = ""
        for _, row in cat_df.iterrows():
            yrs = row.get(yrs_col, 0)
            yrs_val = int(float(yrs)) if yrs else 0
            prof = row.get(prof_col, "")
            max_yrs = 15
            pct = min(int((yrs_val / max_yrs) * 100), 100)
            name = row["skill_name"]
            skills_rows += f"""
            <div class="sk-row">
                <span class="sk-name">{name}</span>
                <div class="sk-bar-bg">
                    <div class="sk-bar-fill" style="width:{pct}%;background:{color};"></div>
                </div>
                <span class="sk-val">{yrs_val}y · {prof}</span>
            </div>"""

        cards_html += f"""
        <div class="sk-card">
            <div class="sk-header" style="border-left:4px solid {color};">
                <span class="sk-icon">{icon}</span>
                <span class="sk-title">{label}</span>
            </div>
            {skills_rows}
        </div>"""

    _html(f"""
    <div class="sk-grid">{cards_html}</div>
    <style>
        .sk-grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 14px; margin-top: 8px; }}
        @media (max-width: 768px) {{ .sk-grid {{ grid-template-columns: 1fr; }} }}
        .sk-card {{
            background: #fff; border: 1px solid #E8EDF1; border-radius: 12px;
            padding: 16px; transition: box-shadow 0.2s;
        }}
        .sk-card:hover {{ box-shadow: 0 4px 14px rgba(0,0,0,0.08); }}
        .sk-header {{
            display: flex; align-items: center; gap: 8px;
            padding: 0 0 10px 8px; margin-bottom: 10px;
            border-bottom: 1px solid #F0F2F5;
        }}
        .sk-icon {{ font-size: 1.3rem; }}
        .sk-title {{ font-size: 1.05rem; font-weight: 700; color: #1B3A4B; }}
        .sk-row {{
            display: flex; align-items: center; gap: 8px;
            margin-bottom: 6px;
        }}
        .sk-name {{
            font-size: 0.92rem; color: #444; min-width: 0; flex: 1;
            white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
        }}
        .sk-bar-bg {{
            width: 80px; min-width: 80px; height: 6px;
            background: #F0F2F5; border-radius: 3px; overflow: hidden;
        }}
        .sk-bar-fill {{ height: 100%; border-radius: 3px; transition: width 0.5s ease; }}
        .sk-val {{ font-size: 0.88rem; color: #888; min-width: 90px; text-align: right; white-space: nowrap; }}
    </style>
    """)

    other_df = df[~df["category"].isin(cat_order)].sort_values(["category", "rating"], ascending=[True, False])
    if not other_df.empty:
        other_icons = {"Client-Facing": "🤝", "Leadership": "👑"}
        other_colors = {"Client-Facing": "#065A82", "Leadership": "#1C7C54"}
        other_cards = ""
        for cat in other_df["category"].unique():
            cat_df = other_df[other_df["category"] == cat].sort_values("rating", ascending=False)
            color = other_colors.get(cat, "#1B3A4B")
            icon = other_icons.get(cat, "💡")
            skills_rows = ""
            for _, row in cat_df.iterrows():
                yrs = row.get(yrs_col, 0)
                yrs_val = int(float(yrs)) if yrs else 0
                prof = row.get(prof_col, "")
                max_yrs = 15
                pct = min(int((yrs_val / max_yrs) * 100), 100)
                skills_rows += f"""
                <div class="sk-row">
                    <span class="sk-name">{row["skill_name"]}</span>
                    <div class="sk-bar-bg">
                        <div class="sk-bar-fill" style="width:{pct}%;background:{color};"></div>
                    </div>
                    <span class="sk-val">{yrs_val}y · {prof}</span>
                </div>"""
            other_cards += f"""
            <div class="sk-card">
                <div class="sk-header" style="border-left:4px solid {color};">
                    <span class="sk-icon">{icon}</span>
                    <span class="sk-title">{cat}</span>
                </div>
                {skills_rows}
            </div>"""
        _html(f'<div class="sk-grid" style="margin-top:14px;">{other_cards}</div>')


def render_clients(clients_df):
    if clients_df.empty:
        return
    _html('<div class="section-header">Clients & Industries</div>')

    domain_colors = {
        "Financial": "#1B6B93",
        "Insurance": "#2E8B57",
        "Mortgage": "#E24A33",
        "Mortgage Insurance": "#D4A017",
        "Manufacturing": "#FF6347",
        "Healthcare": "#7B2D8E",
        "Healthcare IT": "#9B59B6",
        "Technology": "#4682B4",
        "Cloud": "#FF9900",
        "Government": "#6C757D",
        "Retail": "#FF8C00",
        "Pro Serv": "#065A82",
    }

    LOGO_FILES = {
        "Capital Group": "capitalgroup.png",
        "TD Bank": "tdbank.png",
        "Guardian Life Insurance": "guardianlife.png",
        "FIS Global": "fisglobal.png",
        "Johnson & Johnson": "jnj.png",
        "BondCliQ": "bondcliq.png",
        "Moody's": "moodys.png",
        "Essent Mortgage": "essent.png",
        "Rocket Mortgage": "rocketmortgage.png",
        "BCBS IL": "bcbsil.png",
        "Nissan North America": "nissan.png",
        "Wells Fargo": "wellsfargo.png",
        "Horace Mann": "horacemann.png",
        "Illinois State Board of Education": "isbe.png",
        "CareFusion": "carefusion.png",
        "Illinois Office of Comptroller": "ioc.png",
        "Caterpillar": "caterpillar.png",
        "Henkel": "henkel.png",
        "Microsoft": "microsoft.png",
        "SuperValu": "supervalu.png",
        "Databricks": "databricks.png",
        "AWS": "aws.png",
        "Hortonworks": "hortonworks.png",
    }

    logos_dir = Path(__file__).parent.parent / "images" / "clients"

    cards = ""
    for _, row in clients_df.iterrows():
        name = row.get("client_name", "")
        domain = row.get("domain", "")
        color = domain_colors.get(domain, "#1B3A4B")

        logo_html = f'<div class="client-logo-fallback">{name[0]}</div>'
        logo_file = logos_dir / LOGO_FILES.get(name, "")
        if logo_file.exists():
            b64 = base64.b64encode(logo_file.read_bytes()).decode()
            logo_html = f'<img src="data:image/png;base64,{b64}" alt="{name}" class="client-logo" />'

        cards += f"""
        <div class="client-card">
            {logo_html}
            <div class="client-name">{name}</div>
            <span class="client-domain" style="background:{color};">{domain}</span>
        </div>"""

    _html(f'<div class="clients-grid">{cards}</div>')


def render_experience(work_df, highlights_df):
    if work_df.empty:
        return
    _html('<div class="section-header">Work Experience</div>')

    sorted_df = work_df.copy()
    if "start_date" in sorted_df.columns:
        sorted_df = sorted_df.sort_values("start_date", ascending=False)

    for _, row in sorted_df.iterrows():
        company = row.get("company", "")
        title = row.get("title", "") or row.get("title_at_employer", "")
        role = row.get("role_at_customer", "")
        location = row.get("location", "")
        start = row.get("start_date", "")
        end = row.get("end_date", "") or "Present"
        if str(end).strip().lower() in ("none", "null", "nan", ""):
            end = "Present"
        industry = row.get("industry", "")
        team = row.get("team_size_managed", 0)
        desc = row.get("description", "")
        exp_id = row.get("experience_id")

        badge_html = ""
        if industry:
            badge_html += f'<span class="badge badge-technical">{industry}</span>'
        if int(team) > 0 if team else False:
            badge_html += f'<span class="badge badge-leadership">Managing {team}</span>'

        highlights_html = ""
        if not highlights_df.empty and exp_id:
            exp_highlights = highlights_df[
                highlights_df["experience_id"].astype(str) == str(exp_id)
            ]
            if "highlight_id" in exp_highlights.columns:
                exp_highlights = exp_highlights.sort_values("highlight_id")
            for _, h in exp_highlights.iterrows():
                cat = h.get("category", "Technical")
                badge_cls = f"badge-{cat.lower()}"
                highlights_html += f"""
                <div class="highlight">
                    <span class="badge {badge_cls}">{cat}</span>
                    {h.get('highlight', '')}
                    <span style="color:#1C7C54; font-weight:600; font-size:0.92rem;">
                        → {h.get('impact_metric', '')}
                    </span>
                </div>"""

        role_html = f'<div style="font-size:1.05rem; margin-top:2px;"><span style="color:#6C757D;">Role at Customer:</span> <span style="color:#065A82; font-weight:600;">{role}</span></div>' if role and role != title else ""
        title_label = f"Title at {company}" if role and role != title else ""
        title_line = f'<div style="font-size:1.05rem;"><span style="color:#6C757D;">{title_label}:</span> <span style="font-weight:600;">{title}</span></div>' if title_label else f"<h4>{title}</h4>"
        _html(f"""
        <div class="exp-card">
            {title_line}
            {role_html}
            <div class="subtitle">{company}</div>
            <div class="meta">📍 {location} &nbsp;|&nbsp; 📅 {start} — {end} {badge_html}</div>
            <div style="color:#444; font-size:1.0rem; margin-bottom:8px;">{desc}</div>
            {highlights_html}
        </div>
        """)


def render_education(edu_df):
    if edu_df.empty:
        return
    _html('<div class="section-header">Education</div>')

    df = edu_df.copy()
    df["start_dt"] = pd.to_datetime(df["start_date"])
    end_col = "end_date"
    df["end_dt"] = pd.to_datetime(df[end_col])
    df["months"] = ((df["end_dt"] - df["start_dt"]).dt.days / 30.44).round().astype(int)
    df["duration_label"] = df["months"].apply(
        lambda m: f"{m // 12}y {m % 12}m" if m >= 12 and m % 12 else (f"{m // 12}y" if m >= 12 else f"{m}m")
    )
    df = df.sort_values("start_dt", ascending=True)

    earliest = df["start_dt"].min()
    latest = df["end_dt"].max()
    total_days = max((latest - earliest).days, 1)

    palette = ["#1C7C54", "#7B2D8E"]
    cards_html = ""
    legend_items = ""

    for idx, (_, row) in enumerate(df.iterrows()):
        color = palette[idx % len(palette)]
        inst = row.get("institution", "")
        degree = row.get("degree", "")
        field = row.get("field_of_study", "")
        gpa = row.get("gpa", "")
        honors = row.get("honors", "")
        coursework = row.get("relevant_coursework", "")
        start_fmt = row["start_dt"].strftime("%Y")
        end_fmt = row["end_dt"].strftime("%Y")

        left_pct = ((row["start_dt"] - earliest).days / total_days) * 100
        width_pct = max(((row["end_dt"] - row["start_dt"]).days / total_days) * 100, 14)

        gpa_str = ""
        if gpa:
            gpa_str = f" · GPA: {gpa}"

        hover_text = f"{degree} in {field}&#10;{inst}&#10;{row['start_dt'].strftime('%b %Y')} — {row['end_dt'].strftime('%b %Y')}&#10;{honors}"
        if coursework:
            hover_text += f"&#10;📚 {coursework}"

        cards_html += f'''
        <div style="position:absolute;left:{left_pct}%;width:{width_pct}%;top:0;bottom:0;padding:0 1px;box-sizing:border-box;">
            <div style="height:100%;background:{color};border-radius:8px;padding:8px 8px 6px;
                        box-shadow:0 2px 8px rgba(0,0,0,0.12);border:1px solid {color};
                        display:flex;flex-direction:column;justify-content:space-between;overflow:hidden;
                        cursor:default;" title="{hover_text}">
                <div style="overflow:hidden;flex:1;min-height:0;">
                    <div style="color:#fff;font-weight:700;font-size:0.73rem;line-height:1.3;
                                word-wrap:break-word;overflow-wrap:break-word;">
                        🎓 {degree} in {field}
                    </div>
                    <div style="color:rgba(255,255,255,0.8);font-size:0.68rem;margin-top:2px;line-height:1.25;
                                word-wrap:break-word;overflow-wrap:break-word;">
                        {inst}{gpa_str}
                    </div>
                </div>
                <div style="display:flex;justify-content:space-between;align-items:flex-end;margin-top:4px;flex-shrink:0;">
                    <span style="color:rgba(255,255,255,0.65);font-size:0.62rem;">{start_fmt}–{end_fmt}</span>
                    <span style="background:rgba(255,255,255,0.2);color:#fff;font-size:0.6rem;font-weight:700;
                                 padding:1px 4px;border-radius:4px;">{row["duration_label"]}</span>
                </div>
            </div>
        </div>'''

        legend_items += (
            f'<div style="display:flex;align-items:center;gap:4px;font-size:0.78rem;">'
            f'<span style="display:inline-block;width:10px;height:10px;background:{color};border-radius:2px;flex-shrink:0;"></span>'
            f'<span style="color:#555;white-space:nowrap;">{inst}</span></div>'
        )

    year_markers = ""
    for y in range(earliest.year, latest.year + 2, 3):
        y_date = datetime(y, 1, 1)
        pct = ((y_date - earliest).days / total_days) * 100
        if 0 <= pct <= 100:
            year_markers += f'<div style="position:absolute;left:{pct}%;top:-16px;transform:translateX(-50%);font-size:0.68rem;color:#999;font-weight:600;">{y}</div>'
            year_markers += f'<div style="position:absolute;left:{pct}%;top:0;bottom:0;width:1px;background:rgba(0,0,0,0.05);"></div>'

    _html(f'''
    <div style="display:flex;gap:12px;margin-bottom:20px;flex-wrap:wrap;align-items:center;">
        {legend_items}
    </div>
    <div style="position:relative;height:150px;margin:24px 0 20px;padding:0 2px;">
        {year_markers}
        {cards_html}
    </div>
    ''')


def render_certifications(certs_df):
    if certs_df.empty:
        return
    _html('<div class="section-header">Certifications</div>')
    for _, row in certs_df.iterrows():
        active_str = str(row.get("is_active", "")).lower()
        is_active = active_str in ("true", "1")
        status = "✅ Active" if is_active else "⏰ Expired"
        status_color = "#1C7C54" if is_active else "#DC3545"

        _html(f"""
        <div class="info-card">
            <h5>{row.get('certification_name', '')}</h5>
            <div class="detail">
                {row.get('issuing_organization', '')} &nbsp;|&nbsp;
                📅 Issued: {row.get('issue_date', '')} &nbsp;|&nbsp;
                <span style="color:{status_color}; font-weight:600;">{status}</span>
            </div>
        </div>
        """)


def render_projects(projects_df):
    if projects_df.empty:
        return
    _html('<div class="section-header">Key Projects</div>')

    cards = ""
    for _, row in projects_df.iterrows():
        is_active = str(row.get("is_current", "")).lower() in ("true", "1")
        status_cls = "proj-active" if is_active else "proj-complete"
        status_label = "Active" if is_active else "Completed"
        status_icon = "🔄" if is_active else "✅"
        client = row.get("client", "")
        client_html = f'<span class="proj-client">{client}</span>' if client else ""

        techs = str(row.get("technologies_used", ""))
        tech_pills = "".join(f'<span class="proj-tech">{t.strip()}</span>' for t in techs.split(",") if t.strip())

        url = row.get("url", "")
        link_html = f'<a href="{url}" target="_blank" style="font-size:0.78rem;color:#065A82;text-decoration:none;font-weight:600;">▶️ Watch Presentation</a>' if url else ""

        cards += f"""
        <div class="proj-card">
            <div class="proj-header">
                <div class="proj-status {status_cls}">{status_icon} {status_label}</div>
                {client_html}
            </div>
            <div class="proj-title">{row.get('project_name', '')}</div>
            <div class="proj-role">{row.get('role', '')}</div>
            <div class="proj-desc">{row.get('description', '')}</div>
            <div class="proj-impact">
                <span class="proj-impact-label">Impact:</span> {row.get('impact', '')}
            </div>
            <div class="proj-techs">{tech_pills}</div>
            {link_html}
        </div>"""

    _html(f"""
    <div class="proj-grid">{cards}</div>
    <style>
        .proj-grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 14px; margin-top: 8px; }}
        @media (max-width: 768px) {{ .proj-grid {{ grid-template-columns: 1fr; }} }}
        .proj-card {{
            background: #fff; border: 1px solid #E8EDF1; border-radius: 12px;
            padding: 18px 20px; transition: transform 0.2s ease, box-shadow 0.2s ease;
            display: flex; flex-direction: column; gap: 8px;
        }}
        .proj-card:hover {{ transform: translateY(-2px); box-shadow: 0 6px 18px rgba(0,0,0,0.08); }}
        .proj-header {{ display: flex; align-items: center; justify-content: space-between; }}
        .proj-status {{
            font-size: 0.72rem; font-weight: 700; padding: 3px 10px;
            border-radius: 20px; text-transform: uppercase; letter-spacing: 0.5px;
        }}
        .proj-active {{ background: #E8F5E9; color: #2E7D32; }}
        .proj-complete {{ background: #E3F2FD; color: #1565C0; }}
        .proj-client {{
            font-size: 0.78rem; font-weight: 600; color: #065A82;
            background: #F0F7FA; padding: 3px 10px; border-radius: 20px;
        }}
        .proj-title {{ font-size: 0.95rem; font-weight: 700; color: #1B3A4B; line-height: 1.3; }}
        .proj-role {{ font-size: 0.82rem; color: #065A82; font-weight: 600; }}
        .proj-desc {{ font-size: 0.84rem; color: #555; line-height: 1.45; }}
        .proj-impact {{
            font-size: 0.82rem; color: #1C7C54; line-height: 1.4;
            background: #F0FFF4; padding: 8px 12px; border-radius: 8px; border-left: 3px solid #1C7C54;
        }}
        .proj-impact-label {{ font-weight: 700; }}
        .proj-techs {{ display: flex; flex-wrap: wrap; gap: 5px; margin-top: 2px; }}
        .proj-tech {{
            font-size: 0.7rem; background: #F4F6F8; color: #555;
            padding: 2px 8px; border-radius: 12px; border: 1px solid #E0E4E8;
        }}
    </style>
    """)


def render_publications(pubs_df):
    if pubs_df.empty:
        return
    _html('<div class="section-header">Publications & Thought Leadership</div>')

    type_icons = {
        "Article": "📄",
        "Whitepaper / Research Guide": "📘",
        "Talk": "🎤",
        "Blog": "✍️",
    }
    type_colors = {
        "Article": "#1B6B93",
        "Whitepaper / Research Guide": "#7B2D8E",
        "Talk": "#E24A33",
        "Blog": "#2E8B57",
    }
    publisher_icons = {
        "LinkedIn Pulse": "🔗",
        "Lumenalta Labs": "🏢",
    }

    cards = ""
    for _, row in pubs_df.iterrows():
        url = row.get("url", "")
        title = row.get("title", "")
        pub_type = row.get("publication_type", "") or row.get("type", "")
        publisher = row.get("publisher", "")
        pub_date = row.get("publication_date", "") or row.get("date", "")
        icon = type_icons.get(pub_type, "📝")
        color = type_colors.get(pub_type, "#1B3A4B")
        pub_icon = publisher_icons.get(publisher, "📰")

        title_html = f'<a href="{url}" target="_blank" class="pub-title">{title}</a>' if url else f'<span class="pub-title">{title}</span>'

        cards += f"""
        <div class="pub-card">
            <div class="pub-icon" style="background:{color};">{icon}</div>
            <div class="pub-content">
                {title_html}
                <div class="pub-meta">
                    <span class="pub-type" style="color:{color};">{pub_type}</span>
                    <span class="pub-sep">·</span>
                    <span>{pub_icon} {publisher}</span>
                    <span class="pub-sep">·</span>
                    <span>📅 {pub_date}</span>
                </div>
            </div>
            {'<a href="' + url + '" target="_blank" class="pub-link">↗</a>' if url else ''}
        </div>"""

    _html(f"""
    <div class="pubs-container">{cards}</div>
    <style>
        .pubs-container {{ display: flex; flex-direction: column; gap: 10px; margin-top: 8px; }}
        .pub-card {{
            display: flex; align-items: flex-start; gap: 14px;
            background: #fff; border: 1px solid #E8EDF1; border-radius: 12px;
            padding: 16px 18px; transition: transform 0.2s ease, box-shadow 0.2s ease;
        }}
        .pub-card:hover {{ transform: translateX(4px); box-shadow: 0 4px 14px rgba(0,0,0,0.08); }}
        .pub-icon {{
            width: 42px; height: 42px; min-width: 42px; border-radius: 10px;
            display: flex; align-items: center; justify-content: center;
            font-size: 1.2rem; color: #fff;
        }}
        .pub-content {{ flex: 1; min-width: 0; }}
        .pub-title {{
            font-size: 0.92rem; font-weight: 600; color: #1B3A4B;
            text-decoration: none; line-height: 1.35; display: block;
        }}
        a.pub-title:hover {{ color: #065A82; }}
        .pub-meta {{
            font-size: 0.78rem; color: #6C757D; margin-top: 6px;
            display: flex; flex-wrap: wrap; align-items: center; gap: 6px;
        }}
        .pub-type {{ font-weight: 600; }}
        .pub-sep {{ color: #ccc; }}
        .pub-link {{
            font-size: 1.1rem; color: #065A82; text-decoration: none;
            padding: 4px 8px; border-radius: 8px; flex-shrink: 0;
            transition: background 0.2s;
        }}
        .pub-link:hover {{ background: #E8EDF1; }}
    </style>
    """)


# ────────────────────────────────────────────────────────────────
# Genie Chat Component
# ────────────────────────────────────────────────────────────────

QUICK_QUESTIONS = [
    "Why should we hire Krish?",
    "What are Krish's top skills?",
    "What clients has Krish worked with?",
    "Tell me about Krish's Genie experience",
    "What certifications does Krish hold?",
    "Does Krish have pre-sales experience?",
    "What is Krish's biggest achievement?",
    "What is Krish's work authorization?",
]


def render_genie_chat():
    _html("""
    <div class="genie-banner">
        <h3>🐒 Abu — Ask Me Anything About Krish (Powered by Databricks AI/BI Genie 🧞)</h3>
        <p>Ask any question about Krish's career, skills, experience, or qualifications.</p>
    </div>
    """)

    if "conversation_id" not in st.session_state:
        st.session_state.conversation_id = None
    if "last_qa" not in st.session_state:
        st.session_state.last_qa = None

    # Suggested questions at the top
    st.markdown("""
    <style>
    div[data-testid="stHorizontalBlock"] button[kind="secondary"] {
        min-height: 58px;
        display: flex;
        align-items: center;
        justify-content: center;
    }
    </style>
    **Suggested questions:**
    """, unsafe_allow_html=True)
    row1 = st.columns(4)
    for i, q in enumerate(QUICK_QUESTIONS[:4]):
        if row1[i].button(q, key=f"quick_{i}", use_container_width=True):
            st.session_state.pending_question = q
            st.rerun()
    row2 = st.columns(4)
    for i, q in enumerate(QUICK_QUESTIONS[4:]):
        if row2[i].button(q, key=f"quick_{i+4}", use_container_width=True):
            st.session_state.pending_question = q
            st.rerun()

    # Input field below suggestions
    with st.form("genie_form", clear_on_submit=True):
        input_col, btn_col = st.columns([6, 1])
        with input_col:
            user_input = st.text_input(
                "Ask Abu",
                placeholder="Ask anything about Krish's career, skills, or experience...",
                key="genie_input",
                label_visibility="collapsed",
            )
        with btn_col:
            send_clicked = st.form_submit_button("Ask 🐒", type="primary", use_container_width=True)

    st.divider()

    # Handle pending question from button click
    pending = st.session_state.pop("pending_question", None)
    question = pending or (user_input if send_clicked else None)

    if question:
        with st.spinner("🐒 Abu is thinking..."):
            result = genie_ask(question, st.session_state.conversation_id)

        st.session_state.conversation_id = result.get("conversation_id")
        st.session_state.last_qa = {
            "question": question,
            "answer": result["text"],
            "df": result.get("df"),
            "sql": result.get("sql"),
        }

    # Show only the latest Q&A
    if st.session_state.last_qa:
        qa = st.session_state.last_qa
        with st.chat_message("user", avatar="👤"):
            st.markdown(qa["question"])
        with st.chat_message("assistant", avatar="🐒"):
            st.markdown(qa["answer"])
            if qa.get("df") is not None and not qa["df"].empty:
                st.dataframe(qa["df"], use_container_width=True, hide_index=True)
            if qa.get("sql"):
                with st.expander("View generated SQL"):
                    st.code(qa["sql"], language="sql")


# ────────────────────────────────────────────────────────────────
# PDF Resume Generator
# ────────────────────────────────────────────────────────────────

def _pdf_safe(text):
    """Replace Unicode characters not supported by Helvetica."""
    return (str(text)
            .replace("\u2014", "--")   # em-dash
            .replace("\u2013", "-")    # en-dash
            .replace("\u2018", "'")    # left single quote
            .replace("\u2019", "'")    # right single quote
            .replace("\u201c", '"')    # left double quote
            .replace("\u201d", '"')    # right double quote
            .replace("\u2022", "-")    # bullet
            .replace("\u2026", "...")   # ellipsis
            .replace("\u00e9", "e")    # accented e
            )


def generate_pdf(data):
    """Generate a clean, professional resume PDF."""
    S = _pdf_safe
    pdf = FPDF()
    pdf.set_margin(10)
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=12)
    profile = data.get("profile", {})

    pdf.set_font("Helvetica", "B", 22)
    pdf.cell(0, 10, S(profile.get("full_name", "")), new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 5, S(profile.get("headline", "")), new_x="LMARGIN", new_y="NEXT", align="C")
    loc = f"{profile.get('location_city', '')}, {profile.get('location_state', '')}"
    contact_line1 = f"{loc}  |  {profile.get('email', '')}  |  {profile.get('phone', '')}"
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 5, S(contact_line1), new_x="LMARGIN", new_y="NEXT", align="C")
    contact_line2 = f"{profile.get('linkedin_url', '')}  |  {profile.get('website_url', '')}"
    pdf.cell(0, 5, S(contact_line2), new_x="LMARGIN", new_y="NEXT", align="C")
    pdf.ln(1)
    pdf.set_draw_color(27, 58, 75)
    pdf.set_line_width(0.5)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(3)

    def section_hdr(title):
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_text_color(27, 58, 75)
        pdf.cell(0, 6, title.upper(), new_x="LMARGIN", new_y="NEXT")
        pdf.set_draw_color(27, 58, 75)
        pdf.set_line_width(0.2)
        pdf.line(10, pdf.get_y(), 200, pdf.get_y())
        pdf.ln(1.5)
        pdf.set_text_color(0, 0, 0)

    section_hdr("Professional Summary")
    pdf.set_font("Helvetica", "", 7.5)
    pdf.set_x(10)
    pdf.multi_cell(0, 3.2, S(profile.get("summary", "")))
    pdf.ln(1.5)

    section_hdr("Work Experience")
    for exp in data.get("work_experience", []):
        title = exp.get("title_at_employer", exp.get("title", ""))
        company = exp.get("company", "")
        role = exp.get("role_at_customer", "")
        start = exp.get("start_date", "")[:7]
        end = exp.get("end_date") or "Present"
        if end != "Present":
            end = end[:7]

        pdf.set_font("Helvetica", "B", 8.5)
        pdf.set_x(10)
        pdf.cell(0, 4.5, S(f"{title} -- {company}"), new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "I", 7)
        pdf.set_text_color(100, 100, 100)
        meta = f"{start} to {end}  |  {exp.get('location', '')}"
        if role:
            meta += f"  |  {role}"
        pdf.set_x(10)
        pdf.cell(0, 3.5, S(meta), new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Helvetica", "", 7)
        for h in exp.get("highlights", [])[:4]:
            desc = h.get("description", "")
            impact = h.get("impact_metric", "")
            bullet = f"  - {desc}"
            if impact:
                bullet += f" [{impact}]"
            pdf.set_x(10)
            pdf.multi_cell(0, 3, S(bullet))
        pdf.ln(1)

    section_hdr("Skills")
    skills_by_cat = {}
    for s in data.get("skills", []):
        cat = s.get("category", "Other")
        label = cat.split(". ", 1)[1] if ". " in cat else cat
        skills_by_cat.setdefault(label, []).append(s["skill_name"])
    for cat, skills in skills_by_cat.items():
        pdf.set_font("Helvetica", "B", 7.5)
        pdf.set_x(10)
        pdf.cell(0, 3.5, S(f"{cat}: ") + "  ", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 7)
        pdf.set_x(10)
        pdf.multi_cell(0, 3, S("    " + ", ".join(skills)))
    pdf.ln(1)

    section_hdr("Education")
    for edu in data.get("education", []):
        pdf.set_font("Helvetica", "B", 8.5)
        pdf.set_x(10)
        pdf.cell(0, 4.5, S(f"{edu['degree']} in {edu['field_of_study']} -- {edu['institution']}"), new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "I", 7)
        pdf.set_text_color(100, 100, 100)
        gpa = f"  |  GPA: {edu.get('gpa', '')}" if edu.get("gpa") else ""
        pdf.set_x(10)
        pdf.cell(0, 3.5, S(f"{edu.get('end_date', '')[:7]}{gpa}"), new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(0, 0, 0)

    pdf.ln(1)
    section_hdr("Certifications")
    pdf.set_font("Helvetica", "", 7)
    for c in data.get("certifications", []):
        pdf.set_x(10)
        pdf.cell(0, 3, S(f"  - {c['name']} ({c['issuing_organization']}, {c.get('issue_date', '')[:7]})"), new_x="LMARGIN", new_y="NEXT")

    return bytes(pdf.output())


# ────────────────────────────────────────────────────────────────
# Testimonials / Recommendations
# ────────────────────────────────────────────────────────────────

TESTIMONIALS = [
    {
        "quote": "Krish's live Genie demo was the decisive moment in our evaluation. He didn't just show technology — he showed how it solves real business problems. That's what won us over against four global competitors.",
        "name": "Capital Group Engagement",
        "role": "Pre-Sales Win",
        "avatar": "🏆",
    },
    {
        "quote": "Krish built our Databricks Center of Excellence from the ground up — training programs, certification tracks, delivery frameworks, everything. He personally drove 50+ certifications across the company and helped us reach Silver Partner status.",
        "name": "Lumenalta Leadership",
        "role": "CoE & Partnership",
        "avatar": "🚀",
    },
    {
        "quote": "The ingestion, orchestration, and ETL frameworks Krish built at TD Bank became the standard for our enterprise Databricks platform. His Unity Catalog governance implementation set the foundation for how we manage data access and lineage across teams.",
        "name": "TD Bank Engagement",
        "role": "Enterprise Databricks Delivery",
        "avatar": "🏦",
    },
]


def render_testimonials():
    _html('<div class="section-header">What People Say</div>')

    cards = ""
    for t in TESTIMONIALS:
        cards += f"""
        <div style="flex:1; min-width:260px; background:white; border-radius:14px; padding:22px 24px;
                    box-shadow:0 2px 10px rgba(0,0,0,0.06); border-top:3px solid #065A82;">
            <div style="font-size:2rem; margin-bottom:8px; opacity:0.15;">❝</div>
            <p style="color:#495057; font-size:0.95rem; line-height:1.7; font-style:italic; margin:0 0 16px;">
                {t['quote']}
            </p>
            <div style="display:flex; align-items:center; gap:10px; border-top:1px solid #F0F2F5; padding-top:12px;">
                <div style="width:40px; height:40px; border-radius:50%; background:#E8EDF1;
                            display:flex; align-items:center; justify-content:center; font-size:1.3rem;">
                    {t['avatar']}
                </div>
                <div>
                    <div style="font-weight:700; color:#1B3A4B; font-size:0.95rem;">{t['name']}</div>
                    <div style="color:#6C757D; font-size:0.82rem;">{t['role']}</div>
                </div>
            </div>
        </div>"""

    _html(f"""
    <div style="display:flex; gap:16px; flex-wrap:wrap;">
        {cards}
    </div>
    """)


# ────────────────────────────────────────────────────────────────
# Visitor Analytics (GoatCounter — privacy-friendly, no cookies)
# ────────────────────────────────────────────────────────────────

def inject_analytics():
    """GoatCounter — privacy-friendly backend analytics."""
    st.components.v1.html("""
    <script>
        var gc = document.createElement('script');
        gc.src = 'https://gc.zgo.at/count.js';
        gc.async = true;
        gc.dataset.goatcounter = 'https://thedatabrickster.goatcounter.com/count';
        gc.dataset.goatcounterSettings = JSON.stringify({
            path: '/resume',
            title: 'Krish Kilaru - AI Resume',
            referrer: document.referrer || 'direct'
        });
        document.head.appendChild(gc);

        // Fallback: pixel-based tracking if JS blocked
        var img = new Image();
        img.src = 'https://thedatabrickster.goatcounter.com/count?p=/resume&t=Krish+Kilaru+-+AI+Resume&r=' + encodeURIComponent(document.referrer);
    </script>
    """, height=0)


def inject_seo_meta():
    """Inject Open Graph & SEO meta tags for rich link previews on LinkedIn/Slack."""
    st.components.v1.html("""
    <script>
        if (!document.querySelector('meta[name="viewport"]')) {
            var vp = document.createElement('meta');
            vp.setAttribute('name', 'viewport');
            vp.setAttribute('content', 'width=device-width, initial-scale=1.0');
            document.head.appendChild(vp);
        }
        if (!document.querySelector('meta[property="og:title"]')) {
            var metas = [
                {p:'og:title', c:'Krish Kilaru — Databricks Solutions Architect | Interactive AI Resume'},
                {p:'og:description', c:'19+ years in data engineering. 10 Databricks certifications. Explore skills, projects, and ask Abu the AI chatbot anything.'},
                {p:'og:type', c:'website'},
                {p:'og:url', c:'https://thedatabrickster.streamlit.app'},
                {p:'og:image', c:'https://www.databricks.com/sites/default/files/2025-10/professional-badge-de.png?v=1761143167'},
                {n:'description', c:'Interactive AI-powered resume of Krish Kilaru — Databricks Solutions Architect with 19+ years of data engineering experience. Features a Genie-powered Q&A chatbot.'},
                {n:'author', c:'Krish Kilaru'},
                {n:'keywords', c:'Databricks, Solutions Architect, Data Engineer, Resume, AI Resume, Krish Kilaru, Unity Catalog, Delta Lake'}
            ];
            metas.forEach(function(m) {
                var tag = document.createElement('meta');
                if (m.p) tag.setAttribute('property', m.p);
                if (m.n) tag.setAttribute('name', m.n);
                tag.setAttribute('content', m.c);
                document.head.appendChild(tag);
            });
        }
    </script>
    """, height=0)


# ────────────────────────────────────────────────────────────────
# Main App
# ────────────────────────────────────────────────────────────────

def main():
    # Load all data
    profile_df = load_table("profile")
    work_df = load_table("work_experience")
    highlights_df = load_table("work_highlights")
    skills_df = load_table("skills")
    edu_df = load_table("education")
    certs_df = load_table("certifications")
    projects_df = load_table("projects")
    pubs_df = load_table("publications")
    timeline_df = load_table("career_timeline")
    clients_df = load_table("clients")

    # Dark mode toggle (top-right)
    toggle_col1, toggle_col2 = st.columns([9, 1])
    with toggle_col2:
        if st.toggle("🌙", value=st.session_state.dark_mode, help="Dark mode"):
            if not st.session_state.dark_mode:
                st.session_state.dark_mode = True
                st.rerun()
        else:
            if st.session_state.dark_mode:
                st.session_state.dark_mode = False
                st.rerun()

    # Header
    render_profile_header(profile_df, certs_df)

    # Tabs
    tab_dashboard, tab_genie, tab_meeting, tab_pdf, tab_about = st.tabs(["📊  Resume Dashboard", "🐒  Ask Abu Anything", "📅  Book a Meeting", "📄  Download Resume", "🛠  How This Was Built"])

    with tab_dashboard:
        render_metrics(profile_df, work_df, skills_df, certs_df, clients_df)
        render_summary(profile_df)
        render_education(edu_df)
        render_career_timeline(timeline_df)
        render_skills_charts(skills_df)
        render_clients(clients_df)
        render_experience(work_df, highlights_df)
        render_projects(projects_df)
        render_publications(pubs_df)
        render_testimonials()

        # Footer
        _html(
            "<hr style='border:none; border-top:1px solid #E8EDF1; margin:30px 0 10px;'>"
            "<p style='text-align:center; color:#999; font-size:0.8rem;'>"
            "Powered by Databricks AI/BI · Data model in Unity Catalog · "
            "Abu-powered Q&A 🐒</p>"
        )
        # Visitor counter
        st.markdown(
            '<div style="text-align:center;">'
            '<img src="https://visitor-badge.laobi.icu/badge?page_id=thedatabrickster.streamlit.app" alt="visitors" />'
            '</div>',
            unsafe_allow_html=True,
        )

    with tab_genie:
        render_genie_chat()

    with tab_meeting:
        calendly_url = "https://calendly.com/thedatabrickster"
        if not profile_df.empty:
            calendly_url = profile_df.iloc[0].get("calendly_url", calendly_url) or calendly_url
        _html(f"""
        <div style="text-align:center; margin-bottom:16px;">
            <h3 style="color:#1B3A4B; margin-bottom:4px;">📅 Schedule a Meeting with Krish</h3>
            <p style="color:#6C757D; font-size:0.9rem;">Pick a time that works for you — let's connect!</p>
        </div>
        """)
        st.components.v1.iframe(f"{calendly_url}?hide_gdpr_banner=1", height=700, scrolling=True)

    with tab_pdf:
        _html("""
        <div style="text-align:center; margin-bottom:20px;">
            <h3 style="color:#1B3A4B; margin-bottom:6px;">📄 Download Krish's Resume</h3>
            <p style="color:#6C757D; font-size:0.9rem;">
                Get a clean, ATS-friendly PDF version of this resume for your records or to share with hiring managers.
            </p>
        </div>
        """)
        data = load_resume_json()
        if data:
            pdf_bytes = generate_pdf(data)
            dl_col1, dl_col2, dl_col3 = st.columns([1, 2, 1])
            with dl_col2:
                st.download_button(
                    label="📄 Download Resume as PDF",
                    data=pdf_bytes,
                    file_name="Krish_Kilaru_Resume.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                )
        _html("""
        <div style="margin-top:24px; background:#F8F9FA; border-radius:12px; padding:20px 24px;
                    border:1px solid #E8EDF1; text-align:center;">
            <p style="color:#6C757D; font-size:0.82rem; margin:0;">
                💡 <strong>Tip:</strong> This PDF is auto-generated from the same data that powers this interactive resume.
                For the full experience — including the AI chatbot, skills visualizations, and project details —
                share the link: <a href="https://thedatabrickster.streamlit.app" style="color:#065A82;">thedatabrickster.streamlit.app</a>
            </p>
        </div>
        """)

    with tab_about:
        render_about_app()

    inject_analytics()
    inject_seo_meta()


def render_about_app():
    _html("""
    <div style="max-width:900px; margin:0 auto;">
        <div style="text-align:center; margin-bottom:28px;">
            <h2 style="color:#1B3A4B; margin-bottom:6px;">🛠 How This App Was Built</h2>
            <p style="color:#6C757D; font-size:0.95rem;">
                This isn't a traditional resume — it's a <strong>full-stack data application</strong>
                built on the Databricks Lakehouse platform with an AI-powered Q&amp;A chatbot.
            </p>
        </div>
    </div>
    """)

    arch_col1, arch_col2 = st.columns(2)

    with arch_col1:
        _html("""
        <div style="background:linear-gradient(135deg,#1B3A4B 0%,#065A82 100%); color:white;
                    border-radius:14px; padding:24px 28px; height:100%;">
            <h3 style="margin:0 0 16px; font-size:1.15rem;">📐 Architecture</h3>
            <div style="font-size:0.88rem; line-height:1.7; opacity:0.95;">
                <p style="margin:0 0 10px;">All resume data lives as <strong>Delta tables</strong> in
                <strong>Databricks Unity Catalog</strong>, following the Lakehouse pattern.</p>
                <p style="margin:0 0 10px;">The app queries tables via the <strong>Databricks SQL Statement Execution API</strong>,
                with local JSON fallback for resilience.</p>
                <p style="margin:0;">The <strong>Abu chatbot 🐒</strong> uses <strong>Databricks AI/BI Genie</strong>
                for natural-language SQL generation, backed by a curated FAQ engine for recruiter questions.</p>
            </div>
        </div>
        """)

    with arch_col2:
        _html("""
        <div style="background:linear-gradient(135deg,#0D7C66 0%,#1A9E78 100%); color:white;
                    border-radius:14px; padding:24px 28px; height:100%;">
            <h3 style="margin:0 0 16px; font-size:1.15rem;">🔄 Data Flow</h3>
            <div style="font-size:0.88rem; line-height:1.7; opacity:0.95;">
                <p style="margin:0 0 6px;"><strong>1.</strong> Resume data authored in <code style="background:rgba(255,255,255,0.2); padding:1px 5px; border-radius:4px;">resume_data.json</code></p>
                <p style="margin:0 0 6px;"><strong>2.</strong> Pushed to Delta tables via SQL Statement Execution API</p>
                <p style="margin:0 0 6px;"><strong>3.</strong> Genie Space configured with table instructions &amp; comments</p>
                <p style="margin:0 0 6px;"><strong>4.</strong> Streamlit app reads from Databricks (JSON fallback)</p>
                <p style="margin:0;"><strong>5.</strong> Abu chatbot routes: FAQ → Genie API → Local Q&amp;A</p>
            </div>
        </div>
        """)

    st.markdown("")

    _html("""
    <div style="max-width:900px; margin:0 auto;">
        <h3 style="color:#1B3A4B; margin-bottom:16px; text-align:center;">🧰 Tools & Technologies</h3>
    </div>
    """)

    tools = [
        ("Databricks Unity Catalog", "Centralized metadata & governance for all resume Delta tables", "#FF3621", "🏛️"),
        ("Databricks SQL Warehouse", "Serverless compute for querying resume data via SQL", "#FF3621", "⚡"),
        ("Databricks AI/BI Genie", "Natural language → SQL for the Abu chatbot Q&A", "#FF3621", "🧞"),
        ("Delta Lake", "ACID-compliant open table format for reliable data storage", "#FF3621", "🔺"),
        ("SQL Statement Execution API", "REST API for executing SQL & managing data from Python", "#FF3621", "🔌"),
        ("Streamlit", "Python framework for the interactive web UI & dashboard", "#FF4B4B", "🎈"),
        ("Streamlit Community Cloud", "Free hosting platform for the public-facing app", "#FF4B4B", "☁️"),
        ("Python", "Core language for data processing, API calls, and app logic", "#3776AB", "🐍"),
        ("Pandas", "DataFrame manipulation for transforming resume data", "#150458", "🐼"),
        ("Plotly", "Interactive charts & visualizations for skills and metrics", "#3F4F75", "📊"),
        ("HTML / CSS", "Custom layouts — timelines, skill cards, client grid, banners", "#E34F26", "🎨"),
        ("Git & GitHub", "Version control & CI/CD trigger for Streamlit Cloud deployments", "#24292E", "💻"),
        ("Databricks REST API", "Direct API integration for Genie conversations", "#FF3621", "🔗"),
        ("Base64 Encoding", "Embedding local images (profile photo, client logos) in HTML", "#6C757D", "🖼️"),
        ("Calendly", "Embedded scheduling widget for recruiter meetings", "#006BFF", "📅"),
    ]

    cols_per_row = 3
    rows_html = ""
    for i, (name, desc, color, icon) in enumerate(tools):
        if i % cols_per_row == 0:
            if i > 0:
                rows_html += "</div>"
            rows_html += '<div style="display:flex; gap:14px; margin-bottom:14px;">'
        rows_html += f'''
        <div style="flex:1; background:white; border-radius:12px; padding:18px 20px;
                    border-left:4px solid {color}; box-shadow:0 2px 8px rgba(0,0,0,0.06);">
            <div style="font-size:1.3rem; margin-bottom:6px;">{icon}</div>
            <div style="font-weight:700; color:#1B3A4B; font-size:0.9rem; margin-bottom:4px;">{name}</div>
            <div style="color:#6C757D; font-size:0.78rem; line-height:1.4;">{desc}</div>
        </div>
        '''
    rows_html += "</div>"

    _html(f'<div style="max-width:900px; margin:0 auto;">{rows_html}</div>')

    st.markdown("")

    _html("""
    <div style="max-width:900px; margin:0 auto;">
        <div style="background:#F8F9FA; border-radius:14px; padding:24px 28px; border:1px solid #E8EDF1;">
            <h3 style="color:#1B3A4B; margin:0 0 14px; font-size:1.05rem;">💡 Why Build a Resume This Way?</h3>
            <div style="display:flex; gap:20px; flex-wrap:wrap;">
                <div style="flex:1; min-width:200px;">
                    <p style="color:#495057; font-size:0.85rem; line-height:1.6; margin:0;">
                        <strong>For recruiters:</strong> Instead of a static PDF, you get an interactive experience —
                        ask Abu any question, explore skills visually, and book a meeting in one click.
                    </p>
                </div>
                <div style="flex:1; min-width:200px;">
                    <p style="color:#495057; font-size:0.85rem; line-height:1.6; margin:0;">
                        <strong>As a showcase:</strong> This app <em>is</em> the portfolio. It demonstrates
                        Databricks, Unity Catalog, Genie, Delta Lake, Streamlit, and full-stack data app
                        development — the exact skills Krish brings to every engagement.
                    </p>
                </div>
            </div>
        </div>
    </div>
    """)

    _html("""
    <div style="max-width:900px; margin:16px auto 0;">
        <div style="background:linear-gradient(135deg,#2C3E50,#3498DB); border-radius:14px;
                    padding:20px 28px; color:white; text-align:center;">
            <p style="margin:0; font-size:0.88rem; opacity:0.9;">
                🐒 <strong>Fun fact:</strong> Abu answers recruiter questions using a 3-tier system —
                <strong>Curated FAQ</strong> (instant) → <strong>Databricks Genie API</strong> (SQL generation) →
                <strong>Local Q&amp;A Engine</strong> (keyword search across all resume data).
            </p>
        </div>
    </div>
    """)


if __name__ == "__main__":
    main()
