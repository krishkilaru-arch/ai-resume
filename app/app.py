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
    page_title="Interactive AI Resume",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="collapsed",
)

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
        font-size: 1.1rem;
    }
    .exp-card .subtitle {
        color: #065A82;
        font-weight: 600;
        font-size: 0.95rem;
    }
    .exp-card .meta {
        color: #6C757D;
        font-size: 0.85rem;
        margin-bottom: 12px;
    }
    .exp-card .highlight {
        padding: 6px 0;
        font-size: 0.9rem;
        color: #212529;
    }
    .exp-card .badge {
        display: inline-block;
        font-size: 0.72rem;
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
        font-size: 1.1rem;
        opacity: 0.9;
        margin-top: 6px;
        font-weight: 400;
    }
    .profile-header .location {
        font-size: 0.9rem;
        opacity: 0.75;
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
    .profile-header .links a {
        color: #B8D4E3;
        text-decoration: none;
        margin-right: 20px;
        font-size: 0.85rem;
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

def _greeting_response():
    return {
        "text": (
            "Hey there! I'm **Abu** 🐒 — Krish's trusty sidekick, here to guide you through "
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

    api_result = _genie_ask_api(question, conversation_id)
    if api_result and api_result.get("text"):
        genie_text = api_result["text"].lower()
        if any(phrase in genie_text for phrase in [
            "unrelated to", "cannot answer", "not related",
            "i'm here to help you analyze", "please let me know what",
            "please ask a question about", "available tables",
        ]):
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

    links_html = ""
    if linkedin:
        links_html += f'<a href="{linkedin}">LinkedIn</a>'
    if github:
        links_html += f'<a href="{github}">GitHub</a>'
    if email:
        links_html += f'<a href="mailto:{email}">{email}</a>'

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
    if certs_df is not None and not certs_df.empty:
        org_col = "issuing_organization" if "issuing_organization" in certs_df.columns else "issuing_org"
        name_col = "certification_name" if "certification_name" in certs_df.columns else "name"
        db_certs = certs_df[certs_df[org_col].str.lower() == "databricks"]
        badge_names = {n for n, _ in CERT_BADGES}
        if not db_certs.empty:
            for _, cert in db_certs.iterrows():
                cn = cert.get(name_col, "")
                if cn not in badge_names:
                    short = cn.replace("Databricks Certified ", "").replace("Partner Training - ", "").replace("Academy Accreditation - ", "").replace("Knowledge Badge - ", "")
                    text_badges_html += f'<span class="cert-badge" title="{cn}">🏅 {short}</span>'

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
                <div class="headline">{headline}</div>
                <div class="location">📍 {location}</div>
                <div class="links" style="margin-top:10px;">{links_html}</div>
                {text_row}
            </div>
            {right_col}
        </div>
    </div>
    """)


def render_metrics(profile_df, work_df, skills_df, certs_df):
    yrs = profile_df.iloc[0].get("years_of_experience", "—") if not profile_df.empty else "—"
    companies = len(work_df) if not work_df.empty else 0
    total_skills = len(skills_df) if not skills_df.empty else 0
    expert_skills = 0
    if not skills_df.empty and "proficiency_level" in skills_df.columns:
        expert_skills = len(skills_df[skills_df["proficiency_level"] == "Expert"])
    active_certs = 0
    if not certs_df.empty and "is_active" in certs_df.columns:
        active_certs = len(certs_df[certs_df["is_active"].astype(str).str.lower().isin(["true", "1"])])

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Years Experience", yrs)
    c2.metric("Companies", companies)
    c3.metric("Total Skills", total_skills)
    c4.metric("Expert Skills", expert_skills)
    c5.metric("Certifications", active_certs)


def render_summary(profile_df):
    if profile_df.empty:
        return
    summary = profile_df.iloc[0].get("summary", "")
    if summary:
        _html('<div class="section-header">Professional Summary</div>')
        _html(f"<p style='font-size:0.95rem; line-height:1.7; color:#333;'>{summary}</p>"
                    )


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

    cards_html = ""
    for _, row in df.iterrows():
        is_work = row["event_type"] == "Work"
        is_current = row["is_current"]
        icon = "💼" if is_work else "🎓"
        color = org_colors[row["organization"]]
        start_fmt = row["start_dt"].strftime("%Y")
        end_fmt = "Now" if is_current else row["end_dt"].strftime("%Y")

        left_pct = ((row["start_dt"] - earliest).days / total_days) * 100
        width_pct = max(((row["end_dt"] - row["start_dt"]).days / total_days) * 100, 14)

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
    start_year = earliest.year
    end_year = latest.year + 1
    for y in range(start_year, end_year + 1, 3):
        y_date = datetime(y, 1, 1)
        pct = ((y_date - earliest).days / total_days) * 100
        if 0 <= pct <= 100:
            year_markers += f'<div style="position:absolute;left:{pct}%;top:-16px;transform:translateX(-50%);font-size:0.68rem;color:#999;font-weight:600;">{y}</div>'
            year_markers += f'<div style="position:absolute;left:{pct}%;top:0;bottom:0;width:1px;background:rgba(0,0,0,0.05);"></div>'

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
    cat_labels = {k: k.split(". ", 1)[1] for k in cat_order}

    chart_df = df[df["category"].isin(cat_order)].copy()
    if chart_df.empty:
        return

    chart_df["cat_sort"] = chart_df["category"].map({c: i for i, c in enumerate(cat_order)})
    chart_df = chart_df.sort_values(["cat_sort", "rating"], ascending=[True, False])

    fig = go.Figure()

    for cat in cat_order:
        cat_df = chart_df[chart_df["category"] == cat].sort_values("rating", ascending=True)
        if cat_df.empty:
            continue
        fig.add_trace(go.Bar(
            x=cat_df["rating"],
            y=cat_df["skill_name"],
            orientation="h",
            name=cat_labels[cat],
            marker=dict(color=cat_color_map[cat], line=dict(width=0)),
            text=cat_df["rating"].apply(lambda r: f" {r}/10"),
            textposition="outside",
            textfont=dict(size=11, color="#444", family="Arial"),
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Rating: %{x}/10<br>"
                "<extra></extra>"
            ),
        ))

    fig.update_xaxes(
        range=[0, 11.5],
        dtick=2,
        title=dict(text="Rating", font=dict(size=11, color="#888")),
        tickfont=dict(size=10),
        gridcolor="rgba(0,0,0,0.05)",
        zeroline=False,
    )
    fig.update_yaxes(
        title="",
        tickfont=dict(size=12),
    )
    fig.update_layout(
        height=max(500, len(chart_df) * 28 + 100),
        margin=dict(l=10, r=30, t=10, b=40),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            xanchor="center", x=0.5, title="",
            font=dict(size=11),
        ),
        bargap=0.22,
        font=dict(size=12),
    )

    st.plotly_chart(fig, use_container_width=True)

    other_df = df[~df["category"].isin(cat_order)].sort_values(["category", "rating"], ascending=[True, False])
    if not other_df.empty:
        _html('<div style="font-size:1rem;font-weight:700;color:#1B3A4B;margin-top:16px;margin-bottom:8px;">Additional Skills</div>')
        cats = other_df["category"].unique()
        cols = st.columns(min(len(cats), 3))
        for i, cat in enumerate(cats):
            with cols[i % len(cols)]:
                cat_df = other_df[other_df["category"] == cat]
                _html(f'<div style="font-size:0.85rem;font-weight:700;color:#1B3A4B;margin-top:12px;margin-bottom:6px;">{cat}</div>')
                for _, row in cat_df.iterrows():
                    r = int(row["rating"])
                    filled = "█" * r + "░" * (10 - r)
                    prof = row[prof_col]
                    color = "#065A82" if prof == "Expert" else "#1C7C54" if prof == "Advanced" else "#F4A261"
                    _html(
                        f'<div style="font-size:0.78rem;margin-bottom:3px;">'
                        f'<span style="color:#333;">{row["skill_name"]}</span> '
                        f'<span style="color:{color};font-family:monospace;font-size:0.7rem;">{filled}</span> '
                        f'<span style="color:#888;font-size:0.7rem;">{r}/10</span>'
                        f'</div>'
                    )


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
                    <span style="color:#1C7C54; font-weight:600; font-size:0.82rem;">
                        → {h.get('impact_metric', '')}
                    </span>
                </div>"""

        role_html = f'<div style="font-size:0.93rem; margin-top:2px;"><span style="color:#6C757D;">Role at Customer:</span> <span style="color:#065A82; font-weight:600;">{role}</span></div>' if role and role != title else ""
        title_label = f"Title at {company}" if role and role != title else ""
        title_line = f'<div style="font-size:0.93rem;"><span style="color:#6C757D;">{title_label}:</span> <span style="font-weight:600;">{title}</span></div>' if title_label else f"<h4>{title}</h4>"
        _html(f"""
        <div class="exp-card">
            {title_line}
            {role_html}
            <div class="subtitle">{company}</div>
            <div class="meta">📍 {location} &nbsp;|&nbsp; 📅 {start} — {end} {badge_html}</div>
            <div style="color:#444; font-size:0.9rem; margin-bottom:8px;">{desc}</div>
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
    "Tell me about Krish Kilaru",
    "What are Krish's top technical skills?",
    "What is Krish's current role?",
    "Show Krish's career progression",
    "What certifications does Krish hold?",
    "Does Krish have leadership experience?",
    "What industries has Krish worked in?",
    "What was Krish's most impactful project?",
]


def render_genie_chat():
    _html("""
    <div class="genie-banner">
        <h3>🐒 Abu — Ask Me Anything About Krish</h3>
        <p>Ask any question about Krish's career, skills, experience, or qualifications. Powered by Databricks AI/BI Genie 🧞</p>
    </div>
    """)

    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "conversation_id" not in st.session_state:
        st.session_state.conversation_id = None

    # Quick question buttons
    st.markdown("**Suggested questions:**")
    btn_cols = st.columns(4)
    for i, q in enumerate(QUICK_QUESTIONS):
        if btn_cols[i % 4].button(q, key=f"quick_{i}", width="stretch"):
            st.session_state.pending_question = q
            st.rerun()

    st.divider()

    # Display chat history
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"], avatar="👤" if msg["role"] == "user" else "🐒"):
            st.markdown(msg["content"])
            if msg.get("df") is not None and not msg["df"].empty:
                st.dataframe(msg["df"], width="stretch", hide_index=True)
            if msg.get("sql"):
                with st.expander("View generated SQL"):
                    st.code(msg["sql"], language="sql")

    # Handle pending question from button click
    pending = st.session_state.pop("pending_question", None)

    # Chat input
    user_input = st.chat_input("Ask anything about my career, skills, or experience...")

    question = pending or user_input
    if question:
        st.session_state.messages.append({"role": "user", "content": question})

        with st.chat_message("user", avatar="👤"):
            st.markdown(question)

        with st.chat_message("assistant", avatar="🐒"):
            with st.spinner("Querying resume data..."):
                result = genie_ask(question, st.session_state.conversation_id)

            source = result.get("source", "local")
            if source in ("genie", "greeting"):
                st.caption("Powered by Databricks AI/BI Genie")
            else:
                api_err = st.session_state.pop("_genie_api_error", None)
                debug_parts = []
                if not GENIE_SPACE_ID:
                    debug_parts.append("GENIE_SPACE_ID not set")
                if not WAREHOUSE_ID:
                    debug_parts.append("WAREHOUSE_ID not set")
                w = get_workspace_client()
                if not w:
                    wsc_err = st.session_state.get("_wsc_error", "unknown")
                    debug_parts.append(f"WorkspaceClient: {wsc_err}")
                if api_err:
                    debug_parts.append(f"API error: {api_err[:150]}")
                if debug_parts:
                    st.caption(f"Local Q&A fallback ({'; '.join(debug_parts)})")

            st.markdown(result["text"])
            if result.get("df") is not None and not result["df"].empty:
                st.dataframe(result["df"], width="stretch", hide_index=True)
            if result.get("sql"):
                with st.expander("View generated SQL"):
                    st.code(result["sql"], language="sql")

            st.session_state.conversation_id = result.get("conversation_id")

        st.session_state.messages.append({
            "role": "assistant", "content": result["text"],
            "df": result.get("df"), "sql": result.get("sql"),
        })


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

    # Header
    render_profile_header(profile_df, certs_df)

    # Tabs
    tab_dashboard, tab_genie = st.tabs(["📊  Resume Dashboard", "🐒  Ask Abu Anything"])

    with tab_dashboard:
        render_metrics(profile_df, work_df, skills_df, certs_df)
        render_summary(profile_df)
        render_education(edu_df)
        render_career_timeline(timeline_df)
        render_skills_charts(skills_df)
        render_clients(clients_df)
        render_experience(work_df, highlights_df)
        render_projects(projects_df)
        render_publications(pubs_df)

        # Footer
        _html(
            "<hr style='border:none; border-top:1px solid #E8EDF1; margin:30px 0 10px;'>"
            "<p style='text-align:center; color:#999; font-size:0.8rem;'>"
            "Powered by Databricks AI/BI · Data model in Unity Catalog · "
            "Abu-powered Q&A 🐒</p>"
        )

    with tab_genie:
        render_genie_chat()


if __name__ == "__main__":
    main()
