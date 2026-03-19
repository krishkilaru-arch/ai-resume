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
from pathlib import Path
from datetime import datetime, timedelta

HAS_DATABRICKS_SDK = False
try:
    from databricks.sdk import WorkspaceClient
    HAS_DATABRICKS_SDK = True
except ImportError:
    pass

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

st.markdown("""
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
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 10px 24px;
        font-weight: 600;
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
""", unsafe_allow_html=True)


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


@st.cache_data(ttl=600)
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
                "title": exp.get("title_at_employer", exp.get("title", "")), "location": exp["location"],
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

    if table == "projects":
        rows = []
        for i, p in enumerate(raw, 1):
            rows.append({
                "project_id": i, "project_name": p["name"],
                "description": p["description"], "role": p["role"],
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


@st.cache_data(ttl=300)
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
    (["work experience", "job history", "employment", "work history", "where have they worked", "previous job", "past role"], "work_history"),
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


def genie_ask(question, conversation_id=None):
    """Try real Genie API first, fall back to local Q&A engine."""
    api_result = _genie_ask_api(question, conversation_id)
    if api_result and api_result.get("text"):
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
                    f"Example: *\"What are their top skills?\"* or *\"Tell me about their Databricks experience\"*")
        sql = f"-- Full-text search across resume tables\n-- Query: '{question}'"

    return {"text": text, "sql": sql, "df": df,
            "conversation_id": "local", "status": "COMPLETED"}


# ────────────────────────────────────────────────────────────────
# Dashboard Components
# ────────────────────────────────────────────────────────────────

def render_profile_header(profile_df):
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

    st.markdown(f"""
    <div class="profile-header">
        <h1>{name}</h1>
        <div class="headline">{headline}</div>
        <div class="location">📍 {location}</div>
        <div class="links" style="margin-top:10px;">{links_html}</div>
    </div>
    """, unsafe_allow_html=True)


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
        st.markdown('<div class="section-header">Professional Summary</div>', unsafe_allow_html=True)
        st.markdown(f"<p style='font-size:0.95rem; line-height:1.7; color:#333;'>{summary}</p>",
                    unsafe_allow_html=True)


def render_career_timeline(timeline_df):
    if timeline_df.empty:
        return
    st.markdown('<div class="section-header">Career Timeline</div>', unsafe_allow_html=True)

    df = timeline_df.copy()
    today_str = datetime.now().strftime("%Y-%m-%d")
    df["end_plot"] = df["end_date"].apply(lambda x: today_str if x in ("Present", None, "") else x)
    df["start_plot"] = pd.to_datetime(df["start_date"])
    df["end_plot"] = pd.to_datetime(df["end_plot"])
    df["label"] = df["title"] + " @ " + df["organization"]

    color_map = {"Work": "#065A82", "Education": "#1C7C54", "Certification": "#F4A261"}

    fig = px.timeline(
        df, x_start="start_plot", x_end="end_plot", y="label",
        color="event_type", color_discrete_map=color_map,
    )
    fig.update_yaxes(autorange="reversed", title="")
    fig.update_xaxes(title="")
    fig.update_layout(
        height=max(200, len(df) * 55 + 80),
        margin=dict(l=0, r=20, t=10, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, title=""),
        font=dict(size=12),
        plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, width="stretch")


def render_skills_charts(skills_df):
    if skills_df.empty:
        return
    st.markdown('<div class="section-header">Skills & Expertise</div>', unsafe_allow_html=True)

    df = skills_df.copy()
    if "years_of_experience" in df.columns:
        df["years_of_experience"] = pd.to_numeric(df["years_of_experience"], errors="coerce")

    col1, col2 = st.columns([3, 2])

    with col1:
        tech_df = df[df["category"] != "Soft Skills"].sort_values("years_of_experience", ascending=True)
        fig = px.bar(
            tech_df, x="years_of_experience", y="skill_name",
            color="proficiency_level", orientation="h",
            color_discrete_map={"Expert": "#065A82", "Advanced": "#1C7C54", "Intermediate": "#F4A261"},
            labels={"years_of_experience": "Years", "skill_name": "", "proficiency_level": "Level"},
        )
        fig.update_layout(
            height=max(300, len(tech_df) * 28 + 60),
            margin=dict(l=0, r=10, t=10, b=30),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.5, xanchor="center", title=""),
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(size=11),
        )
        st.plotly_chart(fig, width="stretch")

    with col2:
        cat_counts = df.groupby("category").size().reset_index(name="count").sort_values("count", ascending=False)
        fig2 = px.pie(
            cat_counts, values="count", names="category", hole=0.45,
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig2.update_layout(
            height=320,
            margin=dict(l=0, r=0, t=10, b=10),
            legend=dict(font=dict(size=10)),
            font=dict(size=11),
        )
        fig2.update_traces(textposition="inside", textinfo="label+value")
        st.plotly_chart(fig2, width="stretch")

        if "proficiency_level" in df.columns:
            prof_counts = df["proficiency_level"].value_counts().reset_index()
            prof_counts.columns = ["level", "count"]
            for _, row in prof_counts.iterrows():
                color = {"Expert": "#065A82", "Advanced": "#1C7C54", "Intermediate": "#F4A261"}.get(row["level"], "#999")
                st.markdown(
                    f"<span style='color:{color}; font-weight:700;'>{row['level']}</span>: "
                    f"{row['count']} skills",
                    unsafe_allow_html=True,
                )


def render_experience(work_df, highlights_df):
    if work_df.empty:
        return
    st.markdown('<div class="section-header">Work Experience</div>', unsafe_allow_html=True)

    for _, row in work_df.iterrows():
        company = row.get("company", "")
        title = row.get("title", "")
        location = row.get("location", "")
        start = row.get("start_date", "")
        end = row.get("end_date", "Present")
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

        st.markdown(f"""
        <div class="exp-card">
            <h4>{title}</h4>
            <div class="subtitle">{company}</div>
            <div class="meta">📍 {location} &nbsp;|&nbsp; 📅 {start} — {end} {badge_html}</div>
            <div style="color:#444; font-size:0.9rem; margin-bottom:8px;">{desc}</div>
            {highlights_html}
        </div>
        """, unsafe_allow_html=True)


def render_education(edu_df):
    if edu_df.empty:
        return
    st.markdown('<div class="section-header">Education</div>', unsafe_allow_html=True)
    for _, row in edu_df.iterrows():
        gpa = row.get("gpa", "")
        honors = row.get("honors", "")
        detail_parts = []
        if gpa:
            detail_parts.append(f"GPA: {gpa}/4.0")
        if honors:
            detail_parts.append(honors)
        detail = " · ".join(detail_parts)
        coursework = row.get("relevant_coursework", "")

        st.markdown(f"""
        <div class="info-card">
            <h5>{row.get('degree', '')} in {row.get('field_of_study', '')}</h5>
            <div style="color:#065A82; font-weight:600; font-size:0.9rem;">
                {row.get('institution', '')}
            </div>
            <div class="detail">📅 {row.get('start_date', '')} — {row.get('end_date', '')} &nbsp;|&nbsp; {detail}</div>
            {'<div class="detail" style="margin-top:4px;">📚 ' + coursework + '</div>' if coursework else ''}
        </div>
        """, unsafe_allow_html=True)


def render_certifications(certs_df):
    if certs_df.empty:
        return
    st.markdown('<div class="section-header">Certifications</div>', unsafe_allow_html=True)
    for _, row in certs_df.iterrows():
        active_str = str(row.get("is_active", "")).lower()
        is_active = active_str in ("true", "1")
        status = "✅ Active" if is_active else "⏰ Expired"
        status_color = "#1C7C54" if is_active else "#DC3545"

        st.markdown(f"""
        <div class="info-card">
            <h5>{row.get('certification_name', '')}</h5>
            <div class="detail">
                {row.get('issuing_organization', '')} &nbsp;|&nbsp;
                📅 Issued: {row.get('issue_date', '')} &nbsp;|&nbsp;
                <span style="color:{status_color}; font-weight:600;">{status}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)


def render_projects(projects_df):
    if projects_df.empty:
        return
    st.markdown('<div class="section-header">Key Projects</div>', unsafe_allow_html=True)

    cols = st.columns(min(len(projects_df), 3))
    for i, (_, row) in enumerate(projects_df.iterrows()):
        with cols[i % len(cols)]:
            status = "🔄 Active" if str(row.get("is_current", "")).lower() in ("true", "1") else "✅ Complete"
            st.markdown(f"""
            <div class="info-card" style="min-height:200px;">
                <h5>{row.get('project_name', '')}</h5>
                <div style="font-size:0.82rem; color:#065A82; font-weight:600; margin-bottom:6px;">
                    {row.get('role', '')} · {status}
                </div>
                <div class="detail" style="margin-bottom:8px;">{row.get('description', '')}</div>
                <div style="font-size:0.82rem; color:#1C7C54; font-weight:600;">
                    Impact: {row.get('impact', '')}
                </div>
                <div class="detail" style="margin-top:6px;">
                    🛠 {row.get('technologies_used', '')}
                </div>
            </div>
            """, unsafe_allow_html=True)


def render_publications(pubs_df):
    if pubs_df.empty:
        return
    st.markdown('<div class="section-header">Publications & Talks</div>', unsafe_allow_html=True)
    for _, row in pubs_df.iterrows():
        url = row.get("url", "")
        title = row.get("title", "")
        link_html = f'<a href="{url}" target="_blank">{title}</a>' if url else title
        st.markdown(f"""
        <div class="info-card">
            <h5>{link_html}</h5>
            <div class="detail">
                {row.get('publisher', '')} &nbsp;|&nbsp;
                📅 {row.get('publication_date', '')} &nbsp;|&nbsp;
                📝 {row.get('publication_type', '')}
            </div>
        </div>
        """, unsafe_allow_html=True)


# ────────────────────────────────────────────────────────────────
# Genie Chat Component
# ────────────────────────────────────────────────────────────────

QUICK_QUESTIONS = [
    "Tell me about this candidate",
    "What are their top technical skills?",
    "What is their current role?",
    "Show their career progression",
    "What certifications do they hold?",
    "Do they have leadership experience?",
    "What industries have they worked in?",
    "What was their most impactful project?",
]


def render_genie_chat():
    st.markdown("""
    <div class="genie-banner">
        <h3>🧞 AI/BI Genie — Ask Me Anything</h3>
        <p>Ask any question about this candidate's career, skills, experience, or qualifications.
           Powered by a structured data model with 9 tables in Unity Catalog format.
           Each question generates a SQL query and returns results — just like Databricks Genie.</p>
    </div>
    """, unsafe_allow_html=True)

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
        with st.chat_message(msg["role"], avatar="👤" if msg["role"] == "user" else "🧞"):
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

        with st.chat_message("assistant", avatar="🧞"):
            with st.spinner("Querying resume data..."):
                result = genie_ask(question, st.session_state.conversation_id)

            source = result.get("source", "local")
            if source == "genie":
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

    # Header
    render_profile_header(profile_df)

    # Tabs
    tab_dashboard, tab_genie = st.tabs(["📊  Resume Dashboard", "🧞  Ask Me Anything"])

    with tab_dashboard:
        render_metrics(profile_df, work_df, skills_df, certs_df)
        render_summary(profile_df)
        render_career_timeline(timeline_df)
        render_skills_charts(skills_df)
        render_experience(work_df, highlights_df)

        col_edu, col_cert = st.columns(2)
        with col_edu:
            render_education(edu_df)
        with col_cert:
            render_certifications(certs_df)

        render_projects(projects_df)
        render_publications(pubs_df)

        # Footer
        st.markdown("---")
        st.markdown(
            "<p style='text-align:center; color:#999; font-size:0.8rem;'>"
            "Powered by Databricks AI/BI · Data model in Unity Catalog · "
            "Genie-powered Q&A</p>",
            unsafe_allow_html=True,
        )

    with tab_genie:
        render_genie_chat()


if __name__ == "__main__":
    main()
