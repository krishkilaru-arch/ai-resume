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

CATALOG = os.getenv("RESUME_CATALOG", "resume_catalog")
SCHEMA = os.getenv("RESUME_SCHEMA", "career_profile")
WAREHOUSE_ID = os.getenv("SQL_WAREHOUSE_ID", "")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "")
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
        return None
    try:
        return WorkspaceClient()
    except Exception:
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
                    "company": exp["company"], "title": exp["title"],
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
                "title": exp["title"], "location": exp["location"],
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
                "title": exp["title"], "organization": exp["company"],
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
# Genie Q&A
# ────────────────────────────────────────────────────────────────

def genie_ask(question, conversation_id=None):
    """Send a question to Genie and return the response."""
    w = get_workspace_client()
    if not w or not GENIE_SPACE_ID:
        return {
            "text": "Genie is not configured. Set `GENIE_SPACE_ID` and `SQL_WAREHOUSE_ID` to enable Q&A.",
            "sql": None, "df": None, "conversation_id": None, "status": "NOT_CONFIGURED",
        }

    try:
        if conversation_id:
            resp = w.genie.create_message(
                space_id=GENIE_SPACE_ID,
                conversation_id=conversation_id,
                content=question,
            )
            conv_id = conversation_id
            msg_id = resp.id
        else:
            resp = w.genie.start_conversation(
                space_id=GENIE_SPACE_ID,
                content=question,
            )
            conv_id = resp.conversation_id
            msg_id = resp.message_id

        # Poll for completion
        message = None
        for _ in range(60):
            message = w.genie.get_message(GENIE_SPACE_ID, conv_id, msg_id)
            status = message.status if hasattr(message, "status") else None
            if status and status.value in ("COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"):
                break
            time.sleep(1)

        answer_text = message.content or "" if message else ""
        sql_query = None
        result_df = None

        if message and message.attachments:
            for att in message.attachments:
                if hasattr(att, "text") and att.text:
                    answer_text = att.text.content
                if hasattr(att, "query") and att.query:
                    sql_query = att.query.query
                    try:
                        qr = w.genie.get_message_query_result(GENIE_SPACE_ID, conv_id, msg_id)
                        if qr and hasattr(qr, "statement_response") and qr.statement_response:
                            sr = qr.statement_response
                            cols = [c.name for c in sr.manifest.schema.columns]
                            rows = sr.result.data_array if sr.result else []
                            result_df = pd.DataFrame(rows, columns=cols)
                    except Exception:
                        if sql_query:
                            result_df = query_sql(sql_query)

        return {
            "text": answer_text, "sql": sql_query,
            "df": result_df, "conversation_id": conv_id,
            "status": message.status.value if message and message.status else "UNKNOWN",
        }

    except Exception as e:
        return {
            "text": f"Sorry, I encountered an error: {str(e)}",
            "sql": None, "df": None, "conversation_id": conversation_id,
            "status": "ERROR",
        }


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
    st.plotly_chart(fig, use_container_width=True)


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
        st.plotly_chart(fig, use_container_width=True)

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
        st.plotly_chart(fig2, use_container_width=True)

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
        <h3>🧞 Ask Me Anything</h3>
        <p>Ask any question about this candidate's career, skills, experience, or qualifications.
           Powered by Databricks AI/BI Genie with structured resume data.</p>
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
        if btn_cols[i % 4].button(q, key=f"quick_{i}", use_container_width=True):
            st.session_state.pending_question = q
            st.rerun()

    st.divider()

    # Display chat history
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"], avatar="👤" if msg["role"] == "user" else "🧞"):
            st.markdown(msg["content"])
            if msg.get("df") is not None and not msg["df"].empty:
                st.dataframe(msg["df"], use_container_width=True, hide_index=True)
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

            st.markdown(result["text"])
            if result.get("df") is not None and not result["df"].empty:
                st.dataframe(result["df"], use_container_width=True, hide_index=True)
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
