# AI/BI Genie Resume

An interactive resume powered by Databricks AI/BI Genie. Instead of a static PDF, recruiters and hiring managers can **ask natural language questions** about your career — and get instant, accurate answers backed by structured data.

## What You Get

| Component | Description |
|-----------|-------------|
| **Genie Space** | Conversational AI where anyone can ask "What are their top skills?" or "Tell me about their Databricks experience" |
| **Dashboard** | Visual resume with career timeline, skills charts, achievement highlights, and career stats |
| **Data Model** | 9 structured tables in Unity Catalog with rich column descriptions optimized for Genie |

## Data Model (9 Tables)

```
resume_catalog.career_profile
├── profile              # Name, contact, summary, preferences
├── work_experience      # Job history with dates, industry, team size
├── work_highlights      # Achievement bullets with impact metrics
├── skills               # Technical & soft skills with proficiency levels
├── education            # Degrees, GPA, honors, coursework
├── certifications       # Professional certs with active status
├── projects             # Notable projects with tech stack & impact
├── publications         # Blog posts, talks, papers
└── career_timeline      # Denormalized chronological career view
```

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Customize Your Resume Data

Edit `config/resume_data.json` with your actual career information. The file is structured with clear sections for profile, work experience, skills, education, certifications, projects, and publications.

### 3. Configure Databricks Connection

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
```

### 4. Deploy to Databricks

```bash
# Upload notebook and run it
python src/deploy.py --upload --run

# Or with custom catalog/schema
python src/deploy.py --upload --run --catalog my_catalog --schema my_schema
```

### 5. Set Up Genie Space

1. In your Databricks workspace, go to **AI/BI > Genie**
2. Click **New Genie Space**
3. Name it: **"[Your Name]'s Interactive Resume"**
4. Select all 9 tables from `resume_catalog.career_profile`
5. Paste the instructions from `genie/genie_space_instructions.md`
6. Add the sample questions from the same file
7. Click **Create**

### 6. Set Up Dashboard

1. Go to **AI/BI > Dashboards**
2. Click **Create Dashboard**
3. Add widgets using queries from `dashboards/resume_dashboard_queries.sql`
4. Each query section includes the suggested widget type

### 7. Share

- Share your Genie Space URL on your resume, LinkedIn, or portfolio
- Add to your email signature: *"Ask my AI resume anything: [URL]"*

## Alternative: Manual Notebook Upload

If you prefer not to use the deploy script:

1. Open your Databricks workspace
2. Go to **Workspace > Import**
3. Upload `notebooks/01_setup_resume_tables.py`
4. Edit the resume data directly in the notebook (it includes inline data)
5. Run all cells

## Project Structure

```
resume/
├── app/                                      # Databricks App (Streamlit)
│   ├── app.py                                # Main app — dashboard + Genie Q&A
│   ├── app.yaml                              # Databricks App deployment config
│   └── requirements.txt                      # App Python dependencies
├── config/
│   └── resume_data.json                      # Your resume data (edit this!)
├── notebooks/
│   └── 01_setup_resume_tables.py             # Databricks notebook — creates all tables
├── dashboards/
│   └── resume_dashboard_queries.sql          # SQL queries for dashboard widgets
├── genie/
│   └── genie_space_instructions.md           # Genie Space setup & instructions
├── src/
│   └── deploy.py                             # CLI deployment script
├── requirements.txt
└── README.md
```

## Databricks App (Recommended for Sharing)

The `app/` directory contains a **Streamlit-based Databricks App** with two tabs:

- **Resume Dashboard** — Visual resume with career timeline, skill charts, experience cards, education, certifications, and projects
- **Ask Me Anything** — Genie-powered chat where anyone can ask natural language questions about the candidate's career

### Local Preview

The app works locally without Databricks by loading data from `config/resume_data.json`:

```bash
cd app
pip install -r requirements.txt
streamlit run app.py
```

### Deploy as a Databricks App

1. **Create the app** in your Databricks workspace:
   - Navigate to **Compute > Apps**
   - Click **Create App**
   - Name it (e.g., `my-resume`)

2. **Deploy the code** using the Databricks CLI:
   ```bash
   databricks apps deploy my-resume --source-code-path app/
   ```

3. **Configure environment variables** in the app settings:
   | Variable | Description |
   |----------|-------------|
   | `SQL_WAREHOUSE_ID` | Your SQL Warehouse ID (required for dashboard) |
   | `GENIE_SPACE_ID` | Your Genie Space ID (required for Q&A) |
   | `RESUME_CATALOG` | Catalog name (default: `resume_catalog`) |
   | `RESUME_SCHEMA` | Schema name (default: `career_profile`) |

4. **Grant permissions** — The app's service principal needs:
   - `USE CATALOG` on the resume catalog
   - `USE SCHEMA` on the career_profile schema
   - `SELECT` on all 9 tables
   - Access to the Genie Space (add as viewer)

5. **Share the app URL** — Anyone with the link can view the dashboard and ask questions. No Databricks login required for published apps.

### Sharing with Recruiters

| Method | Pros | Setup |
|--------|------|-------|
| **Databricks App URL** | Full interactive experience (dashboard + Q&A) | Deploy app, share URL |
| **Published Dashboard** | Visual resume, no login needed | Publish dashboard with embedded credentials |
| **Genie Space URL** | Conversational Q&A | Share with workspace users |
| **Resume/LinkedIn link** | Maximum visibility | Add app URL to your profiles |

**Pro tip:** Add this to your resume: *"Interactive AI Resume: [App URL] — ask my resume anything"*

## Sample Genie Questions

Once deployed, users can ask questions like:

- *"Tell me about this candidate"*
- *"What are their top technical skills?"*
- *"How many years of experience do they have with Databricks?"*
- *"What achievements did they have at TechCorp?"*
- *"Show me their career progression"*
- *"What certifications do they hold?"*
- *"Do they have leadership experience?"*
- *"What is their biggest business impact?"*

## Why This Approach Works

- **Genie uses table/column comments** to understand your data — every column has a rich description
- **Denormalized `career_timeline` table** makes chronological questions easy
- **Separate `work_highlights` table** enables granular achievement queries
- **Structured `skills` table** with proficiency levels allows capability matching
- **General instructions** in the Genie Space guide the AI's response tone and behavior
- **Databricks App** makes it shareable with anyone — no Databricks login required
