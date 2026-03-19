# Databricks notebook source
# MAGIC %md
# MAGIC # Resume Data Model Setup
# MAGIC
# MAGIC This notebook creates the complete resume data model in Unity Catalog
# MAGIC and populates it with your career data from `resume_data.json`.
# MAGIC
# MAGIC **Tables Created:**
# MAGIC - `profile` — Personal info, summary, contact details
# MAGIC - `work_experience` — Job history with company, title, dates
# MAGIC - `work_highlights` — Achievement bullet points per role
# MAGIC - `skills` — Technical and soft skills with proficiency levels
# MAGIC - `education` — Degrees and academic background
# MAGIC - `certifications` — Professional certifications
# MAGIC - `projects` — Notable projects and their impact
# MAGIC - `publications` — Blog posts, talks, papers
# MAGIC - `career_timeline` — Denormalized chronological view of entire career

# COMMAND ----------

import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Update these variables or upload your `resume_data.json` to DBFS/Volumes.

# COMMAND ----------

CATALOG = "resume_catalog"
SCHEMA = "career_profile"

# Option 1: Load from Volumes (recommended for production)
# resume_json_path = f"/Volumes/{CATALOG}/{SCHEMA}/raw/resume_data.json"

# Option 2: Load from DBFS
# resume_json_path = "/dbfs/FileStore/resume/resume_data.json"

# Option 3: Inline data (paste your JSON here for quick setup)
resume_data = {
  "profile": {
    "full_name": "YOUR FULL NAME",
    "headline": "Senior Data Engineer | Analytics Leader | Cloud Architect",
    "summary": "Results-driven data engineering leader with 10+ years of experience designing and implementing large-scale data platforms. Proven track record of building high-performance data pipelines, leading cross-functional teams, and driving data-informed decision-making across Fortune 500 organizations. Expert in Databricks, Spark, AWS, and modern data stack technologies.",
    "email": "your.email@example.com",
    "phone": "+1-555-000-0000",
    "linkedin_url": "https://linkedin.com/in/yourprofile",
    "github_url": "https://github.com/yourprofile",
    "website_url": "https://yourwebsite.com",
    "location_city": "San Francisco",
    "location_state": "CA",
    "location_country": "United States",
    "years_of_experience": 10,
    "willing_to_relocate": True,
    "work_authorization": "US Citizen",
    "preferred_work_model": "Hybrid"
  },
  "work_experience": [
    {
      "company": "TechCorp Inc.",
      "title": "Senior Data Engineer",
      "location": "San Francisco, CA",
      "employment_type": "Full-time",
      "start_date": "2022-01-01",
      "end_date": None,
      "is_current": True,
      "industry": "Technology",
      "team_size_managed": 5,
      "description": "Lead the data platform team building next-generation analytics infrastructure on Databricks and AWS.",
      "highlights": [
        {"description": "Architected and deployed a unified data lakehouse on Databricks serving 500+ analysts, reducing query latency by 70%", "category": "Technical", "impact_metric": "70% reduction in query latency"},
        {"description": "Led migration of 200+ legacy ETL pipelines to Delta Live Tables, improving data freshness from 24hrs to 15min", "category": "Technical", "impact_metric": "96% improvement in data freshness"},
        {"description": "Managed a team of 5 data engineers, implementing agile practices and reducing deployment cycle time by 60%", "category": "Leadership", "impact_metric": "60% faster deployments"},
        {"description": "Implemented Unity Catalog governance framework across 3 business units, ensuring GDPR and SOC2 compliance", "category": "Business", "impact_metric": "100% compliance achieved"}
      ]
    },
    {
      "company": "DataFlow Analytics",
      "title": "Data Engineer",
      "location": "New York, NY",
      "employment_type": "Full-time",
      "start_date": "2019-03-01",
      "end_date": "2021-12-31",
      "is_current": False,
      "industry": "Financial Services",
      "team_size_managed": 0,
      "description": "Built and maintained real-time data pipelines for trading analytics and risk management platforms.",
      "highlights": [
        {"description": "Designed real-time streaming pipeline processing 2M+ events/sec using Spark Structured Streaming and Kafka", "category": "Technical", "impact_metric": "2M+ events/sec throughput"},
        {"description": "Built ML feature store serving 50+ models, reducing feature engineering time by 80%", "category": "Technical", "impact_metric": "80% reduction in feature engineering time"},
        {"description": "Reduced cloud infrastructure costs by $500K/year through optimization of Spark cluster configurations", "category": "Business", "impact_metric": "$500K annual savings"}
      ]
    },
    {
      "company": "StartupXYZ",
      "title": "Junior Data Engineer",
      "location": "Austin, TX",
      "employment_type": "Full-time",
      "start_date": "2016-06-01",
      "end_date": "2019-02-28",
      "is_current": False,
      "industry": "E-commerce",
      "team_size_managed": 0,
      "description": "First data hire responsible for building the company's data infrastructure from scratch.",
      "highlights": [
        {"description": "Built the entire data warehouse from scratch using AWS Redshift and Airflow, enabling the company's first self-service analytics", "category": "Technical", "impact_metric": "0 to 1 data platform build"},
        {"description": "Created customer 360 data model that drove a 25% increase in customer retention", "category": "Business", "impact_metric": "25% improvement in retention"}
      ]
    }
  ],
  "education": [
    {"institution": "University of California, Berkeley", "degree": "Master of Science", "field_of_study": "Computer Science - Data Systems", "start_date": "2014-08-01", "end_date": "2016-05-31", "gpa": 3.85, "honors": "Magna Cum Laude", "relevant_coursework": "Distributed Systems, Machine Learning, Database Systems, Statistical Methods"},
    {"institution": "University of Texas at Austin", "degree": "Bachelor of Science", "field_of_study": "Computer Science", "start_date": "2010-08-01", "end_date": "2014-05-31", "gpa": 3.7, "honors": "Dean's List", "relevant_coursework": "Data Structures, Algorithms, Linear Algebra, Probability & Statistics"}
  ],
  "skills": [
    {"skill_name": "Apache Spark", "category": "Big Data", "proficiency": "Expert", "years_used": 7},
    {"skill_name": "Databricks", "category": "Cloud Platform", "proficiency": "Expert", "years_used": 5},
    {"skill_name": "Delta Lake", "category": "Big Data", "proficiency": "Expert", "years_used": 5},
    {"skill_name": "Python", "category": "Programming", "proficiency": "Expert", "years_used": 10},
    {"skill_name": "SQL", "category": "Programming", "proficiency": "Expert", "years_used": 10},
    {"skill_name": "Scala", "category": "Programming", "proficiency": "Advanced", "years_used": 5},
    {"skill_name": "AWS", "category": "Cloud Platform", "proficiency": "Expert", "years_used": 8},
    {"skill_name": "Azure", "category": "Cloud Platform", "proficiency": "Advanced", "years_used": 3},
    {"skill_name": "Kafka", "category": "Big Data", "proficiency": "Advanced", "years_used": 5},
    {"skill_name": "Airflow", "category": "Orchestration", "proficiency": "Expert", "years_used": 6},
    {"skill_name": "dbt", "category": "Data Transformation", "proficiency": "Advanced", "years_used": 3},
    {"skill_name": "Terraform", "category": "Infrastructure", "proficiency": "Advanced", "years_used": 4},
    {"skill_name": "Docker", "category": "Infrastructure", "proficiency": "Advanced", "years_used": 5},
    {"skill_name": "Kubernetes", "category": "Infrastructure", "proficiency": "Intermediate", "years_used": 3},
    {"skill_name": "Unity Catalog", "category": "Data Governance", "proficiency": "Expert", "years_used": 3},
    {"skill_name": "Delta Live Tables", "category": "Data Transformation", "proficiency": "Expert", "years_used": 3},
    {"skill_name": "Machine Learning", "category": "AI/ML", "proficiency": "Advanced", "years_used": 5},
    {"skill_name": "MLflow", "category": "AI/ML", "proficiency": "Advanced", "years_used": 4},
    {"skill_name": "Git", "category": "DevOps", "proficiency": "Expert", "years_used": 10},
    {"skill_name": "CI/CD", "category": "DevOps", "proficiency": "Advanced", "years_used": 6},
    {"skill_name": "Team Leadership", "category": "Soft Skills", "proficiency": "Advanced", "years_used": 4},
    {"skill_name": "Agile/Scrum", "category": "Soft Skills", "proficiency": "Expert", "years_used": 7},
    {"skill_name": "Technical Writing", "category": "Soft Skills", "proficiency": "Advanced", "years_used": 8},
    {"skill_name": "Stakeholder Management", "category": "Soft Skills", "proficiency": "Advanced", "years_used": 5}
  ],
  "certifications": [
    {"name": "Databricks Certified Data Engineer Professional", "issuing_organization": "Databricks", "issue_date": "2023-06-15", "expiry_date": "2025-06-15", "credential_id": "DBCE-PRO-12345", "credential_url": "https://credentials.databricks.com/12345"},
    {"name": "AWS Solutions Architect - Professional", "issuing_organization": "Amazon Web Services", "issue_date": "2022-03-10", "expiry_date": "2025-03-10", "credential_id": "AWS-SAP-67890", "credential_url": "https://aws.amazon.com/verification/67890"},
    {"name": "Google Cloud Professional Data Engineer", "issuing_organization": "Google Cloud", "issue_date": "2021-11-20", "expiry_date": "2023-11-20", "credential_id": "GCP-DE-11111", "credential_url": "https://google.accredible.com/11111"}
  ],
  "projects": [
    {"name": "Enterprise Data Lakehouse Platform", "description": "Designed and implemented a multi-tenant data lakehouse on Databricks serving the entire organization's analytics needs.", "role": "Lead Architect", "technologies": "Databricks, Delta Lake, Unity Catalog, Terraform, AWS S3, Spark", "start_date": "2022-03-01", "end_date": None, "is_current": True, "impact": "Consolidated 5 disparate data systems into one platform, saving $2M annually", "url": None},
    {"name": "Real-time Fraud Detection Pipeline", "description": "Built a real-time streaming pipeline for detecting fraudulent transactions using ML models.", "role": "Senior Engineer", "technologies": "Spark Streaming, Kafka, MLflow, Delta Lake, Python", "start_date": "2020-01-01", "end_date": "2021-06-30", "is_current": False, "impact": "Reduced fraud losses by 40%, processing 2M+ transactions per second with <100ms latency", "url": None},
    {"name": "Customer 360 Analytics Platform", "description": "Created a unified customer data model integrating data from 15+ sources for a holistic customer view.", "role": "Data Engineer", "technologies": "AWS Redshift, Airflow, Python, dbt, Looker", "start_date": "2017-06-01", "end_date": "2019-02-28", "is_current": False, "impact": "Enabled personalized marketing campaigns that increased customer retention by 25%", "url": None}
  ],
  "publications": [
    {"title": "Building Scalable Data Lakehouses: Lessons from the Trenches", "publisher": "Databricks Blog", "date": "2023-09-15", "type": "Blog Post", "url": "https://example.com/blog/lakehouse-lessons"},
    {"title": "Real-time Feature Engineering at Scale", "publisher": "Data Engineering Conference 2022", "date": "2022-10-20", "type": "Conference Talk", "url": "https://example.com/talks/feature-engineering"}
  ]
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog and Schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Using: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 1: Profile

# COMMAND ----------

profile = resume_data["profile"]

profile_df = spark.createDataFrame([{
    "full_name": profile["full_name"],
    "headline": profile["headline"],
    "summary": profile["summary"],
    "email": profile["email"],
    "phone": profile["phone"],
    "linkedin_url": profile["linkedin_url"],
    "github_url": profile["github_url"],
    "website_url": profile["website_url"],
    "location_city": profile["location_city"],
    "location_state": profile["location_state"],
    "location_country": profile["location_country"],
    "years_of_experience": profile["years_of_experience"],
    "willing_to_relocate": profile["willing_to_relocate"],
    "work_authorization": profile["work_authorization"],
    "preferred_work_model": profile["preferred_work_model"],
    "last_updated": datetime.now().strftime("%Y-%m-%d")
}])

profile_df.write.mode("overwrite").saveAsTable("profile")

spark.sql("""
    ALTER TABLE profile SET TBLPROPERTIES (
        'comment' = 'Personal profile information including name, contact details, location, and professional summary. This is the main identity table for the resume owner.'
    )
""")

spark.sql("ALTER TABLE profile ALTER COLUMN full_name COMMENT 'Full legal name of the candidate'")
spark.sql("ALTER TABLE profile ALTER COLUMN headline COMMENT 'Professional headline or tagline summarizing the candidate in one line'")
spark.sql("ALTER TABLE profile ALTER COLUMN summary COMMENT 'Professional summary paragraph describing career highlights, expertise areas, and value proposition'")
spark.sql("ALTER TABLE profile ALTER COLUMN email COMMENT 'Primary email address for professional contact'")
spark.sql("ALTER TABLE profile ALTER COLUMN phone COMMENT 'Phone number for contact'")
spark.sql("ALTER TABLE profile ALTER COLUMN linkedin_url COMMENT 'URL to LinkedIn profile'")
spark.sql("ALTER TABLE profile ALTER COLUMN github_url COMMENT 'URL to GitHub profile showing open source contributions'")
spark.sql("ALTER TABLE profile ALTER COLUMN website_url COMMENT 'Personal or portfolio website URL'")
spark.sql("ALTER TABLE profile ALTER COLUMN location_city COMMENT 'Current city of residence'")
spark.sql("ALTER TABLE profile ALTER COLUMN location_state COMMENT 'Current state or province of residence'")
spark.sql("ALTER TABLE profile ALTER COLUMN location_country COMMENT 'Current country of residence'")
spark.sql("ALTER TABLE profile ALTER COLUMN years_of_experience COMMENT 'Total years of professional work experience'")
spark.sql("ALTER TABLE profile ALTER COLUMN willing_to_relocate COMMENT 'Whether the candidate is open to relocating for a position'")
spark.sql("ALTER TABLE profile ALTER COLUMN work_authorization COMMENT 'Work authorization status (e.g., US Citizen, Green Card, H1B, etc.)'")
spark.sql("ALTER TABLE profile ALTER COLUMN preferred_work_model COMMENT 'Preferred work arrangement: Remote, Hybrid, or On-site'")
spark.sql("ALTER TABLE profile ALTER COLUMN last_updated COMMENT 'Date when this resume data was last updated'")

print("✓ Profile table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 2: Work Experience

# COMMAND ----------

def _calc_duration_months(start_str, end_str):
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.now() if end_str is None else datetime.strptime(end_str, "%Y-%m-%d")
    return (end.year - start.year) * 12 + (end.month - start.month)

work_rows = []
for i, exp in enumerate(resume_data["work_experience"], 1):
    work_rows.append({
        "experience_id": i,
        "company": exp["company"],
        "title": exp["title"],
        "location": exp["location"],
        "employment_type": exp["employment_type"],
        "start_date": exp["start_date"],
        "end_date": exp["end_date"] if exp["end_date"] else "Present",
        "is_current_role": exp["is_current"],
        "industry": exp["industry"],
        "team_size_managed": exp["team_size_managed"],
        "description": exp["description"],
        "duration_months": _calc_duration_months(exp["start_date"], exp["end_date"])
    })

work_df = spark.createDataFrame(work_rows)
work_df.write.mode("overwrite").saveAsTable("work_experience")

spark.sql("""
    ALTER TABLE work_experience SET TBLPROPERTIES (
        'comment' = 'Professional work history with one row per job role. Contains company name, job title, dates, location, industry, and role description. Use this table to answer questions about career history, job progression, industries worked in, and tenure at each company.'
    )
""")

spark.sql("ALTER TABLE work_experience ALTER COLUMN experience_id COMMENT 'Unique identifier for each work experience entry, ordered chronologically (1 = most recent)'")
spark.sql("ALTER TABLE work_experience ALTER COLUMN company COMMENT 'Name of the employer or organization'")
spark.sql("ALTER TABLE work_experience ALTER COLUMN title COMMENT 'Job title or role held at the company'")
spark.sql("ALTER TABLE work_experience ALTER COLUMN location COMMENT 'City and state/country where the role was based'")
spark.sql("ALTER TABLE work_experience ALTER COLUMN employment_type COMMENT 'Type of employment: Full-time, Part-time, Contract, or Internship'")
spark.sql("ALTER TABLE work_experience ALTER COLUMN start_date COMMENT 'Date when the role started (YYYY-MM-DD format)'")
spark.sql("ALTER TABLE work_experience ALTER COLUMN end_date COMMENT 'Date when the role ended (YYYY-MM-DD format), or Present if currently employed'")
spark.sql("ALTER TABLE work_experience ALTER COLUMN is_current_role COMMENT 'Boolean flag indicating if this is the current/active position'")
spark.sql("ALTER TABLE work_experience ALTER COLUMN industry COMMENT 'Industry sector of the company (e.g., Technology, Financial Services, E-commerce)'")
spark.sql("ALTER TABLE work_experience ALTER COLUMN team_size_managed COMMENT 'Number of direct reports or team members managed in this role. 0 means individual contributor.'")
spark.sql("ALTER TABLE work_experience ALTER COLUMN description COMMENT 'Brief description of the role responsibilities and scope'")
spark.sql("ALTER TABLE work_experience ALTER COLUMN duration_months COMMENT 'Total duration of the role in months, calculated from start and end dates'")

print("✓ Work Experience table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 3: Work Highlights (Achievement Bullets)

# COMMAND ----------

highlight_rows = []
highlight_id = 1
for i, exp in enumerate(resume_data["work_experience"], 1):
    for h in exp.get("highlights", []):
        highlight_rows.append({
            "highlight_id": highlight_id,
            "experience_id": i,
            "company": exp["company"],
            "title": exp["title"],
            "highlight": h["description"],
            "category": h["category"],
            "impact_metric": h["impact_metric"]
        })
        highlight_id += 1

highlights_df = spark.createDataFrame(highlight_rows)
highlights_df.write.mode("overwrite").saveAsTable("work_highlights")

spark.sql("""
    ALTER TABLE work_highlights SET TBLPROPERTIES (
        'comment' = 'Individual achievement bullet points for each work experience. Each row represents one accomplishment with its category (Technical, Leadership, or Business) and quantified impact metric. Join with work_experience on experience_id for full context. Use this table to answer questions about specific achievements, impact, and accomplishments.'
    )
""")

spark.sql("ALTER TABLE work_highlights ALTER COLUMN highlight_id COMMENT 'Unique identifier for each achievement bullet point'")
spark.sql("ALTER TABLE work_highlights ALTER COLUMN experience_id COMMENT 'Foreign key linking to work_experience table'")
spark.sql("ALTER TABLE work_highlights ALTER COLUMN company COMMENT 'Company name (denormalized for easier querying)'")
spark.sql("ALTER TABLE work_highlights ALTER COLUMN title COMMENT 'Job title (denormalized for easier querying)'")
spark.sql("ALTER TABLE work_highlights ALTER COLUMN highlight COMMENT 'Description of the achievement or accomplishment, typically with quantified results'")
spark.sql("ALTER TABLE work_highlights ALTER COLUMN category COMMENT 'Category of achievement: Technical (engineering/architecture), Leadership (management/mentoring), or Business (cost savings/revenue/compliance)'")
spark.sql("ALTER TABLE work_highlights ALTER COLUMN impact_metric COMMENT 'Quantified impact or result of this achievement (e.g., 70% reduction in latency, $500K savings)'")

print("✓ Work Highlights table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 4: Skills

# COMMAND ----------

skill_rows = []
for i, s in enumerate(resume_data["skills"], 1):
    skill_rows.append({
        "skill_id": i,
        "skill_name": s["skill_name"],
        "category": s["category"],
        "proficiency_level": s["proficiency"],
        "years_of_experience": s["years_used"]
    })

skills_df = spark.createDataFrame(skill_rows)
skills_df.write.mode("overwrite").saveAsTable("skills")

spark.sql("""
    ALTER TABLE skills SET TBLPROPERTIES (
        'comment' = 'Technical and professional skills with proficiency levels and years of experience. Categories include Programming, Big Data, Cloud Platform, Infrastructure, AI/ML, DevOps, Data Governance, Data Transformation, Orchestration, and Soft Skills. Proficiency levels are Expert, Advanced, or Intermediate. Use this table to answer questions about technical capabilities, skill depth, and areas of expertise.'
    )
""")

spark.sql("ALTER TABLE skills ALTER COLUMN skill_id COMMENT 'Unique identifier for each skill'")
spark.sql("ALTER TABLE skills ALTER COLUMN skill_name COMMENT 'Name of the skill or technology (e.g., Python, Apache Spark, Team Leadership)'")
spark.sql("ALTER TABLE skills ALTER COLUMN category COMMENT 'Skill category: Programming, Big Data, Cloud Platform, Infrastructure, AI/ML, DevOps, Data Governance, Data Transformation, Orchestration, or Soft Skills'")
spark.sql("ALTER TABLE skills ALTER COLUMN proficiency_level COMMENT 'Self-assessed proficiency: Expert (deep expertise, can architect solutions), Advanced (strong hands-on experience), or Intermediate (working knowledge)'")
spark.sql("ALTER TABLE skills ALTER COLUMN years_of_experience COMMENT 'Number of years actively using this skill in professional settings'")

print("✓ Skills table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 5: Education

# COMMAND ----------

edu_rows = []
for i, e in enumerate(resume_data["education"], 1):
    edu_rows.append({
        "education_id": i,
        "institution": e["institution"],
        "degree": e["degree"],
        "field_of_study": e["field_of_study"],
        "start_date": e["start_date"],
        "end_date": e["end_date"],
        "gpa": float(e["gpa"]),
        "honors": e["honors"],
        "relevant_coursework": e["relevant_coursework"]
    })

edu_df = spark.createDataFrame(edu_rows)
edu_df.write.mode("overwrite").saveAsTable("education")

spark.sql("""
    ALTER TABLE education SET TBLPROPERTIES (
        'comment' = 'Academic education history including degrees, institutions, GPA, honors, and relevant coursework. Use this table to answer questions about educational background, qualifications, and academic achievements.'
    )
""")

spark.sql("ALTER TABLE education ALTER COLUMN education_id COMMENT 'Unique identifier for each education entry'")
spark.sql("ALTER TABLE education ALTER COLUMN institution COMMENT 'Name of the university or educational institution'")
spark.sql("ALTER TABLE education ALTER COLUMN degree COMMENT 'Type of degree earned (e.g., Master of Science, Bachelor of Science)'")
spark.sql("ALTER TABLE education ALTER COLUMN field_of_study COMMENT 'Major or field of study (e.g., Computer Science, Data Science)'")
spark.sql("ALTER TABLE education ALTER COLUMN start_date COMMENT 'Date when the program started'")
spark.sql("ALTER TABLE education ALTER COLUMN end_date COMMENT 'Date when the degree was completed/conferred'")
spark.sql("ALTER TABLE education ALTER COLUMN gpa COMMENT 'Grade Point Average on a 4.0 scale'")
spark.sql("ALTER TABLE education ALTER COLUMN honors COMMENT 'Academic honors or distinctions received (e.g., Magna Cum Laude, Deans List)'")
spark.sql("ALTER TABLE education ALTER COLUMN relevant_coursework COMMENT 'Key courses relevant to professional career, comma-separated'")

print("✓ Education table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 6: Certifications

# COMMAND ----------

cert_rows = []
for i, c in enumerate(resume_data["certifications"], 1):
    cert_rows.append({
        "certification_id": i,
        "certification_name": c["name"],
        "issuing_organization": c["issuing_organization"],
        "issue_date": c["issue_date"],
        "expiry_date": c.get("expiry_date"),
        "is_active": c.get("expiry_date", "2099-01-01") >= datetime.now().strftime("%Y-%m-%d") if c.get("expiry_date") else True,
        "credential_id": c.get("credential_id"),
        "credential_url": c.get("credential_url")
    })

certs_df = spark.createDataFrame(cert_rows)
certs_df.write.mode("overwrite").saveAsTable("certifications")

spark.sql("""
    ALTER TABLE certifications SET TBLPROPERTIES (
        'comment' = 'Professional certifications and credentials with issuing organizations and validity dates. Use this table to answer questions about professional qualifications, cloud certifications, and credential status.'
    )
""")

spark.sql("ALTER TABLE certifications ALTER COLUMN certification_id COMMENT 'Unique identifier for each certification'")
spark.sql("ALTER TABLE certifications ALTER COLUMN certification_name COMMENT 'Full name of the certification (e.g., Databricks Certified Data Engineer Professional)'")
spark.sql("ALTER TABLE certifications ALTER COLUMN issuing_organization COMMENT 'Organization that issued the certification (e.g., Databricks, AWS, Google Cloud)'")
spark.sql("ALTER TABLE certifications ALTER COLUMN issue_date COMMENT 'Date when the certification was earned'")
spark.sql("ALTER TABLE certifications ALTER COLUMN expiry_date COMMENT 'Date when the certification expires, NULL if no expiration'")
spark.sql("ALTER TABLE certifications ALTER COLUMN is_active COMMENT 'Whether the certification is currently valid and not expired'")
spark.sql("ALTER TABLE certifications ALTER COLUMN credential_id COMMENT 'Unique credential ID for verification'")
spark.sql("ALTER TABLE certifications ALTER COLUMN credential_url COMMENT 'URL to verify the certification online'")

print("✓ Certifications table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 7: Projects

# COMMAND ----------

project_rows = []
for i, p in enumerate(resume_data["projects"], 1):
    project_rows.append({
        "project_id": i,
        "project_name": p["name"],
        "description": p["description"],
        "role": p["role"],
        "technologies_used": p["technologies"],
        "start_date": p["start_date"],
        "end_date": p["end_date"] if p["end_date"] else "Present",
        "is_current": p["is_current"],
        "impact": p["impact"],
        "url": p.get("url")
    })

projects_df = spark.createDataFrame(project_rows)
projects_df.write.mode("overwrite").saveAsTable("projects")

spark.sql("""
    ALTER TABLE projects SET TBLPROPERTIES (
        'comment' = 'Notable projects with descriptions, technologies used, the candidates role, and business impact. Use this table to answer questions about hands-on project experience, technology usage in context, and measurable project outcomes.'
    )
""")

spark.sql("ALTER TABLE projects ALTER COLUMN project_id COMMENT 'Unique identifier for each project'")
spark.sql("ALTER TABLE projects ALTER COLUMN project_name COMMENT 'Name or title of the project'")
spark.sql("ALTER TABLE projects ALTER COLUMN description COMMENT 'Detailed description of what the project was about and its goals'")
spark.sql("ALTER TABLE projects ALTER COLUMN role COMMENT 'The candidates role in the project (e.g., Lead Architect, Senior Engineer)'")
spark.sql("ALTER TABLE projects ALTER COLUMN technologies_used COMMENT 'Comma-separated list of technologies, frameworks, and tools used in this project'")
spark.sql("ALTER TABLE projects ALTER COLUMN start_date COMMENT 'Project start date'")
spark.sql("ALTER TABLE projects ALTER COLUMN end_date COMMENT 'Project end date, or Present if ongoing'")
spark.sql("ALTER TABLE projects ALTER COLUMN is_current COMMENT 'Whether this project is currently active'")
spark.sql("ALTER TABLE projects ALTER COLUMN impact COMMENT 'Quantified business impact and results of the project'")
spark.sql("ALTER TABLE projects ALTER COLUMN url COMMENT 'URL to project demo, repo, or documentation if publicly available'")

print("✓ Projects table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 8: Publications

# COMMAND ----------

pub_rows = []
for i, p in enumerate(resume_data["publications"], 1):
    pub_rows.append({
        "publication_id": i,
        "title": p["title"],
        "publisher": p["publisher"],
        "publication_date": p["date"],
        "publication_type": p["type"],
        "url": p.get("url")
    })

pubs_df = spark.createDataFrame(pub_rows)
pubs_df.write.mode("overwrite").saveAsTable("publications")

spark.sql("""
    ALTER TABLE publications SET TBLPROPERTIES (
        'comment' = 'Published works including blog posts, conference talks, research papers, and other thought leadership content. Use this table to answer questions about speaking engagements, published articles, and thought leadership.'
    )
""")

spark.sql("ALTER TABLE publications ALTER COLUMN publication_id COMMENT 'Unique identifier for each publication'")
spark.sql("ALTER TABLE publications ALTER COLUMN title COMMENT 'Title of the publication, talk, or blog post'")
spark.sql("ALTER TABLE publications ALTER COLUMN publisher COMMENT 'Publishing platform or conference name'")
spark.sql("ALTER TABLE publications ALTER COLUMN publication_date COMMENT 'Date of publication or presentation'")
spark.sql("ALTER TABLE publications ALTER COLUMN publication_type COMMENT 'Type: Blog Post, Conference Talk, Research Paper, Tutorial, or Video'")
spark.sql("ALTER TABLE publications ALTER COLUMN url COMMENT 'URL to access the publication'")

print("✓ Publications table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 9: Career Timeline (Denormalized View)
# MAGIC
# MAGIC A unified chronological view of the entire career for timeline-based questions.

# COMMAND ----------

timeline_rows = []
tid = 1

for exp in resume_data["work_experience"]:
    timeline_rows.append({
        "timeline_id": tid,
        "event_type": "Work",
        "title": exp["title"],
        "organization": exp["company"],
        "start_date": exp["start_date"],
        "end_date": exp["end_date"] if exp["end_date"] else "Present",
        "is_current": exp["is_current"],
        "location": exp["location"],
        "description": exp["description"],
        "category": exp["industry"]
    })
    tid += 1

for edu in resume_data["education"]:
    timeline_rows.append({
        "timeline_id": tid,
        "event_type": "Education",
        "title": f"{edu['degree']} in {edu['field_of_study']}",
        "organization": edu["institution"],
        "start_date": edu["start_date"],
        "end_date": edu["end_date"],
        "is_current": False,
        "location": None,
        "description": f"GPA: {edu['gpa']}, {edu['honors']}",
        "category": "Academia"
    })
    tid += 1

for cert in resume_data["certifications"]:
    timeline_rows.append({
        "timeline_id": tid,
        "event_type": "Certification",
        "title": cert["name"],
        "organization": cert["issuing_organization"],
        "start_date": cert["issue_date"],
        "end_date": cert.get("expiry_date"),
        "is_current": cert.get("expiry_date", "2099-01-01") >= datetime.now().strftime("%Y-%m-%d") if cert.get("expiry_date") else True,
        "location": None,
        "description": f"Credential: {cert.get('credential_id', 'N/A')}",
        "category": "Professional Development"
    })
    tid += 1

timeline_df = spark.createDataFrame(timeline_rows)
timeline_df.write.mode("overwrite").saveAsTable("career_timeline")

spark.sql("""
    ALTER TABLE career_timeline SET TBLPROPERTIES (
        'comment' = 'Unified chronological timeline of all career events including work experience, education, and certifications. Each row represents one career event ordered by date. Use this table for timeline questions, career progression analysis, and understanding the full career journey in chronological order.'
    )
""")

spark.sql("ALTER TABLE career_timeline ALTER COLUMN timeline_id COMMENT 'Unique identifier for each timeline event'")
spark.sql("ALTER TABLE career_timeline ALTER COLUMN event_type COMMENT 'Type of career event: Work, Education, or Certification'")
spark.sql("ALTER TABLE career_timeline ALTER COLUMN title COMMENT 'Title of the event (job title, degree name, or certification name)'")
spark.sql("ALTER TABLE career_timeline ALTER COLUMN organization COMMENT 'Organization associated with this event (company, university, or certifying body)'")
spark.sql("ALTER TABLE career_timeline ALTER COLUMN start_date COMMENT 'When this career event started'")
spark.sql("ALTER TABLE career_timeline ALTER COLUMN end_date COMMENT 'When this career event ended, or Present/NULL if ongoing'")
spark.sql("ALTER TABLE career_timeline ALTER COLUMN is_current COMMENT 'Whether this is a current/active event'")
spark.sql("ALTER TABLE career_timeline ALTER COLUMN location COMMENT 'Location where this event took place'")
spark.sql("ALTER TABLE career_timeline ALTER COLUMN description COMMENT 'Brief description or additional context for the event'")
spark.sql("ALTER TABLE career_timeline ALTER COLUMN category COMMENT 'Category: industry for work, Academia for education, Professional Development for certifications'")

print("✓ Career Timeline table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

tables = ["profile", "work_experience", "work_highlights", "skills", "education", "certifications", "projects", "publications", "career_timeline"]

print("=" * 60)
print(f"  Resume Data Model: {CATALOG}.{SCHEMA}")
print("=" * 60)

for table in tables:
    count = spark.table(table).count()
    print(f"  {table:<25} {count:>5} rows")

print("=" * 60)
print("\n✓ All tables created successfully!")
print(f"\nNext steps:")
print(f"  1. Create a Genie Space using these tables")
print(f"  2. Create a Dashboard with the provided SQL queries")
print(f"  3. Share the Genie Space URL on your resume!")
