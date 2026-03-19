#!/usr/bin/env python3
"""Push resume_data.json to Databricks SQL tables via Statement Execution API."""

import json, os, time, requests
from datetime import date, datetime

PROFILE = "resume"
WAREHOUSE_ID = "f7d6d4db7bd35b8b"
CATALOG = "workspace"
SCHEMA = "career_profile"
FQ = f"{CATALOG}.{SCHEMA}"


def get_databricks_config(profile):
    cfg_path = os.path.expanduser("~/.databrickscfg")
    host = token = None
    in_profile = False
    with open(cfg_path) as f:
        for line in f:
            line = line.strip()
            if line == f"[{profile}]":
                in_profile = True
                continue
            if line.startswith("[") and in_profile:
                break
            if in_profile:
                if line.startswith("host"):
                    host = line.split("=", 1)[1].strip()
                elif line.startswith("token"):
                    token = line.split("=", 1)[1].strip()
    return host, token


HOST, TOKEN = get_databricks_config(PROFILE)
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


def run_sql(statement):
    url = f"{HOST}/api/2.0/sql/statements"
    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "statement": statement,
        "catalog": CATALOG,
        "schema": SCHEMA,
        "wait_timeout": "30s",
    }
    r = requests.post(url, headers=HEADERS, json=payload)
    data = r.json()
    status = data.get("status", {}).get("state", "UNKNOWN")
    if status == "FAILED":
        err = data.get("status", {}).get("error", {}).get("message", "")
        print(f"  FAILED: {err[:300]}")
        return None
    if status == "SUCCEEDED":
        return data
    stmt_id = data.get("statement_id")
    for _ in range(30):
        time.sleep(2)
        r2 = requests.get(f"{url}/{stmt_id}", headers=HEADERS)
        data = r2.json()
        st = data.get("status", {}).get("state", "UNKNOWN")
        if st in ("SUCCEEDED", "FAILED"):
            if st == "FAILED":
                err = data.get("status", {}).get("error", {}).get("message", "")
                print(f"  FAILED: {err[:300]}")
                return None
            return data
    print("  TIMEOUT")
    return None


def esc(s):
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


def calc_months(start_str, end_str):
    try:
        sd = datetime.strptime(start_str, "%Y-%m-%d")
        ed = datetime.strptime(end_str, "%Y-%m-%d") if end_str else datetime.now()
        return (ed.year - sd.year) * 12 + (ed.month - sd.month)
    except Exception:
        return 0


def main():
    data_path = os.path.join(os.path.dirname(__file__), "..", "config", "resume_data.json")
    with open(data_path) as f:
        data = json.load(f)

    today = date.today().isoformat()

    # 1. Profile (15 cols + last_updated)
    print(">>> profile")
    run_sql(f"DELETE FROM {FQ}.profile")
    p = data["profile"]
    run_sql(f"""INSERT INTO {FQ}.profile VALUES (
        {esc(p['full_name'])}, {esc(p['headline'])}, {esc(p['summary'])},
        {esc(p['email'])}, {esc(p['phone'])}, {esc(p['linkedin_url'])},
        {esc(p['github_url'])}, {esc(p['website_url'])},
        {esc(p['location_city'])}, {esc(p['location_state'])}, {esc(p['location_country'])},
        {p['years_of_experience']}, {str(p['willing_to_relocate']).lower()},
        {esc(p['work_authorization'])}, {esc(p['preferred_work_model'])},
        '{today}'
    )""")
    print("  1 row")

    # 2. Work Experience (experience_id, company, title, location, employment_type,
    #    start_date, end_date, is_current_role, industry, team_size_managed, description, duration_months)
    print(">>> work_experience")
    run_sql(f"DELETE FROM {FQ}.work_experience")
    for i, w in enumerate(data["work_experience"], 1):
        end_dt = w.get("end_date")
        dur = calc_months(w["start_date"], end_dt)
        title = w.get("title_at_employer", w.get("title", ""))
        run_sql(f"""INSERT INTO {FQ}.work_experience VALUES (
            {i}, {esc(w['company'])}, {esc(title)},
            {esc(w['location'])}, {esc(w['employment_type'])},
            {esc(w['start_date'])}, {esc(end_dt)}, {str(w['is_current']).lower()},
            {esc(w['industry'])}, {w['team_size_managed']}, {esc(w['description'])}, {dur}
        )""")
    print(f"  {len(data['work_experience'])} rows")

    # 3. Work Highlights (highlight_id, experience_id, company, title, highlight, category, impact_metric)
    print(">>> work_highlights")
    run_sql(f"DELETE FROM {FQ}.work_highlights")
    hid = 1
    for i, w in enumerate(data["work_experience"], 1):
        title = w.get("title_at_employer", w.get("title", ""))
        for h in w.get("highlights", []):
            run_sql(f"""INSERT INTO {FQ}.work_highlights VALUES (
                {hid}, {i}, {esc(w['company'])}, {esc(title)},
                {esc(h['description'])}, {esc(h['category'])}, {esc(h['impact_metric'])}
            )""")
            hid += 1
    print(f"  {hid - 1} rows")

    # 4. Education (education_id, institution, degree, field_of_study, start_date, end_date, gpa, honors, relevant_coursework)
    print(">>> education")
    run_sql(f"DELETE FROM {FQ}.education")
    for i, e in enumerate(data["education"], 1):
        gpa_val = e["gpa"]
        if gpa_val > 5:
            gpa_val = round(gpa_val / 10.0 * 4.0, 2)
        run_sql(f"""INSERT INTO {FQ}.education VALUES (
            {i}, {esc(e['institution'])}, {esc(e['degree'])}, {esc(e['field_of_study'])},
            {esc(e['start_date'])}, {esc(e['end_date'])}, {gpa_val},
            {esc(e['honors'])}, {esc(e['relevant_coursework'])}
        )""")
    print(f"  {len(data['education'])} rows")

    # 5. Skills (skill_id, skill_name, category, proficiency_level, years_of_experience)
    print(">>> skills")
    run_sql(f"DELETE FROM {FQ}.skills")
    for i, s in enumerate(data["skills"], 1):
        run_sql(f"""INSERT INTO {FQ}.skills VALUES (
            {i}, {esc(s['skill_name'])}, {esc(s['category'])},
            {esc(s['proficiency'])}, {s['years_used']}
        )""")
    print(f"  {len(data['skills'])} rows")

    # 6. Certifications (certification_id, certification_name, issuing_organization,
    #    issue_date, expiry_date, is_active, credential_id, credential_url)
    print(">>> certifications")
    run_sql(f"DELETE FROM {FQ}.certifications")
    for i, c in enumerate(data["certifications"], 1):
        expiry = c.get("expiry_date")
        is_active = "true"
        if expiry:
            try:
                is_active = str(datetime.strptime(expiry, "%Y-%m-%d").date() >= date.today()).lower()
            except Exception:
                pass
        run_sql(f"""INSERT INTO {FQ}.certifications VALUES (
            {i}, {esc(c['name'])}, {esc(c['issuing_organization'])},
            {esc(c['issue_date'])}, {esc(expiry)}, {is_active},
            {esc(c['credential_id'])}, NULL
        )""")
    print(f"  {len(data['certifications'])} rows")

    # 7. Projects (project_id, project_name, description, role, technologies_used,
    #    start_date, end_date, is_current, impact, url)
    print(">>> projects")
    run_sql(f"DELETE FROM {FQ}.projects")
    for i, pr in enumerate(data["projects"], 1):
        end_dt = pr.get("end_date")
        run_sql(f"""INSERT INTO {FQ}.projects VALUES (
            {i}, {esc(pr['name'])}, {esc(pr['description'])}, {esc(pr['role'])},
            {esc(pr['technologies'])}, {esc(pr['start_date'])}, {esc(end_dt)},
            {str(pr['is_current']).lower()}, {esc(pr['impact'])}, NULL
        )""")
    print(f"  {len(data['projects'])} rows")

    # 8. Publications (publication_id, title, publisher, publication_date, publication_type, url)
    print(">>> publications")
    run_sql(f"DELETE FROM {FQ}.publications")
    for i, pub in enumerate(data["publications"], 1):
        run_sql(f"""INSERT INTO {FQ}.publications VALUES (
            {i}, {esc(pub['title'])}, {esc(pub['publisher'])},
            {esc(pub['date'])}, {esc(pub['type'])}, {esc(pub.get('url'))}
        )""")
    print(f"  {len(data['publications'])} rows")

    print("\n=== ALL TABLES UPDATED ===")


if __name__ == "__main__":
    main()
