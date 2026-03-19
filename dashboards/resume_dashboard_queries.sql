-- =============================================================================
-- AI/BI RESUME DASHBOARD - SQL Queries
-- =============================================================================
-- Import these queries as individual widgets in your Databricks AI/BI Dashboard.
-- Each query is labeled with a suggested widget type and title.
-- =============================================================================

-- Use the resume catalog/schema
USE CATALOG resume_catalog;
USE SCHEMA career_profile;

-- =============================================================================
-- WIDGET 1: Profile Header (Text/Markdown widget)
-- Title: "Profile"
-- Widget Type: Text/Detail
-- =============================================================================

SELECT
    full_name,
    headline,
    summary,
    email,
    linkedin_url,
    github_url,
    location_city || ', ' || location_state AS location,
    years_of_experience || ' years' AS experience,
    preferred_work_model,
    work_authorization
FROM profile;


-- =============================================================================
-- WIDGET 2: Career Journey Timeline (Bar/Timeline chart)
-- Title: "Career Journey"
-- Widget Type: Horizontal Bar Chart
-- X-axis: duration_months, Y-axis: label, Color: event_type
-- =============================================================================

SELECT
    event_type,
    title || ' @ ' || organization AS label,
    start_date,
    end_date,
    CASE
        WHEN end_date = 'Present' THEN
            (YEAR(CURRENT_DATE()) - YEAR(TO_DATE(start_date))) * 12 +
            (MONTH(CURRENT_DATE()) - MONTH(TO_DATE(start_date)))
        ELSE
            (YEAR(TO_DATE(end_date)) - YEAR(TO_DATE(start_date))) * 12 +
            (MONTH(TO_DATE(end_date)) - MONTH(TO_DATE(start_date)))
    END AS duration_months,
    is_current,
    category
FROM career_timeline
ORDER BY start_date DESC;


-- =============================================================================
-- WIDGET 3: Skills by Category (Grouped Bar Chart)
-- Title: "Technical Skills"
-- Widget Type: Bar Chart
-- X-axis: skill_name, Y-axis: years_of_experience, Color: category
-- =============================================================================

SELECT
    skill_name,
    category,
    proficiency_level,
    years_of_experience
FROM skills
WHERE category != 'Soft Skills'
ORDER BY years_of_experience DESC;


-- =============================================================================
-- WIDGET 4: Skills Proficiency Distribution (Pie/Donut Chart)
-- Title: "Skill Proficiency Distribution"
-- Widget Type: Pie Chart
-- =============================================================================

SELECT
    proficiency_level,
    COUNT(*) AS skill_count
FROM skills
GROUP BY proficiency_level
ORDER BY
    CASE proficiency_level
        WHEN 'Expert' THEN 1
        WHEN 'Advanced' THEN 2
        WHEN 'Intermediate' THEN 3
    END;


-- =============================================================================
-- WIDGET 5: Skills by Category Count (Donut Chart)
-- Title: "Skills by Category"
-- Widget Type: Donut Chart
-- =============================================================================

SELECT
    category,
    COUNT(*) AS skill_count
FROM skills
GROUP BY category
ORDER BY skill_count DESC;


-- =============================================================================
-- WIDGET 6: Work Experience Summary (Table widget)
-- Title: "Work Experience"
-- Widget Type: Table
-- =============================================================================

SELECT
    w.company,
    w.title,
    w.location,
    w.start_date,
    w.end_date,
    w.duration_months,
    w.industry,
    w.team_size_managed,
    COUNT(h.highlight_id) AS num_achievements
FROM work_experience w
LEFT JOIN work_highlights h ON w.experience_id = h.experience_id
GROUP BY ALL
ORDER BY w.start_date DESC;


-- =============================================================================
-- WIDGET 7: Key Achievements by Category (Counter + Table)
-- Title: "Achievements Breakdown"
-- Widget Type: Counter (total) + Table (details)
-- =============================================================================

-- Counter: Total achievements
SELECT COUNT(*) AS total_achievements FROM work_highlights;

-- Table: Achievements by category
SELECT
    category,
    COUNT(*) AS count,
    COLLECT_LIST(impact_metric) AS impact_metrics
FROM work_highlights
GROUP BY category
ORDER BY count DESC;


-- =============================================================================
-- WIDGET 8: Top Achievements (Table with highlighting)
-- Title: "Key Achievements & Impact"
-- Widget Type: Table
-- =============================================================================

SELECT
    h.company,
    h.title AS role,
    h.category,
    h.highlight AS achievement,
    h.impact_metric
FROM work_highlights h
ORDER BY h.experience_id, h.highlight_id;


-- =============================================================================
-- WIDGET 9: Education (Table widget)
-- Title: "Education"
-- Widget Type: Table
-- =============================================================================

SELECT
    institution,
    degree || ' in ' || field_of_study AS degree_info,
    gpa,
    honors,
    relevant_coursework,
    end_date AS graduation_date
FROM education
ORDER BY end_date DESC;


-- =============================================================================
-- WIDGET 10: Certifications (Table with status indicator)
-- Title: "Professional Certifications"
-- Widget Type: Table
-- =============================================================================

SELECT
    certification_name,
    issuing_organization,
    issue_date,
    expiry_date,
    CASE
        WHEN is_active THEN '✅ Active'
        ELSE '❌ Expired'
    END AS status,
    credential_url
FROM certifications
ORDER BY issue_date DESC;


-- =============================================================================
-- WIDGET 11: Projects Portfolio (Table widget)
-- Title: "Project Portfolio"
-- Widget Type: Table
-- =============================================================================

SELECT
    project_name,
    role,
    technologies_used,
    impact,
    start_date,
    end_date,
    CASE WHEN is_current THEN '🔄 Active' ELSE '✅ Complete' END AS status
FROM projects
ORDER BY start_date DESC;


-- =============================================================================
-- WIDGET 12: Industry Experience (Pie Chart)
-- Title: "Industry Experience"
-- Widget Type: Pie Chart
-- =============================================================================

SELECT
    industry,
    SUM(duration_months) AS total_months,
    ROUND(SUM(duration_months) / 12.0, 1) AS total_years
FROM work_experience
GROUP BY industry
ORDER BY total_months DESC;


-- =============================================================================
-- WIDGET 13: Publications & Thought Leadership (Table)
-- Title: "Publications & Speaking"
-- Widget Type: Table
-- =============================================================================

SELECT
    title,
    publisher,
    publication_type,
    publication_date,
    url
FROM publications
ORDER BY publication_date DESC;


-- =============================================================================
-- WIDGET 14: Career Stats Counters (Counter widgets)
-- Title: Various counter widgets across the top of the dashboard
-- Widget Type: Counter
-- =============================================================================

-- Total Years of Experience
SELECT years_of_experience AS total_years FROM profile;

-- Companies Worked At
SELECT COUNT(DISTINCT company) AS companies FROM work_experience;

-- Total Skills
SELECT COUNT(*) AS total_skills FROM skills;

-- Expert Skills
SELECT COUNT(*) AS expert_skills FROM skills WHERE proficiency_level = 'Expert';

-- Active Certifications
SELECT COUNT(*) AS active_certs FROM certifications WHERE is_active = true;

-- Industries Covered
SELECT COUNT(DISTINCT industry) AS industries FROM work_experience;


-- =============================================================================
-- WIDGET 15: Technology Cloud / Word Frequency (for word cloud widget)
-- Title: "Technology Stack"
-- Widget Type: Word Cloud (if available) or Tag display
-- =============================================================================

SELECT
    skill_name AS technology,
    years_of_experience AS weight,
    proficiency_level,
    category
FROM skills
WHERE category NOT IN ('Soft Skills')
ORDER BY years_of_experience DESC;
