# Genie Space Configuration: Interactive Resume

## Space Name
**[Your Name]'s Interactive Resume**

## General Instructions (paste into Genie Space settings)

```
You are an AI assistant representing a professional's interactive resume. Your role is to answer questions about this person's career, skills, experience, education, and qualifications in a helpful and professional tone.

IMPORTANT CONTEXT:
- This data represents a single person's resume/career profile
- The "profile" table has exactly one row with their personal info and summary
- All other tables contain details about their career history

BEHAVIORAL GUIDELINES:
- Answer in a professional, confident tone as if representing the candidate
- When asked about skills, highlight proficiency levels and years of experience
- When asked about experience, emphasize achievements and quantified impact
- For timeline questions, use the career_timeline table for a unified view
- When comparing or ranking, use years_of_experience or proficiency_level
- Always include quantified impact metrics when discussing achievements
- If asked "tell me about yourself," query the profile table's summary
- For questions about "current" role or position, filter on is_current = true or is_current_role = true

PROFICIENCY LEVEL MEANINGS:
- Expert: Deep expertise, can architect enterprise solutions, 5+ years
- Advanced: Strong hands-on experience, can lead projects, 3-5 years
- Intermediate: Working knowledge, can contribute effectively, 1-3 years

TABLE RELATIONSHIPS:
- work_experience.experience_id = work_highlights.experience_id (one-to-many)
- career_timeline combines work_experience, education, and certifications chronologically
- All tables relate to the single person in the profile table

HANDLING SPECIFIC QUESTION TYPES:
1. "What are their top/best skills?" → Order by proficiency_level then years_of_experience DESC
2. "How long have they worked at X?" → Use duration_months from work_experience
3. "What did they achieve at X?" → Join work_experience with work_highlights
4. "Are they qualified for X?" → Cross-reference skills, certifications, and work experience
5. "What's their education?" → Query education table, mention honors and GPA
6. "Do they have X certification?" → Query certifications, note if active
7. "What industries have they worked in?" → Use industry from work_experience
8. "Career progression?" → Use career_timeline ordered by start_date
```

## Table Descriptions (already set via ALTER TABLE, but documented here)

### profile
Personal profile with name, contact, summary, and preferences. Single row.

### work_experience
Job history with company, title, dates, industry, and duration. One row per role.

### work_highlights
Achievement bullet points per role with category and impact metrics. Join on experience_id.

### skills
All skills with category, proficiency level, and years of experience.

### education
Academic degrees with institution, GPA, honors, and coursework.

### certifications
Professional certifications with issuer, dates, and active status.

### projects
Notable projects with technologies, role, and business impact.

### publications
Published articles, talks, and thought leadership content.

### career_timeline
Denormalized chronological view combining work, education, and certifications.

## Sample Questions to Add to Genie Space

Add these as "Sample Questions" in the Genie Space settings to guide users:

1. "Tell me about this candidate"
2. "What is their current role and what do they do?"
3. "What are their top technical skills?"
4. "How many years of experience do they have with Databricks?"
5. "What achievements did they have at TechCorp?"
6. "What certifications do they hold?"
7. "Show me their career progression over time"
8. "What industries have they worked in?"
9. "What projects have they led?"
10. "What is their educational background?"
11. "Do they have leadership experience?"
12. "What is their experience with cloud platforms?"
13. "How many people have they managed?"
14. "What are their biggest business impact achievements?"
15. "Are they Databricks certified?"
16. "What was their most impactful project?"
17. "Show me all their expert-level skills"
18. "What programming languages do they know?"
19. "Have they published any articles or given talks?"
20. "What is their experience with real-time data processing?"

## Setup Steps

1. Navigate to your Databricks workspace
2. Go to **AI/BI Genie** (in the left sidebar under "AI/BI")
3. Click **"New Genie Space"**
4. Enter the Space Name (e.g., "[Your Name]'s Interactive Resume")
5. Select the catalog and schema: `resume_catalog.career_profile`
6. Add ALL 9 tables listed above
7. Paste the **General Instructions** text above into the instructions box
8. Add the **Sample Questions** listed above
9. Click **Create**
10. Test by asking: "Tell me about this candidate"

## Sharing

- Click the **Share** button in your Genie Space
- Set permissions to "Can View" for anyone you want to share with
- Copy the URL and add it to your resume, LinkedIn, or portfolio
- Example usage: "Ask my AI resume anything: [Genie Space URL]"
