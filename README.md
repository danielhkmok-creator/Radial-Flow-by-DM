# Radial Flow by DM

Relative pedestrian-flow potential explorer for Hong Kong (GTFS + OpenStreetMap + pedestrian routing).
Scores are relative model indicators, not observed pedestrian counts.

Copyright (c) 2026 DM. All rights reserved.

## Run locally
    pip install -r requirements.txt
    streamlit run streamlit_app.py          # web app
    python radial_flow_by_dm.py              # desktop window
    python radial_flow_by_dm.py --location "22.3193,114.1694" --start-time "2026-10-01 08:00" --end-time "2026-10-01 10:00"

## Deploy (Streamlit Community Cloud)
1. Push this folder to GitHub.
2. share.streamlit.io -> New app -> select repo, branch, main file `streamlit_app.py`.

## Optional usage log (add later, no code change)
Create a Supabase table `usage_log` (logged_at timestamptz, location_input text, start_time text, end_time text),
then add to Streamlit Secrets:

    [supabase]
    url = "https://YOUR-PROJECT.supabase.co"
    key = "YOUR-SERVICE-KEY"

Rows older than 30 days are deleted automatically. Never commit secrets.
