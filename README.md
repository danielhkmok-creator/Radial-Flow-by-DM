# Radial Flow by DM

Relative pedestrian-flow potential explorer for Hong Kong (GTFS + OpenStreetMap + pedestrian routing).
Scores are relative model indicators, not observed pedestrian counts.

Copyright (c) 2026 DM. All rights reserved.
Test it online: https://radial-flow-by-dm.streamlit.app/

## Run locally
    pip install -r requirements.txt
    streamlit run streamlit_app.py          # web app
    python radial_flow_by_dm.py              # desktop window
    python radial_flow_by_dm.py --location "22.3193,114.1694" --start-time "2026-10-01 08:00" --end-time "2026-10-01 10:00"

## Build the Hong Kong POI snapshot (once, on your own PC)
Streamlit Cloud IPs are often refused by the free Overpass servers, so the app reads a pre-built file instead.

    python build_poi_snapshot.py             # makes data/hk_pois.csv.gz (+ data/hk_pois_meta.json)

- Finished tiles are kept in `data/.snapshot_tiles/`, so an interrupted run can simply be repeated.
- If the result would be larger than 200 MB, the file is NOT produced.
- Browser upload on github.com is limited to 25 MB per file; use GitHub Desktop or `git push` for larger files.
- Commit `data/hk_pois.csv.gz` to GitHub. The app uses it automatically; live Overpass is only the fallback.

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

## Data attribution
Points of interest: (c) OpenStreetMap contributors, ODbL 1.0 (https://www.openstreetmap.org/copyright).
Transport timetables: Hong Kong Transport Department GTFS via data.gov.hk.
