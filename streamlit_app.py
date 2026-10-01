# ---------------------------------------------------------------------------
# Radial Flow by DM - Streamlit web app.
# Reuses the same engine as the desktop/CLI version (radial_flow_by_dm.py); routing stays on ThreadPoolExecutor.
# ---------------------------------------------------------------------------
from __future__ import annotations

import tempfile
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

import radial_flow_by_dm as rf
from usage_log import log_usage

# Minimum seconds between runs per visitor: protects the shared public routing servers from 429 throttling.
COOLDOWN_SECONDS = 60

st.set_page_config(page_title=rf.APP_NAME, page_icon="🧭", layout="wide")

# Page header and honest-scope notice.
st.title(rf.APP_NAME)
st.caption("Relative pedestrian-flow potential for a Hong Kong location and time window. "
           "Scores are model indicators, not observed pedestrian counts.")

# Per-visitor session state: private output folder and last-run timestamp.
if "session_id" not in st.session_state:
    st.session_state.session_id = uuid.uuid4().hex[:12]
    st.session_state.last_run = 0.0
    st.session_state.result = None

# Input form (no personal data is collected).
today = datetime.now(rf.HK_TZ).date()
with st.form("inputs"):
    location_text = st.text_input("Location (latitude,longitude or Google Maps link)",
                                  value="22.3193,114.1694")
    label = st.text_input("Location label", value="Subject location")
    col_a, col_b, col_c = st.columns(3)
    run_date = col_a.date_input("Date (HKT)", value=today)
    start_clock = col_b.time_input("Start time", value=datetime.strptime("08:00", "%H:%M").time())
    end_clock = col_c.time_input("End time", value=datetime.strptime("10:00", "%H:%M").time())

    with st.expander("Advanced settings"):
        col_1, col_2, col_3, col_4 = st.columns(4)
        radius_m = col_1.number_input("Radius (m)", 300, 1500, 600, step=50)
        tolerance_m = col_2.number_input("Route tolerance (m)", 20, 200, 60, step=10)
        max_sources = col_3.number_input("Max source stops", 2, 12, 6)
        max_destinations = col_4.number_input("Max destinations", 3, 25, 8)
        workers = st.slider("Parallel workers (ThreadPoolExecutor)", rf.MIN_WORKERS, rf.MAX_WORKERS,
                            rf.DEFAULT_WORKERS)
        include_osm = st.checkbox("Load OpenStreetMap destinations", value=True)
        include_residential = st.checkbox("Include residential destinations", value=False)

    submitted = st.form_submit_button("Run analysis", type="primary")

# Run the analysis when the form is submitted.
if submitted:
    wait = COOLDOWN_SECONDS - (time.time() - st.session_state.last_run)
    if wait > 0:
        st.warning(f"Please wait {int(wait) + 1}s before the next run (shared public servers).")
    else:
        status_box = st.status("Running analysis...", expanded=True)
        log_lines: list[str] = []

        # Logger runs in the main script thread only; it streams progress into the status box.
        def logger(message: str) -> None:
            log_lines.append(message)
            status_box.write(message)

        try:
            session_root = Path(tempfile.gettempdir()) / "radial_flow" / st.session_state.session_id
            client = rf.HttpClient()
            subject = rf.parse_location(location_text, client)
            config = rf.Config(
                subject=subject,
                subject_label=label.strip() or "Subject location",
                start_datetime=datetime.combine(run_date, start_clock, tzinfo=rf.HK_TZ),
                end_datetime=datetime.combine(run_date, end_clock, tzinfo=rf.HK_TZ),
                radius_m=float(radius_m),
                target_tolerance_m=float(tolerance_m),
                max_sources=int(max_sources),
                max_destinations=int(max_destinations),
                include_osm_optional=include_osm,
                include_residential_optional=include_residential,
                output_dir=session_root / "output",
                # GTFS/Overpass cache is shared across visitors on the same container.
                cache_dir=Path(tempfile.gettempdir()) / "radial_flow_cache",
                workers=int(workers),
            )
            result = rf.FootfallAnalyzer(config, logger=logger).run()
            st.session_state.result = {
                "summary": result["summary"],
                "map_html": Path(result["map_path"]).read_text(encoding="utf-8"),
                "output_dir": str(config.output_dir),
            }
            st.session_state.last_run = time.time()
            status_box.update(label="Analysis completed", state="complete", expanded=False)

            # Optional Supabase log (no-op until Secrets are configured): input + date/time only.
            log_usage(location_text, config.start_datetime, config.end_datetime)
        except rf.AppError as error:
            status_box.update(label="Analysis failed", state="error")
            st.error(str(error))
        except Exception as error:
            status_box.update(label="Analysis failed", state="error")
            st.error(f"Unexpected error: {error}")

# Show the latest result: completeness flag, key metrics, map and downloads.
result = st.session_state.result
if result:
    summary = result["summary"]
    if summary["completeness"] != "Complete":
        st.warning(summary["completeness"])

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Scheduled departures", summary["total_scheduled_departures"])
    m2.metric("Peak slot", summary["peak_30m_slot"] or "-")
    m3.metric("Pass-through share", f"{(summary['target_pass_through_share'] or 0) * 100:.1f}%")
    m4.metric("Data quality", summary["data_quality"])

    components.html(result["map_html"], height=720, scrolling=False)

    st.subheader("Departures per 30 minutes")
    st.bar_chart({row["slot_label"]: row["scheduled_departures"] for row in summary["time_profile"]})

    # Download the generated CSV/JSON files.
    output_dir = Path(result["output_dir"])
    download_columns = st.columns(5)
    for column, file_name in zip(download_columns, (
            "latest_summary.json", "latest_flow_pairs.csv", "latest_sources.csv",
            "latest_destinations.csv", "latest_time_profile.csv")):
        file_path = output_dir / file_name
        if file_path.exists():
            column.download_button(file_name, file_path.read_bytes(), file_name=file_name)

st.divider()
st.caption(f"{rf.APP_NAME} v{rf.APP_VERSION} · Data: HK Transport Department GTFS, OpenStreetMap, "
           "OSRM/Valhalla routing. Relative indicators only.")
