import json
from pathlib import Path
import pandas as pd
from datetime import datetime

STATE_DIR = Path("data/state")

def generate_dashboard():
    if not STATE_DIR.exists():
        print("No state directory found.")
        return

    stats = []
    for f in STATE_DIR.glob("*.json"):
        try:
            with open(f, "r", encoding="utf-8") as j:
                state = json.load(j)
            
            pipeline_id = f.stem
            last_run = state.get("last_run_at_utc", "Never")
            last_success = state.get("last_success_at_utc", "Never")
            status = state.get("last_status", "unknown")
            message = state.get("last_message", "")
            downloaded_at = state.get("downloaded_at_utc", "N/A")
            
            # Smart Source Detection
            source_url = state.get("source_url_used") or state.get("download_url_used") or state.get("api_url_used") or state.get("resolved_pdf_url")
            
            # Check for multi-file pipelines
            is_multi = False
            if not source_url:
                for k in ["download_url_03", "contracts_url", "foreigners_url"]:
                    if k in state:
                        source_url = state.get(k)
                        is_multi = True
                        break
            
            if "file_sha256_price" in state or "file_sha256_receipts" in state:
                is_multi = True

            # Labeling
            source_label = source_url if source_url else "N/A"
            
            # Detect Satellite/Dependent pipelines
            is_satellite = False
            msg_low = message.lower()
            if "master pipeline" in msg_low or "source appendix b" in msg_low or "source file detected" in msg_low:
                is_satellite = True
            
            if is_satellite:
                source_label = "[Satellite] Derived from Master"
            elif source_url:
                prefix = "[Multi-Source] " if is_multi else ""
                if "api" in source_url.lower() or "sdmx" in source_url.lower():
                    source_label = f"{prefix}[API] {source_url}"
                elif source_url.lower().endswith(".pdf") or ".pdf" in source_url.lower():
                    source_label = f"{prefix}[PDF] {source_url}"
                elif source_url.lower().endswith(".xls") or source_url.lower().endswith(".xlsx"):
                    source_label = f"{prefix}[Excel] {source_url}"
                else:
                    source_label = f"{prefix}{source_url}"
            elif "last_download_path" in state:
                path = state["last_download_path"].lower()
                if path.endswith(".pdf"):
                    source_label = "[PDF] Website Source"
                elif path.endswith(".xls") or path.endswith(".xlsx"):
                    source_label = "[Excel] Website Source"

            # Formatting dates for readability
            def fmt_date(iso_str):
                if not iso_str or iso_str == "Never": return iso_str
                try:
                    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
                    return dt.strftime("%Y-%m-%d %H:%M")
                except:
                    return iso_str

            stats.append({
                "Pipeline": pipeline_id,
                "Status": status.upper(),
                "Last Run": fmt_date(last_run),
                "Last Success": fmt_date(last_success),
                "Latest Download": fmt_date(downloaded_at),
                "Source": source_label,
                "Message": message
            })
        except Exception as e:
            print(f"Error reading {f}: {e}")

    if not stats:
        print("No pipeline states found.")
        return

    df = pd.DataFrame(stats)
    df = df.sort_values("Pipeline")

    # Console Output (truncated URLs for readability)
    df_console = df.copy()
    df_console["Source"] = df_console["Source"].apply(lambda x: (x[:47] + "...") if len(str(x)) > 50 else x)
    
    pd.set_option('display.max_colwidth', 50)
    print("\n--- Pipeline Status Dashboard ---")
    print(df_console.to_string(index=False))

    # Markdown Output (full URLs)
    md_content = "# ETL Pipeline Dashboard\n\n"
    md_content += f"Last Dashboard Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    md_content += df.to_markdown(index=False)
    
    with open("DASHBOARD.md", "w", encoding="utf-8") as md:
        md.write(md_content)
    print(f"\nDashboard saved to DASHBOARD.md")

if __name__ == "__main__":
    generate_dashboard()
