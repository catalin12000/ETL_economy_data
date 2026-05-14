import os
import re
import sys
from pathlib import Path

# Force UTF-8 output
sys.stdout.reconfigure(encoding='utf-8')

def scan_pipelines():
    pipelines_dir = Path("etl/pipelines")
    report = []
    
    # Header
    report.append("# Downloader Inventory Report\n")
    report.append("Use this list to create sub-tickets for downloader verification and maintenance.\n")
    report.append("| Pipeline ID | Country | Source URL/Page | Methodology | Status |")
    report.append("| :--- | :--- | :--- | :--- | :--- |")
    
    for pipeline_path in sorted(pipelines_dir.iterdir()):
        if not pipeline_path.is_dir() or pipeline_path.name == "__pycache__":
            continue
            
        pipeline_py = pipeline_path / "pipeline.py"
        if not pipeline_py.exists():
            continue
            
        content = pipeline_py.read_text(encoding="utf-8")
        
        # Determine Country
        pid = pipeline_path.name
        country = "Cyprus" if pid.startswith("cy_") else "Greece/EU"
        
        # Find URL
        url_match = re.search(r'(API_URL|FILE_URL|SOURCE_PAGE|LANDING_URL)\s*=\s*[f]?"([^"]+)"', content)
        url = url_match.group(2) if url_match else "Dynamic/Not Found"
        
        # Determine Methodology
        method = "Unknown"
        if "cystatdb" in content or "ec.europa.eu" in content:
            method = "API"
        elif "BeautifulSoup" in content:
            method = "Web Scraper"
        elif ".xls" in content or ".xlsx" in content:
            method = "Direct File"
        
        # Status (Simple check if extract.py exists)
        has_extract = (pipeline_path / "extract.py").exists()
        status = "✅ Extraction Ready" if has_extract else "📥 Downloader Only"
        
        report.append(f"| `{pid}` | {country} | {url} | {method} | {status} |")

    return "\n".join(report)

if __name__ == "__main__":
    print(scan_pipelines())
