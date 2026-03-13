import subprocess
import json
import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed




URLS = [
    "https://www.tajhotels.com/en-in",
    "https://www.vivantahotels.com/en-in",
    "https://www.gateway-hotels.com/en-in",
    "https://www.seleqtionshotels.com/en-in",
    "https://www.gingerhotels.com/",
    "https://www.amastaysandtrails.com/en-in"
]

LIGHTHOUSE_PATH = r"C:\Users\Deekshitha.B\AppData\Roaming\npm\lighthouse.cmd"

MAX_RETRIES = 2
TIMEOUT_SECONDS = 120
MAX_WORKERS = 3  




logging.basicConfig(
    filename="lighthouse_execution.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("===== Lighthouse Execution Started =====")




def zero_metrics():
    return {
        "performance_score": 0,
        "accessibility_score": 0,
        "best_practices_score": 0,
        "seo_score": 0,
        "fcp": 0,
        "lcp": 0,
        "cls": 0
    }



def run_lighthouse(url, mode, report_file):
    for attempt in range(1, MAX_RETRIES + 2):
        logging.info(f"{url} ({mode}) - Attempt {attempt}")

        command = [
            LIGHTHOUSE_PATH,
            url,
            "--output=json",
            f"--output-path={report_file}",
            "--chrome-flags=--headless --no-sandbox --disable-dev-shm-usage"
        ]

        if mode.lower() == "desktop":
            command.append("--preset=desktop")

        try:
            result = subprocess.run(
                command,
                shell=True,
                timeout=TIMEOUT_SECONDS
            )

            if result.returncode == 0 and os.path.exists(report_file):
                with open(report_file, "r", encoding="utf-8") as f:
                    logging.info(f"{url} ({mode}) SUCCESS")
                    return json.load(f)

        except subprocess.TimeoutExpired:
            logging.error(f"{url} ({mode}) TIMEOUT")

        except Exception as e:
            logging.error(f"{url} ({mode}) ERROR: {e}")

        time.sleep(5)

    logging.error(f"{url} ({mode}) FINAL FAILURE")
    return None



def extract_metrics(data):
    try:
        return {
            "performance_score": round(data["categories"]["performance"]["score"] * 100, 2),
            "accessibility_score": round(data["categories"]["accessibility"]["score"] * 100, 2),
            "best_practices_score": round(data["categories"]["best-practices"]["score"] * 100, 2),
            "seo_score": round(data["categories"]["seo"]["score"] * 100, 2),
            "fcp": round(data["audits"]["first-contentful-paint"]["numericValue"] / 1000, 2),
            "lcp": round(data["audits"]["largest-contentful-paint"]["numericValue"] / 1000, 2),
            "cls": round(data["audits"]["cumulative-layout-shift"]["numericValue"], 3)
        }
    except Exception:
        return zero_metrics()




def process_url(url):
    safe_name = url.replace("https://", "").replace("/", "_")

    mobile_report = f"mobile_{safe_name}.json"
    desktop_report = f"desktop_{safe_name}.json"

    mobile_data = run_lighthouse(url, "mobile", mobile_report)
    desktop_data = run_lighthouse(url, "desktop", desktop_report)

    mobile_success = mobile_data is not None
    desktop_success = desktop_data is not None

    mobile_metrics = extract_metrics(mobile_data) if mobile_success else zero_metrics()
    desktop_metrics = extract_metrics(desktop_data) if desktop_success else zero_metrics()

    status = "success" if mobile_success and desktop_success else "failed"

    return {
        "url": url,

        "desktop_performance": desktop_metrics["performance_score"],
        "desktop_accessibility": desktop_metrics["accessibility_score"],
        "desktop_best_practices": desktop_metrics["best_practices_score"],
        "desktop_seo": desktop_metrics["seo_score"],

        "mobile_performance": mobile_metrics["performance_score"],
        "mobile_accessibility": mobile_metrics["accessibility_score"],
        "mobile_best_practices": mobile_metrics["best_practices_score"],
        "mobile_seo": mobile_metrics["seo_score"],

        "mobile_fcp": mobile_metrics["fcp"],
        "mobile_lcp": mobile_metrics["lcp"],
        "mobile_cls": mobile_metrics["cls"],

        "desktop_fcp": desktop_metrics["fcp"],
        "desktop_lcp": desktop_metrics["lcp"],
        "desktop_cls": desktop_metrics["cls"],

        "status": status,
        "createdtimestamp": datetime.now()
    }




rows = []

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(process_url, url) for url in URLS]

    for future in as_completed(futures):
        rows.append(future.result())

df = pd.DataFrame(rows)

print("\nData Prepared:")
print(df)




engine = create_engine(
    "postgresql://search_sandbox:search_sandbox@172.168.168.232:5432/Dashboard"
)

df.to_sql(
    "website_lighthouse_scores_v4",
    engine,
    if_exists="append",
    index=False
)

logging.info("===== Lighthouse Execution Completed Successfully =====")
print("\nData inserted successfully!")