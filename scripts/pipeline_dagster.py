import dagster as dg
import pandas as pd
from pretraitement_pipelines import script_pretraitement
from scan_soda import script_scan
import os

INPUT_PATH = "./dataset/Cars/dataset_cars_projet.csv"
CLEAN_PATH = "./dataset/Cleaning/dataset_cleaning_pipeline.csv"

@dg.asset
def dataset_raw(context: dg.AssetExecutionContext) -> pd.DataFrame:
    context.log.info("Reading raw dataset")
    return pd.read_csv(INPUT_PATH, delimiter=",")

@dg.asset(deps=[dataset_raw])
def dataset_clean(context: dg.AssetExecutionContext, dataset_raw: pd.DataFrame) -> pd.DataFrame:
    context.log.info("Running preprocessing")
    cleaned = script_pretraitement(dataset_raw)
    return cleaned

@dg.asset(deps=[dataset_clean])
def soda_scan(context: dg.AssetExecutionContext, dataset_clean: pd.DataFrame):
    context.log.info("Running soda scan")
    result = script_scan(dataset_clean, output_report="results/soda_report.txt")

    if not result:
        raise ValueError("Scan returned no result")

    context.log.info(f"Soda scan completed: {result['report_path']}")
    return result

defs = dg.Definitions(assets=[dataset_raw, dataset_clean, soda_scan])

if __name__ == "__main__":
    if os.path.exists(CLEAN_PATH):
        df_clean = pd.read_csv(CLEAN_PATH)
        res = script_scan(df_clean)
        print("Scan finished:", res.get("report_path"))
    else:
        print("Error: cleaned dataset not found.")
