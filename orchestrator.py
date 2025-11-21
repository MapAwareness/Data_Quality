import dagster as dg
import pandas as pd
from pretraitement_pipelines import script_pretraitement
from scan_soda import  script_scan

file_raw = './dataset/Airplane_Crashes_and_Fatalities_Since_1908.csv' 
file_clean = './dataset/dataset_nettoye.csv' 
dataset_raw = pd.read_csv(file_raw, delimiter=",")
dataset_clean = pd.read_csv(file_clean, delimiter=",")

@dg.asset
def nettoyage(context: dg.AssetExecutionContext):
    script_pretraitement(dataset_raw)

@dg.asset(deps=[nettoyage])
def soda_scan(context: dg.AssetExecutionContext):
    script_scan(dataset_clean)

defs = dg.Definitions(assets=[nettoyage, soda_scan])