import dagster as dg
import pandas as pd
from pretraitement_pipelines import script_pretraitement
from scan_soda import  script_scan

file_raw = './dataset/Cars/dataset_cars_projet.csv'

file_option_clean = './dataset/Cleaning/dataset_cleaning_pipeline_options.csv'
file_carType_clean = './dataset/Cleaning/dataset_cleaning_pipeline_carType.csv.csv'
file_doorsSeats_clean = './dataset/Cleaning/dataset_cleaning_pipeline_doorsSeats.csv'

dataset_raw = pd.read_csv(file_raw, delimiter=",")

dataset_options_clean = pd.read_csv(file_clean, delimiter=", ")
dataset_carType_clean = pd.read_csv(file_clean, delimiter="-")
dataset_doorsSeats_clean = pd.read_csv(file_clean, delimiter=" ; ")

@dg.asset
def nettoyage(context: dg.AssetExecutionContext):
    script_pretraitement(dataset_raw)

@dg.asset(deps=[nettoyage])
def soda_scan(context: dg.AssetExecutionContext): // à voir si faut diviser en plusieurs methodes
    script_scan(dataset_options_clean)
    script_scan(dataset_carType_clean)
    script_scan(dataset_doorsSeats_clean)

defs = dg.Definitions(assets=[nettoyage, soda_scan])