import pandas as pd
import os

def clean_data(dataset: pd.DataFrame) -> pd.DataFrame:
    df = dataset.copy()

    # Suppression des espaces dans les colonnes
    df.columns = df.columns.str.strip()

    # Nettoyage FuelType, Transmission, FuelEfficiency
    df['FuelType'] = df['FuelType'].fillna('Unknown')
    df['Transmission'] = df['Transmission'].fillna('Unknown')
    df['FuelEfficiency(L/100km)'] = df['FuelEfficiency(L/100km)'].fillna(df['FuelEfficiency(L/100km)'].median())

    # Correction CarAge
    df['CarAge'] = df['CarAge'].apply(lambda x: df['CarAge'].median() if x == 116 else x)

    # Correction AccidentHistory
    df['AccidentHistory'] = df['AccidentHistory'].replace('Peut-être', 'Unknown')

    # Correction Insurance
    df['Insurance'] = df['Insurance'].replace('On sait pas', 'Unknown')

    # Correction RegistrationStatus
    df['RegistrationStatus'] = df['RegistrationStatus'].replace('Voiture volée', 'Stolen')

    # Correction Doors / Seats
    df[['Doors', 'Seats']] = df['Doors ; Seats'].str.extract(r'\[(.*);(.*)\]').astype(float)
    df['Doors'] = df['Doors'].replace({15.0: 5, 2.78: 3, 4.0: 4})
    df['Seats'] = df['Seats'].replace({0: 5})
    df = df.drop("Doors ; Seats", axis=1)

    # Remplacer les valeurs négatives
    numeric_cols = ['Year', 'CarAge', 'Mileage(km)', 'Horsepower', 'Torque', 'Price($)']
    for col in numeric_cols:
        df[col] = df[col].apply(lambda x: max(x, 0))

    # Supprimer les lignes vides
    df = df.fillna("Unknown")

    return df

def script_pretraitement(dataset: pd.DataFrame) -> pd.DataFrame:
    os.makedirs("./dataset/Cleaning", exist_ok=True)
    cleaned = clean_data(dataset)
    cleaned.to_csv("./dataset/Cleaning/dataset_cleaning_pipeline.csv", index=False)
    return cleaned

if __name__ == '__main__':
    input_path = "../dataset/Cars/dataset_cars_projet.csv"
    df = pd.read_csv(input_path, delimiter=",")
    cleaned = script_pretraitement(df)
    print("Preprocessing completed.")
