import pandas as pd

def conversion_date(dataset: pd.DataFrame):
    # Traitement des O dans le dataset :
    # On crée une nouvelle colonne pour cette donnée
    new_col = "Date_parsed"
    dataset[new_col] = dataset["Date"].str.replace("O", "0")

    # Uniformisation du format :
    dataset[new_col] = dataset[new_col].str.replace("-", "/")

    dataset[new_col] = pd.to_datetime(
                                dataset[new_col], 
                                format='%m/%d/%Y', 
                                errors='coerce')
    
    if dataset[new_col].isna().sum() > 0:
        # On vérifie qu'il n'y a plus de NaT
        raise ValueError(f"conversion_date : Il reste des NaT : \
                         {dataset['Date'][dataset[new_col].isna()]}")
    dataset = dataset.drop("Date", axis=1)

    return dataset


def conversion_type(dataset: pd.DataFrame):

    # Lower case pour éviter les doublons
    df = dataset.copy().dropna(subset=['Type'])
    lowered = df['Type'].str.lower()

    # On enlève les modèles en de ...
    lowered = lowered.str.replace("de ", "", regex=False)

    # Séparation en modèle, version :
    split = lowered.str.split(' ', n=1, expand=True)
    df['Modele'] = split[0]
    df['Version'] = split[1]
    
    if df['Modele'].isna().sum() > 0:
        # On vérifie qu'il n'y a plus de NaN
        raise ValueError(f"conversion_type : Modele à NaN : \
                         {dataset['Type'][df['Modele'].isna()]}")
    df = df.drop("Type", axis=1)
    return df


def conversion_index(dataset: pd.DataFrame):
    # Inplace pour ne pas faire de copie, modifier direct original
    dataset = dataset.drop("index", axis=1)
    return dataset


def conversion_location(dataset: pd.DataFrame):
    # Conversion en strings
    dataset["Location"] = dataset["Location"].astype(str)
    dataset["Location"] = dataset["Location"].str.lower()
    dataset[['Ville', 'Pays']] = dataset["Location"].str.split(",",n=1, expand=True)

    # Removing the eventual county :
    dataset["Pays"] = dataset["Pays"].str.split(",").str[-1].str.strip()

    # Tout mettre en UK :
    dataset["Pays"].replace({
            "great britain": "uk",
            "england": "uk",
            "united kingdom": "uk"
        })
    
    dataset = dataset.drop("Location", axis=1)
    return dataset


def conversion_numerics(dataset:pd.DataFrame):
    # Drop valeurs négatives
    dataset = dataset[dataset['Aboard'] >= 0]
    dataset = dataset[dataset['Fatalities'] >= 0]
    dataset = dataset[dataset['Ground'] >= 0]

    # Valeurs manquantes : 
    dataset["Aboard_filled"] = dataset.groupby("Modele")["Aboard"]\
                                    .transform(lambda x: x.fillna(x.mean()))
    dataset["Fatalities_filled"] = dataset.groupby("Modele")["Fatalities"]\
                                    .transform(lambda x: x.fillna(x.mean()))
    dataset = dataset.fillna({"Ground": 0})

    if dataset["Aboard_filled"].isna().sum() > 0:
        raise ValueError("Aboard_filled contient des NaN")
    if dataset["Fatalities_filled"].isna().sum() > 0:
        raise ValueError("Aboard_filled contient des NaN")
    if dataset["Ground"].isna().sum() > 0:
        raise ValueError("Ground contient des NaN")
    return dataset

def script_pretraitement(dataset):
    # Load the dataset into a pandas DataFrame
    
    try:
        dataset_traite = (dataset
            .pipe(conversion_date)
            .pipe(conversion_type)
            .pipe(conversion_index)
            .pipe(conversion_location)
            .pipe(conversion_numerics)
        )
    except Exception as e:
        raise Exception(f"Erreur dans la pipeline de nettoyage : {e}")
    dataset_traite.to_csv('./dataset/dataset_nettoye_pipeline.csv', index=False)


if __name__=='__main__':
    file_path = './dataset/Airplane_Crashes_and_Fatalities_Since_1908.csv' 
    dataset = pd.read_csv(file_path, delimiter=",")

    script_pretraitement(dataset)
    print("!!!!!!!!!!!")
    print("DATASET NETTOYE")
    print("!!!!!!!!!!!")
    