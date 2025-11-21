import pandas as pd
from ydata_profiling import ProfileReport

file_path = './dataset/dataset_nettoye_pipeline.csv' 

df = pd.read_csv(file_path, delimiter=",")

# 2. Générer le rapport de profilage
profil = ProfileReport(df, title="Profiling post pipeline nettoyage")

# 3. Enregistrer le rapport dans un fichier HTML
profil.to_file("results/rapport_profiling_post_pipeline.html")

# Alternative pour les Jupyter Notebooks/VS Code Interactive Windows :
# profil.to_notebook_iframe()