import pandas as pd
import os

def script_scan(dataset: pd.DataFrame, output_report: str = "results/soda_report.txt"):
    os.makedirs("results", exist_ok=True)

    report_content = []

    # Exemple de "tests" simplifiés
    report_content.append("Checking missing values...")
    if dataset.isna().sum().sum() == 0:
        report_content.append("OK: No missing values.")
    else:
        report_content.append("WARNING: Missing values detected.")

    report_content.append("Checking negative numbers...")
    numeric = ['Year', 'CarAge', 'Mileage(km)', 'Price($)']
    for col in numeric:
        if (dataset[col] < 0).any():
            report_content.append(f"ALERT: Negative values detected in {col}.")
        else:
            report_content.append(f"OK: {col} valid.")

    with open(output_report, "w") as f:
        for line in report_content:
            f.write(line + "\n")

    return {"report_path": output_report, "status": "completed"}

if __name__ == '__main__':
    path = "./dataset/Cleaning/dataset_cleaning_pipeline.csv"
    df = pd.read_csv(path)
    res = script_scan(df)
    print("Scan finished:", res["report_path"])
