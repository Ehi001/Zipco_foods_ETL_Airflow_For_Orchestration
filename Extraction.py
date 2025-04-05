import pandas as pd

# Data extraction

def run_extraction():
    try:
        data = pd.read_csv(r'C:\Users\Oaikhena\Downloads\10alytics Data Engineering Course\Zipco_foods_ETL_Airflow_For_Orchestration\zipco_transaction.csv')
        print('Data Extracted Successfully')
    except Exception as e:
        print(f'An error occurred: {e}')
