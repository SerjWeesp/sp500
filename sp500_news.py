import os
import pandas as pd
from google.cloud import bigquery

# 1. Authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"D:\GitHub\sp500\gcp-key.json"

# 2. Configuration
DATA_DIR = r"D:\GitHub\sp500\data"
NAMES_CSV = os.path.join(DATA_DIR, "sp500_names_14052026.csv")
OUTPUT_FILE = os.path.join(DATA_DIR, "sp500_news_metadata_2011_2026.parquet")

START_DATE = 20110101
END_DATE = 20260514

def main():
    client = bigquery.Client()
    
    # Load and clean S&P 500 names
    sp500_df = pd.read_csv(NAMES_CSV)
    sp500_df['Search_Name'] = sp500_df['Security'].str.upper().str.replace(r'\s*(INC\.|CORP\.|CO\.|LTD\.|PLC).*', '', regex=True)
    
    # Create a single massive Regex pattern for all 500 companies
    clean_names = [name.strip() for name in sp500_df['Search_Name'].tolist() if name.strip()]
    regex_pattern = r"(" + "|".join(clean_names) + r")"
    
    print(f"Compiled Regex pattern for {len(clean_names)} companies.")

    # The Single-Pass Query (Fixed Schema Columns)
    query = """
                SELECT 
                    SQLDATE as Date,
                    Actor1Name,
                    COUNT(SOURCEURL) as News_Volume,
                    AVG(AvgTone) as Daily_Average_Sentiment,
                    MIN(AvgTone) as Min_Sentiment,
                    MAX(AvgTone) as Max_Sentiment
                FROM 
                    `gdelt-bq.gdeltv2.events`
                WHERE 
                    SQLDATE BETWEEN @start_date AND @end_date
                    AND REGEXP_CONTAINS(IFNULL(Actor1Name, ''), @regex_pattern)
                GROUP BY 
                    SQLDATE, Actor1Name
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "INT64", START_DATE),
            bigquery.ScalarQueryParameter("end_date", "INT64", END_DATE),
            bigquery.ScalarQueryParameter("regex_pattern", "STRING", regex_pattern),
        ]
    )

    # ==========================================
    # SAFETY CHECK: DRY RUN (Costs $0.00)
    # ==========================================
    dry_run_config = bigquery.QueryJobConfig(
        dry_run=True, 
        use_query_cache=False,
        query_parameters=job_config.query_parameters
    )
    
    try:
        dry_run_job = client.query(query, job_config=dry_run_config)
        gb_billed = dry_run_job.total_bytes_processed / (1024 ** 3)
        print(f"--- DRY RUN ESTIMATE ---")
        print(f"This query will process: {gb_billed:.2f} GB")
        print(f"Remaining Free Tier: ~{1000 - gb_billed:.2f} GB")
        print(f"------------------------")
        
        if gb_billed > 500:
            print("CRITICAL: Query is too large! Aborting to save free tier.")
            return
            
    except Exception as e:
        print(f"Dry run failed: {e}")
        return

    # ==========================================
    # ACTUAL EXECUTION
    # ==========================================
    user_input = input("Proceed with actual data extraction? (y/n): ")
    if user_input.lower() != 'y':
        print("Aborting.")
        return

    print("Executing query across Google's clusters. This may take 1-3 minutes...")
    query_job = client.query(query, job_config=job_config)
    
    all_news_df = query_job.to_dataframe()
    
    if not all_news_df.empty:
        all_news_df.to_parquet(OUTPUT_FILE, 
                                engine='pyarrow', 
                                compression='zstd', 
                                index=False)
        print(f"\nSuccessfully extracted and saved {len(all_news_df)} records to Parquet!")
    else:
        print("\nQuery executed successfully but returned 0 records.")

if __name__ == "__main__":
    main()