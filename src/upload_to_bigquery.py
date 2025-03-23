import pandas as pd
from google.cloud import bigquery

def upload_to_bigquery(csv_file, project_id, dataset_id, table_id):
    df = pd.read_csv(csv_file)
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job = client.load_table_from_dataframe(df, table_ref, job_config=bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE 
    ))
    
    job.result()
    print(f"Data successfully uploaded to {table_ref}")

if __name__ == "__main__":
    upload_to_bigquery(
        csv_file="youtube_videos.csv",  
        project_id="mediaanalyticsplatform",  
        dataset_id="media_analytics",  
        table_id="youtube_videos"  
    )
