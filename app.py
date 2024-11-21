import streamlit as st
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import snowflake.connector
from datetime import datetime

AWS_ACCESS_KEY_ID = 'AKIAUGO4KTTLZC42A6GD'  
AWS_SECRET_ACCESS_KEY = '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'  
BUCKET_NAME = 'streamlit-app-testing-ear'  
REGION_NAME = 'us-east-1'  

def snowflake_connection(df):
    user = 'VISHALTRIALSNOWFLAKE'  
    password = '4694936566@aA'    
    account = 'eqb93259'
    warehouse = 'COMPUTE_WH'        
    database = 'TEST'         
    schema = 'TRAIL'               
    table_name = 'STUDENT_NAME'
    full_table_name = f"{database}.{schema}.{table_name}"

    conn = None
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
            login_timeout=60  # Optional: Increase timeout
        )

        with conn.cursor() as cur:
            for index, row in df.iterrows():
                sql = f"INSERT INTO {full_table_name} VALUES ({', '.join(['%s'] * len(row))})"
                cur.execute(sql, tuple(row))

        print(f"Successfully wrote {len(df)} rows to {full_table_name}.")

    except Exception as e:
        print("Error: ", e)
    finally:
        if conn:
            conn.close()

s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=REGION_NAME)

st.title("EAR File Upload")

uploaded_file = st.file_uploader("Please Upload CSV file", type="csv", key='file_uploader')

if uploaded_file is not None:
    st.success("File has been selected.")
else:
    st.write("Waiting for the file")

if st.button("Submit"):
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file)
            
            # Initialize progress bar
            progress_bar = st.progress(0)
            progress_value = 0.0
            
            # Write to Snowflake
            snowflake_connection(df)
            progress_value += 0.5  # Increment by 50% after writing to Snowflake
            progress_bar.progress(progress_value)

            # Prepare for S3 upload
            current_time = datetime.now()
            new_filename = f"EAR_{current_time.strftime('%m_%d_%Y_%H_%M_%S')}.csv"
            uploaded_file.name = new_filename
            
            # Upload to S3
            s3_client.upload_fileobj(uploaded_file, BUCKET_NAME, uploaded_file.name)
            progress_value += 0.5  # Increment by another 50% after uploading to S3
            progress_bar.progress(progress_value)

            st.success("File successfully uploaded.")
            st.subheader("Data Preview")
            st.write(df.head(2))
            st.warning("You can now upload a new file.")

        except NoCredentialsError:
            st.error("AWS credentials not found. Please check your credentials.")
        except ClientError as e:
            st.error(f"An error occurred: {e.response['Error']['Message']}")
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")
    else:
        st.warning("Please upload a file before clicking submit.")
