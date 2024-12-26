from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable

TOKEN = Variable.get("HUBSPOT_PRIVATE_APP_TOKEN")
POSTGRES_CONN_ID='postgres_default'
API_CONN_ID='hubspot_api'

default_args={
    'owner':'airflow',
    'start_date':days_ago(1)
}

## DAG
with DAG(dag_id='hubspot_etl_pipeline_scd_3',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dags:
    
    @task()
    def extract_contact_data():
        """Extract contact data from hubspot API using Airflow Connection."""

        # Use HTTP Hook to get connection details from Airflow connection

        http_hook=HttpHook(http_conn_id=API_CONN_ID,method='GET')

        ## Build the API endpoint
        ## https://api.hubapi.com/crm/v3/objects/contacts?limit=10&archived=false
        endpoint=f'/crm/v3/objects/contacts?limit=10&archived=false'

        ## Make the request via the HTTP Hook
        response=http_hook.run(endpoint, headers = {
        'content-type': 'application/json',
        'authorization': 'Bearer %s' % TOKEN
        })

        if response.status_code == 200:
            print(response);
            return response.json()
        else:
            raise Exception(f"Failed to fetch contact data: {response.status_code}")
        
    @task()
    def transform_contact_data(contact_data):
        """Transform the extracted contact data."""
        all_contacts = contact_data['results']
        transformed_contacts = []

        for contact in all_contacts:
            transformed_data = {
                'hubspot_id': contact['id'],
                'firstname': contact['properties']['firstname'],
                'lastname': contact['properties']['lastname'], 
                'email': contact['properties']['email'], 
                'created_at': contact['createdAt']
            }
            transformed_contacts.append(transformed_data);

        return transformed_contacts
    
    @task()
    def load_contact_data(transformed_data):
        """Load transformed data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS contact_data_dim3 (
            hubspot_id numeric PRIMARY KEY,
            previous_firstname varchar(255),
            firstname varchar(255),
            previous_lastname varchar(255),
            lastname varchar(255),
            previous_email varchar(255),
            email varchar(255),
            created_at varchar(255)
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS contact_data_stg3 (
            hubspot_id numeric PRIMARY KEY,
            firstname varchar(255),
            lastname varchar(255),
            email varchar(255),
            created_at varchar(255)
        );
        """)

        cursor.execute("DELETE FROM contact_data_stg3;")

        for contact in transformed_data: 
            cursor.execute("""
            INSERT INTO contact_data_stg3 (hubspot_id, firstname, lastname, email, created_at)
            VALUES (%s, %s, %s, %s, %s);
            """, (
                contact['hubspot_id'],
                contact['firstname'],
                contact['lastname'],
                contact['email'],
                contact['created_at']
            ))

        
        # Insert transformed data into the table
        cursor.execute("""
        INSERT INTO contact_data_dim3 (hubspot_id, previous_firstname, firstname, previous_lastname, lastname, previous_email, email, created_at)
        SELECT hubspot_id, NULL AS previous_firstname, firstname, NULL AS previous_lastname, lastname, NULL AS previous_email, email, created_at
        FROM contact_data_stg3
        ON CONFLICT (hubspot_id) 
        DO UPDATE SET
            previous_firstname = contact_data_dim3.firstname, 
            firstname = EXCLUDED.firstname, 
            previous_lastname = contact_data_dim3.lastname, 
            lastname = EXCLUDED.lastname, 
            previous_email = contact_data_dim3.email, 
            email = EXCLUDED.email;
        """)

        conn.commit()
    

        cursor.close()

    ## DAG Worflow- ETL Pipeline
    contact_data= extract_contact_data()
    transformed_data=transform_contact_data(contact_data)
    load_contact_data(transformed_data)