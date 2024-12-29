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
    def extract_product_data():
        """Extract contact data from hubspot API using Airflow Connection."""

        # Use HTTP Hook to get connection details from Airflow connection

        http_hook=HttpHook(http_conn_id=API_CONN_ID,method='GET')

        ## Build the API endpoint
        ## https://api.hubapi.com/crm/v3/objects/contacts?limit=10&archived=false
        endpoint=f'/crm/v3/objects/products?properties=description,name,price'

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
    def transform_product_data(product_data):
        """Transform the extracted product data."""
        all_products = product_data['results']
        transformed_products = []

        for product in all_products:
            transformed_data = {
                'product_id': product['id'],
                'name': product['properties']['name'],
                'description': product['properties']['description'], 
                'price': product['properties']['price']
            }
            transformed_products.append(transformed_data);

        return transformed_products
    
    @task()
    def load_product_data(transformed_data):
        """Load transformed data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim3_products (
            product_id numeric PRIMARY KEY,
            previous_name varchar(255),
            name varchar(255),
            previous_description varchar(255),
            description varchar(255),
            previous_price float(24),
            price float(24)
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stg_products (
            product_id numeric PRIMARY KEY,
            name varchar(255),
            description varchar(255),
            price float(24)
        );
        """)

        cursor.execute("DELETE FROM stg_products;")

        for product in transformed_data: 
            cursor.execute("""
            INSERT INTO stg_products (product_id, name, description, price)
            VALUES (%s, %s, %s, %s);
            """, (
                product['product_id'],
                product['name'],
                product['description'],
                product['price']
            ))

        
        # Insert transformed data into the table
        cursor.execute("""
        INSERT INTO dim3_products (product_id, previous_name, name, previous_description, description, previous_price, price)
        SELECT product_id, NULL AS previous_name, name, NULL AS previous_description, description, NULL AS previous_price, price
        FROM stg_products
        ON CONFLICT (product_id) 
        DO UPDATE SET
            previous_name = dim3_products.name, 
            name = EXCLUDED.name, 
            previous_description = dim3_products.description, 
            description = EXCLUDED.description, 
            previous_price = dim3_products.price, 
            price = EXCLUDED.price;
        """)

        conn.commit()
    

        cursor.close()

    ## DAG Worflow- ETL Pipeline
    product_data= extract_product_data()
    transformed_data=transform_product_data(product_data)
    load_product_data(transformed_data)