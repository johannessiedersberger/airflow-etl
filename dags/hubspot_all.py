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
with DAG(dag_id='hubspot_etl_pipeline_scd_1_all',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dags:
    
    @task()
    def extract_data():
        """Extract contact data from hubspot API using Airflow Connection."""

        # Use HTTP Hook to get connection details from Airflow connection

        http_hook=HttpHook(http_conn_id=API_CONN_ID,method='GET')

        ## Build the API endpoint
        ## https://api.hubapi.com/crm/v3/objects/contacts
        endpointContacs="/crm/v3/objects/contacts"
        endpointCompanies="/crm/v3/objects/companies"
        endpointDeals="/crm/v3/objects/deals?associations=contacts,companies"
        endpointProduct="/crm/v3/objects/products?properties=description,name,price"
        endpointLiteItem="/crm/v3/objects/line_item?associations=deals,products"

        ## Make the request via the HTTP Hook
        authHeader={'content-type': 'application/json','authorization': 'Bearer %s' % TOKEN}

        responseContacts=http_hook.run(endpointContacs, headers = authHeader)
        responseCompaines=http_hook.run(endpointCompanies, headers=authHeader)
        responseDeals=http_hook.run(endpointDeals, headers=authHeader)
        responseProducts=http_hook.run(endpointProduct, headers=authHeader)
        responseLineItems=http_hook.run(endpointLiteItem, headers=authHeader)

        responses = [responseContacts, responseCompaines, responseCompaines, responseProducts, responseLineItems]

        for resp in responses: 
            if resp.status_code != 200:
                raise Exception(f"Failed to fetch contact data: {responseContacts.status_code}")

        return [responseContacts.json(), responseCompaines.json(), responseDeals.json(), responseProducts.json(), responseLineItems.json()]
            

        
    @task()
    def transform_data(data):
        """Transform the extracted data."""
        transformed_contacts = transform_contact_data(data[0])
        transformed_companies = transform_company_data(data[1])
        transformed_deals = transform_deal_data(data[2])
        transformed_products = transform_product_data(data[3])
        transformed_line_items = transform_line_item_data(data[4])

        return [transformed_contacts, transformed_companies, transformed_deals, transformed_products, transformed_line_items]
    
    def transform_contact_data(contact_data):
        all_contacts = contact_data['results']
        transformed_contacts = []

        for contact in all_contacts:
            transformed_data = {
                'contact_id': contact['id'],
                'firstname': contact['properties']['firstname'],
                'lastname': contact['properties']['lastname'], 
                'email': contact['properties']['email'], 
            }
            transformed_contacts.append(transformed_data);

        return transformed_contacts
    
    def transform_company_data(company_data):
        all_companies = company_data['results']
        transformed_companies = []

        for company in all_companies:
            transformed_data = {
                'company_id': company['id'],
                'name': company['properties']['name'],
            }
            transformed_companies.append(transformed_data);

        return transformed_companies
    
    def transform_deal_data(deal_data):
        all_deals = deal_data['results']
        transformed_deals = []

        for deal in all_deals:
            transformed_data = {
                'deal_id': deal['id'],
                'contact_id': deal['associations']['contacts']['results'][0]['id'],
                'company_id': deal['associations']['companies']['results'][0]['id']
            }
            transformed_deals.append(transformed_data);

        return transformed_deals
    
    def transform_product_data(product_data):
        all_products = product_data['results']
        transformed_products = []

        for product in all_products:
            transformed_data = {
                'product_id': product['id'],
                'name': product['properties']['name'],
                'price': product['properties']['price'], 
                'description': product['properties']['description']
            }
            transformed_products.append(transformed_data);
    
        return transformed_products
    
    def transform_line_item_data(product_data):
        all_line_items = product_data['results']
        transformed_line_items = []

        for line_item in all_line_items:
            transformed_data = {
                'line_item_id': line_item['id'],
                'deal_id': line_item['associations']['deals']['results'][0]['id'],
                'product_id': line_item['properties']['hs_product_id'], 
                'quantity': line_item['properties']['quantity']
            }
            transformed_line_items.append(transformed_data)

        return transformed_line_items
    
    @task()
    def load_data(transformed_data):
        """Load transformed data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_contacts (
            contact_id numeric PRIMARY KEY,
            firstname varchar(255),
            lastname varchar(255),
            email varchar(255)
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_companies (
            company_id numeric PRIMARY KEY,
            name varchar(255)
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_deals (
            deal_id numeric PRIMARY KEY,
            contact_id numeric REFERENCES dim_contacts (contact_id), 
            company_id numeric REFERENCES dim_companies (company_id)
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_products (
            product_id numeric PRIMARY KEY,
            name varchar(255), 
            description varchar(255), 
            price float(24)
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_line_items (
            line_item_id numeric PRIMARY KEY,
            deal_id numeric REFERENCES dim_deals (deal_id), 
            product_id numeric REFERENCES dim_products (product_id), 
            quantity integer
        );
        """)

        for contact in transformed_data[0]: 
            print(contact)
            # Insert transformed data into the table
            cursor.execute("""
            INSERT INTO dim_contacts (contact_id, firstname, lastname, email)
            VALUES (%s, %s, %s, %s) ON CONFLICT (contact_id) DO UPDATE
            SET firstname = %s, lastname = %s, email = %s
            """, (
                contact['contact_id'],
                contact['firstname'],
                contact['lastname'],
                contact['email'],
                ### update values
                contact['firstname'],
                contact['lastname'],
                contact['email'],
            ))
        conn.commit()

        for company in transformed_data[1]: 
            # Insert transformed data into the table
            cursor.execute("""
            INSERT INTO dim_companies (company_id, name)
            VALUES (%s, %s) ON CONFLICT (company_id) DO UPDATE
            SET name = %s
            """, (
                company['company_id'],
                company['name'],
                ### update values
                company['name']
            ))
        conn.commit()

        for deal in transformed_data[2]: 
            # Insert transformed data into the table
            cursor.execute("""
            INSERT INTO dim_deals (deal_id, contact_id, company_id)
            VALUES (%s, %s, %s) ON CONFLICT (deal_id) DO UPDATE
            SET company_id = %s, contact_id = %s
            """, (
                deal['deal_id'],
                deal['contact_id'],
                deal['company_id'],
                ### update values
                deal['company_id'],
                deal['contact_id'],
            ))
        conn.commit()

        for product in transformed_data[3]: 
            # Insert transformed data into the table
            cursor.execute("""
            INSERT INTO dim_products (product_id, name, description, price)
            VALUES (%s, %s, %s, %s) ON CONFLICT (product_id) DO UPDATE
            SET name = %s, description = %s, price = %s
            """, (
                product['product_id'],
                product['name'],
                product['description'],
                product['price'],
                ### update values
                product['name'],
                product['description'],
                product['price']
            ))
        conn.commit()

        for line_item in transformed_data[4]: 
            # Insert transformed data into the table
            cursor.execute("""
            INSERT INTO fact_line_items (line_item_id, deal_id, product_id, quantity)
            VALUES (%s, %s, %s, %s) ON CONFLICT (line_item_id) DO UPDATE
            SET deal_id = %s, product_id = %s, quantity = %s
            """, (
                line_item['line_item_id'],
                line_item['deal_id'],
                line_item['product_id'],
                line_item['quantity'],
                ### update values
                line_item['deal_id'],
                line_item['product_id'],
                line_item['quantity']
            ))
        conn.commit()
    

        cursor.close()

    ## DAG Worflow- ETL Pipeline
    extracted_data= extract_data()
    transformed_data=transform_data(extracted_data)
    load_data(transformed_data)