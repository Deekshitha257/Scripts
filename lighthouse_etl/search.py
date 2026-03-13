import pandas as pd
from urllib.parse import urlparse, parse_qs
from sqlalchemy import create_engine



DATABASE_URL = "postgresql+psycopg2://search_sandbox:search_sandbox@172.168.168.232:5432/Dashboard"

engine = create_engine(DATABASE_URL)



df = pd.read_csv(
    'jan-search-logs.csv',
    encoding='latin1',
    low_memory=False
)

filtered_df = df[['timestamp [UTC]', 'url', 'resultCode']].copy()



filtered_df['timestamp_utc'] = pd.to_datetime(
    filtered_df['timestamp [UTC]'],
    errors='coerce'
)

filtered_df['timestamp_ist'] = (
    filtered_df['timestamp_utc']
    .dt.tz_localize('UTC')
    .dt.tz_convert('Asia/Kolkata')
)



def extract_domain(url):
    try:
        return urlparse(url).netloc
    except:
        return None

filtered_df['domain'] = filtered_df['url'].apply(extract_domain)



def extract_query_param_q(url):
    try:
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        return query_params.get('q', [None])[0]
    except:
        return None

filtered_df['query'] = filtered_df['url'].apply(extract_query_param_q)



def extract_category_param(url):
    try:
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        
        if 'category' in query_params:
            return query_params['category'][0]
        elif 'cat' in query_params:
            return query_params['cat'][0]
        return None
    except:
        return None

filtered_df['category_from_url'] = filtered_df['url'].apply(extract_category_param)



filtered_df.to_sql(
    name='search_logs',       
    con=engine,
    if_exists='replace',      
    index=False
)

print("✅ Data successfully pushed to PostgreSQL!")