import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def find_mapped_industry(industry, mapping):
    for key, keywords in mapping.items():
        if any(keyword in industry.lower() for keyword in keywords):
            return key
    return 'Other'

def merge_datasets(api_data, layoffs_data):
    """Merge API data and layoffs data."""
    # Keywords for mapping industries more flexibly
    keywords_mapping = {
        'Consumer': ['consumer', 'retail', 'apparel', 'food', 'beverage'],
        'Finance': ['financial', 'bank', 'mortgage', 'capital', 'credit'],
        'HR': ['employment', 'staffing', 'hr', 'human resources'],
        'Energy': ['energy', 'oil', 'gas', 'solar', 'utility', 'utilities'],
        'Healthcare': ['health', 'biotech', 'medical', 'pharma', 'drug'],
        'Retail': ['retail', 'store', 'shop', 'market'],
        'Real Estate': ['real estate', 'reit']
    }

    # Apply keyword-based mapping to both datasets
    layoffs_data['mapped_industry'] = layoffs_data['industry'].apply(lambda x: find_mapped_industry(x, keywords_mapping))
    api_data['mapped_industry'] = api_data['Industry'].apply(lambda x: find_mapped_industry(x, keywords_mapping))

    # Ensure the columns used for aggregation are numeric
    api_data['Profit Margins'] = pd.to_numeric(api_data['Profit Margins'], errors='coerce')
    api_data['PE Ratio'] = pd.to_numeric(api_data['PE Ratio'], errors='coerce')

    # Calculate average financial metrics per industry in the API dataset
    industry_metrics = api_data.groupby('mapped_industry').agg({
        'Profit Margins': 'mean',
        'PE Ratio': lambda x: np.nanmean(np.where(x != np.inf, x, np.nan))
    }).rename(columns={'Profit Margins': 'profit_margins', 'PE Ratio': 'pe_ratio'}).reset_index()

    # Perform the merge
    merged_data = pd.merge(
        layoffs_data,
        industry_metrics,
        on='mapped_industry',
        how='left'
    )

    # Remove the 'id' column if it exists
    if 'id' in merged_data.columns:
        merged_data.drop('id', axis=1, inplace=True)

    # Logging the merge operation
    logging.info("Data merged successfully.")

    csv_filename = 'merged_data.csv'
    merged_data.to_csv(csv_filename, index=False)
    logging.info(f"Data exported successfully to {csv_filename}.")
    
    return merged_data

def merge_task(**kwargs):
    ti = kwargs['ti']
    api_data = ti.xcom_pull(task_ids='clean_api_data')
    layoffs_data = ti.xcom_pull(task_ids='clean_dataset_data')
    merged_data = merge_datasets(api_data, layoffs_data)
    
    # Push the merged data to XCom for further use
    ti.xcom_push(key='merged_data', value=merged_data)
    
    return merged_data

