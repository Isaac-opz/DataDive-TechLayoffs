import pandas as pd
from sqlalchemy import create_engine

# Excel file
data = pd.read_excel("tech_layoffs.xlsx")

# Renames DataFrame columns to match SQL table column names
data = data.rename(
    columns={
        "#": "id",
        "Company": "company",
        "Location_HQ": "location_hq",
        "Country": "country",
        "Continent": "continent",
        "Laid_Off": "laid_off",
        "Date_layoffs": "date_layoffs",
        "Percentage": "percentage",
        "Company_Size_before_Layoffs": "company_size_before_layoffs",
        "Company_Size_after_layoffs": "company_size_after_layoffs",
        "Industry": "industry",
        "Stage": "stage",
        "Money_Raised_in_$_mil": "money_raised_in_mil",
        "Year": "year",
        "lat": "latitude",
        "lng": "longitude",
    }
)

# PostgreSQL connection parameters
db_username = "postgres"
db_password = "datadive"
db_host = "localhost"
db_port = "5432"
db_name = "tech_layoffs_db"

# SQLAlchemy engine
engine = create_engine(
    f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
)

# Inserts data into the table
data.to_sql("tech_layoffs", engine, if_exists="append", index=False)
