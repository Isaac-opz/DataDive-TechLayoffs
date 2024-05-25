import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load env variables
load_dotenv()

db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

Tech_Layoffs = pd.read_excel("Data/RAW_tech_layoffs.xlsx")

# delete '$' and ',' from Money_Raised_in_$_mil column
Tech_Layoffs["Money_Raised_in_$_mil"] = Tech_Layoffs["Money_Raised_in_$_mil"].replace(
    {"\$": "", ",": ""}, regex=True
)
Tech_Layoffs["Money_Raised_in_$_mil"] = pd.to_numeric(
    Tech_Layoffs["Money_Raised_in_$_mil"], errors="coerce"
)

Tech_Layoffs = Tech_Layoffs.rename(
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

# create connection to the db
Tech_Layoffs_connection = create_engine(
    f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
)

# export the dataframe to the db
Tech_Layoffs.to_sql(
    "tech_layoffs", Tech_Layoffs_connection, if_exists="replace", index=False
)
