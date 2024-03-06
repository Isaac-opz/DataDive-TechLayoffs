from sqlalchemy import create_engine, exc, text
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

load_dotenv()

db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

Tech_Layoffs_connection = create_engine(
    f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
)

# creates a session (connection to the db)
Session = sessionmaker(bind=Tech_Layoffs_connection)

try:
    session = Session()
    print("Conectado con la base de datos")

    # delete the rows with 'Unknown' values in the stage column
    Stage_delete_unkown_query = text(
        "DELETE FROM tech_layoffs WHERE stage = 'Unknown';"
    )
    session.execute(Stage_delete_unkown_query)
    session.commit()
    print("Se eliminaron los registros con valor 'Unknown' correctamente")

except exc.SQLAlchemyError as e:
    print(f"Error de conexión: {e}")
finally:
    session.close()
