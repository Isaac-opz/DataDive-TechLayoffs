import requests
from kafka import KafkaConsumer
import json

# --- Logica consumer ----
consumer = KafkaConsumer(
    'kafka-tech-layoffs',  
    bootstrap_servers=['localhost:9092'],  
    auto_offset_reset='earliest',  
    enable_auto_commit=True, 
    group_id='my-group',  
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  
)

# URL Api DE power bi
api_endpoint = 'https://api.powerbi.com/beta/693cbea0-4ef9-4254-8977-76e05cb5f556/datasets/d94c6c9d-6e04-4438-b984-853e2e2b87e1/rows?experience=power-bi&key=1bezNXjPCwvEyLfl3jNa5zWPZMPxyUA5X9RywwoXxWVBkuH7QDyQZjJs9DxBKZS526XO9vcIHD4KBKpwhgTLdg%3D%3D'


for message in consumer:
    message_value = message.value
    print(f"Dato recibido: {message.value}")
    
    data = {
        "company_id": message_value["company_id"],
        "company": message_value["company"],
        "industry": message_value["industry"],
        "laid_off": message_value["laid_off"],
        "company_size_before_layoffs": message_value["company_size_before_layoffs"],
        "company_size_after_layoffs": message_value["company_size_after_layoffs"],
        "country": message_value["country"],
        "year": message_value["year"]
    }
    
    json_data = json.dumps([data])
    
   
    headers = {'Content-Type': 'application/json'}
    
    response = requests.post(api_endpoint, data=json_data, headers=headers)
    
    if response.status_code == 200:
        print("Datos enviados correctamente al Dashboard en tiempo real")
    else:
        print("Error:", response.text)
