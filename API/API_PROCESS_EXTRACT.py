import requests
import yfinance as yf
import pandas as pd

api_url = "https://financialmodelingprep.com/api/v3/available-traded/list?apikey=7lZIU82kV1JVXJZP2zHFFlJLvdvsswj0"
response = requests.get(api_url)

if response.status_code == 200:
    stock_data = response.json()
    df = pd.DataFrame(stock_data)
    df = df[['symbol', 'name', 'price', 'exchange', 'exchangeShortName']]
else:
    print("Error en la solicitud:", response.status_code)
    raise Exception("No se pudo obtener los datos de la API.")


def get_financial_info(symbol, index, total):
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        print(f"Procesando {index + 1}/{total}: {symbol}")
        return {
            'Industry': info.get('industry', 'Industria no disponible'),
            'Profit Margins': info.get('profitMargins', 'No disponible'),
            'PE Ratio': info.get('trailingPE', 'No disponible'),
            'Country': info.get('country', 'País no disponible'),
            'Full Time Employees': info.get('fullTimeEmployees', 'No disponible'),
            'City': info.get('city', 'Ciudad no disponible')
        }
    except Exception as e:
        print(f"Error al obtener la información de {symbol}: {e}")
        return {
            'Industry': 'Error',
            'Profit Margins': 'Error',
            'PE Ratio': 'Error',
            'Country': 'Error',
            'Full Time Employees': 'Error',
            'City': 'Error'
        }


info_list = []
total_symbols = len(df)
for index, symbol in enumerate(df['symbol']):
    info = get_financial_info(symbol, index, total_symbols)
    info_list.append(info)

df_info = pd.DataFrame(info_list)

df_final = pd.concat([df, df_info], axis=1)

print(df_final.head())

