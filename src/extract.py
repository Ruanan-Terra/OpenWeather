import requests
import os
import json
from dotenv import load_dotenv
import pandas as pd
from azure.storage.blob import BlobServiceClient


class WeatherExtractor:
    def __init__(self, api_key, azure_connection):
        self.api_key = api_key
        self.azure_connection = azure_connection

        self.citys = ["Porto Alegre", "Sao Paulo", "Curitiba", "Rio de Janeiro", "Florianopolis", "Guaiba"]
    
        self.lat_lon_of_cit = []
        self.final_datas = []

    def conect_azure_lake(self):
        # Azure Data Lake
        try:
            blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION)
            self.container_client = blob_service_client.get_container_client("datalake")
            print("Successfully connected to Azure!")
        except Exception as e:
            print(f"Error connecting to Azure. Check your connection string in .env. Details: {e}")
            exit()

    def get_lon_lat_of_cit(self):
        for city_loc in self.citys:
            url_lon_lat = f"http://api.openweathermap.org/geo/1.0/direct?q={city_loc}&limit={2}&appid={API_KEY}"
            response_cit = requests.get(url_lon_lat)

            values_temp = {"City" : city_loc,
                        "lat" : response_cit.json()[0]['lat'],
                        "lon" : response_cit.json()[0]['lon']}
            self.lat_lon_of_cit.append(values_temp)

    def send_data_to_Lake(self):
        for city in self.lat_lon_of_cit:
            url = f"https://api.openweathermap.org/data/2.5/weather?lat={city['lat']}&lon={city['lon']}&appid={API_KEY}&units=metric&lang=pt_br"

            # Place the order
            response = requests.get(url)

            if response.status_code == 200:
                data = response.json()
                print(f"Sucesso! Dados recebidos: {city['City']}")
                
                # START OF INTEGRATION WITH AZURE (BRONZE LEVEL)
                try:
                    # About JSON
                    json_data = json.dumps(data, ensure_ascii=False, indent=4)
                    
                    # Remove spaces from the city name to avoid errors in the URL
                    nome_arquivo = city["City"].replace(' ', '_')
                    caminho_no_azure = f"bronze/weather_{nome_arquivo}.json"
                    
                    # Upload to Data Lake Gen2
                    blob_client = self.container_client.get_blob_client(caminho_no_azure)
                    blob_client.upload_blob(json_data, overwrite=True)
                    print(f"Save to Azure Data Lake: {caminho_no_azure}")
                except Exception as e:
                    print(f"Error saving to Azure: {e}")

                # Simulated Silver layer in Pandas
                self.final_datas.append(pd.DataFrame({"ID": [data["id"]],
                                                "city": [city["City"]],
                                                "lat": [data["coord"]["lat"]],
                                                "lon": [data["coord"]["lon"]],
                                                "weather_main": [data["weather"][0]["main"]],
                                                "weather_description": [data["weather"][0]["description"]],
                                                "weather_icon": [data["weather"][0]["icon"]],
                                                "temperature perception": [data["main"]["feels_like"]],
                                                "minimum temperature": [data["main"]["temp_min"]],
                                                "maximum temperature": [data["main"]["temp_max"]],
                                                "humidity": [data["main"]["humidity"]],
                                                "sea_level": [data.get("main", {}).get("sea_level", "N/A")], # Using .get() to avoid an error if sea_level is not present
                                                "visibility": [data["visibility"]],
                                                "wind_speed": [data["wind"]["speed"]],
                                                "wind_deg": [data["wind"]["deg"]],
                                                "clouds": [data["clouds"]["all"]],
                                                "dataTime": [data["dt"]],
                                                "country": [data["sys"]["country"]],
                                                "sunrise": [data["sys"]["sunrise"]],
                                                "sunset": [data["sys"]["sunset"]]}))
            else:
                print(f"Erro: {response.status_code}")
                print(response.text)

if __name__ == "__main__":
    # Load the variables from the .env file
    load_dotenv()
    API_KEY = os.getenv("OPENWEATHER_API_KEY")
    AZURE_CONNECTION = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    self = WeatherExtractor(API_KEY, AZURE_CONNECTION)
    self.conect_azure_lake()
    self.get_lon_lat_of_cit()
    self.send_data_to_Lake()

