import requests
import os
import json
from dotenv import load_dotenv
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
            else:
                print(f"Erro: {response.status_code}")
                print(response.text)

if __name__ == "__main__":
    # Load the variables from the .env file
    load_dotenv()
    API_KEY = os.getenv("OPENWEATHER_API_KEY")
    AZURE_CONNECTION = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    project = WeatherExtractor(API_KEY, AZURE_CONNECTION)
    project.conect_azure_lake()
    project.get_lon_lat_of_cit()
    project.send_data_to_Lake()

