from kafka import KafkaProducer
from time import sleep
import requests
import json

# Endpoint de l'API Alpha Vantage
url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo'

# Serveurs Kafka bootstrap
bootstrap_servers = ['localhost:9092']

# Topic Kafka pour produire des données
topic = 'data-stream'

# Création d'un producer Kafka
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda m: json.dumps(m).encode('utf-8'))

while True:
    # Make a request to the Alpha Vantage API
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        
        # Vérifier si la clé 'Time Series (5min)' existe dans les données de réponse
        if 'Time Series (5min)' in data:
            time_series = data["Time Series (5min)"]
            
            for timestamp, values in time_series.items():
                # Extraire les valeurs requises
                #HINT : Aller sur l'url "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo" et examiner les champs des données qu'on veut extraire
                open_price = values['1. open']
                high_price = values['2. high']
                low_price = values["3. low"]
                close_price = values["4. close"]
                volume = values["5. volume"]
                
                
                # Créer un dictionnaire avec les valeurs extraites
                price_data = {'timestamp' : timestamp,
                     'open_price': open_price,
            'high_price': high_price,
            'low_price': low_price,
            'close_price': close_price,
            'volume': volume
                }
                
                # Convertir le dictionnaire en JSON et l'envoyer à Kafka
                producer.send(topic, value=price_data)
                
                print("Price data sent to Kafka")
        
    sleep(60)  # Attendre 60 secondes avant de faire la prochaine requête à l'API
