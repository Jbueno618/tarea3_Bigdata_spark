from kafka import KafkaProducer
import time
import csv

producer = KafkaProducer(bootstrap_servers='localhost:9092')

with open('dataset_cosntruccion.csv', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)

    for row in reader:
        try:
            municipio = row['MUN-COMERCIAL']
            estado = row['EST-MATRICULA']

            if municipio and estado:
                mensaje = f"{municipio},{estado}"
                producer.send('sensor_data', mensaje.encode('utf-8'))
                print("Enviado:", mensaje)
                time.sleep(1)

        except:
            pass
