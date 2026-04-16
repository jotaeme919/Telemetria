import paho.mqtt.client as mqtt
import datetime
import time
import json
import random

MQTT = {
    "HOST": "jaragua.lmq.cloudamqp.com",
    "PORT": 1883,
    "KEEPALIVE": 60,
    "TOPIC": "dadosSensor",
    "CLIENT_ID": "django-mqtt-worker",
    "USERNAME": "xeqynmrk:xeqynmrk",
    "PASSWORD": "gqQstCMuzJLNxSePi4VOZopRaByzZz49",
}

# Callback de conexão
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Conectado ao MQTT com sucesso!")
    else:
        print(f"❌ Erro na conexão: {rc}")

# Criar cliente MQTT
client = mqtt.Client(client_id=MQTT["CLIENT_ID"])

# Configurar autenticação
client.username_pw_set(MQTT["USERNAME"], MQTT["PASSWORD"])

# Eventos
client.on_connect = on_connect

# Conectar ao broker
client.connect(MQTT["HOST"], MQTT["PORT"], MQTT["KEEPALIVE"])

# Iniciar loop em background
client.loop_start()

try:
    while True:
        # Quantidade de medições enviadas em uma única mensagem
        quantidade_registros = 3

        # Criar vetor (lista) de JSONs
        payload_lista = []
        for _ in range(quantidade_registros):
            payload = {
                "valor": round(random.uniform(20, 35), 2),
                "medicaoid": random.randint(1, 3),
                "veiculoid": random.randint(1, 2),
                "data": datetime.datetime.now().isoformat()
            }
            payload_lista.append(payload)

        # Converter lista para JSON
        mensagem = json.dumps(payload_lista)

        # Publicar mensagem
        client.publish(MQTT["TOPIC"], mensagem)

        print(f"📤 Enviado: {mensagem}")

        time.sleep(5)

except KeyboardInterrupt:
    print("Encerrando...")

finally:
    client.loop_stop()
    client.disconnect()