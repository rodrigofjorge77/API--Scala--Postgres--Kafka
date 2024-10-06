import json
import matplotlib.pyplot as plt
from confluent_kafka import Consumer, KafkaException, KafkaError
from collections import defaultdict
import time

# Configurações do consumidor Kafka
conf = {
    'bootstrap.servers': '172.27.213.163:9092',  # Endereço do servidor Kafka
    'group.id': 'meu-grupo-de-consumidores',  # ID do grupo de consumidores
    'auto.offset.reset': 'earliest'  # Começa a leitura do início se não tiver commit de offset
}

# Criar o consumidor Kafka
consumer = Consumer(conf)

# Assinar o tópico Kafka
topic = 'testKafka'
consumer.subscribe([topic])

# Dicionário para acumular valores por item
acumulado_por_item = defaultdict(float)

# Função para atualizar o gráfico
def update_graph():
    plt.clf()  # Limpa o gráfico anterior
    items = list(acumulado_por_item.keys())
    valores_acumulados = list(acumulado_por_item.values())

    plt.barh(items, valores_acumulados, color='blue')
    plt.xlabel('Valores Acumulados')
    plt.ylabel('Itens')
    plt.title('Gráfico de Barras Horizontais com Dados do Kafka')
    plt.tight_layout()
    plt.pause(0.1)  # Pausa para permitir renderização do gráfico

# Loop para consumir as mensagens Kafka e atualizar o gráfico
plt.ion()  # Habilita o modo interativo do Matplotlib

try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Espera por novas mensagens

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())

        # Decodifica a mensagem JSON
        mensagem = json.loads(msg.value().decode('utf-8'))

        # Obtém os campos 'item' e 'value'
        item = mensagem.get('item')
        value = mensagem.get('value')

        # Acumula o valor para o respectivo item
        if item and value:
            acumulado_por_item[item] += float(value)  # Converte o valor para float e acumula

        # Atualiza o gráfico a cada 10 segundos
        #time.sleep(10)
        update_graph()

except KeyboardInterrupt:
    print("Encerrando consumidor...")

finally:
    consumer.close()  # Fechar o consumidor Kafka
    plt.ioff()  # Desativar o modo interativo do Matplotlib
    plt.show()  # Manter o gráfico aberto após o término
