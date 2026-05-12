# Importar librerías necesarias
import time        # Para controlar el tiempo de espera entre mensajes
import random      # Para elegir mensajes al azar
from kafka import KafkaProducer  # Cliente productor de Kafka

# Intentar conectarse al broker de Kafka
try:
    # Crear el productor apuntando al contenedor kafka en la red Docker
    producer = KafkaProducer(bootstrap_servers='kafka:9092')
    print("Conexión con Kafka exitosa")
except Exception as e:
    # Si la conexión falla, mostrar el error y terminar el programa
    print(f"Error al conectar con kafka: {e}")
    exit()

# Lista de mensajes que se enviarán al topic
frases = [
    "Proyecto Final 1",
    "Proyecto Final 2",
    "Proyecto Final 3",
    "Proyecto Final 4",
    "Proyecto Final 5",
]

print("Enviando mensajes a Kafka... Presiona Ctrl+C en el terminal para detener.")

try:
    # Bucle infinito que envía mensajes continuamente
    while True:
        # Elegir un mensaje al azar de la lista
        mensaje = random.choice(frases)
        
        # Enviar el mensaje al topic 'actividad-topic'
        # encode('utf-8') convierte el texto a bytes, formato requerido por Kafka
        producer.send('actividad-topic', mensaje.encode('utf-8'))
        
        # Mostrar en consola el mensaje enviado
        print(f"Mensaje enviado: '{mensaje}'")
        
        # Esperar entre 1 y 3 segundos antes de enviar el siguiente mensaje
        # Simula un flujo de datos en tiempo real
        time.sleep(random.uniform(1,3))

except KeyboardInterrupt:
    # Cuando el usuario presiona Ctrl+C, detener el bucle limpiamente
    print("\nProductor detenido por el usuario.")

finally:
    # Bloque que siempre se ejecuta al terminar, con o sin error
    print("Cerrando productor")
    producer.flush()   # Asegura que todos los mensajes pendientes se envíen
    producer.close()   # Cierra la conexión con Kafka limpiamente