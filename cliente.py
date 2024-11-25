# cliente.py

import socket
import json
import random
import struct
import time
import sys
from config import CONFIG_PARAMS

WORKER_0_ADDRESS = (CONFIG_PARAMS['WORKER_0_IP'], CONFIG_PARAMS['WORKER_0_PORT'])

def send_data(sock, message):
    serialized_msg = json.dumps(message).encode('utf-8')
    message_size = len(serialized_msg)
    sock.sendall(struct.pack('!I', message_size) + serialized_msg)

def receive_data(sock):
    size_header = sock.recv(4)
    if not size_header:
        raise ConnectionError("Conexión cerrada antes de recibir el tamaño del mensaje.")
    total_size = struct.unpack('!I', size_header)[0]
    received_data = b""
    while len(received_data) < total_size:
        chunk = sock.recv(min(4096, total_size - len(received_data)))
        if not chunk:
            raise ConnectionError("Conexión cerrada antes de recibir todo el mensaje.")
        received_data += chunk
    return json.loads(received_data.decode('utf-8'))

def load_vector_from_file(file_path):
    try:
        with open(file_path, "r") as file:
            content = file.read().strip()
            if "," in content:
                return list(map(int, content.split(",")))
            else:
                return list(map(int, content.split()))
    except Exception as e:
        print(f"Error al leer el archivo: {e}")
        return []

def save_vector_to_file(vector, filename):
    try:
        with open(filename, "w") as file:
            file.write(" ".join(map(str, vector)))
        print(f"Vector ordenado guardado en el archivo '{filename}'.")
    except Exception as e:
        print(f"Error al guardar el archivo: {e}")

def main():
    print("="*60)
    print(" " * 20 + "Cliente de Ordenamiento")
    print("="*60)
    choice = input("¿Quieres usar un archivo para cargar el vector (F) o generar uno aleatorio (A)? ").strip().upper()
    if choice == "F":
        file_name = input("Ingresa el nombre del archivo (ej., vector.txt): ").strip()
        vector = load_vector_from_file(file_name)
        if not vector:
            print("No se pudo cargar el vector. Terminando programa.")
            return
    elif choice == "A":
        try:
            size = int(input("Ingresa el tamaño del vector aleatorio: "))
            vector = [random.randint(1, 1000000) for _ in range(size)]
        except ValueError:
            print("Entrada inválida. Terminando programa.")
            return
    else:
        print("Opción no válida. Saliendo del programa.")
        return

    try:
        timeout = float(input("Tiempo límite para cada worker (en segundos): "))
    except ValueError:
        print("Entrada inválida. Terminando programa.")
        return

    print("\nSelecciona el algoritmo de ordenamiento:")
    print("1. Merge Sort")
    print("2. Heap Sort")
    print("3. Quick Sort")
    algorithm = input("Elige una opción (1, 2 o 3): ").strip()
    if algorithm not in ['1', '2', '3']:
        print("Algoritmo no válido. Terminando programa.")
        return

    task = {
        "data": vector,
        "time_limit": timeout,
        "sort_algorithm": algorithm,
        "state": None,
        "history": []
    }

    try:
        with socket.create_connection(WORKER_0_ADDRESS, timeout=300) as worker_socket:
            print(f"\nConectando con Worker 0 en {WORKER_0_ADDRESS}...")
            send_data(worker_socket, task)
            print("Tarea enviada exitosamente a Worker 0.")

            while True:
                    response = receive_data(worker_socket)
                    if "sorted_data" in response:
                        print("\nVector ordenado recibido.")
                        print(f"El ordenamiento fue completado por {response['completed_by']}.")
                        print("Detalle de tiempos:")
                        for entry in response.get("history", []):
                            print(f"- {entry['worker']}: {entry['time']:.4f} segundos.")

                        # Mostrar el nombre del archivo donde se guardó el vector
                        if "filename" in response:
                            print(f"El vector ordenado se ha guardado en el archivo: {response['filename']}")
                        else:
                            print("No se proporcionó el nombre del archivo del vector ordenado.")

                        break
    except Exception as e:
        print(f"Error al comunicarse con Worker 0: {e}")

if __name__ == "__main__":
    main()








