# worker_1.py

import socket
import struct
import json
import threading
import time
from config import CONFIG_PARAMS

WORKER_1_ADDRESS = (CONFIG_PARAMS['WORKER_1_IP'], CONFIG_PARAMS['WORKER_1_PORT'])
WORKER_0_ADDRESS = (CONFIG_PARAMS['WORKER_0_IP'], CONFIG_PARAMS['WORKER_0_PORT'])

def send_data(sock, message):
    serialized_msg = json.dumps(message).encode('utf-8')
    # Enviar el tamaño y luego los datos
    total_sent = 0
    message_size = len(serialized_msg)
    sock.sendall(struct.pack('!I', message_size))
    while total_sent < message_size:
        sent = sock.send(serialized_msg[total_sent:])
        if sent == 0:
            raise RuntimeError("Conexión rota")
        total_sent += sent

def receive_data(sock):
    # Recibir el tamaño del mensaje
    size_header = b''
    while len(size_header) < 4:
        chunk = sock.recv(4 - len(size_header))
        if not chunk:
            raise ConnectionError("Conexión cerrada antes de recibir el tamaño del mensaje.")
        size_header += chunk

    total_size = struct.unpack('!I', size_header)[0]
    received_data = b''
    while len(received_data) < total_size:
        chunk = sock.recv(min(4096, total_size - len(received_data)))
        if not chunk:
            raise ConnectionError("Conexión cerrada antes de recibir todo el mensaje.")
        received_data += chunk
    return json.loads(received_data.decode('utf-8'))

# Implementación de los algoritmos con pausa y reanudación

def merge_sort(arr, state, start_time, time_limit):
    if state is None:
        state = {'size': 1, 'left': 0}

    n = len(arr)
    size = state['size']

    while size < n:
        left = state.get('left', 0)
        while left < n:
            if time.time() - start_time >= time_limit:
                state['left'] = left
                state['size'] = size
                return False, state
            mid = min(left + size - 1, n - 1)
            right = min((left + 2 * size - 1), n - 1)
            merge(arr, left, mid, right)
            left += 2 * size
        size *= 2
        state['size'] = size
        state['left'] = 0
    return True, None

def merge(arr, l, m, r):
    n1 = m - l + 1
    n2 = r - m
    L = arr[l:m+1]
    R = arr[m+1:r+1]
    i = j = 0
    k = l
    while i < n1 and j < n2:
        if L[i] <= R[j]:
            arr[k] = L[i]
            i +=1
        else:
            arr[k] = R[j]
            j +=1
        k +=1
    while i < n1:
        arr[k] = L[i]
        i +=1
        k +=1
    while j < n2:
        arr[k] = R[j]
        j +=1
        k +=1

def heap_sort(arr, state, start_time, time_limit):
    n = len(arr)
    if state is None:
        state = {'phase': 'build_heap', 'i': n // 2 -1}
    if state['phase'] == 'build_heap':
        i = state['i']
        while i >=0:
            if time.time() - start_time >= time_limit:
                state['i'] = i
                return False, state
            heapify(arr, n, i)
            i -=1
        state['phase'] = 'extract_elements'
        state['i'] = n -1
    if state['phase'] == 'extract_elements':
        i = state['i']
        while i >=0:
            if time.time() - start_time >= time_limit:
                state['i'] = i
                return False, state
            arr[0], arr[i] = arr[i], arr[0]
            heapify(arr, i, 0)
            i -=1
    return True, None

def heapify(arr, n, i):
    largest = i
    l = 2 * i +1
    r = 2 * i +2
    if l < n and arr[l] > arr[largest]:
        largest = l
    if r < n and arr[r] > arr[largest]:
        largest = r
    if largest != i:
        arr[i], arr[largest] = arr[largest], arr[i]
        heapify(arr, n, largest)

def quick_sort(arr, state, start_time, time_limit):
    if state is None:
        state = {'stack': [(0, len(arr) -1)]}
    stack = state['stack']
    while stack:
        if time.time() - start_time >= time_limit:
            state['stack'] = stack
            return False, state
        low, high = stack.pop()
        if low < high:
            pi = partition(arr, low, high)
            stack.append((low, pi -1))
            stack.append((pi +1, high))
    return True, None

def partition(arr, low, high):
    pivot = arr[high]
    i = low -1
    for j in range(low, high):
        if arr[j] <= pivot:
            i +=1
            arr[i], arr[j] = arr[j], arr[i]
    arr[i +1], arr[high] = arr[high], arr[i +1]
    return i +1

def process_task(task, start_time, time_limit):
    array = task["data"]
    algorithm = task["sort_algorithm"]
    state = task.get("state")
    completed = False

    if algorithm == "1":  # Merge Sort
        completed, state = merge_sort(array, state, start_time, time_limit)
    elif algorithm == "2":  # Heap Sort
        completed, state = heap_sort(array, state, start_time, time_limit)
    elif algorithm == "3":  # Quick Sort
        completed, state = quick_sort(array, state, start_time, time_limit)

    return completed, state

def forward_to_worker(task, target_ip, target_port):
    try:
        with socket.create_connection((target_ip, target_port)) as worker_socket:
            send_data(worker_socket, task)
            response = receive_data(worker_socket)
            return response
    except Exception as ex:
        print(f"Error al reenviar la tarea al worker {target_ip}:{target_port} - {ex}")
        return {"error": f"No se pudo reenviar la tarea al worker {target_ip}:{target_port}"}

def save_vector_to_file(vector, filename):
    try:
        with open(filename, "w") as file:
            file.write(" ".join(map(str, vector)))
        print(f"Vector ordenado guardado en el archivo '{filename}'.")
    except Exception as e:
        print(f"Error al guardar el archivo: {e}")

def handle_task(task):
    start_time = time.time()
    time_limit = task["time_limit"]
    history = task.get("history", [])

    print(f"Worker 1 - Procesando tarea con tiempo límite de {time_limit} segundos.")

    completed, state = process_task(task, start_time, time_limit)
    elapsed_time = time.time() - start_time

    history.append({"worker": "Worker 1", "time": elapsed_time})

    if completed:
        print(f"Worker 1 - Ordenamiento completado en {elapsed_time:.4f} segundos.")

        # Guardar el vector ordenado en un archivo
        output_filename = "vector_ordenado_Worker_1.txt"
        save_vector_to_file(task["data"], output_filename)

        result = {
            "sorted_data": None,  # No enviamos el vector completo para evitar problemas
            "completed_by": "Worker 1",
            "history": history,
            "filename": output_filename
        }
        return result
    else:
        print(f"Worker 1 - Tiempo límite excedido después de {elapsed_time:.4f} segundos. Delegando tarea a Worker 0.")
        task["state"] = state
        task["history"] = history
        # Reenviar la tarea al worker_0
        response = forward_to_worker(task, WORKER_0_ADDRESS[0], WORKER_0_ADDRESS[1])
        return response

def handle_client(client_sock):
    try:
        task = receive_data(client_sock)
        result = handle_task(task)
        send_data(client_sock, result)
    except Exception as e:
        print(f"Error al manejar la tarea: {e}")
        try:
            send_data(client_sock, {"error": f"Error en Worker 1: {e}"})
        except:
            pass
    finally:
        client_sock.close()

def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(WORKER_1_ADDRESS)
    server_socket.listen(5)
    print(f"Worker 1 escuchando en {WORKER_1_ADDRESS}...")

    while True:
        client_sock, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(client_sock,)).start()

if __name__ == "__main__":
    main()










