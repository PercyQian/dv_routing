#!/usr/bin/env python3
import socket
import threading

# read the topology from the file
def load_topology(filename):
    topo = {}
    with open(filename) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'): continue
            parts = line.split()
            router = parts[0]
            neighs = {}
            for seg in parts[1].split(';'):
                nb, cost = seg.split(',')
                neighs[nb] = int(cost)
            topo[router] = neighs
    return topo

# the thread for each client connection
def handle_client(conn, addr, topo, client_table, lock):
    rid = None
    try:
        data = conn.recv(1024).decode().strip()
        # JOIN message format: JOIN <RouterID>
        if data.startswith('JOIN'):
            _, rid = data.split()
            with lock:
                client_table[rid] = conn
            # return the neighbors list
            neighs = topo[rid]
            # MESSAGE format: RESPONSE <RouterID> nb1,cost1;nb2,cost2;...
            msg = 'RESPONSE {} {}'.format(
                rid,
                ';'.join(f"{nb},{c}" for nb,c in neighs.items())
            )
            conn.sendall(msg.encode())
        # loop forwarding UPDATE
        while True:
            data = conn.recv(4096).decode().strip()
            if not data: break
            # UPDATE <source> <dest1,c1;dest2,c2;...>
            if data.startswith('UPDATE'):
                _, src, body = data.split(maxsplit=2)
                # find the direct neighbors of src
                for nb, cost in topo[src].items():
                    if cost >= 0 and nb in client_table:
                        try:
                            client_table[nb].sendall(data.encode())
                        except (BrokenPipeError, ConnectionResetError, OSError) as e:
                            print(f"send to {nb} failed: {e}")
                            # remove the disconnected connection
                            with lock:
                                if nb in client_table:
                                    del client_table[nb]
    except Exception as e:
        print(f"error handling client {addr}: {e}")
    finally:
        # ensure the connection is closed and the client is removed from the table
        if rid and rid in client_table:
            with lock:
                del client_table[rid]
        conn.close()

def main():
    topo = load_topology('config.txt')
    client_table = {}  # RouterID -> conn
    lock = threading.Lock()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('0.0.0.0', 5555))
    sock.listen()
    print("Server listening on port 5555...")

    while True:
        conn, addr = sock.accept()
        threading.Thread(
            target=handle_client,
            args=(conn, addr, topo, client_table, lock),
            daemon=True
        ).start()

if __name__ == '__main__':
    main()
