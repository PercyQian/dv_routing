#!/usr/bin/env python3
import socket
import threading
import time

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
def handle_client(conn, addr, topo, client_table, lock, router_count, router_ready):
    rid = None
    try:
        data = conn.recv(1024).decode().strip()
        # JOIN message format: JOIN <RouterID>
        if data.startswith('JOIN'):
            _, rid = data.split()
            print(f"Received JOIN from router {rid}")
            
            # check if the router is already registered
            with lock:
                if rid in client_table:
                    print(f"Router {rid} already registered, closing duplicate connection")
                    return
                
                # record the connection
                client_table[rid] = conn
                print(f"Connected routers: {list(client_table.keys())}")
                
                if len(client_table) > router_count:
                    print(f"Warning: More routers ({len(client_table)}) than expected ({router_count})")
            
            # send the RESPONSE to the current router to ensure it knows its neighbors
            neighs = topo.get(rid, {})
            msg = 'RESPONSE {} {}'.format(
                rid,
                ';'.join(f"{nb},{c}" for nb,c in neighs.items())
            )
            print(f"Sending to {rid}: {msg}")
            conn.sendall((msg + '\n').encode())
            
            # check if it's the last router, if so, send START to everyone
            with lock:
                if len(client_table) >= router_count and not router_ready.is_set():
                    print(f"All {len(client_table)} routers connected. Setting ready event.")
                    router_ready.set()
                    # notify all clients to start DV algorithm
                    for router_id, router_conn in client_table.items():
                        try:
                            router_conn.sendall(f"START\n".encode())
                            print(f"Sent START to {router_id}")
                        except Exception as e:
                            print(f"Failed to send START to {router_id}: {e}")
            
            # return the neighbors list
            neighs = topo.get(rid, {})  # use get to avoid KeyError
            if not neighs:
                print(f"Warning: Router {rid} has no neighbors in topology!")
            
            # MESSAGE format: RESPONSE <RouterID> nb1,cost1;nb2,cost2;...
            msg = 'RESPONSE {} {}'.format(
                rid,
                ';'.join(f"{nb},{c}" for nb,c in neighs.items())
            )
            print(f"Sending to {rid}: {msg}")
            conn.sendall((msg + '\n').encode())
            
            # then wait for all routers to connect
            if not router_ready.is_set():
                print(f"Router {rid} waiting for all routers to connect...")
                router_ready.wait()
                print(f"Router {rid} continuing after all routers connected.")
            
        # loop forwarding UPDATE and handling EXIT
        while True:
            data = conn.recv(4096).decode().strip()
            if not data: break
            
            if data.startswith("EXIT"):
                _, exit_rid = data.split()
                print(f"Router {exit_rid} exiting")
                # 不需要在这里移除，会在finally里处理
                break
                
            # UPDATE <source> <dest1,c1;dest2,c2;...>
            if data.startswith('UPDATE'):
                _, src, body = data.split(maxsplit=2)
                # only forward to neighbors that can communicate
                for nb, cost in topo[src].items():
                    if cost >= 0 and nb in client_table:  # ensure only forwarding to routers with cost >= 0
                        try:
                            client_table[nb].sendall((data + '\n').encode())
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
                if len(client_table) == 0:
                    print("All routers have exited. Server shutting down.")
                    # can add server shutdown logic here
        conn.close()

def main():
    topo = load_topology('config.txt')
    client_table = {}  # RouterID -> conn
    lock = threading.Lock()
    
    # print the loaded topology information
    print(f"Loaded topology with {len(topo)} routers:")
    for router, neighbors in topo.items():
        print(f"  Router {router}: {neighbors}")
    
    # Count the expected number of routers from topology
    router_count = len(topo)
    print(f"Expecting {router_count} routers to connect")
    
    # Event to signal when all routers are connected
    router_ready = threading.Event()
    
    # add timeout mechanism, if 60 seconds pass without receiving all routers, start the algorithm
    def timeout_handler():
        time.sleep(60)  # wait 60 seconds
        if not router_ready.is_set():
            print("Timeout waiting for all routers. Starting with connected routers.")
            router_ready.set()
            # notify the connected clients to start
            with lock:
                for router_id, router_conn in client_table.items():
                    try:
                        router_conn.sendall(f"START\n".encode())
                    except Exception as e:
                        print(f"Failed to send START to {router_id}: {e}")
    
    # start the timeout thread
    threading.Thread(target=timeout_handler, daemon=True).start()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('0.0.0.0', 5555))
    sock.listen()
    print("Server listening on port 5555...")

    try:
        while True:
            conn, addr = sock.accept()
            threading.Thread(
                target=handle_client,
                args=(conn, addr, topo, client_table, lock, router_count, router_ready),
                daemon=True
            ).start()
    except KeyboardInterrupt:
        print("Server shutting down...")
    finally:
        sock.close()

if __name__ == '__main__':
    main()

required_stable = 4  # increase to 4 times consecutive stable
max_iterations = 50  # increase the maximum number of iterations
