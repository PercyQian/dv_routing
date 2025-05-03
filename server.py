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
            
            # 检查是否已经处理过这个路由器
            with lock:
                if rid in client_table:
                    print(f"Router {rid} already registered, closing duplicate connection")
                    return
                
                # 记录这个路由器的连接
                client_table[rid] = conn
                print(f"Connected routers: {list(client_table.keys())}")
                
                if len(client_table) > router_count:
                    print(f"Warning: More routers ({len(client_table)}) than expected ({router_count})")
            
            # 先给当前路由器发送RESPONSE，确保它知道自己的邻居
            neighs = topo.get(rid, {})
            msg = 'RESPONSE {} {}'.format(
                rid,
                ';'.join(f"{nb},{c}" for nb,c in neighs.items())
            )
            print(f"Sending to {rid}: {msg}")
            conn.sendall((msg + '\n').encode())
            
            # 再检查是否是最后一个路由器，如果是则发送START给所有人
            with lock:
                if len(client_table) >= router_count and not router_ready.is_set():
                    print(f"All {len(client_table)} routers connected. Setting ready event.")
                    router_ready.set()
                    # 通知所有客户端开始DV算法
                    for router_id, router_conn in client_table.items():
                        try:
                            router_conn.sendall(f"START\n".encode())
                            print(f"Sent START to {router_id}")
                        except Exception as e:
                            print(f"Failed to send START to {router_id}: {e}")
            
            # return the neighbors list
            neighs = topo.get(rid, {})  # 使用get避免KeyError
            if not neighs:
                print(f"Warning: Router {rid} has no neighbors in topology!")
            
            # MESSAGE format: RESPONSE <RouterID> nb1,cost1;nb2,cost2;...
            msg = 'RESPONSE {} {}'.format(
                rid,
                ';'.join(f"{nb},{c}" for nb,c in neighs.items())
            )
            print(f"Sending to {rid}: {msg}")
            conn.sendall((msg + '\n').encode())
            
            # 然后等待所有路由器连接
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
                # 只向可以直接通信的邻居转发
                for nb, cost in topo[src].items():
                    if cost >= 0 and nb in client_table:  # 确保只转发给cost >= 0的路由器
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
                    # 可以在这里添加服务器关闭逻辑
        conn.close()

def main():
    topo = load_topology('config.txt')
    client_table = {}  # RouterID -> conn
    lock = threading.Lock()
    
    # 打印读取到的拓扑信息
    print(f"Loaded topology with {len(topo)} routers:")
    for router, neighbors in topo.items():
        print(f"  Router {router}: {neighbors}")
    
    # Count the expected number of routers from topology
    router_count = len(topo)
    print(f"Expecting {router_count} routers to connect")
    
    # Event to signal when all routers are connected
    router_ready = threading.Event()
    
    # 添加超时机制，如果60秒内没有收到所有路由器，也开始算法
    def timeout_handler():
        time.sleep(60)  # 等待60秒
        if not router_ready.is_set():
            print("Timeout waiting for all routers. Starting with connected routers.")
            router_ready.set()
            # 通知已连接的客户端开始
            with lock:
                for router_id, router_conn in client_table.items():
                    try:
                        router_conn.sendall(f"START\n".encode())
                    except Exception as e:
                        print(f"Failed to send START to {router_id}: {e}")
    
    # 启动超时线程
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

required_stable = 4  # 增加到4次连续稳定
max_iterations = 50  # 增加最大迭代次数
