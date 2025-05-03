#!/usr/bin/env python3
import socket
import threading
import time

# parse the neighbors list string nb1,c1;nb2,c2;...
def parse_neighbors(s):
    m = {}
    for seg in s.split(';'):
        if not seg.strip():  # 跳过空段
            continue
        try:
            parts = seg.split(',')
            if len(parts) != 2:
                print(f"warning: invalid segment '{seg}', skipped")
                continue
            nb, cost = parts
            m[nb] = int(cost)
        except Exception as e:
            print(f"error parsing segment '{seg}': {e}")
    return m

class DVClient:
    def __init__(self, router_id):
        self.rid = router_id
        self.neigh = {}      # direct neighbors and cost
        self.dv = {}         # own distance vector
        self.next_hop = {}   # forwarding table: destination -> next hop

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(('127.0.0.1', 5555))

    def join(self):
        self.sock.sendall(f"JOIN {self.rid}".encode())
        
        # 创建一个缓冲区来处理可能混合的消息
        buffer = ""
        response_received = False
        
        while not response_received:
            chunk = self.sock.recv(4096).decode()
            if not chunk:
                raise ConnectionError("Connection closed by server")
            
            buffer += chunk
            lines = buffer.split('\n')
            buffer = lines.pop()  # 保留最后一个不完整的行
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                print(f"[{self.rid}] Processing line: {line}")
                
                # 检查是否是START消息
                if line == "START":
                    print(f"[{self.rid}] Received START message")
                    self.start_received = True
                    continue
                
                # 检查是否是UPDATE消息(存储起来以后处理)
                if line.startswith("UPDATE"):
                    print(f"[{self.rid}] Received UPDATE, storing for later")
                    self.pending_updates = getattr(self, 'pending_updates', [])
                    self.pending_updates.append(line)
                    continue
                
                # 尝试解析RESPONSE消息
                if line.startswith("RESPONSE"):
                    parts = line.split(maxsplit=2)
                    if len(parts) >= 3 and parts[1] == self.rid:
                        print(f"[{self.rid}] Valid RESPONSE: {line}")
                        _, _, body = parts
                        
                        self.neigh = parse_neighbors(body)
                        # 初始化DV
                        for node, cost in self.neigh.items():
                            if cost >= 0:
                                self.dv[node] = cost
                                self.next_hop[node] = node
                        self.dv[self.rid] = 0
                        self.next_hop[self.rid] = self.rid
                        
                        response_received = True
                        break
                    else:
                        print(f"[{self.rid}] Invalid RESPONSE format: {line}")

    def start_listener(self):
        self.file = self.sock.makefile()
        self.start_received = False  # 添加标记
        
        def run():
            while True:
                try:
                    data = self.sock.recv(4096).decode().strip()
                    if not data: break
                    
                    messages = data.split('\n')
                    for message in messages:
                        message = message.strip()
                        if not message:
                            continue
                        
                        # 处理START消息
                        if message == "START":
                            print(f"[{self.rid}] Received START from server")
                            self.start_received = True
                            continue
                        
                        # 处理UPDATE消息
                        try:
                            # UPDATE x nb1,c1;...
                            parts = message.split(maxsplit=2)
                            if len(parts) != 3:
                                print(f"[{self.rid}] warning: received invalid message: {message}")
                                continue
                            
                            _, src, body = parts
                            nb_dv = parse_neighbors(body)
                            if not nb_dv:  # 如果解析结果为空，跳过
                                print(f"[{self.rid}] warning: message body is empty: {body}")
                                continue
                                
                            updated = False
                            # Bellman-Ford relaxation
                            for dst, cost_to_nb in self.neigh.items():
                                if cost_to_nb < 0: continue
                                for d2, c2 in nb_dv.items():
                                    new_cost = cost_to_nb + c2
                                    if d2 not in self.dv or new_cost < self.dv[d2]:
                                        self.dv[d2] = new_cost
                                        self.next_hop[d2] = dst
                                        updated = True
                            if updated:
                                self.send_update()
                        except Exception as e:
                            print(f"[{self.rid}] error handling message: {message}")
                            print(f"error: {e}")
                            continue
                except Exception as e:
                    print(f"[{self.rid}] error receiving data: {e}")
                    break
        threading.Thread(target=run, daemon=True).start()

    def send_update(self):
        body = ';'.join(f"{d},{c}" for d,c in self.dv.items())
        msg = f"UPDATE {self.rid} {body}"
        self.sock.sendall((msg + '\n').encode())

    def run(self):
        self.join()
        print(f"[{self.rid}] initial DV: {self.dv}")
        self.start_listener()
        
        # 等待服务器的START消息或超时
        start_received = False
        try:
            # 接收START消息的逻辑已经在start_listener中处理
            # 这里等待一段时间，如果没收到START就开始DV计算
            for _ in range(20):  # 最多等待20秒
                if hasattr(self, 'start_received') and self.start_received:
                    start_received = True
                    break
                time.sleep(1)
            
            if not start_received:
                print(f"[{self.rid}] No START received from server, starting anyway")
        
            # 发送第一次更新
            self.send_update()
            
            # 检测收敛
            old_dv = {}
            consecutive_stable = 0
            required_stable = 2  # 需要连续多少次稳定才视为收敛
            max_iterations = 30
            iteration = 0
            
            while iteration < max_iterations:
                time.sleep(1)
                iteration += 1
                
                # 检查DV是否变化
                if old_dv == self.dv:
                    consecutive_stable += 1
                    print(f"[{self.rid}] DV stable for {consecutive_stable} iterations")
                    if consecutive_stable >= required_stable:
                        print(f"[{self.rid}] Converged after {iteration} iterations")
                        break
                else:
                    consecutive_stable = 0
                    old_dv = self.dv.copy()
                    print(f"[{self.rid}] DV at iteration {iteration}: {self.dv}")
                    # 每次DV变化时发送更新
                    self.send_update()
            
            # 打印最终结果
            print(f"[{self.rid}] Final DV: {self.dv}")
            print(f"[{self.rid}] Forwarding table: ")
            for dst, nh in self.next_hop.items():
                print(f"   to {dst} next hop {nh}, total cost {self.dv[dst]}")
            
            # 发送EXIT消息
            try:
                self.sock.sendall(f"EXIT {self.rid}\n".encode())
                print(f"[{self.rid}] Sent EXIT message")
            except Exception as e:
                print(f"[{self.rid}] Failed to send EXIT: {e}")
        
        except KeyboardInterrupt:
            print(f"[{self.rid}] Interrupted by user")
        finally:
            self.sock.close()

if __name__ == '__main__':
    import sys
    if len(sys.argv)!=2:
        print("Usage: python client.py <RouterID>")
        sys.exit(1)
    DVClient(sys.argv[1]).run()
