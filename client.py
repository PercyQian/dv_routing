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
        # 使用 makefile 逐行读取服务器消息
        self.file = self.sock.makefile('r')

    def join(self):
        # 发送 JOIN 并同步读取服务器的 RESPONSE
        self.sock.sendall(f"JOIN {self.rid}\n".encode())
        print(f"[{self.rid}] Sent JOIN, waiting for RESPONSE")
        # 逐行读取直到收到合法的 RESPONSE
        while True:
            line = self.file.readline()
            if not line:
                raise ConnectionError("Connection closed during join")
            line = line.strip()
            if not line:
                continue
            print(f"[{self.rid}] Processing handshake line: {line}")
            if line.startswith("RESPONSE"):
                parts = line.split(maxsplit=2)
                if len(parts) == 3 and parts[1] == self.rid:
                    print(f"[{self.rid}] Valid RESPONSE: {line}")
                    _, _, body = parts
                    self.neigh = parse_neighbors(body)
                    for node, cost in self.neigh.items():
                        if cost >= 0:
                            self.dv[node] = cost
                            self.next_hop[node] = node
                    self.dv[self.rid] = 0
                    self.next_hop[self.rid] = self.rid
                    break
                else:
                    print(f"[{self.rid}] Invalid RESPONSE format: {line}")

    def start_listener(self):
        def run():
            for raw in self.file:
                line = raw.strip()
                if not line:
                    continue
                print(f"[{self.rid}] Processing line: {line}")
                # START
                if line == "START":
                    print(f"[{self.rid}] Received START message")
                    continue
                # UPDATE
                if line.startswith("UPDATE"):
                    parts = line.split(maxsplit=2)
                    if len(parts) != 3:
                        print(f"[{self.rid}] warning: invalid UPDATE message: {line}")
                        continue
                    _, src, body = parts
                    nb_dv = parse_neighbors(body)
                    updated = False
                    for dst, cost_to_nb in self.neigh.items():
                        if cost_to_nb < 0:
                            continue
                        for d2, c2 in nb_dv.items():
                            new_cost = cost_to_nb + c2
                            if d2 not in self.dv or new_cost < self.dv[d2]:
                                self.dv[d2] = new_cost
                                self.next_hop[d2] = dst
                                updated = True
                    if updated:
                        self.send_update()
                    continue
                # 其它消息
                print(f"[{self.rid}] warning: unknown message: {line}")
        threading.Thread(target=run, daemon=True).start()

    def send_update(self):
        body = ';'.join(f"{d},{c}" for d,c in self.dv.items())
        msg = f"UPDATE {self.rid} {body}"
        self.sock.sendall((msg + '\n').encode())

    def run(self):
        # 发送JOIN并等待RESPONSE，初始化dv/next_hop
        self.join()
        
        # 初始化时不要立即发送UPDATE，而是等待START
        print(f"[{self.rid}] initial DV: {self.dv}")
        print(f"[{self.rid}] 等待服务器START信号...")
        
        # 启动监听线程，但不立即发送更新
        self.start_received = False
        
        def start_listener():
            for line in self.file:
                line = line.strip()
                if not line:
                    continue
                print(f"[{self.rid}] Processing line: {line}")
                
                # 检测START信号
                if line == "START":
                    print(f"[{self.rid}] 收到START信号，开始DV算法")
                    self.start_received = True
                    # 收到START信号后发送第一次更新
                    self.send_update()
                    continue
                    
                # UPDATE消息处理（和原来相同）
                if line.startswith("UPDATE"):
                    parts = line.split(maxsplit=2)
                    if len(parts) != 3:
                        print(f"[{self.rid}] warning: invalid UPDATE message: {line}")
                        continue
                    _, src, body = parts
                    nb_dv = parse_neighbors(body)
                    updated = False
                    for dst, cost_to_nb in self.neigh.items():
                        if cost_to_nb < 0:
                            continue
                        for d2, c2 in nb_dv.items():
                            new_cost = cost_to_nb + c2
                            if d2 not in self.dv or new_cost < self.dv[d2]:
                                self.dv[d2] = new_cost
                                self.next_hop[d2] = dst
                                updated = True
                    if updated:
                        self.send_update()
                    continue
                
        # 启动监听
        threading.Thread(target=start_listener, daemon=True).start()
        
        # 等待START信号
        while not self.start_received:
            time.sleep(0.5)
        
        # START信号到达后，继续算法...
        # 检测收敛 - 增加稳定次数要求和最大迭代次数
        old_dv = {}
        consecutive_stable = 0
        required_stable = 6  # 增加到 6 次连续稳定
        max_iterations = 60  # 增加最大迭代次数
        
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
        
        self.sock.close()

if __name__ == '__main__':
    import sys
    if len(sys.argv)!=2:
        print("Usage: python client.py <RouterID>")
        sys.exit(1)
    DVClient(sys.argv[1]).run()
