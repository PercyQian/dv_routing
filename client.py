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
                print(f"警告: 格式错误的段 '{seg}'，跳过")
                continue
            nb, cost = parts
            m[nb] = int(cost)
        except Exception as e:
            print(f"解析段 '{seg}' 时出错: {e}")
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
        resp = self.sock.recv(1024).decode().strip()
        # RESPONSE <rid> nb1,c1;...
        _, _, body = resp.split(maxsplit=2)
        self.neigh = parse_neighbors(body)
        # initialize dv
        for node, cost in self.neigh.items():
            if cost >= 0:
                self.dv[node] = cost
                self.next_hop[node] = node
        self.dv[self.rid] = 0
        self.next_hop[self.rid] = self.rid

    def start_listener(self):
        self.file = self.sock.makefile()
        def run():
            while True:
                try:
                    data = self.sock.recv(4096).decode().strip()
                    if not data: break
                    
                    # 处理可能的多条消息
                    messages = data.split('\n')
                    for message in messages:
                        message = message.strip()
                        if not message:
                            continue
                            
                        try:
                            # UPDATE x nb1,c1;...
                            parts = message.split(maxsplit=2)
                            if len(parts) != 3:
                                print(f"[{self.rid}] 警告：收到格式错误的消息：{message}")
                                continue
                            
                            _, src, body = parts
                            nb_dv = parse_neighbors(body)
                            if not nb_dv:  # 如果解析结果为空，跳过
                                print(f"[{self.rid}] 警告：消息体解析为空：{body}")
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
                            print(f"[{self.rid}] 处理消息时出错：{message}")
                            print(f"错误：{e}")
                            continue
                except Exception as e:
                    print(f"[{self.rid}] 接收数据时出错：{e}")
                    break
        threading.Thread(target=run, daemon=True).start()

    def send_update(self):
        body = ';'.join(f"{d},{c}" for d,c in self.dv.items())
        msg = f"UPDATE {self.rid} {body}"
        self.sock.sendall((msg + '\n').encode())

    def run(self):
        self.join()
        print(f"[{self.rid}] 初始 DV: {self.dv}")
        self.start_listener()
        # send one update first
        self.send_update()
        # wait for convergence
        time.sleep(5)
        print(f"[{self.rid}] 收敛后 DV: {self.dv}")
        print(f"[{self.rid}] 转发表: ")
        for dst, nh in self.next_hop.items():
            print(f"  到 {dst} 下一跳 {nh}，总代价 {self.dv[dst]}")
        self.sock.close()

if __name__ == '__main__':
    import sys
    if len(sys.argv)!=2:
        print("Usage: python client.py <RouterID>")
        sys.exit(1)
    DVClient(sys.argv[1]).run()
