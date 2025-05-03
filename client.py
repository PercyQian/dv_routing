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
                    
                    # handle possible multiple messages
                    messages = data.split('\n')
                    for message in messages:
                        message = message.strip()
                        if not message:
                            continue
                            
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
        # send one update first
        self.send_update()
        
        # Wait for convergence using a real convergence detection
        old_dv = {}
        converged = False
        max_iterations = 30
        iteration = 0
        
        while not converged and iteration < max_iterations:
            time.sleep(1)  # Check every second
            iteration += 1
            
            # Check if DV has changed
            if old_dv == self.dv:
                converged = True
            else:
                old_dv = self.dv.copy()
                print(f"[{self.rid}] DV at iteration {iteration}: {self.dv}")
        
        if converged:
            print(f"[{self.rid}] Converged after {iteration} iterations")
        else:
            print(f"[{self.rid}] Did not converge after {max_iterations} iterations")
        
        print(f"[{self.rid}] DV after convergence: {self.dv}")
        print(f"[{self.rid}] forwarding table: ")
        for dst, nh in self.next_hop.items():
            print(f"   to {dst} next hop {nh}, total cost {self.dv[dst]}")
        self.sock.close()

if __name__ == '__main__':
    import sys
    if len(sys.argv)!=2:
        print("Usage: python client.py <RouterID>")
        sys.exit(1)
    DVClient(sys.argv[1]).run()
