#!/usr/bin/env python3
import socket
import threading
import time

# parse the neighbors list string nb1,c1;nb2,c2;...
def parse_neighbors(s):
    m = {}
    for seg in s.split(';'):
        if not seg.strip():  # skip empty segments
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
        # use makefile to read server messages line by line
        self.file = self.sock.makefile('r')

    def join(self):
        # send JOIN and read the server's RESPONSE
        self.sock.sendall(f"JOIN {self.rid}\n".encode())
        print(f"[{self.rid}] Sent JOIN, waiting for RESPONSE")
        # read line by line until a valid RESPONSE is received
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
                    _, src, body = parts
                    
                    # only process updates from directly connected and reachable neighbors
                    if src not in self.neigh or self.neigh[src] < 0:
                        print(f"[{self.rid}] ignoring update from non-direct neighbor or unreachable neighbor: {src}")
                        continue
                    
                    # distance vector from neighbors to destinations
                    nb_dv = parse_neighbors(body)
                    updated = False
                    
                    # try to update the path to other destinations through the current neighbor
                    cost_to_nb = self.neigh[src]  # the cost from current router to src
                    for dst, dst_cost in nb_dv.items():
                        # calculate the new path: current router->src->dst
                        new_cost = cost_to_nb + dst_cost
                        
                        # if a shorter path is found, update DV and forwarding table
                        if dst not in self.dv or new_cost < self.dv[dst]:
                            self.dv[dst] = new_cost
                            self.next_hop[dst] = src  # the next hop is src
                            updated = True
                    if updated:
                        self.send_update()
                    continue
                # other messages
                print(f"[{self.rid}] warning: unknown message: {line}")
        threading.Thread(target=run, daemon=True).start()

    def send_update(self):
        body = ';'.join(f"{d},{c}" for d,c in self.dv.items())
        msg = f"UPDATE {self.rid} {body}"
        self.sock.sendall((msg + '\n').encode())

    def run(self):
        # send JOIN and wait for RESPONSE, initialize dv/next_hop
        self.join()
        
        # do not send UPDATE immediately, wait for START
        print(f"[{self.rid}] initial DV: {self.dv}")
        print(f"[{self.rid}] waiting for server START signal...")
        
        # start the listener thread, but do not send UPDATE immediately
        self.start_received = False
        
        def start_listener():
            for line in self.file:
                line = line.strip()
                if not line:
                    continue
                print(f"[{self.rid}] Processing line: {line}")
                
                # detect START signal
                if line == "START":
                    print(f"[{self.rid}] received START signal, starting DV algorithm")
                    self.start_received = True
                    # send the first UPDATE after receiving START
                    self.send_update()
                    continue
                    
                # UPDATE message processing (same as before)
                if line.startswith("UPDATE"):
                    parts = line.split(maxsplit=2)
                    _, src, body = parts
                    
                    # only process updates from directly connected and reachable neighbors
                    if src not in self.neigh or self.neigh[src] < 0:
                        print(f"[{self.rid}] ignoring update from non-direct neighbor or unreachable neighbor: {src}")
                        continue
                    
                    # distance vector from neighbors to destinations
                    nb_dv = parse_neighbors(body)
                    updated = False
                    
                    # try to update the path to other destinations through the current neighbor
                    cost_to_nb = self.neigh[src]  # the cost from current router to src
                    for dst, dst_cost in nb_dv.items():
                        # calculate the new path: current router->src->dst
                        new_cost = cost_to_nb + dst_cost
                        
                        # if a shorter path is found, update DV and forwarding table
                        if dst not in self.dv or new_cost < self.dv[dst]:
                            self.dv[dst] = new_cost
                            self.next_hop[dst] = src  # the next hop is src
                            updated = True
                    if updated:
                        self.send_update()
                    continue
                
        # start the listener
        threading.Thread(target=start_listener, daemon=True).start()
        
        # wait for START signal
        while not self.start_received:
            time.sleep(0.5)
        
        # after receiving START signal, continue the algorithm...
        # detect convergence - increase the stable count requirement and maximum iterations
        old_dv = {}
        consecutive_stable = 0
        required_stable = 6  # increase to 6 times consecutive stable
        max_iterations = 60  # increase the maximum number of iterations
        
        iteration = 0
        
        while iteration < max_iterations:
            time.sleep(1)
            iteration += 1
            
            # check if DV has changed
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
                # send the UPDATE when DV changes
                self.send_update()
        
        # print the final result
        print(f"[{self.rid}] Final DV: {self.dv}")
        print(f"[{self.rid}] Forwarding table: ")
        for dst, nh in self.next_hop.items():
            print(f"   to {dst} next hop {nh}, total cost {self.dv[dst]}")
        
        # send the EXIT message
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
