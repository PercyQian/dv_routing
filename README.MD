# DV Routing Simulation

## Prerequisites
- Python 3.11
- Multi-process testing on the same machine

## Steps

1. Modify or check `config.txt`, ensure the correct neighbors and costs for each router.
2. Open a terminal, start the server:
   ```bash
   python3 server.py
   ```
3. Open another terminal, start the clients:
   ```bash
   python3 client.py u
   python3 client.py x
   python3 client.py w
   python3 client.py v
   python3 client.py y
   python3 client.py z
   ```

