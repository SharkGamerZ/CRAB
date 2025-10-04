
import sys
import os
import time

print(f"--- [PYTHON WORKER] Test started on node {os.uname().nodename} ---", flush=True)

# 1. Simula l'aggiunta al path che fa cli.py
sys.path.insert(0, '/leonardo/home/userexternal/mmarcel3/CRAB.OLD/src')
print("--- [PYTHON WORKER] Path setup complete. Importing orchestrator...", flush=True)
time.sleep(1) # Piccolo delay per assicurarsi che l'output venga scritto

# 2. Importa il primo modulo
from crab.cli.orchestrator import run_from_cli
print("--- [PYTHON WORKER] orchestrator imported successfully. Importing engine...", flush=True)
time.sleep(1)

# 3. Importa il secondo modulo (quello che importa numpy etc.)
from crab.core.engine import Engine
print("--- [PYTHON WORKER] engine imported successfully. Test finished.", flush=True)
