import pandas as pd
import matplotlib.pyplot as plt
import os
import json
import numpy as np

# --- Impostazioni ---

# Elenca qui le directory degli esperimenti che vuoi analizzare.
esperimenti_dirs = [
    "8_node_128MB_victim_baseline",
    "12_node_128MB_victim_light_aggressor_linear",
    "12_node_128MB_victim_light_aggressor_interleaved",
    "32_node_128MB_victim_strong_aggressor_linear",
    "32_node_128MB_victim_strong_aggressor_interleaved"
]

COLONNA_DATI = '0_Avg-Duration_s'
NUM_RUNS = 5
IGNORARE_PRIMI_N_PUNTI = 1

# --- Funzione Helper per Analizzare la Configurazione (invariata) ---
def parse_config(config_path):
    with open(config_path, 'r') as f:
        config = json.load(f)
    info = {}
    g_opts = config.get('global_options', {})
    apps = config.get('applications', {})
    info['numnodes'] = g_opts.get('numnodes', 'N/A')
    info['allocationmode'] = 'Linear' if g_opts.get('allocationmode') == 'l' else 'Interleaved'
    info['allocationsplit'] = g_opts.get('allocationsplit', '100')
    if "1" in apps:
        aggressor_args = apps["1"].get('args', '')
        if '-msgsize 1024' in aggressor_args:
            info['aggressor_type'] = 'Aggressore Leggero (Latenza)'
        elif '-msgsize 33554432' in aggressor_args:
            info['aggressor_type'] = 'Aggressore Forte (Banda)'
        else:
            info['aggressor_type'] = 'Aggressore Custom'
        info['aggressor_start'] = float(apps["1"].get('start', '0'))
    else:
        info['aggressor_type'] = 'Baseline (Nessun Aggressore)'
        info['aggressor_start'] = 0
    return info

# --- Logica Principale di Analisi e Grafico (Modificata) ---

plt.style.use('seaborn-v0_8-whitegrid')
fig, axes = plt.subplots(
    nrows=len(esperimenti_dirs),
    ncols=1,
    figsize=(18, 5 * len(esperimenti_dirs)),
    sharex=True
)
fig.suptitle('Confronto Performance Esperimenti (Media e Dev. Standard su 5 Run)', fontsize=22, weight='bold')

for i, dir_name in enumerate(esperimenti_dirs):
    ax = axes[i]
    config_path = os.path.join(dir_name, 'config.json')
    data_path = os.path.join(dir_name, 'data_app_0.csv')

    try:
        config_info = parse_config(config_path)
        df_completo = pd.read_csv(data_path)
        punti_per_run = len(df_completo) // NUM_RUNS
        avg_iterations = punti_per_run - IGNORARE_PRIMI_N_PUNTI
        
        title_parts = [
            f"{config_info['numnodes']} Nodi - {config_info['aggressor_type']}",
        ]
        if 'Aggressore' in config_info['aggressor_type']:
            title_parts.append(f"(Split {config_info['allocationsplit']}, Alloc. {config_info['allocationmode']})")
        
        title_parts.append(f"Throughput Medio: {avg_iterations} iter/run")
        ax.set_title('\n'.join(title_parts), fontsize=14)

        # --- INIZIO MODIFICA: CALCOLO MEDIA E DEV. STANDARD ---
        
        # 1. Carica i dati di ogni run in una lista di DataFrame
        runs_data = []
        for j in range(NUM_RUNS):
            start_idx = j * punti_per_run
            end_idx = (j + 1) * punti_per_run
            df_run = df_completo.iloc[start_idx + IGNORARE_PRIMI_N_PUNTI : end_idx].copy()
            if not df_run.empty:
                runs_data.append(df_run[COLONNA_DATI].values)
        
        if not runs_data: continue

        # 2. Allinea le run alla lunghezza minima per poter calcolare la media
        min_len = min(len(run) for run in runs_data)
        aligned_runs = [run[:min_len] for run in runs_data]
        
        # 3. Calcola media e deviazione standard
        mean_perf = np.mean(aligned_runs, axis=0)
        std_perf = np.std(aligned_runs, axis=0)
        
        # 4. Crea l'asse del tempo cumulativo basato sulla media
        #    (è un'approssimazione, ma visivamente corretta)
        time_axis = np.cumsum(mean_perf)
        
        # 5. Disegna la linea della media
        ax.plot(time_axis, mean_perf, label='Media Performance', color='b')
        
        # 6. Disegna l'area ombreggiata per la deviazione standard
        ax.fill_between(time_axis, mean_perf - std_perf, mean_perf + std_perf,
                        color='b', alpha=0.2, label='Deviazione Standard')

        # --- FINE MODIFICA ---

        if config_info['aggressor_start'] > 0:
            ax.axvline(x=config_info['aggressor_start'], color='r', linestyle='--', linewidth=2)

        ax.grid(True, which='both', linestyle='--', linewidth=0.5)
        ax.set_ylabel('Durata Iterazione (s)', fontsize=12)
        ax.legend() # Mostra la legenda (Media, Dev. Std.) su ogni grafico

    except FileNotFoundError:
        ax.text(0.5, 0.5, f"Dati non trovati in:\n'{dir_name}'", ha='center', va='center', color='red')
        ax.set_title(dir_name, fontsize=14)
    except Exception as e:
        ax.text(0.5, 0.5, f"Errore durante l'elaborazione:\n{e}", ha='center', va='center', color='red')
        ax.set_title(dir_name, fontsize=14)

axes[-1].set_xlabel('Tempo Cumulativo Approssimato (s)', fontsize=14)
plt.tight_layout(rect=[0, 0.03, 1, 0.97])
plt.show()
