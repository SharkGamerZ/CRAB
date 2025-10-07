import pandas as pd
import matplotlib.pyplot as plt
import os

# --- Impostazioni ---

# Metti qui il percorso del file che vuoi analizzare.
# Puoi aggiungere più file a questo dizionario per confrontare diversi esperimenti.
esperimenti = {
    "Baseline (4 nodi)": "baseline_run_in_4_node_alloc-2/data_app_0.csv",
    "8 Nodi - Test Isolamento": "8_node_linear_isolation_test-2/data_app_0.csv",
    "8 Nodi - Aggressori Multipli": "8_node_multiple_aggressors-2/data_app_0.csv",
    "8 Nodi - Aggressore Asimmetrico Forte": "8_node_asymmetric_strong_aggressor-2/data_app_0.csv"
}

COLONNA_DATI = '0_Avg-Duration_s'
TEMPO_EVENTO = 3.0
NUM_RUNS = 5

# --- Logica di Analisi e Grafico ---

plt.style.use('seaborn-v0_8-whitegrid')
# Crea una figura con 4 subplot verticali, che condividono lo stesso asse X
fig, axes = plt.subplots(
    nrows=len(esperimenti), 
    ncols=1, 
    figsize=(18, 20), 
    sharex=True
)
fig.suptitle('Analisi Performance per Esperimento (5 Run Sovrapposte)', fontsize=20, weight='bold')


# Itera sia sugli esperimenti che sugli assi dei subplot
for i, (etichetta, percorso_file) in enumerate(esperimenti.items()):
    ax = axes[i] # Seleziona il subplot corrente
    
    try:
        if not os.path.exists(percorso_file):
            ax.text(0.5, 0.5, f"File non trovato:\n{percorso_file}", ha='center', va='center', fontsize=12, color='red')
            ax.set_title(etichetta)
            continue

        df_completo = pd.read_csv(percorso_file)
        punti_per_run = len(df_completo) // NUM_RUNS
        
        if punti_per_run == 0:
            ax.text(0.5, 0.5, "Dati insufficienti", ha='center', va='center', fontsize=12)
            ax.set_title(etichetta)
            continue
            
        durate_run = []
        # Cicla per ognuna delle 5 run
        for j in range(NUM_RUNS):
            start_index = j * punti_per_run
            end_index = (j + 1) * punti_per_run
            df_run = df_completo.iloc[start_index:end_index].copy()

            if df_run.empty: continue

            df_run['Tempo_Cumulativo_s'] = df_run[COLONNA_DATI].cumsum()
            durate_run.append(df_run['Tempo_Cumulativo_s'].iloc[-1])

            # Disegna la linea sul subplot corretto
            ax.plot(df_run['Tempo_Cumulativo_s'], df_run[COLONNA_DATI],
                    alpha=0.7, linewidth=1.5, label=f"Run {j+1}" if i==0 else None) # Legenda solo per il primo grafico

        # Abbellimenti per ogni subplot
        avg_duration = np.mean(durate_run)
        ax.set_title(f"{etichetta} (Durata media run: {avg_duration:.2f}s)", fontsize=16)
        ax.axvline(x=TEMPO_EVENTO, color='r', linestyle='--', linewidth=2)
        ax.grid(True, which='both', linestyle='--', linewidth=0.5)
        ax.set_ylabel('Durata Iterazione (s)', fontsize=12)

    except Exception as e:
        ax.text(0.5, 0.5, f"Errore: {e}", ha='center', va='center', fontsize=12, color='red')
        ax.set_title(etichetta)


# Abbellimenti finali
fig.legend(loc='upper right', bbox_to_anchor=(0.95, 0.98))
axes[-1].set_xlabel('Tempo dall\'Inizio della Run (s)', fontsize=14) # Etichetta X solo sull'ultimo grafico
plt.tight_layout(rect=[0, 0, 1, 0.97]) # Aggiusta il layout per far spazio al titolo principale
plt.show()
