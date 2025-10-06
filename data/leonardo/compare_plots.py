import pandas as pd
import matplotlib.pyplot as plt
import os

# --- Impostazioni ---

# Dizionario che mappa le etichette per il grafico ai percorsi dei rispettivi file CSV.
# Modifica i percorsi se lo script non si trova nella stessa cartella 'leonardo'.
esperimenti = {
    "Baseline (4 nodi)": "baseline_run_in_4_node_alloc-2/data_app_0.csv",
    "8 Nodi - Test Isolamento": "8_node_linear_isolation_test-2/data_app_0.csv",
    "8 Nodi - Aggressori Multipli": "8_node_multiple_aggressors-2/data_app_0.csv",
    "8 Nodi - Aggressore Asimmetrico Forte": "8_node_asymmetric_strong_aggressor-2/data_app_0.csv"
}

# La colonna delle performance da analizzare
COLONNA_DATI = '0_Avg-Duration_s'

# Il timestamp (in secondi) in cui sono partite le app di disturbo
TEMPO_EVENTO = 5.0

# --- Logica di Analisi e Grafico ---

# Usa uno stile grafico piacevole e imposta la dimensione della figura
plt.style.use('seaborn-v0_8-whitegrid')
fig, ax = plt.subplots(figsize=(18, 9))

# Itera su ogni esperimento definito nel dizionario
for etichetta, percorso_file in esperimenti.items():
    try:
        # Controlla se il file esiste prima di tentare di caricarlo
        if not os.path.exists(percorso_file):
            print(f"Attenzione: Il file '{percorso_file}' non è stato trovato e sarà saltato.")
            continue

        # Carica i dati dal file CSV
        df = pd.read_csv(percorso_file)

        # Controlla se la colonna specificata esiste nel DataFrame
        if COLONNA_DATI not in df.columns:
            print(f"Attenzione: La colonna '{COLONNA_DATI}' non è stata trovata in '{percorso_file}'. Il file sarà saltato.")
            continue

        # 1. Crea la colonna del tempo cumulativo
        df['Tempo_Cumulativo_s'] = df[COLONNA_DATI].cumsum()

        # 2. Disegna la linea delle performance per questo esperimento sul grafico
        ax.plot(df['Tempo_Cumulativo_s'], df[COLONNA_DATI],
                label=etichetta,
                linewidth=2,
                alpha=0.9)

    except Exception as e:
        # Gestisce altri possibili errori durante la lettura o l'elaborazione del file
        print(f"Si è verificato un errore imprevisto durante l'elaborazione di '{percorso_file}': {e}")

# 3. Aggiungi una linea verticale per marcare l'evento di disturbo (una sola volta)
ax.axvline(x=TEMPO_EVENTO, color='r', linestyle='--', linewidth=2,
           label=f'Avvio Applicazioni di Disturbo (t={TEMPO_EVENTO}s)')
ax.axvline(x=TEMPO_EVENTO*2, color='r', linestyle='--', linewidth=2,
           label=f'Avvio Applicazioni di Disturbo (t={TEMPO_EVENTO}s)')
ax.axvline(x=TEMPO_EVENTO*3, color='r', linestyle='--', linewidth=2,
           label=f'Avvio Applicazioni di Disturbo (t={TEMPO_EVENTO}s)')
ax.axvline(x=TEMPO_EVENTO*4, color='r', linestyle='--', linewidth=2,
           label=f'Avvio Applicazioni di Disturbo (t={TEMPO_EVENTO}s)')
ax.axvline(x=TEMPO_EVENTO*5, color='r', linestyle='--', linewidth=2,
           label=f'Avvio Applicazioni di Disturbo (t={TEMPO_EVENTO}s)')

# --- Abbellimenti Grafico Finale ---
ax.set_title('Confronto Performance con Interferenza di Rete', fontsize=18, weight='bold')
ax.set_xlabel('Tempo Cumulativo dell\'Esperimento (s)', fontsize=14)
ax.set_ylabel('Durata Media Iterazione (s)', fontsize=14)

# Mostra la legenda per identificare le diverse linee
ax.legend(fontsize=12)

# Abilita una griglia più dettagliata
ax.grid(True, which='both', linestyle='--', linewidth=0.5)

# Imposta la dimensione dei font per gli assi per una migliore leggibilità
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)

# Ottimizza il layout e mostra il grafico
plt.tight_layout()
plt.show()
