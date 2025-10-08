import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import argparse
import os
import json
import math
from pathlib import Path

def get_abs_split(allocation_split_str, num_apps, num_nodes):
    """
    Replica la logica di calcolo dello split assoluto dall'engine.py.
    """
    split_list = [float(x) for x in allocation_split_str.split(':')]
    
    # Assicura che la lista di split sia lunga quanto il numero di app
    # (semplificazione per lo script di analisi dove ci interessano solo 2 app)
    if len(split_list) < num_apps:
        # Aggiungi lo split mancante
        remaining_perc = 100.0 - sum(split_list)
        split_list.append(remaining_perc)

    split_absolute = []
    # Calcola lo split per tutte le app tranne l'ultima
    for split in split_list[:-1]:
        split_a = int(round(num_nodes * split / 100.0))
        split_absolute.append(split_a)
    
    # L'ultima app prende tutti i nodi rimanenti
    split_absolute.append(num_nodes - sum(split_absolute))
    
    return split_absolute

def get_experiment_details(dir_path):
    """
    Legge il config.json di una directory e ne estrae i parametri chiave.
    Classifica l'esperimento come 'isolated' o 'congested'.
    """
    config_path = dir_path / 'config.json'
    if not config_path.is_file():
        return None

    with open(config_path, 'r') as f:
        config = json.load(f)

    apps = config.get('applications', {})
    g_opts = config.get('global_options', {})
    numnodes = int(g_opts.get('numnodes', 0))
    alloc_mode = g_opts.get('allocationmode', 'N/A')
    
    details = {
        'path': dir_path,
        'mode': 'Linear' if alloc_mode == 'l' else 'Interleaved',
        'victim_nodes': 0
    }

    if not apps: return None
    num_apps = len(apps)

    if num_apps == 1:
        details['type'] = 'isolated'
        details['victim_nodes'] = numnodes
    elif num_apps > 1:
        details['type'] = 'congested'
        split_str = g_opts.get('allocationsplit', '0:0')
        
        # --- LOGICA DI CALCOLO CORRETTA ---
        absolute_splits = get_abs_split(split_str, num_apps, numnodes)
        details['victim_nodes'] = absolute_splits[0] # La vittima è sempre la prima
        details['aggressor_nodes'] = absolute_splits[1] # L'aggressore il secondo
        
        aggressor_path = Path(apps.get('1', {}).get('path', ''))
        details['aggressor_type'] = aggressor_path.stem.replace('_b', '').upper()
    else:
        return None

    return details

def load_data(data_path, metric_col='0_Max-Duration_s'):
    """Carica la colonna di metrica specificata e la converte in microsecondi."""
    try:
        df = pd.read_csv(data_path)
        if metric_col not in df.columns:
            metric_col = '0_Avg-Duration_s'
            if metric_col not in df.columns:
                print(f"ERRORE: Colonne di durata non trovate in {data_path}")
                return None
        return df[metric_col].dropna() * 1_000_000
    except (FileNotFoundError, pd.errors.EmptyDataError):
        print(f"AVVISO: File dati mancante o vuoto in {data_path}. Salto.")
        return None

def generate_plot(baseline_data, congested_data, details, y_limits):
    """Genera un singolo grafico a violino di confronto."""
    df_baseline = pd.DataFrame({'Duration (µs)': baseline_data, 'Condition': f"Isolated\n({details['victim_nodes']} Nodi)"})
    
    congested_label = (f"Congested\n"
                       f"({details['victim_nodes']}V + {details['aggressor_nodes']}A)\n"
                       f"{details['aggressor_type']} - {details['mode']}")
    df_congested = pd.DataFrame({'Duration (µs)': congested_data, 'Condition': congested_label})
    
    combined_df = pd.concat([df_baseline, df_congested], ignore_index=True)

    median_baseline = df_baseline['Duration (µs)'].median()
    median_congested = df_congested['Duration (µs)'].median()
    ci = median_congested / median_baseline if median_baseline else float('inf')

    sns.set_theme(style="whitegrid", font_scale=1.1)
    fig, ax = plt.subplots(figsize=(10, 8))
    
    sns.violinplot(data=combined_df, x='Condition', y='Duration (µs)', ax=ax,
                   palette=["#56B4E9", "#D55E00"], cut=0, inner='box')
    
    ax.set_yscale('log')
    ax.set_ylim(y_limits)

    title = f"Impatto dell'Aggressore {details['aggressor_type']} ({details['mode']})"
    ax.set_title(title, fontsize=16, weight='bold', pad=20)
    ax.set_xlabel("Scenario", fontsize=12, labelpad=15)
    ax.set_ylabel("Durata Massima (µs) - Scala Logaritmica", fontsize=12, labelpad=10)

    ci_text = f"Congestion Impact (mediana): {ci:.2f}x"
    ax.text(0.95, 0.95, ci_text, transform=ax.transAxes, fontsize=13, fontweight='bold',
            verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle='round,pad=0.5', fc='wheat', alpha=0.7))

    output_filename = f"plot_V{details['victim_nodes']}A{details['aggressor_nodes']}_{details['aggressor_type']}_{details['mode']}.png"
    plt.tight_layout()
    plt.savefig(output_filename, dpi=300)
    print(f"Grafico salvato in: {output_filename}")
    plt.close(fig)

def main(base_dir):
    p_base_dir = Path(base_dir)
    subdirs = [p for p in p_base_dir.iterdir() if p.is_dir()]
    experiments = [get_experiment_details(d) for d in subdirs]
    experiments = [e for e in experiments if e is not None]

    isolated_exps = {e['victim_nodes']: e for e in experiments if e['type'] == 'isolated'}
    congested_exps = [e for e in experiments if e['type'] == 'congested']

    if not isolated_exps:
        print("ERRORE: Nessun esperimento di baseline (isolato) trovato.")
        return
    print(f"Baseline identificate per: {list(isolated_exps.keys())} nodi.")

    all_durations = []
    for exp in experiments:
        data = load_data(exp['path'] / 'data_app_0.csv')
        if data is not None:
            exp['data'] = data
            all_durations.append(data)

    if not all_durations:
        print("ERRORE: Nessun dato valido trovato in nessuna directory.")
        return

    full_dataset = pd.concat(all_durations)
    y_min = full_dataset.min() * 0.8
    y_max = full_dataset.max() * 1.2
    y_limits = (y_min, y_max)

    print("\n--- Generazione Grafici ---")
    for exp in congested_exps:
        if 'data' in exp:
            victim_nodes = exp['victim_nodes']
            if victim_nodes in isolated_exps:
                baseline_exp = isolated_exps[victim_nodes]
                baseline_data = baseline_exp.get('data')
                if baseline_data is not None:
                    print(f"Confronto: {baseline_exp['path'].name} (Baseline) vs {exp['path'].name}")
                    generate_plot(baseline_data, exp['data'], exp, y_limits)
            else:
                print(f"AVVISO: Nessuna baseline trovata per {victim_nodes} nodi. Salto il confronto per {exp['path'].name}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analizza e confronta automaticamente tutti i test di benchmark CRAB in una directory.")
    parser.add_argument("base_dir", nargs='?', default='.', help="Directory contenente le cartelle dei risultati. Default: directory corrente.")
    args = parser.parse_args()
    main(args.base_dir)
