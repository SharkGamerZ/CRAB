import os
import json
import pandas as pd
import numpy as np
import argparse

def analyze_directory(dir_path):
    """Analizza una singola directory di esperimento in modo robusto."""
    config_path = os.path.join(dir_path, 'config.json')
    data_path = os.path.join(dir_path, 'data_app_0.csv')

    if not os.path.exists(config_path) or not os.path.exists(data_path):
        return None

    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        nodes = int(config['global_options']['numnodes'])
        extrainfo = config['global_options'].get('extrainfo', 'N/A')

        df = pd.read_csv(data_path)
        if df.empty:
            return None
        
        # --- LOGICA DI SELEZIONE COLONNA ---
        # Cerca la colonna più significativa: 'Max-Duration_s'
        target_column = None
        for col in df.columns:
            if 'Max-Duration_s' in col:
                target_column = col
                break
        
        if not target_column:
            print(f"ATTENZIONE: Colonna 'Max-Duration_s' non trovata in {data_path}. Analisi annullata per questa cartella.")
            return None
            
        # Converte i dati da secondi a microsecondi
        time_series_us = df[target_column] * 1_000_000
        
        stats = {
            'nodes': nodes,
            'extrainfo': extrainfo,
            'mean_time_us': time_series_us.mean(),
            'median_time_us': time_series_us.median(),
            'std_dev_us': time_series_us.std(),
            'min_time_us': time_series_us.min(),
            'max_time_us': time_series_us.max(),
        }
        return stats

    except Exception as e:
        print(f"Errore durante l'analisi della directory {dir_path}: {e}")
        return None

def main(base_dir):
    """Funzione principale per eseguire l'analisi."""
    
    try:
        subdirs = sorted([d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))])
    except FileNotFoundError:
        print(f"Errore: La directory '{base_dir}' non esiste.")
        return

    results = [analyze_directory(os.path.join(base_dir, d)) for d in subdirs]
    results = [r for r in results if r is not None]

    if not results:
        print("Analisi fallita: nessun dato valido trovato.")
        return

    results.sort(key=lambda x: x['nodes'])

    # --- Stampa delle Statistiche Individuali ---
    print("\n" + "="*85)
    print("        ANALISI DELLE PERFORMANCE INDIVIDUALI (Basata su Max-Duration_s)")
    print("="*85)
    print(f"{'NODI':<6} {'MEDIA (µs)':<15} {'MEDIANA (µs)':<15} {'DEV. STD (µs)':<18} {'MIN (µs)':<15} {'MAX (µs)':<15}")
    print("-"*85)
    for res in results:
        print(f"{res['nodes']:<6} {res['mean_time_us']:<15.2f} {res['median_time_us']:<15.2f} {res['std_dev_us']:<18.2f} {res['min_time_us']:<15.2f} {res['max_time_us']:<15.2f}")
    print("="*85)

    # --- Analisi di Strong Scaling ---
    if len(results) > 1:
        baseline = results[0]
        baseline_nodes = baseline['nodes']
        baseline_time = baseline['mean_time_us']

        print("\n" + "="*65)
        print(f"             ANALISI DI STRONG SCALING (Baseline: {baseline_nodes} Nodi)")
        print("="*65)
        print(f"{'NODI':<6} {'SPEEDUP':<12} {'IDEAL':<12} {'EFFICIENCY (%)':<18}")
        print("-"*65)
        
        for res in results:
            nodes = res['nodes']
            mean_time = res['mean_time_us']
            
            speedup = baseline_time / mean_time if mean_time else float('inf')
            ideal_speedup = nodes / baseline_nodes
            efficiency = (speedup / ideal_speedup) * 100 if ideal_speedup else float('inf')

            print(f"{nodes:<6} {speedup:<12.2f} {ideal_speedup:<12.1f} {efficiency:<18.1f}")
        print("="*65)

    print("\n--- COMMENTO AI RISULTATI ---\n")
    print("L'analisi ora si basa sulla metrica 'Max-Duration' (tempo del processo più lento), \nche è la più indicativa per le performance di una collettiva.")
    if len(results) > 1 and results[-1]['nodes'] > results[0]['nodes']:
        final_efficiency = ((baseline_time / results[-1]['mean_time_us']) / (results[-1]['nodes'] / baseline_nodes)) * 100
        if final_efficiency < 70:
            print("\nOSSERVAZIONE: L'efficienza diminuisce visibilmente con l'aumentare dei nodi. \nQuesto suggerisce che il costo della comunicazione sta diventando un fattore limitante, \ncome è tipico per le operazioni Allreduce su larga scala.")
        else:
            print("\nOSSERVAZIONE: La scalabilità si mantiene buona in questo range di nodi, indicando \nche l'infrastruttura di rete gestisce bene il carico crescente.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Analizza i risultati dei test di scaling di C.R.A.B.")
    parser.add_argument("base_dir", type=str, help="La directory base che contiene le cartelle dei risultati (es. ./data/leonardo).")
    args = parser.parse_args()
    
    main(args.base_dir)
