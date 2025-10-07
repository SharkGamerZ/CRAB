import os
import sys
import json
import pandas as pd
import argparse

def format_bytes(byte_count):
    """Converte i byte in un formato leggibile (KB, MB)."""
    if byte_count is None:
        return "N/A"
    power = 1024
    n = 0
    power_labels = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB'}
    while byte_count >= power and n < len(power_labels) -1 :
        byte_count /= power
        n += 1
    return f"{byte_count:.2f} {power_labels[n]}"

def analyze_run_directory(dir_path):
    """Analizza una singola directory di un esperimento e ritorna un dizionario con i risultati."""
    result = {
        "dir_name": os.path.basename(dir_path),
        "status": "UNKNOWN",
        "nodes": None,
        "msg_size_bytes": None,
        "mean_us": None,
        "std_us": None,
        "test_type": "unknown",
        "error_details": ""
    }

    # 1. Leggi il file di configurazione per ottenere i metadati
    config_path = os.path.join(dir_path, 'config.json')
    if not os.path.exists(config_path):
        result["status"] = "FAILURE"
        result["error_details"] = "config.json mancante."
        return result
        
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        result["nodes"] = int(config["global_options"]["numnodes"])
        # L'argomento msgsize è in applications -> 0 -> args
        args_str = config["applications"]["0"]["args"]
        result["msg_size_bytes"] = int(args_str.split("-msgsize")[1].strip().split()[0])
        extrainfo = config["global_options"]["extrainfo"]
        if "weak_scaling" in extrainfo:
            result["test_type"] = "weak_scaling"
        elif "msg_scaling" in extrainfo:
            result["test_type"] = "message_scaling"

    except (KeyError, IndexError, json.JSONDecodeError) as e:
        result["status"] = "FAILURE"
        result["error_details"] = f"Errore nel parsing di config.json: {e}"
        return result

    # 2. Controlla il file di errore di SLURM
    error_log_path = os.path.join(dir_path, 'slurm_error.log')
    if os.path.exists(error_log_path) and os.path.getsize(error_log_path) > 0:
        result["status"] = "ERROR"
        result["error_details"] = "slurm_error.log non è vuoto."
        # Non ritorniamo subito, potremmo voler comunque vedere i dati se ci sono

    # 3. Leggi e analizza i dati di performance
    data_path = os.path.join(dir_path, 'data_app_0.csv')
    if not os.path.exists(data_path):
        result["status"] = "FAILURE"
        result["error_details"] += " File data_app_0.csv mancante."
        return result

    try:
        df = pd.read_csv(data_path)
        if df.empty:
            result["status"] = "FAILURE"
            result["error_details"] += " data_app_0.csv è vuoto."
            return result
        
        # Estrai la prima colonna di dati, indipendentemente dal suo nome
        perf_data = df.iloc[:, 0].dropna()
        
        result["mean_us"] = perf_data.mean()
        result["std_us"] = perf_data.std()
        
        # Se non avevamo trovato errori prima, lo stato è OK
        if result["status"] == "UNKNOWN":
            result["status"] = "OK"

    except pd.errors.EmptyDataError:
        result["status"] = "FAILURE"
        result["error_details"] += " data_app_0.csv non contiene dati."
    except Exception as e:
        result["status"] = "FAILURE"
        result["error_details"] += f" Errore nel leggere il CSV: {e}"
        
    return result

def print_report(results):
    """Stampa un report formattato a partire dalla lista di risultati."""
    
    weak_scaling_results = sorted(
        [r for r in results if r["test_type"] == "weak_scaling"], 
        key=lambda x: x["nodes"]
    )
    
    msg_scaling_results = sorted(
        [r for r in results if r["test_type"] == "message_scaling"],
        key=lambda x: x["msg_size_bytes"]
    )
    
    failed_runs = [r for r in results if r["status"] in ["FAILURE", "ERROR"]]

    print("\n" + "="*80)
    print(" " * 25 + "REPORT DIAGNOSTICA FASE 1")
    print("="*80)

    # Report Weak Scaling
    print("\n--- Test 1.1: Weak Scaling (Messaggio Fisso, Nodi Crescenti) ---")
    if weak_scaling_results:
        print(f"{'NODI':<8} | {'STATUS':<10} | {'TEMPO MEDIO (µs)':<20} | {'DEV. STD. (µs)':<20}")
        print("-" * 70)
        for r in weak_scaling_results:
            mean_str = f"{r['mean_us']:.2f}" if r['mean_us'] is not None else "N/A"
            std_str = f"{r['std_us']:.2f}" if r['std_us'] is not None else "N/A"
            print(f"{r['nodes']:<8} | {r['status']:<10} | {mean_str:<20} | {std_str:<20}")
    else:
        print("Nessun risultato trovato per il test di weak scaling.")

    # Report Message Scaling
    print("\n--- Test 1.2: Message Size Scaling (Nodi Fissi, Messaggio Crescente) ---")
    if msg_scaling_results:
        print(f"{'MSG SIZE':<12} | {'STATUS':<10} | {'TEMPO MEDIO (µs)':<20} | {'DEV. STD. (µs)':<20}")
        print("-" * 75)
        for r in msg_scaling_results:
            msg_str = format_bytes(r['msg_size_bytes'])
            mean_str = f"{r['mean_us']:.2f}" if r['mean_us'] is not None else "N/A"
            std_str = f"{r['std_us']:.2f}" if r['std_us'] is not None else "N/A"
            print(f"{msg_str:<12} | {r['status']:<10} | {mean_str:<20} | {std_str:<20}")
    else:
        print("Nessun risultato trovato per il test di message size scaling.")

    # Riepilogo Errori
    if failed_runs:
        print("\n" + "!"*80)
        print(" " * 28 + "RIEPILOGO ERRORI E FALLIMENTI")
        print("!"*80)
        print("Controllare manualmente le seguenti directory:")
        for r in failed_runs:
            print(f"  -> Directory: {r['dir_name']}")
            print(f"     Status: {r['status']} - Dettagli: {r['error_details']}\n")

    print("\n" + "="*80)
    print("Analisi completata.")
    print("="*80)


def main():
    parser = argparse.ArgumentParser(description="Analizza i risultati della Fase 1 dei benchmark CRAB.")
    parser.add_argument("data_directory", type=str, help="La directory radice contenente tutte le cartelle dei risultati (es. data/leonardo).")
    args = parser.parse_args()

    root_dir = args.data_directory
    if not os.path.isdir(root_dir):
        print(f"Errore: La directory '{root_dir}' non è stata trovata.", file=sys.stderr)
        sys.exit(1)

    all_results = []
    print(f"Scansione della directory: {root_dir}")
    
    # Itera su tutte le sottodirectory nella cartella dei risultati
    for dir_name in os.listdir(root_dir):
        dir_path = os.path.join(root_dir, dir_name)
        if os.path.isdir(dir_path):
            print(f"  -> Analizzando {dir_name}...")
            result = analyze_run_directory(dir_path)
            all_results.append(result)

    if not all_results:
        print("Nessuna directory di risultati trovata da analizzare.")
        return
        
    print_report(all_results)

if __name__ == "__main__":
    main()
