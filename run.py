# run.py
import argparse
import json
import os
import subprocess
import sys
from typing import Dict, Any

def load_environment_config(preset_arg: str, presets_file_path: str) -> Dict[str, Any]:
    """
    Carica la configurazione dell'ambiente da un nome di preset o da un file.

    Args:
        preset_arg: Il valore dell'argomento --preset (un nome o un path a un file).
        presets_file_path: Il percorso del file JSON dei preset centralizzati.

    Returns:
        Un dizionario con le variabili d'ambiente da impostare.
    """
    env_config = {}

    # Caso 1: L'argomento è un path a un file JSON
    if os.path.exists(preset_arg) and preset_arg.endswith('.json'):
        print(f"Info: Caricamento dell'ambiente dal file: {preset_arg}")
        with open(preset_arg, 'r') as f:
            env_config = json.load(f)
        # Aggiungiamo BLINK_SYSTEM basato sul nome del file per coerenza
        preset_name = os.path.splitext(os.path.basename(preset_arg))[0]
        if "BLINK_SYSTEM" not in env_config:
            env_config["BLINK_SYSTEM"] = preset_name
        return env_config

    # Caso 2: L'argomento è un nome di preset
    print(f"Info: Caricamento del preset '{preset_arg}' da {presets_file_path}")
    try:
        with open(presets_file_path, 'r') as f:
            all_presets = json.load(f)
    except FileNotFoundError:
        print(f"[Errore] File dei preset non trovato in: {presets_file_path}", file=sys.stderr)
        sys.exit(1)

    # Unisci le variabili comuni (_common) con quelle specifiche del preset
    base_env = all_presets.get("_common", {})
    
    preset_env = all_presets.get(preset_arg)
    if preset_env is None:
        print(f"[Errore] Preset '{preset_arg}' non trovato in {presets_file_path}", file=sys.stderr)
        sys.exit(1)
        
    env_config = {**base_env, **preset_env}

    # Per coerenza con la TUI, imposta BLINK_SYSTEM con il nome del preset
    # se non è già definito esplicitamente nel preset stesso.
    if "BLINK_SYSTEM" not in env_config:
        env_config["BLINK_SYSTEM"] = preset_arg

    return env_config


def prepare_execution_environment(env_config: Dict[str, Any]) -> Dict[str, str]:
    """
    Prepara il dizionario completo dell'ambiente di esecuzione.

    Args:
        env_config: Il dizionario delle variabili d'ambiente specifiche del benchmark.

    Returns:
        Un dizionario dell'ambiente pronto per essere passato a subprocess.
    """
    # Parti dall'ambiente corrente per ereditare variabili come PATH
    execution_env = os.environ.copy()

    # Sostituisci i placeholder e aggiorna l'ambiente
    processed_config = {}
    for key, value in env_config.items():
        if isinstance(value, str):
            # Sostituisci placeholder custom come __CWD__
            value = value.replace("__CWD__", os.getcwd())
        processed_config[key] = str(value) # Assicurati che tutti i valori siano stringhe
    
    execution_env.update(processed_config)

    # Esegui l'espansione delle variabili (es. $HOME, ${LD_LIBRARY_PATH})
    # Questo va fatto dopo che tutte le variabili sono state aggiunte
    final_env = {}
    for key, value in execution_env.items():
        final_env[key] = os.path.expandvars(value)

    return final_env


def main():
    parser = argparse.ArgumentParser(
        description="Punto di ingresso per l'esecuzione dei benchmark tramite CLI.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "app_config_file",
        help="Path al file JSON di configurazione delle applicazioni e opzioni globali."
    )
    parser.add_argument(
        "-p", "--preset",
        default="local",
        help="Nome del preset da usare (da presets.json) o path a un file di ambiente JSON.\nDefault: 'local'."
    )
    parser.add_argument(
        "--presets-file",
        default="presets.json",
        help="Path al file JSON contenente tutti i preset.\nDefault: 'presets.json'."
    )
    args = parser.parse_args()

    # 1. Carica la configurazione dell'ambiente
    env_config = load_environment_config(args.preset, args.presets_file)

    # 2. Prepara l'ambiente di esecuzione completo
    execution_env = prepare_execution_environment(env_config)

    # 3. Costruisci ed esegui il comando per runner.py
    command = ["python", "-u", "runner.py", args.app_config_file]
    
    print("-" * 50)
    print(f"Avvio di runner.py con il preset '{args.preset}'...")
    print(f"Comando: {' '.join(command)}")
    print("-" * 50)

    try:
        # Avvia il sottoprocesso con l'ambiente personalizzato
        process = subprocess.Popen(
            command,
            env=execution_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )

        # Stampa l'output in tempo reale
        for line in iter(process.stdout.readline, ''):
            if line:
                sys.stdout.write(line)
        
        process.wait()
        print("-" * 50)
        print(f"Benchmark terminato con codice di uscita: {process.returncode}")
        print("-" * 50)

    except FileNotFoundError:
        print(f"[Errore] Comando 'python' o 'runner.py' non trovato. Assicurati che siano nel tuo PATH.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[Errore] Si è verificato un errore imprevisto: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
