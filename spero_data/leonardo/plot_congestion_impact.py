import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import argparse
import os

def load_and_prepare_data(filepath, condition_label):
    """
    Carica i dati da un file CSV, estrae la colonna di performance corretta
    e la converte in microsecondi.
    """
    try:
        df = pd.read_csv(filepath)
        column_name = '0_Avg-Duration_s'
        if column_name not in df.columns:
            print(f"ERRORE: La colonna '{column_name}' non è stata trovata in {filepath}.")
            print(f"Colonne disponibili: {list(df.columns)}")
            exit(1)
        
        durations_in_microseconds = df[column_name].dropna() * 1_000_000

        prepared_df = pd.DataFrame({
            'Duration (µs)': durations_in_microseconds,
            'Condition': condition_label
        })
        return prepared_df
    except FileNotFoundError:
        print(f"ERRORE: File non trovato: {filepath}")
        exit(1)
    except Exception as e:
        print(f"ERRORE: Impossibile leggere il file {filepath}. Dettagli: {e}")
        exit(1)

def create_violin_plot(combined_df, title, output_filename, congestion_impact):
    """Crea e salva il violin plot."""
    sns.set_theme(style="whitegrid", font_scale=1.1)
    fig, ax = plt.subplots(figsize=(10, 8))

    sns.violinplot(
        data=combined_df,
        x='Condition',
        y='Duration (µs)',
        ax=ax,
        palette=["#56B4E9", "#D55E00"],
        cut=0,
        inner='box'
    )
    
    ax.set_yscale('log')
    ax.set_title(title, fontsize=16, weight='bold', pad=20)
    ax.set_xlabel("Condizione dell'Esperimento", fontsize=12, labelpad=15)
    ax.set_ylabel("Durata per Iterazione (microsecondi, µs) - Scala Logaritmica", fontsize=12, labelpad=10)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=0, ha='center')
    
    ci_text = f"Congestion Impact (mediana): {congestion_impact:.2f}x"
    ax.text(0.95, 0.95, ci_text,
            transform=ax.transAxes,
            fontsize=13,
            fontweight='bold',
            verticalalignment='top',
            horizontalalignment='right',
            bbox=dict(boxstyle='round,pad=0.5', fc='wheat', alpha=0.7))

    plt.tight_layout()
    plt.savefig(output_filename, dpi=300)
    print(f"Grafico a violino salvato con successo in: {os.path.abspath(output_filename)}")
    plt.close(fig) # Chiude la figura per liberare memoria

def create_line_plot(df_isolated, df_congested, title, output_filename):
    """Crea e salva il grafico a linee."""
    sns.set_theme(style="whitegrid", font_scale=1.1)
    fig, ax = plt.subplots(figsize=(12, 7))

    # Aggiunge un indice per l'asse X che rappresenta il "tempo" (ordine di misurazione)
    df_isolated['Measurement #'] = range(len(df_isolated))
    df_congested['Measurement #'] = range(len(df_congested))

    sns.lineplot(data=df_isolated, x='Measurement #', y='Duration (µs)', ax=ax, label='Isolated', color="#56B4E9")
    sns.lineplot(data=df_congested, x='Measurement #', y='Duration (µs)', ax=ax, label='Congested', color="#D55E00")

    ax.set_yscale('log')
    ax.set_title(f"{title} - Andamento Temporale", fontsize=16, weight='bold', pad=20)
    ax.set_xlabel("Numero della Misurazione", fontsize=12, labelpad=10)
    ax.set_ylabel("Durata per Iterazione (microsecondi, µs) - Scala Logaritmica", fontsize=12, labelpad=10)
    ax.legend(title='Condizione')

    plt.tight_layout()
    plt.savefig(output_filename, dpi=300)
    print(f"Grafico a linee salvato con successo in: {os.path.abspath(output_filename)}")
    plt.close(fig)

def main():
    parser = argparse.ArgumentParser(
        description="Genera grafici per confrontare le performance di un benchmark.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("isolated_csv", help="Percorso del file CSV del test in isolamento.")
    parser.add_argument("congested_csv", help="Percorso del file CSV del test in congestione.")
    parser.add_argument("-o", "--output_prefix", default="tesi_plot", help="Prefisso per i nomi dei file di output.")
    parser.add_argument("-t", "--title", default="Impatto della Congestione di Rete su MPI_Allreduce (8 Nodi Victim)", help="Titolo base per i grafici.")
    args = parser.parse_args()

    # Carica e prepara i dati
    df_isolated = load_and_prepare_data(args.isolated_csv, 'Isolated (8 Nodi)')
    df_congested = load_and_prepare_data(args.congested_csv, 'Congested (8 Victim + 24 Aggressor)')
    
    combined_df = pd.concat([df_isolated, df_congested], ignore_index=True)

    # Calcola statistiche
    median_isolated = df_isolated['Duration (µs)'].median()
    median_congested = df_congested['Duration (µs)'].median()
    
    if median_isolated > 0:
        congestion_impact = median_congested / median_isolated
    else:
        congestion_impact = float('inf')

    print("\n--- Statistiche di Performance ---")
    print(f"Durata Mediana - Isolato:    {median_isolated:.2f} µs")
    print(f"Durata Mediana - Congestionato: {median_congested:.2f} µs")
    print(f"Fattore di rallentamento (Congestion Impact): {congestion_impact:.2f}x")
    print("-" * 34)

    # Crea e salva entrambi i grafici
    create_violin_plot(combined_df, args.title, f"{args.output_prefix}_violin.png", congestion_impact)
    create_line_plot(df_isolated, df_congested, args.title, f"{args.output_prefix}_line.png")

if __name__ == "__main__":
    main()
