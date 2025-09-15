from textual import work
from textual.app import ComposeResult
from textual.containers import Container
from textual.widgets import Button

from .benchmark_tab_selector import BenchmarkTabSelector
from .application_form import ApplicationForm

class ApplicationSetup(Container):
    def __init__(self, app_ref, id: str | None = None) -> None:
        super().__init__(id=id)
        # app_ref è ancora utile per funzionalità come notifiche o push_screen
        self.app_ref = app_ref

        self.benchmark_states: dict[int, dict] = {
            0: {"path": "", "args": "", "collect": False, "start": "", "end": ""}
        }
        self.current_benchmark = 0
        
        # Inizializza i widget figli
        self.tab_selector = BenchmarkTabSelector(benchmark_count=1)
        self.forms_container = Container(
            ApplicationForm(app_ref=self.app_ref, benchmark_id=0), 
            id="benchmark-forms-container"
        )

    def compose(self) -> ComposeResult:
        yield self.tab_selector
        yield self.forms_container


    def on_mount(self):
        self.show_benchmark(0)
        self.tab_selector.update_benchmark_tabs(0)

    def save_current_form_state(self):
        if (self.forms_container and self.forms_container.is_mounted and
            len(self.forms_container.children) > 0):
            current_form = self.forms_container.children[0]
            if isinstance(current_form, ApplicationForm):
                self.benchmark_states[self.current_benchmark] = current_form.get_form_data()

    def show_benchmark(self, index: int):
        if index == self.current_benchmark:
            return

        self.save_current_form_state()
        self.current_benchmark = index


        self.forms_container.remove_children()
        new_form = ApplicationForm(self.app_ref, benchmark_id=index)
        self.forms_container.mount(new_form)
        self.call_later(new_form.set_form_data, self.benchmark_states[index])

        self.tab_selector.update_benchmark_tabs(index)

    def add_benchmark(self):
        self.save_current_form_state()
        new_index = len(self.benchmark_states)
        self.benchmark_states[new_index] = {
            "path": "", "args": "", "collect": False, "start": "", "end": ""
        }
        self.tab_selector.add_benchmark()
        self.show_benchmark(new_index)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id and event.button.id.startswith("benchmark-"):
            index = int(event.button.id.split("-")[1])
            self.show_benchmark(index)
        elif event.button.id == "add-benchmark":
            self.add_benchmark()
            event.stop()



    def get_state(self) -> dict:
        """
        Collects the current state of all configured applications.
        Returns:
            A dictionary with each benchmark's ID as the key and its state.
        """
        self.save_current_form_state()
        return self.benchmark_states.copy()

    @work
    async def set_state(self, state: dict):
        """
        Imposta lo stato dei benchmark configurati, ricaricando l'intera interfaccia.
        Questo metodo è l'equivalente programmatico di caricare uno stato da un file.

        Args:
            state: Un dizionario con l'ID di ogni benchmark come chiave e il suo stato.
        """
        with open("application_setup.log", "w") as log_file:
            log_file.write(f"Setting new state: {state}\n")
        # 1. Pulisce completamente lo stato e l'interfaccia esistenti.
        self.benchmark_states.clear()
        if self.forms_container:
            self.forms_container.remove_children()
        
        # Pulisce le tab esistenti. Assumiamo che esista un metodo `clear_benchmark_forms`
        # nel tuo BenchmarkTabSelector, come suggerito dal codice originale.
        await self.tab_selector.clear_benchmark_forms()
        
        # 2. Carica il nuovo stato.
        for key, value in state.items():
            self.benchmark_states[int(key)] = value
        
        # Se il nuovo stato è vuoto, l'interfaccia resterà vuota.
        if not self.benchmark_states:
            self.current_benchmark = 0
            return

        # 3. Ricostruisce le tab visuali per ogni elemento nel nuovo stato.
        # Chiamiamo direttamente il metodo del selettore di tab per evitare
        # di alterare lo stato (self.benchmark_states) che abbiamo appena impostato.
        for _ in self.benchmark_states:
            self.tab_selector.add_benchmark()

        # 4. Mostra il form per il primo benchmark (indice 0).
        # La funzione show_benchmark si occuperà di:
        # - Montare il widget ApplicationForm.
        # - Caricare i dati da self.benchmark_states[0] nel form.
        # - Impostare la tab 0 come attiva.
        self.show_benchmark(0)

        self.notify(f"Loaded configuration with {self.benchmark_states} applications.")
