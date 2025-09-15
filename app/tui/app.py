from textual.app import App, ComposeResult
from textual.containers import Container, Vertical
from textual.widgets import (
    Button, Header, Footer, RichLog
)
from textual import on, work
from textual.reactive import reactive
from textual_fspicker import FileSave, FileOpen

import json
import os


from .constants import SECTIONS
from .messages import SaveConfiguration, LoadConfiguration, RunBenchmark
from .widgets.applications_setup import ApplicationSetup
from .widgets.benchmark_options import BenchmarkOptions
from .widgets.tab_selector import TabSelector
from .widgets.application_form import ApplicationForm
from .widgets.environment_settings import EnvironmentSettings

class BenchmarkApp(App):
    CSS_PATH = "assets/tui.tcss"
    BINDINGS = [("q", "quit", "Quit"), ("l", "load", "Load"), ("s", "save", "Save")]

    current_tab = reactive(0)

    def __init__(self):
        super().__init__()
        self.current_environment_settings = self._load_default_env()
        
        self.applications_container = ApplicationSetup(self)
        self.benchmark_container = BenchmarkOptions(self)
        self.env_container = EnvironmentSettings()
        self.log_container = Vertical(
            RichLog(id="runner-log", highlight=True, classes="runner-log-tall"),
            id="log-view-container"
        )

    def _load_default_env(self):
        try:
            with open("presets.json", "r") as f:
                presets = json.load(f)
                # Unisce le variabili comuni con quelle del preset 'local'
                common_vars = presets.get("_common", {})
                local_vars = presets.get("local", {})
                common_vars.update(local_vars)
                return common_vars
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def compose(self) -> ComposeResult:
        yield Header()
        yield TabSelector(id="tab-selector", app_ref=self)
        with Container(id="main-content-area"):
            yield self.applications_container
            yield self.benchmark_container
            yield self.env_container
            yield self.log_container
        yield Footer()

    def on_mount(self):
        self.show_tab(0)

    def on_environment_settings_env_changed(self, message: EnvironmentSettings.EnvChanged):
        """Listen for changes from the environment settings widget."""
        self.current_environment_settings = message.new_env

    def save_benchmark_state(self):
        if self.applications_container and hasattr(self.applications_container, 'save_current_form_state'):
            self.applications_container.save_current_form_state()

    def show_tab(self, index: int):
        if self.current_tab == 0 and index != 0:
            self.save_benchmark_state()
        self.current_tab = index
        self.applications_container.display = (index == 0)
        self.benchmark_container.display = (index == 1)
        self.env_container.display = (index == 2)
        self.log_container.display = (index == 3)
        self.update_tabs()

    def update_tabs(self):
        for i, tab_button in enumerate(self.query(".tab")):
            tab_button.variant = "primary" if i == self.current_tab else "default"

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id and event.button.id.startswith("tab-"):
            index = int(event.button.id.split("-")[1])
            self.show_tab(index)
            event.stop()


    @on(LoadConfiguration)
    def handle_load_request(self) -> None:
        # Qui, nel prossimo passo, metteremo la logica di caricamento
        self.notify("Load requested! (Logic to be implemented)")
    
    @on(RunBenchmark)
    def handle_run_request(self) -> None:
        # Qui, nel prossimo passo, metteremo la logica per l'esecuzione
        self.notify("Run benchmark requested! (Logic to be implemented)")


    def key_space(self) -> None:
        self.applications_container.forms_container.query_one(ApplicationForm).run_benchmark()

    def key_l(self) -> None:
        self.load_form_data()

    def key_s(self) -> None:
        self.save_form_data()

    def key_escape(self) -> None:
        self.query().blur()


    @on(SaveConfiguration)
    @work
    async def save_form_data(self) -> None:
        """
        Handles the request to save the complete configuration.
        Gathers data from all main widgets and saves it to a JSON file.
        """

        # 1. Gather data from the main widgets
        global_options_state = self.benchmark_container.get_state()
        applications_state = self.applications_container.get_state()


        # 2 Build the complete data structure
        data_to_save = {
            "global_options": global_options_state,
            "applications": applications_state
        }


        # 3. Prompt the user for the file where to save the config
        try:
            file_path = await self.push_screen_wait(FileSave())
            if not file_path:
                self.notify("Save cancelled.", severity="warning")
                return

            file_path_str = str(file_path)
            if not file_path_str.endswith(".json"):
                file_path_str += ".json"
            # 4. Save the data to the specified file
            with open(file_path_str, "w") as f:
                json.dump(data_to_save, f, indent=4)
            self.notify(f"Configuration saved to {os.path.basename(file_path_str)}", severity="information")

        except Exception as e:
            self.notify(f"Error saving file: {e}", severity="error")

    @on(LoadConfiguration)
    @work
    async def load_form_data(self) -> None:
        """
        Handles the request to load a complete configuration.
        Loads data from a JSON file and populates the main widgets accordingly.
        """
        try:
            file_path = await self.push_screen_wait(FileOpen())
            if not file_path:
                self.notify("Load cancelled.", severity="warning")
                return

            file_path_str = str(file_path)
            with open(file_path_str, "r") as f:
                data_loaded = json.load(f)

            # Checks if the loaded data contains the expected sections
            if "global_options" not in data_loaded or "applications" not in data_loaded:
                self.notify("Invalid configuration file: missing required sections.", severity="error")
                return

            # Load data into the respective containers
            self.benchmark_container.set_state(data_loaded["global_options"])
            await self.applications_container.set_state(data_loaded["applications"])

            self.notify(f"Configuration loaded from {os.path.basename(file_path_str)}", severity="information")

        except FileNotFoundError:
            self.notify("File not found.", severity="error")
        except json.JSONDecodeError:
            self.notify("Invalid JSON format in the file.", severity="error")
        except Exception as e:
            self.notify(f"Error loading file: {e}", severity="error")



    # @on (Button.Pressed, "#load-form")
    # @work
    # async def load_form_data(self):
    #     file_name = await self.app_ref.push_screen_wait(FileOpen())
    #     if not file_name:
    #         self.notify("Load cancelled")
    #         return
    #     data = {}



    #     with open(file_name, "r") as json_file:
    #         try:
    #             data = json.load(json_file)
    #         except json.JSONDecodeError:
    #             self.notify("Invalid JSON format in the file")
    #             return

    #     await self.app_ref.applications_container.tab_selector.clear_benchmark_forms()
    #     self.app_ref.global_benchmark_states.clear()
    #     for i in range(len(data)):
    #         self.global_benchmark_states[i] = {
    #             "path": "", "args": "", "collect": False, "start": "", "end": ""
    #         }
    #         self.app_ref.applications_container.tab_selector.add_benchmark()

    #     for benchmark_id, form_data in data.items():
    #         self.global_benchmark_states[int(benchmark_id)] = form_data
    #         if int(benchmark_id) == self.benchmark_id:
    #             self.set_form_data(form_data)

    #     self.app_ref.applications_container.tab_selector.update_benchmark_tabs(0)
    #     self.notify(f"Loaded data from {file_name}")
    #
    #
    #
    # def run_benchmark(self):
    #     # 1. Prepara la TUI: pulisci il log e mostra la tab corretta
    #     log = self.app.query_one("#runner-log", RichLog)
    #     log.clear()
    #     self.app.show_tab(3)
    #
    #     # 2. Raccogli i dati dalla UI e crea un oggetto stato
    #     self.global_benchmark_states[self.benchmark_id] = self.get_form_data()
    #
    #     # Converti il dizionario di dati in oggetti AppConfig per il nostro BenchmarkState
    #     apps_config = {
    #         bid: AppConfig(**bdata) for bid, bdata in self.global_benchmark_states.items()
    #     }
    #     state_to_run = BenchmarkState(apps=apps_config)
    #
    #     # 3. Definisci la callback che il runner userà per comunicare con la TUI
    #     def log_to_tui(message: str):
    #         self.app.call_from_thread(log.write, message)
    #
    #     # 4. Raccogli le impostazioni dell'ambiente dalla TUI
    #     # (Questo codice potrebbe già essere presente, assicurati che ci sia)
    #     from .environment_settings import EnvironmentSettings
    #     tui_settings = self.app_ref.current_environment_settings.copy()
    #     selected_preset = self.app.query_one(EnvironmentSettings).current_preset_name
    #
    #     # 5. Importa, istanzia e avvia il runner
    #     from app.core.benchmark_runner import BlinkRunner # Assicurati che l'import sia presente
    #     runner = BlinkRunner(state=state_to_run, log_callback=log_to_tui)
    #     runner.run_in_thread(
    #         tui_settings=tui_settings,
    #         selected_preset=selected_preset
    #     )
    #
