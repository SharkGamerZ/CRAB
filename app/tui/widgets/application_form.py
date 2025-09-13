from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import (
    Button, Input, Label, Checkbox, RichLog
)

from textual import on, work
from textual.message import Message

from textual_fspicker import FileSave, FileOpen

from .environment_settings import EnvironmentSettings

from app.core.benchmark_runner import BlinkRunner
from app.core.models import BenchmarkState, AppConfig

import os
import json
import threading
import subprocess


class ApplicationForm(Vertical):
    def __init__(self, app_ref, global_benchmark_states, benchmark_id: int = 0):
        super().__init__()
        self.app_ref = app_ref
        self.global_benchmark_states = global_benchmark_states
        self.benchmark_id = benchmark_id
        self.form_data = {
            "path": "",
            "args": "",
            "collect": False,
            "start": "",
            "end": ""
        }

    def compose(self) -> ComposeResult:
        yield Label("Application Path:")
        yield Label(self.form_data["path"] or "Select a file", id="path")
        yield Button("Browse", id="browse-path", variant="primary", classes="browse-btn")

        yield Label("Arguments:")
        yield Input(placeholder="-msgsize 1048576", id="args", value=self.form_data["args"])

        yield Label("Collect Timings:")
        yield Checkbox("Yes", id="collect", value=self.form_data["collect"])

        yield Label("Start Time (s):")
        yield Input(placeholder="0", id="start", value=self.form_data["start"])

        yield Label("End Time (s), 'f' or empty:")
        yield Input(placeholder="f", id="end", value=self.form_data["end"])

        yield Button("Save", id="save-form", variant="primary", classes="save-btn")
        yield Button("Load", id="load-form", variant="primary", classes="load-btn")
        yield Button("Run Benchmark", id="run-benchmark", variant="success", classes="run-btn")


    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "run-benchmark":
            self.run_benchmark()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id in self.form_data:
            self.form_data[event.input.id] = event.input.value

    def on_checkbox_changed(self, event: Checkbox.Changed) -> None:
        if event.checkbox.id == "collect":
            self.form_data["collect"] = event.checkbox.value

    def get_form_data(self):
        self.form_data["path"] = self.query_one("#path", Label)
        return self.form_data.copy()

    def set_form_data(self, data):
        self.form_data.update(data)
        if self.is_mounted:
            for field_id, value in data.items():
                try:
                    if field_id == "path":
                        widget = self.query_one("#path", Label)
                        widget.update(value)
                        continue

                    widget = self.query_one(f"#{field_id}", (Input, Checkbox))
                    widget.value = value
                except Exception:
                    pass

    @on(Button.Pressed, "#browse-path")
    @work
    async def browse_path(self):
        file_path = await self.app_ref.push_screen_wait(FileOpen())
        if not file_path:
            self.notify("File selection cancelled")
            return
        
        self.query_one("#path", Label).update(str(file_path))

    @on(Button.Pressed, "#save-form")
    @work
    async def save_form_data(self):
        self.app_ref.save_benchmark_state()
        file_name = await self.app_ref.push_screen_wait(FileSave())
        if not file_name:
            self.notify("Save cancelled")
            return
        if not os.path.exists("benchmarks"):
            os.makedirs("benchmarks")
        data_to_save = {}
        for benchmark_id, form_data in self.global_benchmark_states.items():
            form_copy = form_data.copy()
            if form_copy.get("path"):
                form_copy["path"] = os.path.abspath(form_copy["path"])
            data_to_save[str(benchmark_id)] = form_copy
        file_name = str(file_name)
        if not file_name.endswith(".json"):
            file_name += ".json"
        with open(file_name, "w") as json_file:
            json.dump(data_to_save, json_file, indent=4)
        self.notify("Saved")

    @on (Button.Pressed, "#load-form")
    @work
    async def load_form_data(self):
        file_name = await self.app_ref.push_screen_wait(FileOpen())
        if not file_name:
            self.notify("Load cancelled")
            return
        data = {}
        with open(file_name, "r") as json_file:
            try:
                data = json.load(json_file)
            except json.JSONDecodeError:
                self.notify("Invalid JSON format in the file")
                return
        await self.app_ref.applications_container.tab_selector.clear_benchmark_forms()
        self.app_ref.global_benchmark_states.clear()
        for i in range(len(data)):
            self.global_benchmark_states[i] = {
                "path": "", "args": "", "collect": False, "start": "", "end": ""
            }
            self.app_ref.applications_container.tab_selector.add_benchmark()
        for benchmark_id, form_data in data.items():
            self.global_benchmark_states[int(benchmark_id)] = form_data
            if int(benchmark_id) == self.benchmark_id:
                self.set_form_data(form_data)
        self.app_ref.applications_container.tab_selector.update_benchmark_tabs(0)
        self.notify(f"Loaded data from {file_name}")



    def run_benchmark(self):
        # 1. Prepara la TUI: pulisci il log e mostra la tab corretta
        log = self.app.query_one("#runner-log", RichLog)
        log.clear()
        self.app.show_tab(3)

        # 2. Raccogli i dati dalla UI e crea un oggetto stato
        self.global_benchmark_states[self.benchmark_id] = self.get_form_data()
        
        # Converti il dizionario di dati in oggetti AppConfig per il nostro BenchmarkState
        apps_config = {
            bid: AppConfig(**bdata) for bid, bdata in self.global_benchmark_states.items()
        }
        state_to_run = BenchmarkState(apps=apps_config)

        # 3. Definisci la callback che il runner userà per comunicare con la TUI
        def log_to_tui(message: str):
            self.app.call_from_thread(log.write, message)

        # 4. Raccogli le impostazioni dell'ambiente dalla TUI
        # (Questo codice potrebbe già essere presente, assicurati che ci sia)
        from .environment_settings import EnvironmentSettings
        tui_settings = self.app_ref.current_environment_settings.copy()
        selected_preset = self.app.query_one(EnvironmentSettings).current_preset_name
        
        # 5. Importa, istanzia e avvia il runner
        from app.core.benchmark_runner import BlinkRunner # Assicurati che l'import sia presente
        runner = BlinkRunner(state=state_to_run, log_callback=log_to_tui)
        runner.run_in_thread(
            tui_settings=tui_settings,
            selected_preset=selected_preset
        )

