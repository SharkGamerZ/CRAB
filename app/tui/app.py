from textual.app import App, ComposeResult
from textual.containers import Container, Vertical
from textual.widgets import (
    Button, Static, Header, Footer, RichLog
)
from textual import events
from textual.reactive import reactive
import json
import os

from .constants import SECTIONS
from .widgets.benchmark_setup import BenchmarkSetup
from .widgets.tab_selector import TabSelector
from .widgets.application_form import ApplicationForm
from .widgets.environment_settings import EnvironmentSettings

class BenchmarkApp(App):
    CSS_PATH = "assets/tui.tcss"
    BINDINGS = [("q", "quit", "Quit"), ("l", "load", "Load"), ("s", "save", "Save")]

    current_tab = reactive(0)

    def __init__(self):
        super().__init__()
        self.global_benchmark_states = {0: {"path": "", "args": "", "collect": False, "start": "", "end": ""}}
        self.current_environment_settings = self._load_default_env()
        
        self.benchmark_container = BenchmarkSetup(self)
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
        yield TabSelector(id="tab-selector")
        with Container(id="main-content-area"):
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
        if self.benchmark_container and hasattr(self.benchmark_container, 'save_current_form_state'):
            self.benchmark_container.save_current_form_state()

    def show_tab(self, index: int):
        if self.current_tab == 0 and index != 0:
            self.save_benchmark_state()
        self.current_tab = index
        self.benchmark_container.display = (index == 0)
        self.env_container.display = (index == 1)
        self.log_container.display = (index == 2)
        self.update_tabs()

    def update_tabs(self):
        for i, tab_button in enumerate(self.query(".tab")):
            tab_button.variant = "primary" if i == self.current_tab else "default"

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id and event.button.id.startswith("tab-"):
            index = int(event.button.id.split("-")[1])
            self.show_tab(index)
            event.stop()

    def key_space(self) -> None:
        self.benchmark_container.forms_container.query_one(ApplicationForm).run_benchmark()

    def key_l(self) -> None:
        self.benchmark_container.forms_container.query_one(ApplicationForm).load_form_data()

    def key_s(self) -> None:
        self.benchmark_container.forms_container.query_one(ApplicationForm).save_form_data()

    def key_escape(self) -> None:
        self.query().blur()
