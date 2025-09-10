from textual.app import ComposeResult
from textual.containers import Container
from textual.widgets import Button

from .benchmark_tab_selector import BenchmarkTabSelector
from .application_form import ApplicationForm

class BenchmarkSetup(Container):
    def __init__(self, app_ref):
        super().__init__()
        self.app_ref = app_ref
        self.current_benchmark = 0
        self.forms_container = None

    def compose(self) -> ComposeResult:
        benchmark_count = len(self.app_ref.global_benchmark_states)
        self.tab_selector = BenchmarkTabSelector(benchmark_count)
        yield self.tab_selector

        self.forms_container = Container(id="benchmark-forms-container")
        yield self.forms_container

    def on_mount(self):
        self.show_benchmark(0)
        self.tab_selector.update_benchmark_tabs(0)

    def save_current_form_state(self):
        if (self.forms_container and self.forms_container.is_mounted and
            len(self.forms_container.children) > 0):
            current_form = self.forms_container.children[0]
            if isinstance(current_form, ApplicationForm):
                self.app_ref.global_benchmark_states[self.current_benchmark] = current_form.get_form_data()

    def show_benchmark(self, index: int):
        if index == self.current_benchmark and self.forms_container and self.forms_container.children:
            return
        if hasattr(self, 'forms_container') and self.forms_container and self.forms_container.is_mounted:
            self.save_current_form_state()
        self.current_benchmark = index
        if self.forms_container:
            self.forms_container.remove_children()
        new_form = ApplicationForm(self.app_ref, self.app_ref.global_benchmark_states, benchmark_id=index)
        if self.forms_container:
            self.forms_container.mount(new_form)
        if index in self.app_ref.global_benchmark_states:
            self.call_later(new_form.set_form_data, self.app_ref.global_benchmark_states[index])
        self.tab_selector.update_benchmark_tabs(index)

    def add_benchmark(self):
        self.save_current_form_state()
        new_index = len(self.app_ref.global_benchmark_states)
        self.app_ref.global_benchmark_states[new_index] = {
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
