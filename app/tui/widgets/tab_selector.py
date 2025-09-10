from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.widgets import Button

# Importa la costante dal file dedicato, rompendo il ciclo.
from ..constants import SECTIONS

class TabSelector(Horizontal):
    def compose(self) -> ComposeResult:
        for index, section in enumerate(SECTIONS):
            yield Button(section, id=f"tab-{index}", classes="tab")
