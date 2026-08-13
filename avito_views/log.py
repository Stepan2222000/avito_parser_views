"""Журнал: одинаковый в консоль и в файл.

Наблюдатель за событиями библиотеки появится вместе с обходом — сейчас смотреть не за чем.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path


def настроить(файл: Path, подробно: bool = False) -> None:
    файл.parent.mkdir(parents=True, exist_ok=True)
    формат = logging.Formatter("%(asctime)s %(levelname).1s %(name)-12s %(message)s",
                               datefmt="%H:%M:%S")
    корень = logging.getLogger()
    корень.setLevel(logging.DEBUG if подробно else logging.INFO)
    корень.handlers.clear()
    for обработчик in (logging.StreamHandler(sys.stdout), logging.FileHandler(файл)):
        обработчик.setFormatter(формат)
        корень.addHandler(обработчик)
