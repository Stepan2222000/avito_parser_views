"""Журнал и наблюдатель за событиями библиотеки.

Библиотека сама ничего не печатает и страниц не хранит — она отдаёт событие вместе с
телом страницы. Тела держим ровно последнее на воркера: пригодится, когда задача упадёт
на непонятной странице и захочется посмотреть, что пришло.
"""
from __future__ import annotations

import datetime as dt
import logging
import sys
from pathlib import Path

ГРОМКИЕ = {"барьер", "адрес выбыл", "обрыв"}


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


class Наблюдатель:
    """Событие библиотеки — в строку журнала."""

    def __init__(self, кто: str, дампы: Path | None = None):
        self.лог = logging.getLogger(кто)
        self.дампы = дампы
        self.последняя: tuple[str, str] | None = None

    def __call__(self, событие) -> None:
        if событие.тело:
            self.последняя = (событие.ссылка, событие.тело)
        уровень = logging.WARNING if событие.вид in ГРОМКИЕ else logging.INFO
        части = [событие.вид, событие.состояние]
        if событие.код:
            части.append(str(событие.код))
        if событие.объявлений:
            части.append(f"объявлений {событие.объявлений}")
        if событие.подробности:
            части.append(событие.подробности)
        self.лог.log(уровень, "%s | %s | %s", " ".join(ч for ч in части if ч),
                     событие.адрес, событие.ссылка)

    def дамп(self, метка: str) -> Path | None:
        """Сохранить последнюю страницу — только когда разбираемся с падением."""
        if not self.дампы or not self.последняя:
            return None
        self.дампы.mkdir(parents=True, exist_ok=True)
        когда = dt.datetime.now().strftime("%H%M%S")
        куда = self.дампы / f"{метка}-{когда}.html"
        куда.write_text(self.последняя[1], encoding="utf-8")
        return куда
