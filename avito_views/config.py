"""Настройки: переменные окружения, файл .env и значения по умолчанию.

Пока здесь только то, что нужно первому этапу — очереди артикулов. Остальное появится
вместе с обходом: адреса, число воркеров, пределы страниц и сроки переобхода.

Адреса баз держим только в окружении: репозиторий публичный.
"""
from __future__ import annotations

import dataclasses
import os
from pathlib import Path

from avito_views import КОРЕНЬ


def подхватить_env(файл: Path | None = None) -> None:
    """Прочитать .env, не затирая то, что уже задано в окружении."""
    файл = файл or КОРЕНЬ / ".env"
    if not файл.exists():
        return
    for строка in файл.read_text(encoding="utf-8").splitlines():
        строка = строка.strip()
        if not строка or строка.startswith("#") or "=" not in строка:
            continue
        ключ, значение = строка.split("=", 1)
        os.environ.setdefault(ключ.strip(), значение.strip().strip("\"'"))


@dataclasses.dataclass(frozen=True, slots=True)
class Настройки:
    очередь_dsn: str            # наши очереди
    smart_dsn: str              # источник артикулов, только чтение
    путь: str                   # категория, в которой ищем артикул
    журнал: Path


def _обязательный(имя: str) -> str:
    значение = os.environ.get(имя)
    if not значение:
        raise SystemExit(f"не задан {имя}: заполните .env по образцу .env.example")
    return значение


def настройки(**переопределения) -> Настройки:
    подхватить_env()
    готовые = Настройки(
        очередь_dsn=_обязательный("QUEUE_DSN"),
        smart_dsn=_обязательный("SMART_DSN"),
        путь=os.environ.get("CATALOG_PATH", "/all/zapchasti_i_aksessuary"),
        журнал=Path(os.environ.get("LOG", str(КОРЕНЬ / "logs" / "обход.log"))),
    )
    return dataclasses.replace(готовые, **переопределения) if переопределения else готовые
