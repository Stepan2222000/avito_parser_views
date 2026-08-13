"""Настройки: переменные окружения, файл .env и значения по умолчанию.

Пока здесь то, что нужно первым двум этапам: очередь артикулов и обход выдачи. Сроки
переобхода и порог переключения на карточки появятся вместе со своими этапами.

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
    склад_dsn: str              # база библиотеки: объявления, наблюдения, находки
    smart_dsn: str              # источник артикулов, только чтение
    прокси: Path
    путь: str                   # категория, в которой ищем артикул
    сортировка: str | None      # порядок листания выдачи
    воркеров: int
    страниц: int                # предел страниц на артикул
    попыток: int
    остывание: float            # первая проба заблокированного адреса
    проба: float                # как часто перепроверять, если блок держится
    молчание: float             # отдых адреса, который не отвечает
    зависла_через: float        # когда «в работе» считать брошенной
    дней: int                   # переобход карточек, которых не касались столько дней
    журнал: Path
    дампы: Path                 # куда складывать страницы сломавшихся задач


def _обязательный(имя: str) -> str:
    значение = os.environ.get(имя)
    if not значение:
        raise SystemExit(f"не задан {имя}: заполните .env по образцу .env.example")
    return значение


def _число(имя: str, по_умолчанию: float) -> float:
    значение = os.environ.get(имя)
    try:
        return type(по_умолчанию)(значение) if значение else по_умолчанию
    except ValueError:
        return по_умолчанию


def настройки(**переопределения) -> Настройки:
    подхватить_env()
    сортировка = os.environ.get("SORT", "по дате")
    готовые = Настройки(
        очередь_dsn=_обязательный("QUEUE_DSN"),
        склад_dsn=_обязательный("AVITO_DSN"),
        smart_dsn=_обязательный("SMART_DSN"),
        прокси=Path(os.environ.get("PROXIES", str(КОРЕНЬ / "proxies.txt"))),
        путь=os.environ.get("CATALOG_PATH", "/all/zapchasti_i_aksessuary"),
        сортировка=сортировка or None,       # пустое значение — листать как отдаёт Авито
        воркеров=int(_число("WORKERS", 40)),
        страниц=int(_число("PAGES", 100)),
        попыток=int(_число("ATTEMPTS", 3)),
        остывание=_число("COOLDOWN", 900.0),
        проба=_число("COOLDOWN_RETRY", 300.0),
        молчание=_число("QUIET_COOLDOWN", 60.0),
        зависла_через=_число("STALE_AFTER", 1800.0),
        дней=int(_число("REFRESH_DAYS", 7)),
        журнал=Path(os.environ.get("LOG", str(КОРЕНЬ / "logs" / "обход.log"))),
        дампы=Path(os.environ.get("DUMPS", str(КОРЕНЬ / "dumps"))),
    )
    return dataclasses.replace(готовые, **переопределения) if переопределения else готовые
