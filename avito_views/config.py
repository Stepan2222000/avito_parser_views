"""Настройки: переменные окружения, файл .env и значения по умолчанию.

Три базы намеренно разные: артикулы читаются из продуктовой smart, объявления и
просмотры пишет библиотека в свою avito_data, а очереди — наши, в avito_parser_views.
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
    склад_dsn: str              # база библиотеки: объявления, просмотры, наблюдения
    smart_dsn: str              # источник артикулов, только чтение
    прокси: Path
    путь: str                   # категория, в которой ищем артикул
    воркеров: int
    страниц: int                # предел страниц на артикул
    попыток: int
    порог_карточек: int         # с этого числа ждущих карточек воркеры берут карточки
    остывание: float            # первая проба выбывшего адреса
    остывание_предел: float     # докуда растёт отдых, если адрес всё ещё в блоке
    молчание: float             # отдых адреса, который не отвечает
    зависла_через: float        # когда «в работе» считать брошенной
    дней: int                   # переобход карточек старше этого возраста
    журнал: Path

    @property
    def каталожный_путь(self) -> str:
        return self.путь


def _число(имя: str, по_умолчанию: float) -> float:
    значение = os.environ.get(имя)
    try:
        return type(по_умолчанию)(значение) if значение else по_умолчанию
    except ValueError:
        return по_умолчанию


def _обязательный(имя: str) -> str:
    """Адреса баз держим только в окружении: репозиторий публичный."""
    значение = os.environ.get(имя)
    if not значение:
        raise SystemExit(f"не задан {имя}: заполните .env по образцу .env.example")
    return значение


def настройки(**переопределения) -> Настройки:
    подхватить_env()
    готовые = Настройки(
        очередь_dsn=_обязательный("QUEUE_DSN"),
        склад_dsn=_обязательный("AVITO_DSN"),
        smart_dsn=_обязательный("SMART_DSN"),
        прокси=Path(os.environ.get("PROXIES", str(КОРЕНЬ / "proxies.txt"))),
        путь=os.environ.get("CATALOG_PATH", "/all/zapchasti_i_aksessuary"),
        воркеров=int(_число("WORKERS", 40)),
        страниц=int(_число("PAGES", 100)),
        попыток=int(_число("ATTEMPTS", 3)),
        порог_карточек=int(_число("CARDS_THRESHOLD", 100)),
        остывание=_число("COOLDOWN", 900.0),
        остывание_предел=_число("COOLDOWN_MAX", 2400.0),
        молчание=_число("QUIET_COOLDOWN", 60.0),
        зависла_через=_число("STALE_AFTER", 1800.0),
        дней=int(_число("REFRESH_DAYS", 7)),
        журнал=Path(os.environ.get("LOG", str(КОРЕНЬ / "logs" / "обход.log"))),
    )
    # Библиотека берёт свой адрес базы из окружения, если ей не передали Store явно.
    os.environ.setdefault("AVITO_DSN", готовые.склад_dsn)
    return dataclasses.replace(готовые, **переопределения) if переопределения else готовые
