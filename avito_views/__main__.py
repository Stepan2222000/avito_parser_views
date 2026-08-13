"""Командная строка.

Пока сделан первый этап — очередь артикулов:

    python -m avito_views init                 — применить схему очереди
    python -m avito_views load [--limit N]     — залить артикулы из smart в очередь
    python -m avito_views status               — что сейчас в очереди

Обход, адреса и переобход добавляются следующими этапами, по мере того как договоримся
об алгоритмах.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging

from avito_views.config import настройки as собрать_настройки
from avito_views.db import Очередь
from avito_views.log import настроить

лог = logging.getLogger("команда")


async def init(настройки, _) -> None:
    очередь = Очередь(настройки.очередь_dsn)
    try:
        await очередь.применить_схему()
    finally:
        await очередь.закрыть()
    лог.info("схема очереди применена в %s", _база(настройки.очередь_dsn))


async def load(настройки, доводы) -> None:
    очередь = Очередь(настройки.очередь_dsn)
    try:
        итог = await очередь.залить_артикулы(настройки.smart_dsn, настройки.путь,
                                             предел=доводы.limit, заново=доводы.again)
    finally:
        await очередь.закрыть()
    лог.info("артикулы: %s", итог)


async def status(настройки, _) -> None:
    очередь = Очередь(настройки.очередь_dsn)
    try:
        сводка = await очередь.сводка()
    finally:
        await очередь.закрыть()
    print(json.dumps(сводка, ensure_ascii=False, indent=2, default=str))


КОМАНДЫ = {"init": init, "load": load, "status": status}


def _база(dsn: str) -> str:
    return dsn.rsplit("/", 1)[-1]


def разобрать_доводы():
    р = argparse.ArgumentParser(prog="avito_views", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    р.add_argument("команда", choices=sorted(КОМАНДЫ))
    р.add_argument("--limit", type=int, default=None,
                   help="load: сколько артикулов залить")
    р.add_argument("--again", action="store_true",
                   help="load: вернуть в очередь и те артикулы, что уже обошли")
    р.add_argument("--verbose", action="store_true", help="подробный журнал")
    return р.parse_args()


def main() -> None:
    доводы = разобрать_доводы()
    настройки = собрать_настройки()
    настроить(настройки.журнал, доводы.verbose)
    asyncio.run(КОМАНДЫ[доводы.команда](настройки, доводы))


if __name__ == "__main__":
    main()
