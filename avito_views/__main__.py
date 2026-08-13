"""Командная строка.

Сделано два этапа: очередь артикулов и обход выдачи по ним.

    python -m avito_views init                 — применить схемы: нашу и библиотечную
    python -m avito_views load [--limit N]     — залить артикулы из smart в очередь
    python -m avito_views run [--limit N]      — обойти выдачи по артикулам
    python -m avito_views status                — что сейчас в очередях
    python -m avito_views reset                 — вернуть в очередь брошенные задачи
    python -m avito_views proxies               — состояние пула адресов

Разбор карточек и переобход добавляются следующими этапами.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import statistics
import time
from collections import Counter

from avito import Склад, barriers, detect

from avito_views.config import настройки as собрать_настройки
from avito_views.db import Очередь
from avito_views.log import настроить
from avito_views.proxies import Адрес, прочитать
from avito_views.work import обойти

лог = logging.getLogger("команда")

ПРОБНАЯ_ССЫЛКА = "https://www.avito.ru{путь}?q=8M0111544"


async def init(настройки, _) -> None:
    очередь = Очередь(настройки.очередь_dsn)
    try:
        await очередь.применить_схему()
    finally:
        await очередь.закрыть()
    склад = Склад(настройки.склад_dsn)
    try:
        await склад.применить_схему()
    finally:
        await склад.закрыть()
    лог.info("схемы применены: очереди в %s, данные в %s",
             _база(настройки.очередь_dsn), _база(настройки.склад_dsn))


async def load(настройки, доводы) -> None:
    очередь = Очередь(настройки.очередь_dsn)
    try:
        итог = await очередь.залить_артикулы(настройки.smart_dsn, настройки.путь,
                                             предел=доводы.limit, заново=доводы.again)
    finally:
        await очередь.закрыть()
    лог.info("артикулы: %s", итог)


async def run(настройки, доводы) -> None:
    await обойти(настройки, предел=доводы.limit, воркеров=доводы.workers)


async def status(настройки, _) -> None:
    очередь = Очередь(настройки.очередь_dsn)
    try:
        сводка = await очередь.сводка()
    finally:
        await очередь.закрыть()
    print(json.dumps(сводка, ensure_ascii=False, indent=2, default=str))


async def reset(настройки, доводы) -> None:
    очередь = Очередь(настройки.очередь_dsn)
    try:
        сколько = await очередь.сбросить_зависшие(доводы.stale or настройки.зависла_через)
    finally:
        await очередь.закрыть()
    лог.info("возвращено в очередь артикулов: %d", сколько)


async def proxies(настройки, _) -> None:
    """Один запрос с каждого адреса: кто отдаёт выдачу, кто в блоке, кто не отвечает."""
    список = прочитать(настройки.прокси)
    ссылка = ПРОБНАЯ_ССЫЛКА.format(путь=настройки.путь)
    print(f"адресов {len(список)}, пробуем {ссылка}")

    async def один(имя, url):
        адрес = Адрес(имя, url)
        начало = time.monotonic()
        try:
            ответ = await адрес.сессия.get(ссылка)
            тело = ответ.text
            if detect.заглушка(тело):
                состояние = "заглушка"
            elif detect.барьер(тело):
                состояние = f"барьер:{barriers.вид(тело)}"
            elif detect.каталог(тело) or detect.выдача(тело):
                состояние = "выдача"
            else:
                состояние = f"непонятное({ответ.status_code})"
        except Exception as e:  # noqa: BLE001 — проверка пула, любой обрыв это результат
            состояние = f"обрыв:{type(e).__name__}"
        секунд = round(time.monotonic() - начало, 1)
        try:
            await адрес.сессия.close()
        except Exception:  # noqa: BLE001
            pass
        return имя, состояние, секунд

    итоги = await asyncio.gather(*(один(и, u) for и, u in список))
    состояния = Counter(с for _, с, _ in итоги)
    for состояние, сколько in состояния.most_common():
        print(f"   {состояние}: {сколько}")
    задержки = [з for _, с, з in итоги if not с.startswith("обрыв")]
    if задержки:
        print(f"задержка отвечающих: медиана {statistics.median(задержки):.1f} c, "
              f"максимум {max(задержки):.1f} c")
    годных = sum(к for с, к in состояния.items() if not с.startswith("обрыв"))
    print(f"годных сейчас {годных} из {len(итоги)}")


КОМАНДЫ = {"init": init, "load": load, "run": run, "status": status,
           "reset": reset, "proxies": proxies}


def _база(dsn: str) -> str:
    return dsn.rsplit("/", 1)[-1]


def разобрать_доводы():
    р = argparse.ArgumentParser(prog="avito_views", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    р.add_argument("команда", choices=sorted(КОМАНДЫ))
    р.add_argument("--limit", type=int, default=None,
                   help="run: сколько артикулов обойти; load: сколько залить")
    р.add_argument("--workers", type=int, default=None, help="run: сколько воркеров")
    р.add_argument("--again", action="store_true",
                   help="load: вернуть в очередь и те артикулы, что уже обошли")
    р.add_argument("--stale", type=float, default=None,
                   help="reset: возраст брошенной задачи в секундах")
    р.add_argument("--verbose", action="store_true", help="подробный журнал")
    return р.parse_args()


def main() -> None:
    доводы = разобрать_доводы()
    настройки = собрать_настройки()
    настроить(настройки.журнал, доводы.verbose)
    asyncio.run(КОМАНДЫ[доводы.команда](настройки, доводы))


if __name__ == "__main__":
    main()
