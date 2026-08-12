"""Командная строка обхода.

    python -m avito_views init                 — применить схемы и вьюхи для отчётов
    python -m avito_views load [--limit N]     — залить артикулы из smart в очередь
    python -m avito_views run [--limit N]      — обход: каталоги и карточки вперемешку
    python -m avito_views refresh [--days 7]   — вернуть в очередь давние карточки и обойти
    python -m avito_views catalogs             — пустить каталоги заново и добрать новое
    python -m avito_views status               — что в очередях и что уже собрано
    python -m avito_views reset                — вернуть в очередь брошенные задачи
    python -m avito_views proxies              — состояние пула адресов
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import statistics
import time
from collections import Counter
from pathlib import Path

import asyncpg
from avito import Склад, barriers, detect

from avito_views.config import настройки as собрать_настройки
from avito_views.db import Очередь
from avito_views.log import настроить
from avito_views.proxies import Адрес, прочитать
from avito_views.work import обойти

лог = logging.getLogger("команда")

ПРОБНАЯ_ССЫЛКА = "https://www.avito.ru{путь}?q=8M0111544"


async def init(настройки, _) -> None:
    склад = Склад(настройки.склад_dsn)
    try:
        await склад.применить_схему()
        вьюхи = (Path(__file__).parent / "вьюхи.sql").read_text(encoding="utf-8")
        async with (await склад.пул()).acquire() as с:
            await с.execute(вьюхи)
    finally:
        await склад.закрыть()
    очередь = Очередь(настройки.очередь_dsn)
    try:
        await очередь.применить_схему()
    finally:
        await очередь.закрыть()
    лог.info("схема библиотеки и вьюхи применены в %s, очереди — в %s",
             _база(настройки.склад_dsn), _база(настройки.очередь_dsn))


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


async def refresh(настройки, доводы) -> None:
    очередь = Очередь(настройки.очередь_dsn)
    try:
        сколько = await очередь.переочередить(доводы.days)
    finally:
        await очередь.закрыть()
    лог.info("в очередь на переобход вернулось карточек: %d (старше %d дней)",
             сколько, доводы.days)
    if not доводы.only_queue:
        await обойти(настройки, предел=доводы.limit, воркеров=доводы.workers)


async def catalogs(настройки, доводы) -> None:
    очередь = Очередь(настройки.очередь_dsn)
    try:
        сколько = await очередь.переочередить_артикулы()
    finally:
        await очередь.закрыть()
    лог.info("артикулов вернулось в очередь: %d", сколько)
    if not доводы.only_queue:
        await обойти(настройки, предел=доводы.limit, воркеров=доводы.workers)


async def status(настройки, _) -> None:
    очередь = Очередь(настройки.очередь_dsn)
    try:
        сводка = await очередь.сводка()
    finally:
        await очередь.закрыть()
    сводка["собрано в базе библиотеки"] = await _что_в_складе(настройки.склад_dsn)
    print(json.dumps(сводка, ensure_ascii=False, indent=2, default=str))


async def _что_в_складе(dsn: str) -> dict:
    """Цифры собранного читаем из базы библиотеки: у себя мы их не держим."""
    с = await asyncpg.connect(dsn)
    try:
        строка = await с.fetchrow(
            """select (select count(*) from объявления) объявлений,
                      (select count(*) from объявления where снято) снятых,
                      (select count(*) from наблюдения) наблюдений,
                      (select count(*) from объявления where просмотров_всего is not null)
                          с_просмотрами,
                      (select coalesce(sum(просмотров_всего), 0) from объявления) просмотров,
                      (select max(просмотров_всего) from объявления) максимум""")
        return dict(строка) if строка else {}
    except asyncpg.UndefinedTableError:
        return {"схема не применена": True}
    finally:
        await с.close()


async def reset(настройки, доводы) -> None:
    очередь = Очередь(настройки.очередь_dsn)
    try:
        итог = await очередь.сбросить_зависшие(доводы.stale or настройки.зависла_через)
    finally:
        await очередь.закрыть()
    лог.info("возвращено в очередь: %s", итог)


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


КОМАНДЫ = {"init": init, "load": load, "run": run, "refresh": refresh,
           "catalogs": catalogs, "status": status, "reset": reset, "proxies": proxies}


def _база(dsn: str) -> str:
    return dsn.rsplit("/", 1)[-1]


def разобрать_доводы():
    р = argparse.ArgumentParser(prog="avito_views", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    р.add_argument("команда", choices=sorted(КОМАНДЫ))
    р.add_argument("--limit", type=int, default=None,
                   help="сколько задач взять (для load — сколько артикулов залить)")
    р.add_argument("--workers", type=int, default=None, help="сколько воркеров запустить")
    р.add_argument("--days", type=int, default=None,
                   help="возраст карточек для переобхода, по умолчанию из настроек")
    р.add_argument("--again", action="store_true",
                   help="load: вернуть в очередь и те артикулы, что уже были")
    р.add_argument("--only-queue", action="store_true",
                   help="refresh и catalogs: только наполнить очередь, не обходить")
    р.add_argument("--stale", type=float, default=None,
                   help="reset: возраст брошенной задачи в секундах")
    р.add_argument("--verbose", action="store_true", help="подробный журнал")
    return р.parse_args()


def main() -> None:
    доводы = разобрать_доводы()
    настройки = собрать_настройки()
    настроить(настройки.журнал, доводы.verbose)
    if доводы.days is None:
        доводы.days = настройки.дней
    asyncio.run(КОМАНДЫ[доводы.команда](настройки, доводы))


if __name__ == "__main__":
    main()
