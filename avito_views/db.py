"""Очереди в PostgreSQL: заливка артикулов, выдача задач воркерам, учёт исходов.

Задачу воркер забирает через `for update skip locked`: воркеров много, а задача должна
достаться одному. Тип задачи выбирается на месте — сначала карточки, если их скопилось
достаточно, иначе следующий артикул. Так один и тот же воркер занят и каталогами, и
карточками, и очередь не распухает.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
import time
from pathlib import Path

import asyncpg

лог = logging.getLogger("очередь")

# Артикулы короче пяти знаков и чисто числовые до шести знаков Авито понимает как
# обычное число и отдаёт всё, где оно встречается: по «0810» находится восемь тысяч
# объявлений вместо пяти. Такие артикулы в обход не берём.
ОТБОР_АРТИКУЛОВ = """
    select distinct article from part_articles
    where length(article) >= 5
      and not (article ~ '^[0-9]+$' and length(article) <= 6)
    order by article
"""


@dataclasses.dataclass(slots=True)
class ЗадачаАртикул:
    артикул: str
    путь: str
    попыток: int


@dataclasses.dataclass(slots=True)
class ЗадачаКарточка:
    номер: int
    попыток: int


class Очередь:
    def __init__(self, dsn: str, *, размер: int = 20):
        self._dsn = dsn
        self._размер = размер
        self._пул: asyncpg.Pool | None = None
        self._карточек_ждёт = 0
        self._когда_считали = 0.0

    async def пул(self) -> asyncpg.Pool:
        if self._пул is None:
            self._пул = await asyncpg.create_pool(self._dsn, min_size=2,
                                                  max_size=self._размер)
        return self._пул

    async def закрыть(self, *, ждать: float = 10.0) -> None:
        if self._пул is None:
            return
        пул, self._пул = self._пул, None
        try:
            await asyncio.wait_for(пул.close(), ждать)
        except (TimeoutError, asyncio.TimeoutError):
            лог.warning("пул соединений не закрылся за %.0f c, обрываем", ждать)
            пул.terminate()

    async def применить_схему(self) -> None:
        схема = (Path(__file__).parent / "schema.sql").read_text(encoding="utf-8")
        async with (await self.пул()).acquire() as с:
            await с.execute(схема)

    # ------------------------------------------------------------------ заливка

    async def залить_артикулы(self, smart_dsn: str, путь: str, *,
                              предел: int | None = None, заново: bool = False) -> dict:
        """Прочитать артикулы из smart и поставить задачи. Читаем только, не пишем."""
        источник = await asyncpg.connect(smart_dsn)
        try:
            строки = await источник.fetch(ОТБОР_АРТИКУЛОВ)
        finally:
            await источник.close()
        артикулы = [р["article"] for р in строки][:предел]

        async with (await self.пул()).acquire() as с:
            новых = await с.fetch(
                """insert into article_tasks (article, path)
                   select а, $2 from unnest($1::text[]) as t(а)
                   on conflict (article) do nothing
                   returning article""", артикулы, путь)
            вернулось = 0
            if заново:
                вернулось = len(await с.fetch(
                    """update article_tasks set status = 'новая', attempts = 0, error = null
                       where article = any($1::text[]) and status <> 'новая'
                       returning article""", артикулы))
        return {"в источнике": len(артикулы), "новых": len(новых), "возвращено": вернулось}

    # ------------------------------------------------------------------ выдача задач

    async def взять_задачу(self, *, порог: int) -> ЗадачаАртикул | ЗадачаКарточка | None:
        """Что делать дальше: карточка, артикул или ничего (тогда воркеру пора выйти)."""
        if await self._карточек_в_очереди() >= порог:
            карточка = await self.взять_карточку()
            if карточка:
                return карточка
        артикул = await self.взять_артикул()
        if артикул:
            return артикул
        return await self.взять_карточку()

    async def _карточек_в_очереди(self) -> int:
        # Считать на каждую задачу дорого и незачем: порог грубый по смыслу.
        if time.monotonic() - self._когда_считали > 2.0:
            async with (await self.пул()).acquire() as с:
                self._карточек_ждёт = await с.fetchval(
                    "select count(*) from item_tasks where status = 'новая'")
            self._когда_считали = time.monotonic()
        return self._карточек_ждёт

    async def взять_артикул(self) -> ЗадачаАртикул | None:
        async with (await self.пул()).acquire() as с:
            строка = await с.fetchrow(
                """update article_tasks t set status = 'в работе', taken_at = now()
                   where t.article = (select article from article_tasks
                                      where status = 'новая'
                                      order by created_at, article
                                      for update skip locked limit 1)
                   returning t.article, t.path, t.attempts""")
        if строка is None:
            return None
        return ЗадачаАртикул(строка["article"], строка["path"], строка["attempts"])

    async def взять_карточку(self) -> ЗадачаКарточка | None:
        async with (await self.пул()).acquire() as с:
            строка = await с.fetchrow(
                """update item_tasks t set status = 'в работе', taken_at = now()
                   where t.item_id = (select item_id from item_tasks
                                      where status = 'новая'
                                      order by parsed_at nulls first, item_id
                                      for update skip locked limit 1)
                   returning t.item_id, t.attempts""")
        if строка is None:
            return None
        self._карточек_ждёт = max(0, self._карточек_ждёт - 1)
        return ЗадачаКарточка(строка["item_id"], строка["attempts"])

    # ------------------------------------------------------------------ исходы артикула

    async def артикул_готов(self, артикул: str, *, всего: int | None,
                            страниц: int | None, объявлений: int, широкий: bool,
                            заметка: str | None = None) -> None:
        async with (await self.пул()).acquire() as с:
            await с.execute(
                """update article_tasks
                   set status = 'готова', done_at = now(), error = $6,
                       found_total = $2, pages_total = $3, items_found = $4, wide = $5
                   where article = $1""",
                артикул, всего, страниц, объявлений, широкий, заметка)

    async def артикул_упал(self, артикул: str, ошибка: str, *, попыток: int) -> str:
        """Вернуть в очередь или признать неудачу, если попытки исчерпаны."""
        async with (await self.пул()).acquire() as с:
            строка = await с.fetchrow(
                """update article_tasks
                   set attempts = attempts + 1, error = $2,
                       status = case when attempts + 1 >= $3 then 'ошибка' else 'новая' end
                   where article = $1
                   returning status""", артикул, ошибка[:500], попыток)
        return строка["status"] if строка else "нет такой"

    async def артикул_вернуть(self, артикул: str) -> None:
        async with (await self.пул()).acquire() as с:
            await с.execute(
                "update article_tasks set status = 'новая' where article = $1", артикул)

    # ------------------------------------------------------------------ объявления

    async def поставить_объявления(self, артикул: str,
                                   найденные: list[tuple[int, int]]) -> int:
        """Связать артикул с объявлениями и поставить новые номера в очередь.

        Возвращает, сколько номеров раньше не встречалось.
        """
        if not найденные:
            return 0
        номера = [н for н, _ in найденные]
        страницы = [с for _, с in найденные]
        async with (await self.пул()).acquire() as с, с.transaction():
            await с.execute(
                """insert into article_items (article, item_id, page)
                   select $1, н, с from unnest($2::bigint[], $3::int[]) as t(н, с)
                   on conflict (article, item_id)
                   do update set last_seen_at = now(), page = excluded.page""",
                артикул, номера, страницы)
            новые = await с.fetch(
                """insert into item_tasks (item_id)
                   select н from unnest($1::bigint[]) as t(н)
                   on conflict (item_id) do nothing
                   returning item_id""", номера)
        self._когда_считали = 0.0
        return len(новые)

    async def карточка_готова(self, номер: int, просмотров: int | None) -> None:
        async with (await self.пул()).acquire() as с:
            await с.execute(
                """update item_tasks
                   set status = 'готова', parsed_at = now(), views = $2, error = null
                   where item_id = $1""", номер, просмотров)

    async def карточка_снята(self, номер: int) -> None:
        async with (await self.пул()).acquire() as с:
            await с.execute(
                """update item_tasks set status = 'снято', parsed_at = now()
                   where item_id = $1""", номер)

    async def карточка_упала(self, номер: int, ошибка: str, *, попыток: int) -> str:
        async with (await self.пул()).acquire() as с:
            строка = await с.fetchrow(
                """update item_tasks
                   set attempts = attempts + 1, error = $2,
                       status = case when attempts + 1 >= $3 then 'ошибка' else 'новая' end
                   where item_id = $1
                   returning status""", номер, ошибка[:500], попыток)
        return строка["status"] if строка else "нет такой"

    async def карточку_вернуть(self, номер: int) -> None:
        async with (await self.пул()).acquire() as с:
            await с.execute(
                "update item_tasks set status = 'новая' where item_id = $1", номер)

    # ------------------------------------------------------------------ обслуживание

    async def переочередить(self, дней: int) -> int:
        """Вернуть в очередь карточки, которых давно не касались: нужны свежие просмотры."""
        async with (await self.пул()).acquire() as с:
            строки = await с.fetch(
                """update item_tasks set status = 'новая', attempts = 0
                   where status = 'готова'
                     and (parsed_at is null or parsed_at < now() - make_interval(days => $1))
                   returning item_id""", дней)
        self._когда_считали = 0.0
        return len(строки)

    async def переочередить_артикулы(self) -> int:
        """Пустить каталоги заново: ищем объявления, которых раньше не было."""
        async with (await self.пул()).acquire() as с:
            строки = await с.fetch(
                """update article_tasks set status = 'новая', attempts = 0, error = null
                   where status in ('готова', 'ошибка') returning article""")
        return len(строки)

    async def сбросить_зависшие(self, через: float) -> dict:
        """Задачи, взятые и брошенные (процесс убили), вернуть в очередь."""
        секунд = int(через)
        async with (await self.пул()).acquire() as с:
            артикулы = await с.fetch(
                """update article_tasks set status = 'новая'
                   where status = 'в работе'
                     and taken_at < now() - make_interval(secs => $1) returning article""",
                секунд)
            карточки = await с.fetch(
                """update item_tasks set status = 'новая'
                   where status = 'в работе'
                     and taken_at < now() - make_interval(secs => $1) returning item_id""",
                секунд)
        self._когда_считали = 0.0
        return {"артикулов": len(артикулы), "карточек": len(карточки)}

    async def сводка(self) -> dict:
        async with (await self.пул()).acquire() as с:
            артикулы = {р["status"]: р["count"] for р in await с.fetch(
                "select status, count(*) from article_tasks group by status")}
            карточки = {р["status"]: р["count"] for р in await с.fetch(
                "select status, count(*) from item_tasks group by status")}
            широких = await с.fetchval(
                "select count(*) from article_tasks where wide")
            связок = await с.fetchval("select count(*) from article_items")
            свежих = await с.fetchval(
                "select count(*) from item_tasks where parsed_at > now() - interval '1 day'")
            просмотры = await с.fetchrow(
                """select count(*) filter (where views is not null) с_просмотрами,
                          coalesce(sum(views), 0) сумма, max(views) максимум
                   from item_tasks""")
        return {"артикулы": артикулы, "широких артикулов": широких,
                "карточки": карточки, "связок артикул-объявление": связок,
                "карточек за сутки": свежих,
                "просмотры": dict(просмотры) if просмотры else {}}
