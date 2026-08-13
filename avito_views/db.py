"""Очереди в PostgreSQL: заливка артикулов, выдача задач воркерам, учёт исходов.

Задачу воркер забирает через `for update skip locked`: воркеров много, а задача должна
достаться одному. Тип задачи выбирается на месте — сначала карточки, если их скопилось
достаточно, иначе следующий артикул. Так один и тот же воркер занят и каталогами, и
карточками, и очередь не распухает.

Данных здесь нет: собранное пишет библиотека в свою базу. Здесь только ход работы.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
import time
from pathlib import Path

import asyncpg

лог = logging.getLogger("очередь")

# Короткий номер Авито понимает как просто число и отдаёт всё, где оно встречается: по
# «0810» находится восемь тысяч объявлений вместо пяти, по «12224» — восемьсот. Считаем
# длину по очищенной строке, потому что дефисы Авито всё равно игнорирует: «09-812» для
# него то же самое, что пятизначное число, и мусора он даёт столько же.
ОТБОР_АРТИКУЛОВ = """
    with очищенные as (
        select distinct article,
               regexp_replace(article, '[^A-Za-z0-9]', '', 'g') as чистый
        from part_articles
    )
    select article from очищенные
    where length(чистый) >= 5
      and not (чистый ~ '^[0-9]+$' and length(чистый) <= 6)
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
        """Привести очередь в соответствие с источником. В smart только читаем.

        Очередь — это ровно те артикулы, которые сейчас отбирает правило. Пропавшие из
        источника удаляются: держать задачу на номер, которого больше нет, незачем, а
        собранное по нему остаётся в базе библиотеки и никуда не девается.
        """
        источник = await asyncpg.connect(smart_dsn)
        try:
            строки = await источник.fetch(ОТБОР_АРТИКУЛОВ)
        finally:
            await источник.close()
        артикулы = [р["article"] for р in строки][:предел]

        async with (await self.пул()).acquire() as с, с.transaction():
            новых = await с.fetch(
                """insert into очередь_артикулов (артикул, путь)
                   select а, $2 from unnest($1::text[]) as t(а)
                   on conflict (артикул) do nothing
                   returning артикул""", артикулы, путь)
            лишних = await с.fetch(
                """delete from очередь_артикулов
                   where not (артикул = any($1::text[])) returning артикул""", артикулы)
            вернулось = 0
            if заново:
                вернулось = len(await с.fetch(
                    """update очередь_артикулов
                       set статус = 'новая', попыток = 0, ошибка = null
                       where артикул = any($1::text[]) and статус <> 'новая'
                       returning артикул""", артикулы))
        if лишних:
            лог.info("удалено задач на артикулы, которых больше нет в источнике: %d",
                     len(лишних))
        return {"отобрано в источнике": len(артикулы), "новых": len(новых),
                "удалено": len(лишних), "возвращено": вернулось}

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
                    "select count(*) from очередь_объявлений where статус = 'новая'")
            self._когда_считали = time.monotonic()
        return self._карточек_ждёт

    async def взять_артикул(self) -> ЗадачаАртикул | None:
        async with (await self.пул()).acquire() as с:
            строка = await с.fetchrow(
                """update очередь_артикулов о
                   set статус = 'в работе', взята = now()
                   where о.артикул = (select артикул from очередь_артикулов
                                      where статус = 'новая'
                                      order by создана, артикул
                                      for update skip locked limit 1)
                   returning о.артикул, о.путь, о.попыток""")
        if строка is None:
            return None
        return ЗадачаАртикул(строка["артикул"], строка["путь"], строка["попыток"])

    async def взять_карточку(self) -> ЗадачаКарточка | None:
        async with (await self.пул()).acquire() as с:
            строка = await с.fetchrow(
                """update очередь_объявлений о
                   set статус = 'в работе', взята = now()
                   where о.объявление = (select объявление from очередь_объявлений
                                         where статус = 'новая'
                                         order by спаршена nulls first, объявление
                                         for update skip locked limit 1)
                   returning о.объявление, о.попыток""")
        if строка is None:
            return None
        self._карточек_ждёт = max(0, self._карточек_ждёт - 1)
        return ЗадачаКарточка(строка["объявление"], строка["попыток"])

    # ------------------------------------------------------------------ исходы артикула

    async def артикул_готов(self, артикул: str, *, нашлось: int | None,
                            страниц: int | None, собрано: int, широкая: bool,
                            заметка: str | None = None) -> None:
        async with (await self.пул()).acquire() as с:
            await с.execute(
                """update очередь_артикулов
                   set статус = 'готова', сделана = now(), ошибка = $6,
                       нашлось = $2, страниц = $3, собрано = $4, широкая = $5
                   where артикул = $1""",
                артикул, нашлось, страниц, собрано, широкая, заметка)

    async def артикул_упал(self, артикул: str, ошибка: str, *, попыток: int,
                           исчерпан: str = "ошибка") -> str:
        """Вернуть в очередь или, когда попытки исчерпаны, поставить итоговый статус.

        Итог бывает разный: обычная неудача — «ошибка», а страница подбора по номеру,
        пришедшая трижды с разных адресов, — это «пусто», объявлений по номеру нет.
        """
        async with (await self.пул()).acquire() as с:
            строка = await с.fetchrow(
                """update очередь_артикулов
                   set попыток = попыток + 1, ошибка = $2,
                       статус = case when попыток + 1 >= $3 then $4 else 'новая' end,
                       сделана = case when попыток + 1 >= $3 then now() else сделана end,
                       нашлось = case when попыток + 1 >= $3 and $4 = 'пусто'
                                      then 0 else нашлось end,
                       собрано = case when попыток + 1 >= $3 and $4 = 'пусто'
                                      then 0 else собрано end
                   where артикул = $1
                   returning статус""", артикул, ошибка[:500], попыток, исчерпан)
        return строка["статус"] if строка else "нет такой"

    async def артикул_вернуть(self, артикул: str) -> None:
        async with (await self.пул()).acquire() as с:
            await с.execute(
                "update очередь_артикулов set статус = 'новая' where артикул = $1", артикул)

    # ------------------------------------------------------------------ объявления

    async def поставить_объявления(self, номера: list[int]) -> int:
        """Поставить в очередь номера, которых раньше не встречали. Возвращает сколько.

        Связь артикула с объявлением здесь не пишется: библиотека уже ведёт её сама —
        запрос на артикул и находки по нему, с номером страницы и датами.
        """
        if not номера:
            return 0
        async with (await self.пул()).acquire() as с:
            новые = await с.fetch(
                """insert into очередь_объявлений (объявление)
                   select н from unnest($1::bigint[]) as t(н)
                   on conflict (объявление) do nothing
                   returning объявление""", номера)
        self._когда_считали = 0.0
        return len(новые)

    async def карточка_готова(self, номер: int) -> None:
        async with (await self.пул()).acquire() as с:
            await с.execute(
                """update очередь_объявлений
                   set статус = 'готова', спаршена = now(), ошибка = null
                   where объявление = $1""", номер)

    async def карточка_снята(self, номер: int) -> None:
        async with (await self.пул()).acquire() as с:
            await с.execute(
                """update очередь_объявлений set статус = 'снято', спаршена = now()
                   where объявление = $1""", номер)

    async def карточка_упала(self, номер: int, ошибка: str, *, попыток: int) -> str:
        async with (await self.пул()).acquire() as с:
            строка = await с.fetchrow(
                """update очередь_объявлений
                   set попыток = попыток + 1, ошибка = $2,
                       статус = case when попыток + 1 >= $3 then 'ошибка' else 'новая' end
                   where объявление = $1
                   returning статус""", номер, ошибка[:500], попыток)
        return строка["статус"] if строка else "нет такой"

    async def карточку_вернуть(self, номер: int) -> None:
        async with (await self.пул()).acquire() as с:
            await с.execute(
                "update очередь_объявлений set статус = 'новая' where объявление = $1",
                номер)

    # ------------------------------------------------------------------ обслуживание

    async def переочередить(self, дней: int) -> int:
        """Вернуть в очередь карточки, которых давно не касались: нужны свежие просмотры."""
        async with (await self.пул()).acquire() as с:
            строки = await с.fetch(
                """update очередь_объявлений set статус = 'новая', попыток = 0
                   where статус = 'готова'
                     and (спаршена is null or спаршена < now() - make_interval(days => $1))
                   returning объявление""", дней)
        self._когда_считали = 0.0
        return len(строки)

    async def переочередить_артикулы(self) -> int:
        """Пустить каталоги заново: ищем объявления, которых раньше не было."""
        async with (await self.пул()).acquire() as с:
            строки = await с.fetch(
                """update очередь_артикулов set статус = 'новая', попыток = 0, ошибка = null
                   where статус in ('готова', 'ошибка', 'пусто') returning артикул""")
        return len(строки)

    async def сбросить_зависшие(self, через: float) -> dict:
        """Задачи, взятые и брошенные (процесс убили), вернуть в очередь."""
        секунд = int(через)
        async with (await self.пул()).acquire() as с:
            артикулы = await с.fetch(
                """update очередь_артикулов set статус = 'новая'
                   where статус = 'в работе'
                     and взята < now() - make_interval(secs => $1) returning артикул""",
                секунд)
            карточки = await с.fetch(
                """update очередь_объявлений set статус = 'новая'
                   where статус = 'в работе'
                     and взята < now() - make_interval(secs => $1) returning объявление""",
                секунд)
        self._когда_считали = 0.0
        return {"артикулов": len(артикулы), "карточек": len(карточки)}

    async def сводка(self) -> dict:
        async with (await self.пул()).acquire() as с:
            артикулы = {р["статус"]: р["count"] for р in await с.fetch(
                "select статус, count(*) from очередь_артикулов group by статус")}
            карточки = {р["статус"]: р["count"] for р in await с.fetch(
                "select статус, count(*) from очередь_объявлений group by статус")}
            широких = await с.fetchval(
                "select count(*) from очередь_артикулов where широкая")
            собрано = await с.fetchval(
                "select coalesce(sum(собрано), 0) from очередь_артикулов")
            за_сутки = await с.fetchval(
                """select count(*) from очередь_объявлений
                   where спаршена > now() - interval '1 day'""")
        return {"артикулы": артикулы, "широких артикулов": широких,
                "объявлений в выдачах": собрано, "карточки": карточки,
                "карточек за сутки": за_сутки}
