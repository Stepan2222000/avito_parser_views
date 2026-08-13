"""Очередь артикулов в PostgreSQL: заливка из smart и сводка.

Данных здесь нет: собранное пишет библиотека в свою базу. Здесь только ход работы —
что предстоит обойти, что уже обошли и чем задача кончилась.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
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

    async def взять_задачу(self) -> ЗадачаАртикул | ЗадачаКарточка | None:
        """Что делать дальше. Карточки в приоритете: просмотры важнее полноты карты.

        Выдачу воркер берёт, только когда разбирать нечего — так найденное превращается в
        просмотры сразу, а обход артикулов идёт в те промежутки, когда очередь пуста.
        """
        карточка = await self.взять_карточку()
        if карточка:
            return карточка
        return await self.взять_артикул()

    async def взять_артикул(self) -> ЗадачаАртикул | None:
        """Взять следующий артикул. Задача достаётся ровно одному воркеру."""
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

    async def поставить_объявления(self, номера: list[int]) -> dict:
        """Поставить в очередь найденное: новые номера и оживление снятых.

        Раз Авито снова показывает объявление в выдаче, значит оно живо, и просмотры по
        нему опять имеют смысл. Ошибочные не оживляем: их страница не разбирается, и
        повторять это каждый обход выдачи незачем.

        Связь артикула с объявлением здесь не пишется: библиотека уже ведёт её сама —
        запрос на артикул и находки по нему, с номером страницы и датами.
        """
        if not номера:
            return {"новых": 0, "ожило": 0}
        async with (await self.пул()).acquire() as с, с.transaction():
            новые = await с.fetch(
                """insert into очередь_объявлений (объявление)
                   select н from unnest($1::bigint[]) as t(н)
                   on conflict (объявление) do nothing
                   returning объявление""", номера)
            ожили = await с.fetch(
                """update очередь_объявлений
                   set статус = 'новая', попыток = 0, ошибка = null
                   where объявление = any($1::bigint[]) and статус = 'снято'
                   returning объявление""", номера)
        return {"новых": len(новые), "ожило": len(ожили)}

    # ------------------------------------------------------------------ карточки

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
        return ЗадачаКарточка(строка["объявление"], строка["попыток"])

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
        """После исчерпанных попыток статус «ошибка» окончательный: страница в дампах."""
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

    # ------------------------------------------------------------------ переобход

    async def переочередить(self, дней: int) -> int:
        """Вернуть в очередь карточки, которых давно не касались: нужны свежие просмотры.

        Берём только успешно разобранные. Снятые оживают лишь тогда, когда снова
        появляются в выдаче, а «ошибка» окончательна: её страница не разбирается, и
        повторять это каждую неделю незачем.
        """
        async with (await self.пул()).acquire() as с:
            строки = await с.fetch(
                """update очередь_объявлений set статус = 'новая', попыток = 0
                   where статус = 'готова'
                     and (спаршена is null or спаршена < now() - make_interval(days => $1))
                   returning объявление""", дней)
        return len(строки)

    # ------------------------------------------------------------------ обслуживание

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
        return {"артикулов": len(артикулы), "карточек": len(карточки)}

    # ------------------------------------------------------------------ сводка

    async def сводка(self) -> dict:
        async with (await self.пул()).acquire() as с:
            артикулы = {р["статус"]: р["count"] for р in await с.fetch(
                "select статус, count(*) from очередь_артикулов group by статус")}
            числа = await с.fetchrow(
                """select count(*) filter (where широкая) широких,
                          coalesce(sum(собрано), 0) объявлений_в_выдачах
                   from очередь_артикулов""")
            карточки = {р["статус"]: р["count"] for р in await с.fetch(
                "select статус, count(*) from очередь_объявлений group by статус")}
            за_сутки = await с.fetchval(
                """select count(*) from очередь_объявлений
                   where спаршена > now() - interval '1 day'""")
        return {"артикулы": артикулы, **(dict(числа) if числа else {}),
                "карточки": карточки, "карточек за сутки": за_сутки}
