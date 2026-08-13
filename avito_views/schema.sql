-- Очередь артикулов — что предстоит обойти и что уже обошли.
--
-- Данных здесь нет и не будет: объявления, продавцы, наблюдения за просмотрами и связь
-- артикула с объявлениями живут в базе библиотеки. Здесь только ход работы.

create table if not exists очередь_артикулов (
    артикул  text primary key,
    путь     text not null,               -- категория, в которой искали
    статус   text not null default 'новая',   -- новая | в работе | готова | пусто | ошибка
    попыток  integer not null default 0,
    ошибка   text,
    нашлось  bigint,                      -- сколько нашлось по мнению Авито
    страниц  integer,                     -- докуда пускали листать
    собрано  integer,                     -- сколько объявлений забрали
    широкая  boolean not null default false,  -- выдача упёрлась в потолок Авито
    взята    timestamptz,
    сделана  timestamptz,
    создана  timestamptz not null default now()
);

create index if not exists очередь_артикулов_статус
    on очередь_артикулов (статус, создана);

-- Что нашлось в выдачах. Одно объявление — одна строка, независимо от того, сколькими
-- артикулами оно найдено: связь с артикулом ведёт библиотека в своих находках, а здесь
-- только ход разбора карточек.
--
-- «Спаршена» — единственная причина, по которой эта таблица существует отдельно от
-- библиотечной: по ней работает переобход, когда просмотры пора снимать заново.
create table if not exists очередь_объявлений (
    объявление bigint primary key,
    статус     text not null default 'новая',   -- новая | в работе | готова | снято | ошибка
    попыток    integer not null default 0,
    ошибка     text,
    взята      timestamptz,
    спаршена   timestamptz,
    создана    timestamptz not null default now()
);

create index if not exists очередь_объявлений_статус
    on очередь_объявлений (статус, спаршена nulls first);

alter table очередь_объявлений add column if not exists попыток integer not null default 0;
alter table очередь_объявлений add column if not exists ошибка text;
alter table очередь_объявлений add column if not exists взята timestamptz;
alter table очередь_объявлений add column if not exists спаршена timestamptz;
