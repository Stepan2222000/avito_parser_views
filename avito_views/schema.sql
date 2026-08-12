-- Очереди обхода. Объявления, продавцы и наблюдения за просмотрами живут не здесь —
-- их ведёт библиотека в своей базе. Здесь только то, чего у неё нет: что мы собираемся
-- обойти, что уже обошли и по какому артикулу что нашлось.

-- Задача на каталог: один артикул — одна строка.
create table if not exists article_tasks (
    article     text primary key,
    path        text not null,              -- категория, в которой искали
    status      text not null default 'новая',   -- новая | в работе | готова | ошибка
    attempts    integer not null default 0,
    error       text,
    found_total bigint,                     -- сколько нашлось по мнению Авито
    pages_total integer,                    -- докуда пускали листать
    items_found integer,                    -- сколько объявлений собрали
    wide        boolean not null default false,  -- выдача упёрлась в потолок Авито
    taken_at    timestamptz,
    done_at     timestamptz,
    created_at  timestamptz not null default now()
);

create index if not exists article_tasks_status on article_tasks (status, created_at);

-- Задача на карточку: один номер объявления — одна строка, независимо от того,
-- сколькими артикулами он найден. Просмотры дублируем последним значением, чтобы
-- смотреть на очередь без обращения к базе библиотеки.
create table if not exists item_tasks (
    item_id    bigint primary key,
    status     text not null default 'новая',  -- новая | в работе | готова | снято | ошибка
    attempts   integer not null default 0,
    error      text,
    views      integer,
    taken_at   timestamptz,
    parsed_at  timestamptz,
    created_at timestamptz not null default now()
);

create index if not exists item_tasks_status on item_tasks (status, parsed_at nulls first);

-- Связка «артикул — объявление»: у детали бывает несколько артикулов-аналогов, и одно
-- объявление находится сразу по нескольким. Связь нужна, чтобы считать просмотры по
-- артикулу, и она историческая: объявление могло выпасть из выдачи, но остаться живым.
create table if not exists article_items (
    article       text not null,
    item_id       bigint not null,
    page          integer,
    first_seen_at timestamptz not null default now(),
    last_seen_at  timestamptz not null default now(),
    primary key (article, item_id)
);

create index if not exists article_items_item on article_items (item_id);
