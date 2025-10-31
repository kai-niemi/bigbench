create table customer
(
    id         int8         not null default unordered_unique_rowid(),
    email      varchar(128),
    name       varchar(128),
    password   varchar(128) not null,
    address1   varchar(255),
    address2   varchar(255),
    postcode   varchar(16),
    city       varchar(255),
    country    varchar(36),
    updated_at timestamptz  not null default clock_timestamp(),

    primary key (id)
);

create table account
(
    id             int            not null default unordered_unique_rowid(),
    type           varchar(32)    not null,
    version        int            not null default 0,
    balance        numeric(19, 2) not null,
    name           varchar(128)   not null,
    allow_negative integer        not null default 0,

    primary key (id, type, version)
);
