create table if not exists customer
(
    id           int8         not null default unordered_unique_rowid(),
    address1     varchar(255),
    address2     varchar(255),
    city         varchar(255),
    country_code varchar(16),
    country_name varchar(36),
    postcode     varchar(16),
    created_time timestamp    not null,
    email        varchar(128),
    first_name   varchar(128),
    last_name    varchar(128),
    password     varchar(128) not null,
    telephone    varchar(128),
    user_name    varchar(15)  not null,

    primary key (id)
);
