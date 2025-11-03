drop type if exists account_type;
create type account_type as enum ('A', 'L', 'E', 'R', 'C');

drop type if exists transaction_type;
create type transaction_type as enum ('GEN','TMP','PAY');

create table if not exists account
(
    id             uuid           not null default gen_random_uuid(),
    city           string         not null,
    balance        decimal(19, 2) not null,
    currency       string         not null default 'USD',
    balance_money  string as (concat(balance::string, ' ', currency)) virtual,
    name           string(128) not null,
    description    string(256) null,
    type           account_type   not null,
    closed         boolean        not null default false,
    allow_negative integer        not null default 0,
    updated_at     timestamptz    not null default clock_timestamp(),

    primary key (id)
);

create index on account (city) storing (balance, currency);

create table if not exists transaction
(
    id               uuid             not null default gen_random_uuid(),
    city             string           not null,
    booking_date     date             not null default current_date(),
    transfer_date    date             not null default current_date(),
    transaction_type transaction_type not null,

    primary key (id)
);

create table if not exists transaction_item
(
    transaction_id        uuid           not null,
    account_id            uuid           not null,
    city                  string         not null,
    amount                decimal(19, 2) not null,
    currency              string         not null default 'USD',
    amount_money          string as (concat(amount::string, ' ', currency)) virtual,
    note                  string,
    running_balance       decimal(19, 2) not null,
    running_balance_money string as (concat(running_balance::string, ' ', currency)) virtual,

    primary key (transaction_id, account_id)
);

create table if not exists outbox
(
    id             uuid        not null default gen_random_uuid(),
    created_at     timestamptz not null default clock_timestamp(),
    aggregate_type string      not null,
    aggregate_id   string      not null,
    event_type     string      not null,
    payload        jsonb       not null,

    primary key (id)
);

alter table transaction_item
    add constraint fk_txn_item_ref_transaction
        foreign key (transaction_id) references transaction (id);

alter table transaction_item
    add constraint fk_txn_item_ref_account
        foreign key (account_id) references account (id);