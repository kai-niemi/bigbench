create table attachment
(
    id           int8        not null default unordered_unique_rowid(),
    binary_file  bytea,
    checksum     int8        not null,
    content_type varchar(15),
    created_time timestamp   not null,
    description  varchar(256),
    name         varchar(64) not null,
    size         int8        not null,
    primary key (id)
);

create table categorized_product
(
    category_id  int8      not null,
    created_time timestamp not null,
    expires      timestamp,
    product_id   int8      not null,
    username     varchar(16)
);

create table category
(
    category_type varchar(15) not null,
    id            int8        not null default unordered_unique_rowid(),
    created_time  timestamp   not null,
    description   varchar(255),
    left_node     int8        not null,
    name          varchar(64) not null,
    right_node    int8        not null,
    version       int4,
    parent_id     int8,
    primary key (id)
);

create table customer
(
    id                int8         not null default unordered_unique_rowid(),
    address1          varchar(255),
    address2          varchar(255),
    city              varchar(255),
    country_code      varchar(16),
    country_name      varchar(36),
    postcode          varchar(16),
    created_time      timestamp    not null,
    email             varchar(128),
    first_name        varchar(128),
    last_name         varchar(128),
    password          varchar(128) not null,
    telephone         varchar(128),
    user_name         varchar(15)  not null,
    payment_method_id int8,

    primary key (id)
);

create table customer_payment_method
(
    user_id           int8 not null,
    payment_method_id int8 not null,
    primary key (user_id, payment_method_id)
);

create table orders
(
    id                  int8      not null default unordered_unique_rowid(),
    bill_address1       varchar(255),
    bill_address2       varchar(255),
    bill_city           varchar(255),
    bill_country_code   varchar(16),
    bill_country_name   varchar(16),
    bill_postcode       varchar(16),
    bill_to_first_name  varchar(255),
    bill_to_last_name   varchar(255),
    date_placed         timestamp not null,
    deliv_to_first_name varchar(255),
    deliv_to_last_name  varchar(255),
    deliv_address1      varchar(255),
    deliv_address2      varchar(255),
    deliv_city          varchar(255),
    deliv_country_code  varchar(16),
    deliv_country_name  varchar(16),
    deliv_postcode      varchar(16),
    status              varchar(64),
    amount              numeric(19, 2),
    currency            varchar(255),
    version             int4,
    customer_id         int8      not null,
    payment_method_id   int8      not null,
    primary key (id)
);

create table order_item
(
    order_id             int8        not null,
    load_type            varchar(16) not null,
    product_variation_id int8,
    quantity             int4        not null,
    status               varchar(64),
    amount               numeric(19, 2),
    currency             varchar(255),
    weight_kg            float8      not null,
    item_pos             int4        not null,
    primary key (order_id, item_pos)
);

create table payment_method
(
    payment_type   varchar(15) not null,
    id             int8        not null default unordered_unique_rowid(),
    created_time   timestamp   not null,
    account_number varchar(16),
    balance        numeric(19, 2),
    currency       varchar(3),
    name           varchar(255),
    card_type      varchar(15),
    exp_month      int4,
    exp_year       int4,
    number         varchar(255),
    retailer       varchar(255),
    voucher_code   varchar(25),
    primary key (id)
);

create table product
(
    id                 int8         not null default unordered_unique_rowid(),
    created_by         varchar(24),
    created_time       timestamp,
    last_modified_by   varchar(24),
    last_modified_time timestamp,
    attributes         jsonb,
    description        varchar(2048),
    name               varchar(128) not null,
    sku_code           varchar(128) not null,
    version            int4,
    primary key (id)
);

create table product_attachment
(
    product_id    int8 not null,
    attachment_id int8 not null
);

create table product_tag
(
    product_id int8        not null,
    name       varchar(64) not null
);

create table product_variation
(
    id         int8        not null default unordered_unique_rowid(),
    amount     numeric(19, 2),
    currency   varchar(255),
    sku_code   varchar(24) not null,
    version    int4,
    product_id int8        not null,
    primary key (id)
);

create table product_variation_attribute
(
    product_variation_id int8         not null,
    value                varchar(512) not null,
    name                 varchar(55)  not null,
    primary key (product_variation_id, name)
);

alter table if exists category
    add constraint UKasmlej12cqrmj90f817rkuyw unique (name, parent_id);

alter table if exists customer
    add constraint UK_8cqc86mekfecc5kjcwakm36nd unique (user_name);

alter table if exists customer_payment_method
    add constraint UK_jsrefpdc7hkdod3gio6llqmuy unique (payment_method_id);

alter table if exists product
    add constraint UK_gcnmh8ufsexajerget2c6p5mq unique (sku_code);

alter table if exists product_variation
    add constraint UK_5okunt0ypvn3164u9oufio2bl unique (sku_code);

alter table if exists categorized_product
    add constraint FKqcdnn05nhqnlxsu5lm1dp1kl3
        foreign key (product_id)
            references product;

alter table if exists categorized_product
    add constraint FKqxowgs2275fdlt4kvjavq1km2
        foreign key (category_id)
            references category;

-- alter table if exists category
--     add constraint FKioydp395t8qddmamkqk0t277u
--         foreign key (parent_id)
--             references category;

-- alter table if exists category
--     drop constraint if exists FKioydp395t8qddmamkqk0t277u;

alter table if exists customer
    add constraint FK3wq9hq6fx5e2q8ufq28i9unm3
        foreign key (payment_method_id)
            references payment_method;

alter table if exists customer_payment_method
    add constraint FKhtp6ih5tvim3rc2g9v686fu7w
        foreign key (payment_method_id)
            references payment_method;

alter table if exists customer_payment_method
    add constraint FK4ekblfcyttlcyuisgixfinfsu
        foreign key (user_id)
            references customer;

alter table if exists orders
    add constraint FKesy3n2gc3fa0s3trrk3tvyv9a
        foreign key (customer_id)
            references customer;

alter table if exists orders
    add constraint FKr1kae2mabfcclxvbbiddjhb73
        foreign key (payment_method_id)
            references payment_method;

alter table if exists order_item
    add constraint FKj6ab4nyorr28swpwmvv8h0010
        foreign key (product_variation_id)
            references product_variation;

alter table if exists order_item
    add constraint FKt3r2219w9rbs121eas5yf025m
        foreign key (order_id)
            references orders;

alter table if exists product_attachment
    add constraint FKpu9vdm0fqfb1xavcyjvu517wo
        foreign key (attachment_id)
            references attachment;

alter table if exists product_attachment
    add constraint FKbvo06josa4s24ghwr6vofx15h
        foreign key (product_id)
            references product;

alter table if exists product_tag
    add constraint FK20f9fs0kdruhjpgp5kj6mtd46
        foreign key (product_id)
            references product;

alter table if exists product_variation
    add constraint FKsxi24esd8opfe7vtdj170rg0t
        foreign key (product_id)
            references product;

alter table if exists product_variation_attribute
    add constraint FKik3ao8kct887huasbkegwfmqc
        foreign key (product_variation_id)
            references product_variation;

