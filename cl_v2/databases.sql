create table futures_klines_btc_usdt (
    dt DATETIME not null,
    f VARCHAR(5) not null,
    h decimal(20,8) not null,
    l decimal(20, 8) not null,
    o decimal(20, 8) not null,
    c decimal(20, 8) not null,
    v decimal(20, 8) not null,
    UNIQUE INDEX dt_f (dt, f)
);

show create table futures_klines_btc_usdt;

replace into futures_klines_btc_usdt(dt, f, h, l, o, c, v) values ('2021-10-07 10:30:00', '30m', 12.11, 12.12, 13.13, 14.14, 88.88);

select * from futures_klines_btc_usdt;

delete from futures_klines_btc_usdt;

select dt from futures_klines_btc_usdt order by dt desc limit 1;

select count(dt) from futures_klines_btc_usdt;

select dt, f, h, l, o, c, v from futures_klines_btc_usdt where f='d' order by dt desc limit 2000;