use mysql;
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '123456';
UPDATE user SET host = '%' WHERE user = 'root';
flush privileges;
create database currency_klines;