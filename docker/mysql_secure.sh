#!/usr/bin/expect
set passwd 123456
spawn  mysql_secure_installation
expect {
             "Press y|Y for Yes, any other key for No" { send "N\r"; exp_continue }
             "New password" { send "$passwd\r"; exp_continue }
             "Re-enter new password" { send "$passwd\r"; exp_continue }
             ".*Remove anonymous users.*" { send "Y\r"; exp_continue }
             "Disallow root login remotely" { send "N\r"; exp_continue }
             "Remove test database and access to it" { send "N\r"; exp_continue }
             "Reload privilege tables now" { send "Y\r" }
}