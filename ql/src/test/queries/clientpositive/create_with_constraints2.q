CREATE TABLE table1 (a STRING, b STRING, PRIMARY KEY (a) NOT ENFORCED);
CREATE TABLE table2 (a STRING, b STRING, PRIMARY KEY (a) NOT ENFORCED RELY);

DESCRIBE EXTENDED table1;
DESCRIBE EXTENDED table2;

DESCRIBE FORMATTED table1;
DESCRIBE FORMATTED table2;