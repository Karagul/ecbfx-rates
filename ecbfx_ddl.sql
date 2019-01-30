DROP TABLE if exists storehouse_database.ecbfx;
CREATE EXTERNAL TABLE storehouse_database.ecbfx
(tradedate string, AUD string,BGN string,BRL string,CAD string,CHF string,CNY string,CZK string,DKK string,GBP string,HKD string,HRK string,HUF string,IDR string,ILS string,INR string,ISK string,JPY string,KRW string,MXN string,MYR string,NOK string,NZD string,PHP string,PLN string,RON string,RUB string,SEK string,SGD string,THB string,TRY string,USD string,ZAR string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' WITH SERDEPROPERTIES ('field.delim'=',', 'serialization.format'=',') 
STORED AS TEXTFILE LOCATION 'hdfs://CDH/user/laffey_c/ecbfx/ecbfx' 
TBLPROPERTIES ('skip.header.line.count'='1');
