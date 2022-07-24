# Auto loading CSV files to Clickhouse

## !!! WARNING. Use on your own risk. You may loose your data in Clickhouse!!!

Features:
- Auto detecting data types
- Auto creating table in Clickhouse
- Auto loadimg data in Clickhouse
<br/><br/>

Suporting data types:
- datetime
- float
- int
- bool
<br/><br/>

simple usage:
```
from csvtoch import csv_to_CH_autoload
from clickhouse_driver import Client

server = SERVER
user = USER
password = PASSWORD
client_CH = Client(server, user=user, password=password, settings={"use_numpy":True})
_ = Load_CSV_to_CH.csv_to_CH("data.csv", 'mydb', 'mytable', client_CH)
```






