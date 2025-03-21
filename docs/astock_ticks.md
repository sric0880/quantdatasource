# A股ticks数据库字段说明

时区：Asia/Shanghai

## ticks数据：TDengine -> finance_ticks -> ticks

* 超级表：`ticks`
* 子表名: `{symbol}`
* TAGS: `(symbol, exchange)`

|字段名|说明|类型|
|--|--|--|
|dt|时间戳|datetime|
|last_price|最新价|float|
|volume|成交量|int|
|amount|成交额|int|
|bid_price1|买1价|float|
|ask_price1|卖1价|float|
|bid_volume1|买1量|int|
|ask_volume1|卖1量|int|

## K线数据：TDengine -> finance_ticks -> tick_bars

* 超级表：`tick_bars`
* 子表名: `{symbol}_{period}`
* TAGS: `(symbol, period, exchange)`

|字段名|说明|
|--|--|
|dt|K线时间（按收盘）|
|open|开盘价|
|high|最高价|
|low|最低价|
|close|收盘价|
|volume|成交量|
|amount|成交额|
