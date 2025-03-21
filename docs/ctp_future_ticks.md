# 国内期货ticks数据库字段表说明

时区：Asia/Shanghai

## ticks数据：TDengine -> finance_ticks_ctpfuture -> ticks

* 超级表：`ticks`
* 子表名: `{symbol}`
* TAGS: `(symbol, exchange)`

|字段名|说明|类型|
|--|--|--|
|dt|时间戳|datetime|
|last_price|最新价|float|
|volume|成交量|int|
|amount|成交额|int|
|open_interest|持仓量|int|
|bid_price1|买1价|float|
|ask_price1|卖1价|float|
|bid_volume1|买1量|int|
|ask_volume1|卖1量|int|

## K线数据（由ticks数据生成，方便ticks的读取和回放）：TDengine -> finance_ticks_ctpfuture -> tick_bars

* 超级表：`tick_bars`
* 子表名: `{symbol}_{period}`
* TAGS: `(symbol, period, exchange)`

|字段名|说明|
|--|--|
|dt|K线时间（按收盘）|datetime|
|open|开盘价|float|
|high|最高价|float|
|low|最低价|float|
|close|收盘价|float|
|volume|成交量|int|
|amount|成交额|long long int|
|open_interest|持仓量|int|
