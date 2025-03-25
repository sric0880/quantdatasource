# 国内期货(finance_ctpfuture)字段表说明

## MongoDB

### 1. adjust_factors 主力合约价差调价因子

因为tick数据来源于多个历史主力合约，tqsdk没有做任何拼接，为防止回测时价差太大，从tqsdk整理的价差因子。

|字段名|说明|
|--|--|
|symbol|品种代码|
|tradedate|交易日期|
|adjust_factor|价差因子|

### 2. basic_info_futures 期货合约列表详细数据

|字段名|说明|类型|
|--|--|--|
|_id|合约名|string|
|price_tick|合约价格变动单位|float|
|volume_multiple|合约乘数|float|
|max_limit_order_volume|最大限价单手数|int|
|max_market_order_volume|最大市价单手数|int|
|expire_datetime|到期具体日，以秒为单位的 timestamp 值|string|
|expire_rest_days|距离到期日的剩余天数（自然日天数）|int|
|delivery_year|期货交割日年份|int|
|delivery_month|期货交割日月份|int|
|upper_limit|涨停价|float|
|lower_limit|跌停价|float|
|pre_settlement|昨结算|float|
|pre_open_interest|昨持仓|int|
|pre_close|昨收盘|float|

### 3. basic_info_products 期货品种列表数据

|字段名|说明|类型|
|--|--|--|
|_id|品种代码|string|
|exchange|交易所代码|string|
|name|中文名称|string|
|cont_symbols|持仓量最大的3个合约|list of strings|

### 4. contracts_${brokerid} 期货合约保证金率以及手续费数据

分多个表，brokerid对应不同券商

|brokerid|券商|
|--|--|
|8000|兴业期货|
|9999|上期所模拟盘|

|字段名|说明|类型|
|--|--|--|
|_id|合约名|string|
|exchange_id|交易所ID: [CFFEX, GFEX, CZCE, SHFE, DCE, INE]|string|
|volume_multiple|合约乘数|float|
|price_tick|合约价格变动单位|float|
|is_trading|是否交易|bool|
|delivery_year|期货交割日年份|int|
|delivery_month|期货交割日月份|int|
|max_market_order_volume|最大市价单手数|int|
|min_market_order_volume|最小市价单手数|int|
|max_limit_order_volume|最大限价单手数|int|
|min_limit_order_volume|最小限价单手数|int|
|long_margin_ratio_bymoney|多单保证金率（按金额）|float|
|short_margin_ratio_bymoney|空单保证金率（按金额）|float|
|long_margin_ratio_byvolume|多单保证金（按手）|float|
|short_margin_ratio_byvolume|空单保证金（按手）|float|
|commission_of_open_bymoney|开仓手续费（按金额）|float|
|commission_of_open_byvolume|开仓手续费（按手）|float|
|commission_of_close_bymoney|平仓手续费（按金额）|float|
|commission_of_close_byvolume|平仓手续费（按手）|float|
|commission_of_closetoday_bymoney|平今手续费（按金额）|float|
|commission_of_closetoday_byvolume|平今手续费（按手）|float|
|last_price|最新价|float|
|long_margin|多单一手保证金|float|
|short_margin|空单一手保证金|float|
|open_comm|开仓手续费|float|
|close_comm|平仓手续费|float|
|closetoday_comm|平今手续费|float|
|symbol_name|中文名|string|

## TDengine/DuckDB

ticks数据TDengine需要单独建库，时区：Asia/Shanghai

### 1. bars 期货K线数据（分钟线、日线、周线、月线） 

* 超级表：`bars`
* 子表名: `{symbol}_{period}`
* TAGS: `(symbol, period, exchange)`

|字段名|说明|类型|
|--|--|--|
|dt|交易日期|datetime|
|open|开盘价|float|
|high|最高价|float|
|low|最低价|float|
|close|收盘价|float|
|volume|成交量(手)|int|
|amount|成交额(元)|long long int|
|open_interest|持仓量(手)|int|

### 2. bars_daily 期货日线(周线)详细数据

* 超级表：`bars_daily`
* 子表名: `{symbol}`
* TAGS: `(symbol, exchange)`

|字段名|说明|类型|
|--|--|--|
|dt|交易日期|datetime|
|open|开盘价|float|
|high|最高价|float|
|low|最低价|float|
|close|收盘价|float|
|settle|结算价|float|
|volume|成交量(手)|int|
|amount|成交金额(元)|long long int|
|open_interest|持仓量(手)|int|

### 3. ticks ticks数据

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

### 4. tick_bars K线数据

由ticks数据生成，方便ticks的读取和回放

* 超级表：`tick_bars`
* 子表名: `{symbol}_{period}`
* TAGS: `(symbol, period, exchange)`

|字段名|说明|类型|
|--|--|--|
|dt|K线时间（按收盘）|datetime|
|open|开盘价|float|
|high|最高价|float|
|low|最低价|float|
|close|收盘价|float|
|volume|成交量|int|
|amount|成交额|long long int|
|open_interest|持仓量|int|
