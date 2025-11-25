# A股数据库(finance_astock)字段表说明

## MongoDB

### 1. basic_info_stocks 证券基本信息

|字段名|说明|类型|
|--|--|--|
|symbol|股票代码|string|
|name|股票名称|string|
|area|地域|string|
|industry|所属行业|string|
|fullname|股票全称|string|
|enname|英文全称|string|
|cnspell|拼音缩写|string|
|market|市场类型（主板/创业板/科创板/CDR）|string|
|exchange|交易所代码（SSE/SZSE/BSE）|string|
|curr_type|交易货币：CNY|string|
|list_date|上市日期|string(%Y%m%d)|
|delist_date|退市日期|string(%Y%m%d)|
|status|上市状态，其中L上市 D退市 P暂停上市|string|
|is_hs|是否沪深港通标的，N否 H沪股通 S深股通|string|

### 2. basic_info_ths_concepts 同花顺概念板块基本信息

|字段名|说明|类型|
|--|--|--|
|symbol|代码|string|
|name|概念名称|string|
|count|成分个数|int|
|list_date|上市日期|string|

### 3. basic_info_cbs 可转债基本信息

数据表字段说明见：[TusharePro](https://tushare.pro/document/2?doc_id=185)

### 4. 成分股数据

|字段名|说明|类型|
|--|--|--|
|tradedate|交易日期|datetime|
|stock_code|股票代码|string|
|op|操作：纳入:1,剔除:0|int|
|index_code|指数代码|string|

#### 4.1 constituent_index 8大指数

数据来源Wind，更新截止：2022-11-21

#### 4.2 constituent_ths_index 同花顺概念成分股

数据来源tushare，无历史数据，数据起始于2023-03-31

#### 4.3 constituent_sw_industry_l3 申万行业L3历史成分股

数据来源[申万官网](https://www.swsresearch.com/institute_sw/allIndex/downloadCenter/industryType)：[StockClassifyUse_stock.xls](https://www.swsresearch.com/swindex/pdf/SwClass2021/StockClassifyUse_stock.xls)

### 6. analyst_reports A股研报数据(来源：东方财富网)

|字段名|说明|类型|
|--|--|--|
|title|研报标题|string|
|stockName|股票名称|string|
|stockCode|股票代码|string|
|orgCode|券商代码|string|
|orgName|券商全名|string|
|orgSName|券商简称|string|
|publishDate|发布日期|datetime|
|tradingDate|最近交易日|datetime|
|predictNextTwoYearEps|后年盈利预测--每股收益|float|
|predictNextTwoYearPe|后年盈利预测--市盈率|float|
|predictNextYearEps|明年盈利预测--每股收益|float|
|predictNextYearPe|明年盈利预测--市盈率|float|
|predictThisYearEps|今年盈利预测--每股收益|float|
|predictThisYearPe|今年盈利预测--市盈率|float|
|predictLastYearEps|去年盈利预测--每股收益|float|
|predictLastYearPe|去年盈利预测--市盈率|float|
|indvInduCode||int|
|indvInduName|所处行业|string|
|emRatingCode|东财投资评级代码|int|
|emRatingValue|东财投资评级 (买入3、增持2、中性0、减持-2、卖出-3)|int|
|emRatingName|东财投资评级中文名 (买入3、增持2、中性0、减持-2、卖出-3)|string|
|lastEmRatingCode| |int|
|lastEmRatingValue|上一次评级|int|
|lastEmRatingName| |string|
|ratingChange|评级变动 ( 0: '调高', 1: '调低', 2: '首次', 3: '维持', 4: '无' )|int|
|researcher|作者|string|
|newIssuePrice|？|float|
|newPeIssueA|？|float|
|indvAimPriceT|？|float|
|indvAimPriceL|目标价|float|
|sRatingName|投资评级(买入 增持 推荐 谨慎推荐 强烈推荐)|string|
|sRatingCode|投资评级代码|int|
|count|近一月个股研报数|int|

### 7. hot_stocks_ths 同花顺热股数据

数据说明：1h热度排行榜前100数据，每天15点15分更新

|字段名|说明|
|--|--|
|date|交易日期|
|code|代码|
|name|名称|
|order|排名|
|rate|得分|
|hot_rank_chg|排名变化|
|concept_tag|热点概念|
|popularity_tag|人气概念|

### 8. 财务三大报表数据

1. 资产负债表 `finance->finance_balancesheet`
2. 利润表 `finance->finance_income`
3. 利润表当季 `finance->finance_income_q`
4. 现金流量表 `finance->finance_cashflow`
5. 现金流量表当季 `finance->finance_cashflow_q`

数据表字段说明见：[TusharePro](https://tushare.pro/document/2?doc_id=16)

### 9. lhb 龙虎榜数据

|字段名|说明|类型|
|--|--|--|
|trade_date|交易日期|int (index) (%Y%m%d)|
|symbol|代码|string (index)|
|name|名称|string|
|close|收盘价|float|
|pct_change|涨跌幅|float|
|turnover_rate|换手率|float|
|amount|总成交额|float|
|l_sell|龙虎榜卖出额|float|
|l_buy|龙虎榜买入额|float|
|l_amount|龙虎榜成交额|float|
|net_amount|龙虎榜净买入额|float|
|net_rate|龙虎榜净买额占比|float|
|amount_rate|龙虎榜成交额占比|float|
|float_values|当日流通市值|float|
|reason|上榜理由|string or list of strings|
|buy_top5_inst|买入金额最大前5名|`[{'exalter':'营业部名称','buy_amount':'买入额（元）','buy_rate':'买入占总成交比例','sell_amount':'卖出额（元）','sell_rate':'卖出占总成交比例','net_buy':'净成交额（元）'},...]`|
|sell_top5_inst|卖出金额最大前5名|`[{'exalter':'营业部名称','buy_amount':'买入额（元）','buy_rate':'买入占总成交比例','sell_amount':'卖出额（元）','sell_rate':'卖出占总成交比例','net_buy':'净成交额（元）'},...]`|

### 10. basic_info_sw_industry 申万行业(1L,2L,3L)基本信息

|字段名|说明|类型|
|--|--|--|
|l1_name|一级行业|string|
|l2_name|二级行业|string|
|l3_name|三级行业|string|
|industry_code|行业代码|string|
|index_code|指数代码|string|
|version|申万行业标准版本(SW2011：2011年版本，SW2014：2014年版本，SW2021：2021年版本)|string|

### 11. cyq_chips 筹码分布

|字段名|说明|类型|
|--|--|--|
|dt|日期|datetime|
|symbol|标的|string|
|data|筹码分布|sorted list([(price, percentage), ....])|

### 12. market_stats A股全市场统计数据(排除了ST股)

|字段名|说明|
|--|--|
|dt|交易日期|
|count_of_uplimit|涨停数|
|count_of_downlimit|跌停数|
|count_of_yiziup|一字涨停数|
|count_of_yizidown|一字跌停数|
|ratio_of_uplimit|涨停数占所有股数比例|
|ratio_of_downlimit|跌停数占所有股数比例|
|ratio_of_yiziup|一字涨停数占所有股数比例|
|ratio_of_yizidown|一字跌停数占所有股数比例|
|lb1|首板数量|
|lb2|2连板数量|
|lb3|3连板数量|
|lb4|4连板数量|
|lb5|5连板数量|
|lb6|6连板数量|
|lb7|7连板数量|
|lb8|8连板数量|
|lb9|9连板数量|
|lb10|10连板数量|
|lb11|11连板数量|
|lb12|12连板数量|

## 文件

按天保存，每天生成一个文件。

### 1. daily_factors A股日线因子数据

更多因子由其他程序生成。定期将价格数据(按前复权)按symbol合并成duckdb，用于加速回测。回测需要用到的字段：`dt`,`name`,`open`,`high`,`low`,`close`,`maxupordown`,`maxupordown_at_open`

|字段名|说明|
|--|--|
|dt|交易日期|
|symbol|股票代码|
|name|股票名称|
|open|开盘价(不复权)|
|high|最高价(不复权)|
|low|最低价(不复权)|
|close|收盘价(不复权)|
|preclose|前收盘价，若当天发生除权，前收盘价为上个交易日复权之后的收盘价|
|volume|成交量（股）|
|amount|成交额（元）|
|pe_ttm|滚动市盈率|
|pb|市净率|
|mkt_cap|总市值(元)|
|mkt_cap_ashare|流通市值(元)|
|vip_buy_amt|大户资金买入额(万元)|
|vip_sell_amt|大户资金卖出额(万元)|
|inst_buy_amt|机构资金买入额(万元)|
|inst_sell_amt|机构资金卖出额(万元)|
|mid_buy_amt|中户资金买入额(万元)|
|mid_sell_amt|中户资金卖出额(万元)|
|indi_buy_amt|散户资金买入额(万元)|
|indi_sell_amt|散户资金卖出额(万元)|
|master_net_flow_in|主力(机构和大户)净买入(万元)|
|master2_net_flow_in|主力2(机构、大户和中户)净买入(万元)|
|vip_net_flow_in|大户净流入(万元)|
|mid_net_flow_in|中户净流入(万元)|
|inst_net_flow_in|机构净流入(万元)|
|indi_net_flow_in|散户净流入(万元)|
|total_sell_amt|流出资金总额(万元)|
|total_buy_amt|流入资金总额(万元)|
|net_flow_in|资金净流入(万元)|
|turnover|换手率|
|free_shares|流通股本|
|total_shares|总股本|
|maxupordown|标记收盘涨停或跌停状态,1表示涨停,2表示一字板涨停；-1则表示跌停，-2表示一字板跌停；0表示未涨跌停|
|maxupordown_at_open|标记开盘涨停或跌停状态，状态码同上|

### 2. bars_ths_index_daily 同花顺概念板块日线行情数据

|字段名|说明|
|--|--|
|dt|交易日期|
|open|开盘价|
|high|最高价|
|low|最低价|
|close|收盘价|
|avg_price|平均价|
|change|涨跌点位|
|pct_change|涨跌幅|
|volume|成交量|
|turnover_rate|换手率|

### 3. bars_cb_daily 可转债日线详细数据

|字段名|说明|类型|
|--|--|--|
|dt|当天的交易日期|datetime|
|open|开盘价|float|
|high|最高价|float|
|low|最低价|float|
|close|收盘价|float|
|volume|成交量|int|
|amount|成交额|int|
|preclose|前收盘价|float|
|change|涨跌幅|float|
|pct_chg|涨跌幅比例|float|
|convert_price|转股价格|float|
|cb_value|转股价值|float|
|cb_over_rate|转股溢价率|float|
|remain_size|剩余规模（亿元）|big int|
|bond_over_rate|纯债溢价率|float|
|bond_value|纯债价值|float|
|call_price|赎回价格，含税，元/张|float|
|call_price_tax|赎回价格，扣税，元/张|float|
|is_call|是否赎回：1已满足强赎条件、2公告提示强赎、3公告实施强赎、4公告到期赎回、5公告不强赎|int|
|call_type|赎回类型：1到赎、2强赎|int|

### 4. ticks

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

### 5. bars A股K线数据（分钟线）

由ticks数据生成，方便ticks的读取和回放

|字段名|说明|
|--|--|
|dt|K线时间（按收盘）|
|open|开盘价|
|high|最高价|
|low|最低价|
|close|收盘价|
|volume|成交量|
|amount|成交额|
