CREATE DATABASE IF NOT EXISTS finance KEEP 36500 DURATION 3650 VGROUPS 2 MAXROWS 10000 MINROWS 200 CACHEMODEL 'last_row' CACHESIZE 512 BUFFER 1024;
USE finance;

CREATE TABLE IF NOT EXISTS market_stats (dt timestamp,count_of_uplimit smallint unsigned,count_of_downlimit smallint unsigned,count_of_yiziup smallint unsigned,count_of_yizidown smallint unsigned,ratio_of_uplimit float,ratio_of_downlimit float,ratio_of_yiziup float,ratio_of_yizidown float,lb1 smallint unsigned,lb2 smallint unsigned,lb3 smallint unsigned,lb4 smallint unsigned,lb5 smallint unsigned,lb6 smallint unsigned,lb7 smallint unsigned,lb8 smallint unsigned,lb9 smallint unsigned,lb10 smallint unsigned,lb11 smallint unsigned,lb12 smallint unsigned);

CREATE STABLE IF NOT EXISTS bars (dt timestamp, open float, high float, low float, close float, volume bigint unsigned, amount double) TAGS (symbol binary(9), period binary(4));

CREATE STABLE IF NOT EXISTS bars_ths_index_daily(dt timestamp,open float,high float,low float,close float,avg_price float,change float,pct_change float,volume bigint unsigned,turnover_rate float) TAGS (symbol binary(9));

CREATE STABLE IF NOT EXISTS bars_stock_daily (dt timestamp,name nchar(8),open float,high float,low float,close float,volume int unsigned,amount bigint unsigned,preclose float,net_profit_ttm float,cashflow_ttm float,equity float,asset float,debt float,debttoasset float,net_profit_q float,pe_ttm float,pb float,mkt_cap double,mkt_cap_ashare double,vip_buy_amt float,vip_sell_amt float,inst_buy_amt float,inst_sell_amt float,mid_buy_amt float,mid_sell_amt float,indi_buy_amt float,indi_sell_amt float,master_net_flow_in float,master2_net_flow_in float,vip_net_flow_in float,mid_net_flow_in float,inst_net_flow_in float,indi_net_flow_in float,total_sell_amt float,total_buy_amt float,net_flow_in float,turnover float,free_shares bigint unsigned,total_shares bigint unsigned,maxupordown tinyint,maxupordown_at_open tinyint,lb_up_count tinyint unsigned,lb_down_count tinyint unsigned) TAGS (symbol binary(9));

CREATE STABLE IF NOT EXISTS dpjk (dt timestamp, sh000016 float, sh000905 float, sh000300 float, sh000852 float, sz399303 float, sh000001 float, sz399001 float, sz399006 float) TAGS (period binary(4), is_k_min binary(4), n tinyint unsigned);

CREATE STABLE IF NOT EXISTS bars_cb_daily (dt timestamp,open float,high float,low float,close float,volume int unsigned,amount bigint unsigned,preclose float,change float,pct_chg float,convert_price float,cb_value float,cb_over_rate float,remain_size bigint unsigned,bond_over_rate float,bond_value float,call_price float,call_price_tax float,is_call tinyint,call_type tinyint) TAGS (symbol binary(9));
