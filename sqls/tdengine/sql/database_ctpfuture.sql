CREATE DATABASE IF NOT EXISTS finance_ctpfuture KEEP 36500 DURATION 365 VGROUPS 2 MAXROWS 10000 MINROWS 50;
USE finance_ctpfuture;

CREATE STABLE IF NOT EXISTS bars (dt timestamp, open float, high float, low float, close float, volume int unsigned, amount bigint unsigned, open_interest int unsigned) TAGS (symbol binary(9), period binary(4), exchange binary(5));

CREATE STABLE IF NOT EXISTS bars_daily (dt timestamp,open float,high float,low float,close float,settle float, volume int unsigned,amount bigint unsigned,open_interest int unsigned) TAGS (symbol binary(9), exchange binary(5));
