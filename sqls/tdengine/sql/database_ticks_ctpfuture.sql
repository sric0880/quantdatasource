CREATE DATABASE IF NOT EXISTS finance_ticks_ctpfuture KEEP 3650 DURATION 30 MAXROWS 10000 MINROWS 1000 BUFFER 1024;
USE finance_ticks_ctpfuture;

CREATE STABLE IF NOT EXISTS ticks (dt timestamp, last_price float, volume int unsigned, amount int unsigned, open_interest int unsigned, bid_price1 float, ask_price1 float, bid_volume1 int unsigned, ask_volume1 int unsigned) TAGS (symbol binary(9), exchange binary(5));

CREATE STABLE IF NOT EXISTS tick_bars (dt timestamp, open float, high float, low float, close float, volume int unsigned, amount bigint unsigned, open_interest int unsigned) TAGS (symbol binary(9), period binary(4), exchange binary(5));
