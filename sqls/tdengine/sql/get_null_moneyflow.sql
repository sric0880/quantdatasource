SELECT dt, symbol, name, master2_net_flow_in FROM finance.bars_stock_daily where dt >= NOW - 1y and master2_net_flow_in='nan' order by dt >> null_moneyflow.csv;
