from google.cloud.bigquery import SchemaField 

tables = ['EXCHANGE_RATE', 'SG_IR', 'STOCK_FUNDAMENTALS', 'STOCK_INFO', 'STOCK_DIVIDENDS', 'ALL_PRICE', 'ALL_TA', 'STOCK_QUERY']


EXCHANGE_RATE = [SchemaField('Date', 'DATE', 'NULLABLE', None, ())
,SchemaField('Exchange_rate_id', 'STRING', 'NULLABLE', None, ())
,SchemaField('Name_id', 'STRING', 'NULLABLE', None, ())
,SchemaField('High', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Low', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Open', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Close', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Adj_close', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Volume', 'FLOAT', 'NULLABLE', None, ())
,]

SG_IR = [SchemaField('Date', 'DATE', 'NULLABLE', None, ())
,SchemaField('aggregate_volume', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('calculation_method', 'STRING', 'NULLABLE', None, ())
,SchemaField('comp_sora_1m', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('comp_sora_3m', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('comp_sora_6m', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('sor_average', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('sora', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('sora_index', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('standing_facility_borrow', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('standing_facility_deposit', 'FLOAT', 'NULLABLE', None, ())
,]

STOCK_FUNDAMENTALS = [SchemaField('Date', 'DATE', 'NULLABLE', None, ())
,SchemaField('Ticker_id', 'STRING', 'NULLABLE', None, ())
,SchemaField('Total_asset_turnover', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Cash_ratio', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Debt_ratio', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Return_on_equity', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Price_earning_ratio', 'FLOAT', 'NULLABLE', None, ())
,]

STOCK_INFO = [SchemaField('Stock', 'STRING', 'NULLABLE', None, ())
,SchemaField('Stock_id', 'STRING', 'NULLABLE', None, ())
,SchemaField('Stock_industry', 'STRING', 'NULLABLE', None, ())
,SchemaField('Stock_summary', 'STRING', 'NULLABLE', None, ())
,]

STOCK_DIVIDENDS = [SchemaField('Date', 'DATE', 'NULLABLE', None, ())
,SchemaField('Stock', 'STRING', 'NULLABLE', None, ())
,SchemaField('Ticker', 'STRING', 'NULLABLE', None, ())
,SchemaField('Dividends', 'FLOAT', 'NULLABLE', None, ())
,]

ALL_PRICE = [SchemaField('Date', 'DATE', 'NULLABLE', None, ())
,SchemaField('Ticker_id', 'STRING', 'NULLABLE', None, ())
,SchemaField('Name_id', 'STRING', 'NULLABLE', None, ())
,SchemaField('High', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Low', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Open', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Close', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Adj_close', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Volume', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Conversion_factor', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Exchange_rate_ticker', 'STRING', 'NULLABLE', None, ())
,]

ALL_TA = [SchemaField('Date', 'DATE', 'NULLABLE', None, ())
,SchemaField('Ticker_id', 'STRING', 'NULLABLE', None, ())
,SchemaField('Name_id', 'STRING', 'NULLABLE', None, ())
,SchemaField('TA_id', 'STRING', 'NULLABLE', None, ())
,SchemaField('TA_description', 'STRING', 'NULLABLE', None, ())
,SchemaField('Value', 'FLOAT', 'NULLABLE', None, ())
,]

STOCK_QUERY = [SchemaField('Date', 'DATE', 'NULLABLE', None, ())
,SchemaField('Ticker', 'STRING', 'NULLABLE', None, ())
,SchemaField('Name', 'STRING', 'NULLABLE', None, ())
,SchemaField('Adj_Close', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Exchange_rate', 'STRING', 'NULLABLE', None, ())
,SchemaField('FX_rate', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Industry', 'STRING', 'NULLABLE', None, ())
,SchemaField('Description', 'STRING', 'NULLABLE', None, ())
,SchemaField('ROE', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('PE_Ratio', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Sora', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Dividends', 'FLOAT', 'NULLABLE', None, ())
,]