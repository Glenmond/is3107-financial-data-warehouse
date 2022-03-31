from google.cloud.bigquery import SchemaField 

tables = ['COMMODITIES', 'EXCHANGE_RATE', 'SG_IR', 'STOCK_FUNDAMENTALS', 'STOCK_INFO', 'ALL_PRICE', 'ALL_TA', 'US_YIELDS']

COMMODITIES = [SchemaField('Date', 'DATE', 'NULLABLE', None, ())
,SchemaField('Commodities_id', 'STRING', 'NULLABLE', None, ())
,SchemaField('Name_id', 'STRING', 'NULLABLE', None, ())
,SchemaField('High', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Low', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Open', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Close', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Adj_close', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Volume', 'FLOAT', 'NULLABLE', None, ())
,]

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

US_YIELDS = [SchemaField('Date', 'DATE', 'NULLABLE', None, ())
,SchemaField('US_yields_id', 'STRING', 'NULLABLE', None, ())
,SchemaField('Name_id', 'STRING', 'NULLABLE', None, ())
,SchemaField('High', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Low', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Open', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Close', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Adj_close', 'FLOAT', 'NULLABLE', None, ())
,SchemaField('Volume', 'FLOAT', 'NULLABLE', None, ())
,]