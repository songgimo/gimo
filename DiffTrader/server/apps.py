from DiffTrader.server.apis import ProfitSettingTable, ExpectedProfitTable, SlippageDataTable

from flask import Flask
from flask_cors import CORS
from flask_restful import Api


app = Flask(__name__)
api = Api()

cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

api.add_resource(ProfitSettingTable, '/v0/setting/profit-table')
api.add_resource(ExpectedProfitTable, '/v0/trade/expect-profit')
api.add_resource(SlippageDataTable, '/v0/trade/slippage-data')
api.init_app(app)
