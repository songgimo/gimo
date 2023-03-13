from flask_restful import Resource
from flask import jsonify, request
from bs4 import BeautifulSoup
from KiwoomHighChart.query import GetQueries, PutQueries, TableQueries, DeleteQueries
from KiwoomHighChart.config import IndicatorDict

import datetime
import re


class GetDailyCandle(Resource):
    def get(self):
        args = request.args
        stock_kor = args.get('stock_kor')
        candle_query_data = GetQueries.daily_candle_by_stock_kor(stock_kor)
        indicator_query_data = GetQueries.stock_indicator_by_stock_kor(stock_kor)
        if candle_query_data and indicator_query_data:
            candle_query_data = list(candle_query_data)
            indicator_query_data = list(indicator_query_data)
            return dict(
                success=True,
                data=dict(
                    candle=candle_query_data,
                    indicator=indicator_query_data
                ),
                message=str()
            )
        else:
            return dict(
                success=False,
                data='',
                message='Fail to get daily candle by code, [{}]'.format(stock_kor)
            )
    

class GetStockIndicators(Resource):
    def get(self):
        args = request.args
        stock_kor = args.get('stock_kor')
        
        query_data = GetQueries.stock_indicator_by_stock_kor(stock_kor)
        if query_data:
            query_data = list(query_data)
            return dict(
                success=True,
                data=query_data,
                message=str()
            )
        else:
            return dict(
                success=False,
                data=str(),
                message='Fail to get stock indicator data by stock kor, [{}]'.format(stock_kor)
            )
    

class PutStockIndicators(Resource):
    def post(self):
        indicator_html = request.form.get('indicator_html')
        
        TableQueries.set_indicator_table()
        
        try:
            res = self.html_to_list(indicator_html)
            
            if res:
                return dict(
                    success=True,
                    data=str(res),
                    message=str()
                )
            
            else:
                return dict(
                    success=False,
                    data=str(),
                    message=str('The server only receive one table.')
                )

        except Exception as ex:
            return dict(
                success=False,
                data=str(),
                message=str(ex)
            )
        
    def html_to_list(self, indicator):
        soup = BeautifulSoup(indicator, 'lxml')
        
        tbl_ext_set = soup.findAll('table', {'class': '__se_tbl_ext'})
        
        if not len(tbl_ext_set) == 2:
            return False
        else:
            tr_set = tbl_ext_set[0].findAll('tr')
            default_data = self.get_td_list(tr_set[5])
        
            stock_kor, year_list = list(default_data.items())[0]
            now_year_index = year_list.index(str(datetime.datetime.now().year))
            year_list = year_list[:now_year_index + 1]
        
            year_list = [each for each in year_list if int(each)]
            indicator_dic = dict()
        
            for bs_tr in tr_set[15:]:
                data = self.get_td_list(bs_tr)
                if data is not None:
                    indicator_dic.update(data)
            
            scr = dict()
            total_value_list = list()
            for indi_index, indicator_name in enumerate(indicator_dic):
                value_list = indicator_dic[indicator_name]
                scr.update({indicator_name: indi_index})
                for n, year in enumerate(year_list):
                    total_value_list.append([indicator_name, year, value_list[n], indi_index])

            DeleteQueries.indicator(stock_kor)
            PutQueries.indicator(total_value_list, stock_kor)
            
            return tbl_ext_set[1]
            
    def get_td_list(self, bs_tr):
        # 0번은 PER, PBR같은 지표명
        td_set = bs_tr.findAll('td')
        if not td_set[0]:
            return None
        else:
            indicator_name = re.search('[\w\d].+', td_set[0].text)
            
        if indicator_name:
            indicator_name = indicator_name.group()
        else:
            return None
        
        list_ = list()
        for each in td_set[1:]:
            txt = each.text.replace(' ', '')
            if txt == str():
                break
            list_.append(txt)
        return {indicator_name: list_}
