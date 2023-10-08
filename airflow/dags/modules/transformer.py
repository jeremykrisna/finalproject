import requests
import json
import pandas as pd
from sqlalchemy import create_engine

class Transformer:
    def __init__(self, engine_sql, engine_postgres):
        self.engine_sql = engine_sql
        self.engine_postgres = engine_postgres

    def create_dimension_case(self):
        query="select * from covid_jabar"
        
        df_case_type = pd.read_sql(query, con=self.engine_sql)
        
        status=[column for column in df_case_type.columns if '_' in column and 'kode' not in column and 'nama' not in column]
        status_name=[i.rsplit('_')[0] for i in status]
        status_detail=[i.rsplit('_')[-1] for i in status]
        id=[i+1 for i in range(len(status))]
        
        data={'id': id, 'status_name' : status_name, 'status_detail' : status_detail, 'status' : status}
        df_case_type=pd.DataFrame(data)
        
        df_case_type.to_sql('dim_case', con=self.engine_postgres, index=False, if_exists='replace')

    def create_dimension_district(self):
        query = "SELECT kode_kab as district_id,kode_prov as province_id,nama_kab as district_name FROM covid_jabar"
        df_district = pd.read_sql(query, con=self.engine_sql)
        df_district.to_sql('dim_district', con=self.engine_postgres, index=False, if_exists='replace')

    def create_dimension_province(self):
        query = "SELECT kode_prov as province_id,nama_prov AS province_name FROM covid_jabar group by 1,2"
        df_province = pd.read_sql(query, con=self.engine_sql)
        df_province.to_sql('dim_province', con=self.engine_postgres, index=False, if_exists='replace')

    def create_province_daily(self):
        query = "SELECT row_number() OVER (ORDER BY kode_prov, tanggal) as id,kode_prov as province_id, tanggal as date FROM covid_jabar"
        df_province_daily = pd.read_sql(query, con=self.engine_sql)
        df_province_daily.to_sql('province_daily', con=self.engine_postgres, index=False, if_exists='replace')

    def create_district_daily(self):
        query="select * from covid_jabar"
        
        df_district_daily = pd.read_sql(query, con=self.engine_sql)
        
        list_case=['closecontact_dikarantina', 'closecontact_discarded',
        'closecontact_meninggal','confirmation_meninggal','confirmation_sembuh','probable_diisolasi'
        ,'probable_discarded','probable_meninggal','suspect_diisolasi','suspect_discarded'
        ,'suspect_meninggal']
        
        list_date=[date for date in df_district_daily.tanggal]
        
#        sql_date_where = None
        
        for case in list_case:
            for date in list_date:
                result=self.engine_sql.execute("""
                                               select sum("+case+") as total 
                                               from covid_jabar where tanggal = '"+date+"'
                                               """)

                rows = result.fetchall()
                if rows:
                    sql_date_where = rows[0]['total']
                    print(rows[0]['total'])
                    
        kode_kab_sql=[district_id for district_id in df_district_daily.kode_kab]
        date_sql=[date for date in df_district_daily.tanggal]
        list_case_sql=[column for column in df_district_daily.columns]
        
        id=[i+1 for i in range(len(kode_kab_sql))]
                   
        data={'id': id, 'district_id' : kode_kab_sql, 'date' : date_sql , 'total' : sql_date_where}
        print(data)
        df_district_daily=pd.DataFrame(data)

        df_district_daily.to_sql('district_daily', con=self.engine_postgres, index=False, if_exists='replace')