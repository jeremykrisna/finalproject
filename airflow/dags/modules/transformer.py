import pandas as pd
from sqlalchemy import create_engine

class Transformer():
    def __init__(self,engine_sql,engine_postgres):
        self.engine_sql=engine_sql
        self.engine_postgres=engine_postgres
    
    def create_dimension_case(self):
        query="SELECT DISTINCT case_type AS case_type_id FROM covid_jabar"
        df_case_type=pd.read_sql(query,con=self.engine_sql)
        df_case_type=df_case_type.rename(columns={'case_type':'case_type_name'})
        df_case_type.to_sql('dim_case',con=self.engine_postgres,index=False,if_exists='replace')
    
    def create_dimension_district(self):
        query="SELECT DISTINCT district_id, district_name FROM covid_jabar"
        df_district=pd.read_sql(query,con=self.engine_sql)
        df_district.to_sql('dim_district',con=self.engine_postgres,index=False,if_exists='replace')
    
    def create_dimension_province(self):
        query="SELECT DISTINCT province_id, province_name FROM covid_jabar"
        df_province=pd.read_sql(query,con=self.engine_sql)
        df_province.to_sql('dim_province',con=self.engine_postgres,index=False,if_exists='replace')
    
    def create_province_daily(self):
        query="SELECT date, district_id, SUM(new_case) AS new_case, SUM(total_case) AS total_case FROM covid_jabar GROUP BY date, district_id"
        df_province_daily=pd.read_sql(query,con=self.engine_sql)
        df_province_daily.to_sql('province_daily',con=self.engine_postgres,index=False,if_exists='replace')
    
    def create_district_daily(self):
        query="SELECT date, district_id, SUM(new_case) AS new_case, SUM(total_case) AS total_case FROM covid_jabar GROUP BY date, district_id"
        df_district_daily=pd.read_sql(query,con=self.engine_sql)
        df_district_daily.to_sql('district_daily',con=self.engine_postgres,index=False,if_exists='replace')