import pandas as pd
from sqlalchemy import create_engine

class Transformer:
    def __init__(self, engine_sql, engine_postgres):
        self.engine_sql = engine_sql
        self.engine_postgres = engine_postgres

    def create_dimension_case(self):
        query = """SELECT row_number() OVER (ORDER BY closecontact_dikarantina, closecontact_discarded,
        closecontact_meninggal,confirmation_meninggal,confirmation_sembuh,probable_diisolasi,probable_discarded,
        probable_meninggal,suspect_diisolasi,suspect_discarded,suspect_meninggal) as id,
            CASE WHEN closecontact_dikarantina > 0 THEN 'closecontact'
                WHEN closecontact_discarded > 0 THEN 'closecontact'
                WHEN closecontact_meninggal > 0 THEN 'closecontact'
                WHEN confirmation_meninggal > 0 THEN 'confirmation'
                WHEN confirmation_sembuh > 0 THEN 'confirmation'
                WHEN probable_diisolasi > 0 THEN 'probable'
                WHEN probable_discarded > 0 THEN 'probable'
                WHEN probable_meninggal > 0 THEN 'probable'
                WHEN suspect_diisolasi > 0 THEN 'suspect'
                WHEN suspect_discarded > 0 THEN 'suspect'
                WHEN suspect_meninggal > 0 THEN 'suspect'
            ELSE NULL
            END AS status_name,
            CASE WHEN closecontact_dikarantina > 0 THEN 'dikarantina'
                WHEN closecontact_discarded > 0 THEN 'discarded'
                WHEN closecontact_meninggal > 0 THEN 'meninggal'
                WHEN confirmation_meninggal > 0 THEN 'meninggal'
                WHEN confirmation_sembuh > 0 THEN 'sembuh'
                WHEN probable_diisolasi > 0 THEN 'diisolasi'
                WHEN probable_discarded > 0 THEN 'discarded'
                WHEN probable_meninggal > 0 THEN 'meninggal'
                WHEN suspect_diisolasi > 0 THEN 'diisolasi'
                WHEN suspect_discarded > 0 THEN 'discarded'
                WHEN suspect_meninggal > 0 THEN 'meninggal'
            ELSE NULL
            END AS status_detail,
            CASE WHEN closecontact_dikarantina > 0 THEN 'closecontact_dikarantina'
                WHEN closecontact_discarded > 0 THEN 'closecontact_discarded'
                WHEN closecontact_meninggal > 0 THEN 'closecontact_meninggal'
                WHEN confirmation_meninggal > 0 THEN 'confirmation_meninggal'
                WHEN confirmation_sembuh > 0 THEN 'confirmation_sembuh'
                WHEN probable_diisolasi > 0 THEN 'probable_diisolasi'
                WHEN probable_discarded > 0 THEN 'probable_discarded'
                WHEN probable_meninggal > 0 THEN 'probable_meninggal'
                WHEN suspect_diisolasi > 0 THEN 'suspect_diisolasi'
                WHEN suspect_discarded > 0 THEN 'suspect_discarded'
                WHEN suspect_meninggal > 0 THEN 'suspect_meninggal'
            ELSE NULL
            END AS status
            FROM covid_jabar
    """
        df_case_type = pd.read_sql(query, con=self.engine_sql)
#        df_case_type = df_case_type.rename(columns={'case_type': 'case_type_name'})

        # Split the status column into status_name and status_detail columns
#        df_case_type['status_name'] = df_case_type['case_type_id'].str.split('_', 1).str[0]
#        df_case_type['status_detail'] = df_case_type['case_type_id'].str.split('_', 1).str[1]

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
        query = "SELECT row_number() OVER (ORDER BY kode_kab, tanggal) as id,kode_kab as district_id,tanggal as date, count(1) AS new_case FROM covid_jabar group by kode_kab,tanggal"
        df_district_daily = pd.read_sql(query, con=self.engine_sql)
        df_district_daily.to_sql('district_daily', con=self.engine_postgres, index=False, if_exists='replace')