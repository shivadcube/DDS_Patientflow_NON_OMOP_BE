from . import constants as cs

class PatientFlow:
    """
    Class contains all the functions related to Attribute
    """

    def __init__(self):
        """ Initialize all class level variables """
        pass

    def dict_convert(self):
        res = ''
        res = res + "import pandas as pd" + "\n"
        res = res + "import numpy as np" + "\n"
        res = res + "import functools as ft\n"
        res = res + "import warnings\n"
        res = res + "import boto3\n"
        res = res + "import io\n"
        res = res + "from collections import defaultdict\n"
        res = res + "from datetime import datetime\n"
        res = res + "from pyhive import hive\n"
        res = res + "from thrift.transport import THttpClient\n"
        res = res + "import base64\n"
        res = res + "import os\n"
        res = res + "import sys\n"
        res = res + "import math\n"
        res = res + "warnings.simplefilter(action='ignore', category=FutureWarning)\n\n"
        res = res + """def dict_convert(df):
    levels = len(df.columns)-1
    dicts = [{} for i in range(levels)] 
    last_index = None
    for row in df.itertuples(index=False):
        index = tuple(row[0:levels])
        value = row[levels]
        if not last_index: 
            last_index = index

        for (ii,(i,j)) in enumerate(zip(index, last_index)):
            if not i == j: 
                ii = levels - ii -1
                dicts[:ii] =  [{} for _ in dicts[:ii]] 
                break

        for i, key in enumerate(reversed(index)): 
            dicts[i][key] = value
            value = dicts[i]

        last_index = index
    return(dicts[-1])\n\n"""
        return (res)

    def get_table_size(self, db, tbl):
        res = self.dict_convert()
        res = res + "conn = 'https://%s/sql/protocolv1/o/%s/%s'\n\n" % (
            cs.WORKSPACE_URL, cs.WORKSPACE_ID, cs.CLUSTER_ID)
        res = res + "transport = THttpClient.THttpClient(conn)\n\n"
        res = res + "auth = 'token:%s'\n" % cs.TOKEN
        res = res + "PY_MAJOR = sys.version_info[0]\n"
        res = res + "if PY_MAJOR < 3:\n"
        res = res + "\tauth = base64.standard_b64encode(auth)\n"
        res = res + "else:\n"
        res = res + "\tauth = base64.standard_b64encode(auth.encode()).decode()\n\n"
        res = res + "transport.setCustomHeaders({'Authorization': 'Basic %s' % auth})\n"
        res = res + "conn = hive.connect(thrift_transport=transport)\n"
        res = res + "comput = pd.read_sql('ANALYZE TABLE " + str(db) + "." + str(tbl)
        res = res + " COMPUTE STATISTICS', conn)" + "\n"
        res = res + "stats = pd.read_sql('DESCRIBE TABLE "
        res = res + "EXTENDED " + str(db) + "." + str(tbl) + "'"
        res = res + ", conn)\n\n"
        res = res + "print(stats[stats['col_name']=='Statistics']['data_type'].tolist())\n"
        return res

    def read_table(self, analysis_id, db, tbl):
        res = ''
        res = res + "conn = 'https://%s/sql/protocolv1/o/%s/%s'\n\n" % (
            cs.WORKSPACE_URL, cs.WORKSPACE_ID, cs.CLUSTER_ID)
        res = res + "transport = THttpClient.THttpClient(conn)\n\n"
        res = res + "auth = 'token:%s'\n" % cs.TOKEN
        res = res + "PY_MAJOR = sys.version_info[0]\n"
        res = res + "if PY_MAJOR < 3:\n"
        res = res + "\tauth = base64.standard_b64encode(auth)\n"
        res = res + "else:\n"
        res = res + "\tauth = base64.standard_b64encode(auth.encode()).decode()\n\n"
        res = res + "transport.setCustomHeaders({'Authorization': 'Basic %s' % auth})\n"
        res = res + "conn = hive.connect(thrift_transport=transport)\n"
        res = res + "df_" + str(analysis_id) + """ = pd.read_sql("select * from """
        res = res + str(db) + "." + str(tbl) + " where (code_flag='Rx') or "
        res = res + "(code_flag='Px' and source_value like 'Q%') or "
        res = res + """(code_flag='Px' and source_value like 'J%')", conn)""" + "\n\n"

        res = res + "try:\n"
        res = res + "\tflag = {}\n"
        res = res + "\tlen_df = len(df_" + str(analysis_id) + ".head())" + "\n"
        res = res + "\tif len_df == 0:" + "\n"
        res = res + "\t\t raise Exception" + "\n"
        res = res + "\telse:" + "\n"
        res = res + "\t\tflag['record_count'] = len_df\n"
        res = res + "\t\tprint(flag)" + "\n"
        res = res + "except:" + "\n"
        res = res + "\tflag['record_count'] = 0" + "\n"
        res = res + "\tprint(flag)" + "\n"
        return (res)

    def read_data(self, analysis_id, cohort_id, history_id):
        res = self.dict_convert()
        res = res + "df_" + str(analysis_id) + " = pd.read_parquet('"
        res = res + "s3://{}/{}/cohort_{}_{}_analytics_data')\n\n".format(
            cs.CB_OUTPUT_BUCKET, cs.CB_OUTPUT_PATH, cohort_id, history_id)
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id)
        res = res + ".rename(columns={'person_id':'patient_id', "
        res = res + "'gender_concept_id':'gender'}, errors='ignore')" + "\n\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id)
        res = res + ".astype({'patient_id': 'str', 'start_date':'datetime64',"
        res = res + "'activity_date':'datetime64', 'age':'int32', 'days_supply'"
        res = res + ":'int32', 'quantity':'int32', 'npi_number':'int64', "
        res = res + "'source_value':'str', 'days_supply':'str'}, errors='ignore')\n\n"
        res = res + "df_" + str(analysis_id) + "['specialty_code'] = df_"
        res = res + str(analysis_id) + "['specialty_code'].astype(str)"
        res = res + ".replace('\.0', '', regex=True)" + "\n\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id)
        res = res + "[df_" + str(analysis_id) + "['code_flag'].isin(['Rx', 'Px'])]" + "\n\n"
        
        res = res + "try:\n"
        res = res + "\tflag = {}\n"
        res = res + "\tlen_df = len(df_" + str(analysis_id) + ".head())" + "\n"
        res = res + "\tif len_df == 0:" + "\n"
        res = res + "\t\t raise Exception" + "\n"
        res = res + "\telse:" + "\n"
        res = res + "\t\tflag['record_count'] = len_df\n"
        res = res + "\t\tprint(flag)" + "\n"
        res = res + "except:" + "\n"
        res = res + "\tflag['record_count'] = 0" + "\n"
        res = res + "\tprint(flag)" + "\n"
        return (res)

    def upload_file(self, analysis_id, path):
        res = self.dict_convert()
        path_obj = path.split("/")

        res = res + "s3 = boto3.client('s3')\n"

        res = res + "obj = s3.get_object(Bucket='"
        res = res + str(path_obj[2]) + "', Key='" + '/'.join(path_obj[3::]) + "')\n"
        res = res + "df_" + str(analysis_id) + " = pd.read_csv(io.BytesIO(obj['Body']"
        res = res + ".read()))\n\n"

        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id)
        res = res + ".rename(columns={'person_id':'patient_id', "
        res = res + "'gender_concept_id':'gender'}, errors='ignore')" + "\n\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id)
        res = res + ".astype({'patient_id': 'str', 'start_date':'datetime64',"
        res = res + "'activity_date':'datetime64', 'age':'int32', 'days_supply'"
        res = res + ":'int32', 'quantity':'int32', 'npi_number':'int64', "
        res = res + "'source_value':'str'}, errors='ignore').drop_duplicates()\n\n"
        res = res + "df_" + str(analysis_id) + "['specialty_code'] = df_"
        res = res + str(analysis_id) + "['specialty_code'].astype(str)"
        res = res + ".replace('\.0', '', regex=True)" + "\n\n"

        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id)
        res = res + "[df_" + str(analysis_id) + "['code_flag'].isin(['Rx', 'Px'])]" + "\n\n"
        res = res + "try:\n"
        res = res + "\tflag = {}\n"
        res = res + "\tlen_df = len(df_" + str(analysis_id) + ".head())" + "\n"
        res = res + "\tif len_df == 0:" + "\n"
        res = res + "\t\t raise Exception" + "\n"
        res = res + "\telse:" + "\n"
        res = res + "\t\tflag['record_count'] = len_df\n"
        res = res + "\t\tprint(flag)" + "\n"
        res = res + "except:" + "\n"
        res = res + "\tflag['record_count'] = 0" + "\n"
        res = res + "\tprint(flag)" + "\n"
        return (res)

    def read_file(self, analysis_id, path):
        res = self.dict_convert()
        res = res + "df_" + str(analysis_id) + " = "
        res = res + "pd.read_parquet('" + str(path) + "')\n"
        return (res)

    def save_to_s3(self, project_id, analysis_id):
        res = ''
        res = res + "df_" + str(analysis_id)+ ".drop(columns=['lot', 'mono_combo'], errors='ignore')"
        res = res + ".to_parquet('s3://{}/prj_".format(cs.S3_PROJECT_PATH)
        res = res + str(project_id) + "/prj_" + str(project_id)
        res = res + "_" + str(analysis_id) + "')\n"
        return (res)

    def date_range(self, analysis_id):
        res = "date_periods = {'analysis_period':{'start_date':'','end_date':''}"
        res = res + ",'look_forward':{'start_date':'','end_date':''},'look_back'"
        res = res + ":{'start_date':'','end_date':''}}\n\n"
        res = res + "date_periods['analysis_period']['start_date'] = str(np.min(df_"
        res = res + str(analysis_id) + "['activity_date']))\n\n"
        res = res + "date_periods['analysis_period']['end_date'] = str(np.max(df_"
        res = res + str(analysis_id) + "['activity_date']))\n\n"
        res = res + "date_periods\n"
        return (res)

    def drop_attribute(self, analysis_id, col_name):
        res = "df_" + str(analysis_id) + " = df_" + str(analysis_id)
        res = res + ".drop(['{}'], axis=1)".format(col_name)
        return (res)

    def age_range_attribute(self, analysis_id, splits, labels, col_name):
        splits.pop() 
        splits = map(str, splits)
        splits = ', '.join(splits)
        splt_str = "[float('-inf'), float('inf')]"
        for i in range(len(splt_str)):
            if splt_str[i] == ',':
                lst = list(splt_str)
                lst.insert(i, ', ' + splits)
                lst = ''.join(lst)
        res = "splits = " + str(lst) + "\n"
        res = res + "labels = " + str(labels) + "\n"
        res = res + "df_" + str(analysis_id) + "['" + str(col_name) + "'] = pd.cut(x=df_"
        res = res + str(analysis_id) + "['age'], bins=splits, labels=labels)\n"
        return (res)

    def gender_attribute(self, analysis_id, map_lst, dim_lst, col_name):
        res = ''
        res = res + "df_" + str(analysis_id) + "['" + str(col_name) + "'] = "
        res = res + "np.select(["
        if (len(dim_lst) >= 1):
            for i in range(len(dim_lst)):
                res = res + "(df_" + str(analysis_id)
                res = res + "['gender'].isin(['" + str(map_lst[i]) + "'])), "

            result = list(res)
            del result[-2:-1]
            res = ''.join(result)
            res = res + "], " + str(dim_lst) + ", default = 'others')"
            return (res)
        else:
            return ("Invalid Gender Mappings")

    def generate_drug_class(self, analysis_id, drug_class_lst, drug_class_codes, col_name):
        res = ''
        res = res + "df_" + str(analysis_id) + "['" + str(col_name) + "'] = "
        res = res + "np.select(["
        if (len(drug_class_lst) >= 1):
            for i in range(len(drug_class_lst)):
                res = res + "(df_" + str(analysis_id)
                res = res + "['source_value'].isin(" + str(drug_class_codes[i]) + ")), "
            result = list(res)
            del result[-2:-1]
            res = ''.join(result)
            res = res + "], " + str(drug_class_lst) + ", default = 'undefined_drugs')"
            return (res)
        else:
            return ("Invalid Drug Class Mappings")

    def generate_provider_specialty(self, analysis_id, provider_specialty_lst,
                                    provider_specialty_codes, col_name):
        res = ''
        res = res + "df_" + str(analysis_id) + "['" + str(col_name) + "'] = "
        res = res + "np.select(["
        if (len(provider_specialty_lst) >= 1):
            for i in range(len(provider_specialty_lst)):
                res = res + "(df_" + str(analysis_id)
                res = res + "['specialty_code'].isin(" + str(provider_specialty_codes[i])
                res = res + ")), "
            result = list(res)
            del result[-2:-1]
            res = ''.join(result)
            res = res + "], " + str(provider_specialty_lst) + ", default='others')\n"
            return (res)
        else:
            return ("Invalid Provider Specialty Mappings")

    def cross_tab(self, *argv):
        res = "dict(df.groupBy(" + str(
            argv) + ").agg(countDistinct('patient_id').alias('count')).rdd.groupBy(lambda x:x[0]).map(lambda x:(x[0],{y[1]:y[2] for y in x[1]})).collect())\n"
        return (res)

    def range_filter(self, analysis_id, start_date, end_date):
        res = "mask = (df_" + str(analysis_id) + "['activity_date'] >= '"
        res = res + str(start_date) + "') & (df_" + str(analysis_id)
        res = res + "['activity_date'] <= '" + str(end_date) + "')\n"
        return res

    def get_attribute_data(self, analysis_id, attribute_list, start_date, end_date):
        res = self.range_filter(analysis_id, start_date, end_date)
        res = res + "ndf = df_" + str(analysis_id) + ".loc[mask]"
        res = res + ".groupby(" + str(attribute_list) + ")['patient_id'].nunique()"
        res = res + ".reset_index().rename(columns={'patient_id':'count'})\n"
        res = res + "dict_convert(ndf)\n"
        return (res)

    def analysis_summary(self, analysis_id, attribute_list, start_date, end_date):
        res = ''
        res = res + "summary = dict()\n"
        res = res + self.range_filter(analysis_id, start_date, end_date)
        res = res + "summary['Treated'] = df_" + str(analysis_id) + ".loc[mask]['patient_id'].nunique()\n"
        attribute_df = []
        if len(attribute_list) > 1:
            for i in range(len(attribute_list)):
                res = res + "ndf" + str(i + 1) + "= df_" + str(analysis_id) + ".loc[mask]"
                res = res + ".groupby(" + str(attribute_list[0:i + 1]) + ")['patient_id'].nunique()"
                res = res + ".reset_index().rename(columns={'patient_id':'count'})\n\n"
                attribute_df.append("ndf" + str(i + 1))
            res = res + "ndf = ndf1"
            for j in range(len(attribute_df) - 1):
                res = res + ".merge(" + str(attribute_df[j + 1])
                res = res + ", on=" + str(attribute_list[0:j + 1]) + ", how='left')"
            res = res + "\n\n"
        else:
            res = res + "ndf = df_" + str(analysis_id) + ".loc[mask]"
            res = res + ".groupby(" + str(attribute_list) + ")['patient_id'].nunique()"
            res = res + ".reset_index().rename(columns={'patient_id':'count'})\n\n"
        res = res + "summary['children'] = dict_convert(ndf)\n\n"
        res = res + "summary\n"
        return (res)

    def ndc_codes(self, analysis_id):
        res = "list(filter(None.__ne__, [row.source_value for row in df_"
        res = res + str(analysis_id) + ".select('source_value').collect()]))"
        return (res)

    def speciality(self, analysis_id):
        res = "list(filter(None.__ne__, [row.specialty_code for row in df_"
        res = res + str(analysis_id) + ".select('specialty_code').collect()]))"
        return (res)

    def summary(self, analysis_id, start_date, end_date):
        res = "sum_dict = {}\n"
        res = res + self.age_range_attribute(analysis_id, [10, 20, 30, 40, 50, 60, 150],
                                             ['0-10', '11-20', '21-30', '31-40', '41-50', '51-60', '60+'],
                                             "default_age_range") + "\n"
        res = res + self.range_filter(analysis_id, start_date, end_date) + "\n"
        res = res + "ndf = df_" + str(analysis_id) + ".loc[mask]" + "\n\n"

        res = res + "adf = ndf.groupby(['default_age_range']).agg({'patient_id':'nunique'})"
        res = res + ".reset_index().rename(columns={'patient_id':'count'})\n"
        res = res + "sum_dict['age'] = dict_convert(adf)\n\n"

        res = res + "gdf = ndf.groupby(['gender']).agg({'patient_id':'nunique'})"
        res = res + ".reset_index().rename(columns={'patient_id':'count'})\n"
        res = res + "sum_dict['gender'] = dict_convert(gdf)\n\n"

        res = res + "sum_dict['ndc_codes'] = list(set(df_"
        res = res + str(analysis_id) + "['source_value'].tolist()))\n\n"

        res = res + "sum_dict['speciality'] = list(set(df_"
        res = res + str(analysis_id) + "['specialty_code'].replace('nan', np.nan)"
        res = res + ".dropna().tolist()))\n\n"

        res = res + "specialties = [{row[0]:row[1]} "
        res = res + "for row in zip(df_" + str(analysis_id) + "['specialty_code']"
        res = res + ".replace('nan', np.nan).dropna(), "
        res = res + "df_" + str(analysis_id) + "['specialty_desc']"
        res = res + ".replace('nan', np.nan).dropna()) "
        res = res + "if not pd.isnull(row[0]) and not row[0]=='']\n"

        res = res + "sum_dict['specialty_list'] = list({frozenset(item.items()):item "
        res = res + "for item in specialties}.values())\n\n"

        res = res + "sum_dict['Total'] = ndf['patient_id'].nunique()\n\n"

        res = res + self.drop_attribute(str(analysis_id), 'default_age_range') + "\n\n"
        res = res + "del(ndf)" + "\n\n"
        res = res + "sum_dict"
        return (res)


class LOT:
    """
    Class contains all the functions related to LOT Attribute
    """

    def __init__(self):
        """ Initialize all class level variables """
        pass

    def get_days_supply(self, data):
        if (data['Reg/Line']['OW_Period'] == ''):
            return ("days_of_supply = row['days_of_supply']\n")
        else:
            return ('')

    def create_episode_regimen(self, analysis_id, data, drug_col, start_date, end_date):
        res = ''
        res = res + PatientFlow().range_filter(analysis_id, start_date, end_date)
        res = res + "data_" + str(analysis_id) + " = df_" + str(analysis_id)
        res = res + ".loc[mask]" + "\n\n"

        res = res + "data_" + str(analysis_id) + "['end_date'] = data_" + str(
            analysis_id) + "['activity_date']+pd.DateOffset"
        res = res + "(" + str(data['Grace_Period']['Drop_Off_Period']) + ")\n"
        res = res + "data_" + str(analysis_id) + "['drop_period_regimen'] = pd.np.empty((len(data_" + str(
            analysis_id) + "), 0)).tolist()\n"

        if (data['Grace_Period']['Drop_Off_Period'] != '0'):
            res = res + "for name, pdf in data_" + str(analysis_id) + ".groupby('patient_id'):\n"
            res = res + "\tfor i, row in pdf.iterrows():\n"
            res = res + "\t\tdata_" + str(analysis_id) + ".at[i, 'drop_period_regimen'] = pdf[(pdf"
            res = res + "['activity_date'] >= row['activity_date']) & (pdf"
            res = res + "['activity_date'] <= row['end_date'])]['" + str(drug_col) + "']"
            res = res + ".unique().tolist()\n"

        res = res + "sameDayDrugs = data_" + str(analysis_id) + ".groupby(['patient_id','activity_date'])['"
        res = res + str(drug_col) + "'].apply(list).reset_index().rename(columns"
        res = res + "={'" + str(drug_col) + "':'same_day_drugs'})\n"
        res = res + "data_" + str(analysis_id) + " = pd.merge(data_" + str(
            analysis_id) + ", sameDayDrugs, on=['patient_id',"
        res = res + "'activity_date'], how='left').sort_values(by=['patient_id',"
        res = res + "'activity_date'])\n"

        res = res + "max_days_supply = data_" + str(analysis_id) + ".groupby(['patient_id','activity_date'])"
        res = res + "['days_supply'].max().reset_index().rename(columns="
        res = res + "{'days_supply':'days_of_supply'})\n"
        res = res + "data_" + str(analysis_id) + " = pd.merge(data_" + str(
            analysis_id) + ", max_days_supply, on=['patient_id',"
        res = res + "'activity_date'], how='left').sort_values(by="
        res = res + "['patient_id','activity_date'])\n"

        res = res + "data_" + str(analysis_id) + "['same_drugs'] = data_" + str(analysis_id) + "['" + str(drug_col)
        res = res + "'].apply(lambda x: x.split(','))\n\n"
        res = res + "prev_regimen = data_" + str(analysis_id) + "['same_drugs'][0]\n"
        res = res + "episode_date = data_" + str(analysis_id) + "['activity_date'][0]\n"
        res = res + "prev_pid = data_" + str(analysis_id) + "['patient_id'][0]\n"
        res = res + "case_flag = ''\n"
        res = res + "lot = 1\n"
        res = res + "days_of_supply = data_" + str(analysis_id) + "['days_of_supply'][0]\n"
       
        if (len(data['Reg/Line']['Exception_List']) != 0):
            res = res + "drugs_lst_ = " + str([i["Drug_Class_Values"] for i in data['Reg/Line']['Exception_List']]) + "\n"
        
        res = res + "data_" + str(analysis_id) + " = data_" + str(analysis_id)
        res = res + ".sort_values(by=['patient_id', 'activity_date', '" + str(drug_col)
        res = res + "'], ascending=[True, True, False])\n\n"
        
        res = res + "for i, row in data_" + str(analysis_id) + ".iterrows():\n"
        if (data['Reg/Line']['OW_Period'] == ''):
            res = res + "  window_opt = (row['activity_date']-episode_date)"
            res = res + ".days <= days_of_supply" + "\n"
        else:
            res = res + "\twindow_opt = (row['activity_date']-episode_date)"
            res = res + ".days <= " + str(data['Reg/Line']['OW_Period']) + "\n"
        res = res + "\tdrugNotInRegimen = not set((row['same_day_drugs'])).issubset(set(prev_regimen))" + "\n"
        res = res + "\tif(prev_pid == row['patient_id']):" + "\n"
        res = res + "\t\tif window_opt==True and drugNotInRegimen==True:" + "\n"
        res = res + "\t\t\tprev_regimen = list(set(prev_regimen)|set(row['same_drugs']))" + "\n"
        if(len(data['Reg/Line']['Exception_List']) != 0):
            res = res + "\t\t\tflag=0" + "\n"
            res = res + "\t\t\tfor m in range(len(drugs_lst_)):" + "\n"
            res = res + "\t\t\t\tif flag == 1:" + "\n"
            res = res + "\t\t\t\t\tbreak" + "\n"
            res = res + "\t\t\t\tfor n in range(len(drugs_lst_[m])):" + "\n"
            res = res + "\t\t\t\t\tif set([drugs_lst_[m][n-1],drugs_lst_[m][n]]).issubset(set(prev_regimen)):" + "\n"
            res = res + "\t\t\t\t\t\tepisode_date = row['activity_date']" + "\n"
            res = res + "\t\t\t\t\t\tprev_regimen = row['same_drugs']" + "\n"
            res = res + "\t\t\t\t\t\tflag=1" + "\n"
            res = res + "\t\t\t\t\t\tbreak" + "\n"
        res = res + "\t\telif window_opt==True and drugNotInRegimen==False:" + "\n"
        res = res + "\t\t\tpass" + "\n"
        res = res + "\t\telif window_opt==False and drugNotInRegimen==False:" + "\n"
        res = res + "\t\t\tprev_regimen = row['same_drugs']" + "\n"
        res = res + "\t\t\tepisode_date = row['activity_date']" + "\n"
        res = res + "\t\t\tdays_of_supply = row['days_of_supply']" + "\n"
        res = res + "\t\telif window_opt==False and drugNotInRegimen==True:" + "\n"
        res = res + "\t\t\tprev_regimen = row['same_drugs']" + "\n"
        res = res + "\t\t\tepisode_date = row['activity_date']" + "\n"
        res = res + "\t\t\tdays_of_supply = row['days_of_supply']" + "\n"
        res = res + "\telse:" + "\n"
        res = res + "\t\tprev_regimen = row['same_drugs']" + "\n"
        res = res + "\t\tlot = 1" + "\n"
        res = res + "\t\tepisode_date = row['activity_date']" + "\n"
        res = res + "\t\tdays_of_supply = row['days_of_supply']" + "\n"
        res = res + "\tdata_{0}.at[i, 'regimen'] = str(sorted(prev_regimen))".format(str(analysis_id)) + "\n"
        res = res + "\tdata_{0}.at[i, 'episode_date'] = episode_date".format(str(analysis_id)) + "\n"
        res = res + "\tprev_pid = row['patient_id']" + "\n\n"

        res = res + "regimens = data_{0}.groupby(['patient_id','episode_date'])".format(str(analysis_id))
        res = res + ".agg({'drop_period_regimen':'first', 'regimen':'last'}).reset_index()" + "\n"
        res = res + "regimens['episode_regimen']= regimens['regimen']" + "\n"
        res = res + "regimens['drop_period_regimen']= regimens['drop_period_regimen']"
        res = res + ".apply(lambda x: sorted(x))" + "\n"
        res = res + "data_{0} = data_{0}.rename(columns={{'regimen':'old_regimen', ".format(str(analysis_id))
        res = res + "'drop_period_regimen':'old_drop_period_regimen'})" + "\n"
        res = res + "data_{0} = pd.merge(data_{0}, regimens, on=['patient_id','episode_date'], how='left')".format(str(analysis_id)) + "\n"
        res = res + "data_{0} = data_{0}.drop(['regimen', 'old_regimen', 'old_drop_period_regimen'], axis = 1)".format(str(analysis_id))
        return (res)

    def line_except(self, analysis_id, data, drug_col):
        res = ''
        if (len(data['Reg/Line']['Exception_List']) != 0):

            res = res + "drugs_lst_ = " + str([i["Drug_Class_Values"] for i in data['Reg/Line']['Exception_List']]) + "\n"
            return res
        return res

    def add_sub_except(self, analysis_id, data, drug_col):
        res = ''
        if (len(data['Add/Sub']['Exception_List']) != 0):
            for i in range(len(data['Add/Sub']['Exception_List'])):
                if (not data['Add/Sub']['Exception_List'][i]['Drug_Class_Values']):
                    res = res + "add_drugs_lst_" + str(i + 1) + " = data_" + str(analysis_id) + "[data_" + str(
                        analysis_id) + "['"
                    res = res + str(data['Add/Sub']['Exception_List'][i]['Drug_Class_Attribute'])
                    res = res + "']=='"
                    res = res + str(data['Add/Sub']['Exception_List'][i]['Drug_Class_Name'])
                    res = res + "']['" + str(drug_col) + "'].unique().tolist()\n\n"
                else:
                    res = res + "add_drugs_lst_" + str(i + 1) + " = " + str(data['Add/Sub']
                                                                            ['Exception_List'][i][
                                                                                'Drug_Class_Values']) + "\n"
            return res
        return res

    def data_prep(self, analysis_id, data, drug_col):
        res = ''
        res = res + "prev_regimen = data_" + str(analysis_id) + "['same_drugs'][0]\n"
        res = res + "index_date = data_" + str(analysis_id) + "['activity_date'][0]\n"
        res = res + "prev_pid = data_" + str(analysis_id) + "['patient_id'][0]\n"
        res = res + "prev_date = data_" + str(analysis_id) + "['activity_date'][0]\n"
        res = res + "lot = 1\n"
        if (data['Reg/Line']['OW_Period'] == ''):
            res = res + "days_of_supply = data_" + str(analysis_id) + "['days_of_supply'][0]\n"

        res = res + self.line_except(analysis_id, data, drug_col)

        res = res + self.add_sub_except(analysis_id, data, drug_col)

        if (len(data['Subset_Regimen']['Exception_List']) != 0):
            for i in range(len(data['Subset_Regimen']['Exception_List'])):
                if (not data['Subset_Regimen']['Exception_List'][i]['Drug_Class_Values']):
                    res = res + "drop_drugs_lst_" + str(i + 1) + " = data_" + str(analysis_id) + "[data_" + str(
                        analysis_id) + "['"
                    res = res + str(data['Subset_Regimen']['Exception_List'][i]['Drug_Class_Attribute'])
                    res = res + "']=='"
                    res = res + str(data['Subset_Regimen']['Exception_List'][i]['Drug_Class_Name'])
                    res = res + "']['" + str(drug_col) + "'].unique().tolist()\n\n"
                else:
                    res = res + "drop_drugs_lst_" + str(i + 1) + " = " + str(data['Subset_Regimen']
                                                                             ['Exception_List'][i][
                                                                                 'Drug_Class_Values']) + "\n"
        res = res + "for i, row in data_" + str(analysis_id) + ".iterrows():\n"
        return (res)

    def reg_exception(self, data):
        res = ''
        for i in range(len(data['Reg/Line']['Exception_List'])):
            ch_regimen = data['Reg/Line']['Exception_List'][i]['Change_Regimen']
            pr_line = data['Reg/Line']['Exception_List'][i]['Progress_Line']
            res = res + ""
            res = res + "\t\telif window_opt==True and drugNotInRegimen==True "
            res = res + "and line_flag_" + str(i + 1) + "==True:\n"
            if (ch_regimen == True and pr_line == True):
                res = res + "\t\t\tprev_regimen = row['same_drugs']\n"
                res = res + "\t\t\tindex_date = row['activity_date']\n"
                res = res + "\t\t\tlot = lot + 1\n"
                res = res + "\t\t\tcase_flag = 'drug_added'\n"
                if (data['Reg/Line']['OW_Period'] == ''):
                    res = res + "\t\t\tdays_of_supply = row['days_of_supply']\n"
            if (ch_regimen == True and pr_line == False):
                res = res + "\t\t\tprev_regimen = row['same_drugs']\n"
                if (data['Reg/Line']['OW_Period'] == ''):
                    res = res + "\t\t\tdays_of_supply = row['days_of_supply']\n"
        res = res[4:-1]
        res = list(res)
        res.insert(0, "\t\t")
        res = ''.join(res)
        return (res)

    def drop_off(self, data):
        res = ''
        ch_regimen = data['Subset_Regimen']['Change_Regimen']
        pr_line = data['Subset_Regimen']['Progress_Line']
        if (pr_line == True and ch_regimen == True):
            res = res + "\t\t\tprev_regimen = row['same_drugs']\n"
            res = res + "\t\t\tindex_date = row['activity_date']\n"
            res = res + "\t\t\tlot = lot + 1\n"
            res = res + "\t\t\tcase_flag = 'restart_false_subset_regimen'\n"
        elif (pr_line == False and ch_regimen == True):
            res = res + "\t\t\tindex_date = row['activity_date']\n"
            res = res + "\t\t\tprev_regimen = row['same_drugs']\n"
            res = res + "\t\t\tcase_flag = 'restart_false_subset_regimen'\n"
        elif (pr_line == False and ch_regimen == False):
            res = res + "\t\t\tcase_flag = 'restart_false_subset_regimen'\n"
        return (res)

    def restart(self, data):
        res = ''
        pr_line = data['Restart']['Progress_Line']
        res = res + "\t\telif drug_restart==True and same_regimen==True:\n"
        if (pr_line == True):
            res = res + "\t\t\tindex_date = row['activity_date']\n"
            res = res + "\t\t\tlot = lot + 1\n"
            res = res + "\t\t\tcase_flag = 'restart_same_regimen'\n"
            if (data['Reg/Line']['OW_Period'] == ''):
                res = res + "\t\t\tdays_of_supply = row['days_of_supply']:\n"
        elif (pr_line == False):
            res = res + "\t\t\tcase_flag = 'restart_same_regimen'\n"
            if (data['Reg/Line']['OW_Period'] == ''):
                res = res + "\t\t\tdays_of_supply = row['days_of_supply']:\n"

        if (data['Restart']['Regimen_Type'] == "Same_or_Subset"):
            res = res + "\t\telif drug_restart==True and drop_off_2==True:\n"
            if (pr_line == True):
                res = res + "\t\t\tindex_date = row['activity_date']\n"
                res = res + "\t\t\tlot = lot + 1\n"
                res = res + "\t\t\tcase_flag = 'restart_subset_regimen'\n"
                if (data['Reg/Line']['OW_Period'] == ''):
                    res = res + "\t\t\tdays_of_supply = row['days_of_supply']\n"
            elif (pr_line == False):
                res = res + "\t\t\tcase_flag = 'restart_subset_regimen'\n"
                if (data['Reg/Line']['OW_Period'] == ''):
                    res = res + "\t\t\tdays_of_supply = row['days_of_supply']\n"

        res = res + "\t\telif drug_restart==True and drop_off_2==True and drop_off_3==True:\n"
        if (pr_line == True):
            res = res + "\t\t\tindex_date = row['activity_date']\n"
            res = res + "\t\t\tlot = lot + 1\n"
            res = res + "\t\t\tcase_flag = 'restart_Grace_Period_regimen'\n"
            if (data['Reg/Line']['OW_Period'] == ''):
                res = res + "\t\t\tdays_of_supply = row['days_of_supply']\n"
        elif (pr_line == False):
            res = res + "\t\t\tcase_flag = 'restart_Grace_Period_regimen'\n"
            if (data['Reg/Line']['OW_Period'] == ''):
                res = res + "\t\t\tdays_of_supply = row['days_of_supply']\n"

        return res

    def add_exception(self, data):
        res = ''
        for i in range(len(data['Add/Sub']['Exception_List'])):
            ch_regimen = data['Add/Sub']['Exception_List'][i]['Change_Regimen']
            pr_line = data['Add/Sub']['Exception_List'][i]['Progress_Line']
            res = res + ""
            res = res + "\t\telif window_opt==False and drugNotInRegimen==True "
            res = res + "and add_flag_" + str(i + 1) + "==True and Add_Grace_Period_"
            res = res + str(i + 1) + " == True :\n"
            if (ch_regimen == True and pr_line == True):
                res = res + "\t\t\tprev_regimen = row['same_drugs']\n"
                res = res + "\t\t\tindex_date = row['activity_date']\n"
                res = res + "\t\t\tlot = lot + 1\n"
                res = res + "\t\t\tcase_flag = 'add_drug_exception'\n"
                if (data['Reg/Line']['OW_Period'] == ''):
                    res = res + "\t\t\tdays_of_supply = row['days_of_supply']\n"
            elif (ch_regimen == True and pr_line == False):
                res = res + "\t\t\tprev_regimen = row['same_drugs']\n"
                if (data['Reg/Line']['OW_Period'] == ''):
                    res = res + "\t\t\tdays_of_supply = row['days_of_supply']\n"
                res = res + "\t\t\tcase_flag = 'add_drug_exception'\n"
            else:
                res = res + "\t\t\tcase_flag = 'add_drug_exception'\n"
        res = res[4::]
        res = list(res)
        res.insert(0, "\t\t")
        res = ''.join(res)
        return (res)

    def drop_off_excption(self, data):
        res = ''
        if (len(data['Subset_Regimen']['Exception_List']) != 0):
            for i in range(len(data['Subset_Regimen']['Exception_List'])):
                res = res + "\tdrop_off_" + str(i + 1) + " = not all(j in "
                res = res + "row['drop_period_regimen'] for j in "
                res = res + "list(set(prev_regimen)-set(drop_drugs_lst_"
                res = res + str(i + 1) + ")))\n\n"
            return res
        else:
            res = res + "\tdrop_off_2 = set((row['episode_regimen']))"
            res = res + ".issubset(set(prev_regimen))" + "\n\n"
            res = res + "\tdrop_off_3 = set(prev_regimen).issubset(set(row"
            res = res + "['drop_period_regimen']))" + "\n\n"
            return res

    def create_regimen(self, analysis_id, data, col_name, drug_col, start_date, end_date):
        res = self.create_episode_regimen(analysis_id, data, drug_col, start_date, end_date) + "\n\n"
        res = res + self.data_prep(analysis_id, data, drug_col)
        if (data['Reg/Line']['OW_Period'] == ''):
            res = res + "\twindow_opt = (row['activity_date']-index_date)"
            res = res + ".days <= days_of_supply" + "\n\n"
        else:
            res = res + "\twindow_opt = (row['activity_date']-index_date).days <= "
            res = res + str(data['Reg/Line']['OW_Period']) + "\n\n"

        res = res + "\tdrugNotInRegimen = set((row['same_day_drugs']))"
        res = res + ".issubset(set(prev_regimen))\n\n"

        res = res + "\tsame_regimen = set(row['episode_regimen']) == set(prev_regimen)\n\n"


        ############################# Restart Satatement #####################
        res = res + "\tdrug_restart = (row['activity_date']-prev_date).days > "
        res = res + str(data['Restart']['Restart_Period']) + "\n\n"

        res = res + self.drop_off_excption(data)

        if (len(data['Add/Sub']['Exception_List']) != 0):
            for i in range(len(data['Add/Sub']['Exception_List'])):
                res = res + "\tadd_flag_" + str(i + 1) + " = any(i in "
                res = res + "row['episode_regimen'] for i in add_drugs_lst_"
                res = res + str(i + 1) + ")\n"

        if (len(data['Add/Sub']['Exception_List']) != 0):
            for i in range(len(data['Add/Sub']['Exception_List'])):
                res = res + "\tAdd_Grace_Period_" + str(i + 1) + " = (row['activity_date']"
                res = res + " - index_date).days"
                if(data['Add/Sub']['Exception_List'][i]['OP'] == "Post"):
                    res = res + " > "
                else:
                    res = res + " <= "
                res = res + str(data['Add/Sub']['Exception_List'][i]['Grace_Period']) + "\n"

        res = res + "\tif(prev_pid == row['patient_id']):\n"

        res = res + "\t\tif window_opt==True and drugNotInRegimen==False:\n"
        res = res + "\t\t\tprev_regimen = list(set(prev_regimen)|set(row['same_drugs']))\n"
        if(len(data['Reg/Line']['Exception_List']) != 0):
            res = res + "\t\t\tflag=0" + "\n"
            res = res + "\t\t\tfor m in range(len(drugs_lst_)):" + "\n"
            res = res + "\t\t\t\tif flag == 1:" + "\n"
            res = res + "\t\t\t\t\tbreak" + "\n"
            res = res + "\t\t\t\tfor n in range(len(drugs_lst_[m])):" + "\n"
            res = res + "\t\t\t\t\tif set([drugs_lst_[m][n-1],drugs_lst_[m][n]]).issubset(set(prev_regimen)):" + "\n"
            res = res + "\t\t\t\t\t\tindex_date = row['activity_date']" + "\n"
            res = res + "\t\t\t\t\t\tlot = lot +1" + "\n"
            res = res + "\t\t\t\t\t\tprev_regimen = row['same_drugs']" + "\n"
            res = res + "\t\t\t\t\t\tcase_flag = 'First_Line_Exception'\n"
            res = res + "\t\t\t\t\t\tflag=1" + "\n"
            res = res + "\t\t\t\t\t\tbreak" + "\n"

        res = res + "\t\telif window_opt==True and drugNotInRegimen==True:\n"
        res = res + "\t\t\tcase_flag = 'First_Line_started'\n"

        ########################### RESTART ##############################

        res = res + self.restart(data)
        res = res + "\t\telif drug_restart==False and same_regimen==True:\n"
        res = res + "\t\t\tcase_flag = 'restart_false_same_regimen'\n"

        ##################################################################

        if (data['Reg/Line']['OW_Period'] == ''):
            res = res + "\t\t\tdays_of_supply = row['days_of_supply']\n"

        ######################### DROP-OFF ##################################

        if (len(data['Subset_Regimen']['Exception_List']) != 0):
            for i in range(len(data['Subset_Regimen']['Exception_List'])):
                res = res + "\t\telif drop_off_" + str(i + 1) + " == True "
                res = res + "and same_regimen==True:\n"
                res = res + "\t\t\tprev_regimen = row['same_drugs']\n"
                res = res + "\t\t\tindex_date = row['activity_date']\n"
                res = res + "\t\t\tlot = lot + 1\n"
                res = res + "\t\t\tcase_flag = 'drop_off'\n"
        else:

            res = res + "\t\telif drug_restart==False and drop_off_2==True and drop_off_3==True:\n"
            res = res + "\t\t\tcase_flag = 'restart_false_Grace_Period_regimen'\n"
            res = res + "\t\telif drug_restart==False and drop_off_2==True:\n"
            res = res + self.drop_off(data)

        ####################################################################

        ######################### DRUG-ADD/SUB #############################

        res = res + "\t\telif same_regimen==False:\n"
        if (len(data['Add/Sub']['Exception_List']) != 0):
            for i in range(len(data['Add/Sub']['Exception_List'])):
                if i == 0:
                    res = res + "\t\t\tif"
                else:
                    res = res + "\t\t\telif"    
                res = res + " (Add_Grace_Period_" + str(i+1) + " and "
                res = res + "add_flag_" + str(i+1) + " and bool(set(prev_regimen)"
                res = res + ".intersection(set(row['episode_regimen']))) and (set(prev_regimen).issubset(set("
                res = res + str(data["Add/Sub"]["Exception_List"][i]["Drug_Class_Therapy"]) + ")))):" + "\n"

                res = res + "\t\t\t\tprev_regimen = row['same_drugs']\n"
                if(data['Add/Sub']['Exception_List'][i]['Progress_Line']):
                    res = res + "\t\t\t\tlot = lot + 1\n"
                res = res + "\t\t\t\tindex_date = row['activity_date']\n"
                res = res + "\t\t\t\tcase_flag = 'Drug_add/sub_Exception'\n"
            res = res + "\t\t\telse:" + "\n"
            res = res + "\t\t\t\tprev_regimen = row['same_drugs']\n"
            res = res + "\t\t\t\tlot = lot + 1\n"
            res = res + "\t\t\t\tindex_date = row['activity_date']\n"
            res = res + "\t\t\t\tcase_flag = 'Drug add/sub'\n"
        else:
            res = res + "\t\t\tprev_regimen = row['same_drugs']\n"
            res = res + "\t\t\tlot = lot + 1\n"
            res = res + "\t\t\tindex_date = row['activity_date']\n"
            res = res + "\t\t\tcase_flag = 'Drug add/sub'\n"

        ####################################################################

        res = res + "\t\telse:\n"
        res = res + "\t\t\tcase_flag = 'others'\n"

        res = res + "\telse:\n"
        res = res + "\t\tprev_regimen = row['same_drugs']\n"
        res = res + "\t\tlot = 1\n"
        res = res + "\t\tindex_date = row['activity_date']\n"
        res = res + "\t\tcase_flag = 'New_regimen_started'\n"
        if (data['Reg/Line']['OW_Period'] == ''):
            res = res + "\t\tdays_of_supply = row['days_of_supply']\n"
        res = res + "\tdata_" + str(analysis_id) + ".at[i, '" + str(col_name) + "'] = lot\n"
        res = res + "\tdata_" + str(analysis_id) + ".at[i, 'regimen'] = sorted(prev_regimen)\n"
        res = res + "\tdata_" + str(analysis_id) + ".at[i, 'index_date'] = index_date\n"
        res = res + "\tdata_" + str(analysis_id) + ".at[i, 'case_flag'] = case_flag\n"
        res = res + "\tprev_pid = row['patient_id']\n"
        res = res + "\tprev_date = row['activity_date']\n"

        res = res + "\nregimens = data_" + str(analysis_id) + ".groupby(['patient_id','" + str(col_name)
        res = res + "','index_date'])['regimen'].last().reset_index()\n"
        res = res + "regimens['regimen']= regimens['regimen']"
        res = res + ".apply(lambda x: ','.join(sorted(x)) if type(x) is list else ''.join(x))\n"
        res = res + "data_" + str(analysis_id) + " = data_" + str(
            analysis_id) + ".rename(columns={'regimen':'old_regimen'})\n"
        res = res + "data_" + str(analysis_id) + " = pd.merge(data_" + str(
            analysis_id) + ", regimens, on=['patient_id','" + str(col_name)
        res = res + "','index_date'], how='left')\n\n"
        res = res + "data_" + str(analysis_id) + " = data_" + str(
            analysis_id) + ".drop(['old_regimen', 'same_day_drugs', 'same_drugs',"
        res = res + "'episode_regimen', 'drop_period_regimen', 'episode_date', "
        res = res + "'episode_regimen', 'end_date', 'days_of_supply'], axis = 1)\n"
        res = res + "data_" + str(analysis_id) + ".fillna(value=np.nan, inplace=True)\n"

        res = res + "df_columns = list(df_" + str(analysis_id) + ".columns)" + "\n\n"
        res = res + "unwanted_cols = {'" + str(col_name) + "', 'regimen', 'case_flag', "
        res = res + "'index_date'}" + "\n\n"
        res = res + "df_columns = [ele for ele in df_columns if ele not in "
        res = res + "unwanted_cols]" + "\n\n"

        res = res + "data_columns = list(data_" + str(analysis_id) + ".columns)" + "\n\n"

        res = res + "df_" + str(analysis_id) + " = pd.merge(df_" + str(analysis_id)
        res = res + "[df_columns], data_" + str(analysis_id) + "[data_columns], "
        res = res + "on=df_columns, how='left')" + "\n\n"

        res = res + "del(data_" + str(analysis_id) + ")" + "\n\n"
        return (res)


    def patient_share_graph(self, analysis_id, col_name, start_date, end_date, lot_dict={}):
        res = ''
        res = res + PatientFlow().range_filter(analysis_id, start_date, end_date)
        res = res + "patient_ids = df_" + str(analysis_id) + ".loc[mask]"
        if bool(lot_dict) == True:
            res = res + ".loc["
            for key, val in lot_dict.items():
                res = res + "(df_" + str(analysis_id) + "['" + str(key) + "'] == '" + str(val) + "') & "
            result = list(res)
            del result[-3:-1]
            result.insert(-1, "]")
            res = ''.join(result)
        res = res + "['patient_id'].unique()\n\n"
        res = res + "df = df_" + str(analysis_id) + ".loc[df_" + str(analysis_id)
        res = res + "['patient_id'].isin(patient_ids)]" + "\n\n"
        res = res + "df = df"
        res = res + ".groupby(['" + str(col_name) + "']).agg({'patient_id': 'nunique'})"
        res = res + ".reset_index().rename(columns={'patient_id':'total_p_count'})\n\n"
        res = res + "df['total_percentage'] = (df['total_p_count']"
        res = res + "/df['total_p_count'].max())*100" + "\n\n"
        return res

    def patient_share_table(self, analysis_id, col_name, start_date, end_date, lot_dict={}):
        res = ''
        res = res + "def patient_split_with_line(line_number):\n"
        res = res + "\tsummary = defaultdict(dict)\n\n"
        res = res + "\tobj = defaultdict(dict)\n\n"

        res = res + "\t" + PatientFlow().range_filter(analysis_id, start_date, end_date)
        res = res + "\tpatient_ids = df_" + str(analysis_id) + ".loc[mask]"
        if bool(lot_dict) == True:
            res = res + ".loc["
            for key, val in lot_dict.items():
                res = res + "(df_" + str(analysis_id) + "['" + str(key) + "'] == '" + str(val) + "') & "
            result = list(res)
            del result[-3:-1]
            result.insert(-1, "]")
            res = ''.join(result)

        res = res + "['patient_id'].unique().tolist()\n\n"

        res = res + "\tdf1 = df_" + str(analysis_id) + ".loc[df_" + str(analysis_id)
        res = res + "['patient_id'].isin(patient_ids)]\n\n"

        res = res + "\tTotal = df1.loc[df1['" + str(col_name) + "'] == "
        res = res + "line_number]['patient_id'].nunique()\n\n"

        res = res + "\tsummary['Line'] = line_number\n\n"
        res = res + "\tsummary['Total'] = Total\n\n"

        res = res + "\tContinued = df1.loc[(df1['min_activity_date'] < '"

        res = res + str(start_date) + "') & (df1['" + str(col_name)
        res = res + "'] == line_number)]"
        res = res + "['patient_id'].nunique()\n\n"

        res = res + "\tif Continued!=0:\n"
        res = res + "\t\tobj['Person_count'] = Continued\n"
        res = res + "\t\tobj['Percentage'] = (Continued/Total)*100\n"
        res = res + "\t\tsummary['Continued'] = dict(obj)\n"
        res = res + "\telse:\n"
        res = res + "\t\tobj['Person_count'] = Continued\n"
        res = res + "\t\tobj['Percentage'] = 0\n"
        res = res + "\t\tsummary['Continued'] = dict(obj)\n\n"

        res = res + "\tNew = df1.loc[(df1['min_activity_date'] >= '"
        res = res + str(start_date) + "') & (df1['"
        res = res + str(col_name) + "'] == line_number)]"
        res = res + "['patient_id'].nunique()\n\n"

        res = res + "\tif New!=0:\n"
        res = res + "\t\tobj['Person_count'] = New\n"
        res = res + "\t\tobj['Percentage'] = (New/Total)*100\n"
        res = res + "\t\tsummary['New'] = dict(obj)\n"
        res = res + "\telse:\n"
        res = res + "\t\tobj['Person_count'] = New\n"
        res = res + "\t\tobj['Percentage'] = 0\n"
        res = res + "\t\tsummary['New'] = dict(obj)\n\n"

        res = res + "\tend_date = datetime.strptime('" + str(end_date)
        res = res + "', '%Y-%m-%d %H:%M:%S') - pd.DateOffset(90)\n\n"
        res = res + "\tcnt=0\n\n"
        res = res + "\tpatient_ids = []\n\n"
        res = res + "\tpdf = df1.sort_values(['patient_id','activity_date'], ascending"
        res = res + "=False).groupby('patient_id').first()\n\n"
        res = res + "\tfor i, row in pdf.iterrows():\n\n"
        res = res + "\t\tif row['max_activity_date'] < end_date and row['" + str(col_name)
        res = res + "']== line_number:\n\n"
        res = res + "\t\t\tcnt = cnt + 1\n\n"
        res = res + "\t\t\tpatient_ids.append(i)\n\n"

        res = res + "\tif cnt!=0:\n"
        res = res + "\t\tobj['Person_count'] = cnt\n"
        res = res + "\t\tobj['Percentage'] = (cnt/Total)*100\n"
        res = res + "\t\tsummary['DroppedOff'] = dict(obj)\n"
        res = res + "\telse:\n"
        res = res + "\t\tobj['Person_count'] = cnt\n"
        res = res + "\t\tobj['Percentage'] = 0\n"
        res = res + "\t\tsummary['DroppedOff'] = dict(obj)\n\n"

        res = res + "\tProgressed = df1.loc[(df1['"

        res = res + str(col_name) + "'] == line_number+1"

        res = res + ")]['patient_id'].nunique()\n\n"

        res = res + "\tif Progressed!=0:\n"
        res = res + "\t\tobj['Person_count'] = Progressed\n"
        res = res + "\t\tobj['Percentage'] = (Progressed/Total)*100\n"
        res = res + "\t\tsummary['Progressed'] = dict(obj)\n"
        res = res + "\telse:\n"
        res = res + "\t\tobj['Person_count'] = Progressed\n"
        res = res + "\t\tobj['Percentage'] = 0\n"
        res = res + "\t\tsummary['Progressed'] = dict(obj)\n"

        res = res + "\treturn dict(summary)\n"
        return (res)

    def patient_share(self, analysis_id, col_name, start_date, end_date, lot_dict={}):
        res = ''
        res = res + "\n######################## Start of Patient Share Across and Patient Split Within Line of Therapy ########################\n\n"
        res = res + "df_" + str(analysis_id) + "['lot'] = np.select([(df_" + str(analysis_id) + "['"
        res = res + str(col_name) + "']==1), (df_" + str(analysis_id) + "['"
        res = res + str(col_name) + "']==2), (df_" + str(analysis_id) + "['"
        res = res + str(col_name) + "']==3), (df_" + str(analysis_id) + "['"
        res = res + str(col_name) + "']==4)], [1, 2, 3, 4], default = 5).astype(int)" + "\n\n"
        col_name = 'lot'

        res = res + "df = df_" + str(analysis_id) + ".groupby(['patient_id'])"
        res = res + ".agg(min_activity_date=('activity_date', np.min), max_activity_date="
        res = res + "('activity_date', np.max)).reset_index()" + "\n\n"

        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id)
        res = res + ".merge(df, on=['patient_id'], how='left')" + "\n\n"

        res = res + "del(df)\n\n"

        res = res + "result = dict()\n"
        res = res + "\n######################## Start of Patient Share Across Line of Therapy ########################\n\n"
        res = res + self.patient_share_graph(analysis_id, col_name, start_date, end_date, lot_dict)
        res = res + "result['graph'] = dict_convert(df)\n\n"
        res = res + "\n######################## End of Patient Share Across Line of Therapy ########################\n"
        res = res + "\n######################## Start of Patient Split Within Line of Therapy ########################\n\n"
        res = res + self.patient_share_table(analysis_id, col_name, start_date, end_date, lot_dict)
        res = res + "\ntable = []\n"
        res = res + "for i, df in df_" + str(analysis_id)
        res = res + ".groupby(['" + str(col_name) + "']):" + "\n"
        res = res + "\ttry:" + "\n"
        res = res + "\t\tif i in [1, 2, 3, 4, 5]:" + "\n"
        res = res + "\t\t\ttable.append(patient_split_with_line(i))" + "\n"
        res = res + "\texcept Exception:" + "\n"
        res = res + "\t\tcontinue" + "\n\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id)
        res = res + ".drop(columns=['min_activity_date', 'max_activity_date'])" + "\n\n"
        res = res + "result['table'] = table\n\n"
        res = res + "\n######################## End of Patient Split Within Line of Therapy ########################\n\n"
        res = res + "result"
        res = res + "\n\n######################## End of Patient Share Across and Patient Split Within Line of Therapy ########################\n"
        return res

    def regimen_reduction_graphs(self, col_name):
        res = "df1 = df.groupby(['" + str(col_name) + "', 'regimen'])"
        res = res + ".agg({'days_on_therapy':'sum', 'patient_id': 'nunique'})"
        res = res + ".reset_index().rename(columns={'days_on_therapy':'total_dot',"
        res = res + " 'patient_id':'patient_count'})\n\n"

        res = res + "################### Stack chart #############\n\n"

        res = res + "df1 = df1.groupby(['" + str(col_name) + "', 'regimen'])"
        res = res + ".agg({'patient_count':'sum', 'total_dot':'sum'}).reset_index()"
        res = res + ".rename(columns={'patient_count':'patient_count1', "
        res = res + "'total_dot':'total_dot_lot_reg1'})\n\n"


        res = res + "df1['row_num'] = df1.groupby(['" + str(col_name) + "'])['patient_count1']"
        res = res + ".rank(method = 'first', ascending = False)\n"
        res = res + "df1.sort_values(by= ['" + str(col_name) + "', 'patient_count1'], inplace = True)\n"
        res = res + "df1 = df1.reset_index(drop = True)\n\n"

        res = res + "df1['category'] = np.select([(df1['row_num'] <= 12), (df1['row_num'] > 12)],"
        res = res + " [df1['row_num'].astype(int), 'Others'])\n"

        res = res + "df1_new = df1.groupby(['" + str(col_name) + "', 'category'])"
        res = res + ".agg({'patient_count1':'sum', 'total_dot_lot_reg1':'sum'}).reset_index()"
        res = res + ".rename(columns={'patient_count1':'patient_count', "
        res = res + "'total_dot_lot_reg1':'total_dot_lot_reg'})\n\n"

        res = res + "df1['count_'] = df1.groupby(['" + str(col_name) + "','category'])['patient_count1']"
        res = res + ".rank(method='first', ascending = False)\n"
        res = res + "df1.sort_values(by= ['" + str(col_name) + "', 'category' ,'patient_count1'], inplace = True)\n"
        res = res + "df1 = df1.reset_index(drop = True)\n\n"

        res = res + "mask_1 = (df1['count_'] == 1)\n"
        res = res + "df1 = df1.loc[mask_1]\n\n"

        res = res + "df1 = df1.merge(df1_new, on=['" + str(col_name) + "', 'category'], how = 'left')\n\n"

        res = res + "df1['regimen'] = np.select"
        res = res + "([(df1['category'] == 'Others') , (df1['category'] != 'Others')], "
        res = res + "['Others', df1['regimen']])\n\n"

        res = res + "df1 = df1.drop(['patient_count1', 'total_dot_lot_reg1', "
        res = res + "'row_num', 'category', 'count_'], axis = 1)\n\n"

        res = res + "df1['lot_reg_avg_days'] = df1['total_dot_lot_reg']/df1"
        res = res + "['patient_count']\n\n"

        res = res + "df1_average = df1.groupby('" + str(col_name) + "')"
        res = res + ".agg({'patient_count': 'sum'}).reset_index()"
        res = res + ".rename(columns={'patient_count':'total_p_count'})\n\n"

        res = res + "df1 = df1.merge(df1_average, on=['" + str(col_name) + "'], "
        res = res + "how='left')\n\n"

        res = res + "df1['percentage'] = (df1['patient_count']/df1"
        res = res + "['total_p_count'])*100\n\n"

        res = res + "df1 = df1.sort_values(by=['" + str(col_name)
        res = res + "', 'percentage', 'regimen'], ascending=False)" + "\n\n"

        res = res + "df1['col2'] = df1['patient_count'].astype(str) + ',' + "
        res = res + "df1['lot_reg_avg_days'].astype(str) + ',' + df1['percentage']"
        res = res + ".astype(str)\n\n"

        res = res + "final_dict = dict()\n"
        res = res + "final_dict['Stack'] = dict_convert(df1[['" + str(col_name)
        res = res + "', 'regimen', 'col2']])\n"
        res = res + "final_dict\n"
        return (res)

    def deep_dive_lot_regimen(self, analysis_id, col_name, start_date, end_date,
                              *argv):  # line_number=None, regimen=None, lot_dict={}):
        res = ''
        res = res + "if 'lot' not in df_" + str(analysis_id) + ".columns:\n"
        res = res + "\tdf_" + str(analysis_id) + "['lot'] = np.select([(df_" + str(analysis_id) + "['"
        res = res + str(col_name) + "']==1), (df_" + str(analysis_id) + "['"
        res = res + str(col_name) + "']==2), (df_" + str(analysis_id) + "['"
        res = res + str(col_name) + "']==3), (df_" + str(analysis_id) + "['"
        res = res + str(col_name) + "']==4)], [1, 2, 3, 4], default = 5).astype(int)" + "\n\n"
        col_name = 'lot'

        res = res + PatientFlow().range_filter(analysis_id, start_date, end_date)

        if (argv[0] != None and argv[1] != None):
            res = res + "patient_ids = df_" + str(analysis_id)

            if bool(argv[2]) == True:
                res = res + ".loc["
                for key, val in argv[2].items():
                    res = res + "(df_" + str(analysis_id) + "['" + str(key) + "'] == '" + str(val) + "') & "
                result = list(res)
                del result[-3:-1]
                result.insert(-1, "]")
                res = ''.join(result)

            res = res + "['patient_id'].unique().tolist()\n\n"

            res = res + "df = df_" + str(analysis_id) + ".loc[df_" + str(analysis_id)
            res = res + "['patient_id'].isin(patient_ids)]" + "\n\n"

            res = res + "patient_ids = df.loc[(df['regimen'] == '" + str(argv[1])
            res = res + "') & (df['" + str(col_name)
            res = res + "'] == " + str(argv[0]) + ")]"
            res = res + "['patient_id'].unique().tolist()\n\n"

            res = res + "df_all = df_" + str(analysis_id)
            res = res + ".loc[mask].loc[df_" + str(analysis_id)
            res = res + "['patient_id'].isin(patient_ids)].groupby(['patient_id', "
            res = res + "'regimen', '" + str(col_name) + "'])"

            res = res + ".agg({'index_date': 'min', 'activity_date': 'max'})"
            res = res + ".reset_index()\n\n"

            res = res + "df_selected_line = df_all.loc[(df_all['regimen'] == '" + str(argv[1])
            res = res + "') & (df_all['" + str(col_name)
            res = res + "'] == " + str(argv[0]) + ")]\n"
            res = res + "df_remaining = df_all.loc[df_all['" + str(col_name)
            res = res + "'] != " + str(argv[0]) + "]\n"
            res = res + "df = pd.concat([df_selected_line, df_remaining])\n\n"
            res = res + "df['days_on_therapy'] = "

        else:

            res = res + "patient_ids = df_" + str(analysis_id)
            res = res + ".loc[mask]"

            if bool(argv[2]) == True:
                res = res + ".loc["
                for key, val in argv[2].items():
                    res = res + "(df_" + str(analysis_id) + "['" + str(key) + "'] == '" + str(val) + "') & "
                result = list(res)
                del result[-3:-1]
                result.insert(-1, "]")
                res = ''.join(result)

            res = res + "['patient_id'].unique().tolist()\n\n"

            res = res + "df = df_" + str(analysis_id) + ".loc[df_" + str(analysis_id)
            res = res + "['patient_id'].isin(patient_ids)]" + "\n\n"

            res = res + "df = df.groupby(['patient_id', 'regimen', '" + str(col_name) + "'])"
            res = res + ".agg({'index_date': 'min', 'activity_date': 'max'})"
            res = res + ".reset_index()\n\ndf['days_on_therapy'] = "

        res = res + "(df['activity_date']-df['index_date']).dt.days\n\n"

        res = res + self.regimen_reduction_graphs(col_name)
        return (res)

    def regimen_combomono_share(self, analysis_id, col_name, start_date, end_date, lot_dict={}):
        res = ''
        res = res + "if 'lot' not in df_" + str(analysis_id) + ".columns:\n"
        res = res + "\tdf_" + str(analysis_id) + "['lot'] = np.select([(df_" + str(analysis_id) + "['"
        res = res + str(col_name) + "']==1), (df_" + str(analysis_id) + "['"
        res = res + str(col_name) + "']==2), (df_" + str(analysis_id) + "['"
        res = res + str(col_name) + "']==3), (df_" + str(analysis_id) + "['"
        res = res + str(col_name) + "']==4)], [1, 2, 3, 4], default = 5).astype(int)" + "\n\n"


        res = res + PatientFlow().range_filter(analysis_id, start_date, end_date)
        col_name = 'lot'

        res = res + "df_" + str(analysis_id) + "['mono_combo'] = df_" + str(analysis_id) + ".loc[mask]"
        res = res + ".apply(lambda x: 'mono' if(len(x['regimen'].split(','))==1)"
        res = res + " else 'combo', axis=1)\n\n"

        res = res + "patient_ids = df_" + str(analysis_id) + ".loc[mask]"
        if bool(lot_dict) == True:
            res = res + ".loc["
            for key, val in lot_dict.items():
                res = res + "(df_" + str(analysis_id) + "['" + str(key) + "'] == '" + str(val) + "') & "
            result = list(res)
            del result[-3:-1]
            result.insert(-1, "]")
            res = ''.join(result)

        res = res + "['patient_id'].unique().tolist()\n\n"

        res = res + "df = df_" + str(analysis_id) + ".loc[df_" + str(analysis_id)
        res = res + "['patient_id'].isin(patient_ids)]" + "\n\n"

        res = res + "graph_combo = df.groupby(['"
        res = res + str(col_name) + "', 'mono_combo']).agg({'patient_id': 'nunique'})"
        res = res + ".reset_index().rename(columns={'patient_id':'patient_share_count'})\n\n"
        res = res + "graph_p_count = graph_combo.groupby(['" + str(col_name) + "'])"
        res = res + ".agg({'patient_share_count': 'sum'}).reset_index()"
        res = res + ".rename(columns={'patient_share_count':'total_p_count'})\n\n"
        res = res + "df = graph_combo.merge(graph_p_count, on=['" + str(col_name)
        res = res + "'], how='left')\n\n"
        res = res + "df['patient_share_percentage'] = (df['patient_share_count']"
        res = res + "/df['total_p_count'])*100" + "\n\n"
        res = res + "df['col1'] = df['patient_share_count'].astype(str) + ', ' + "
        res = res + "df['patient_share_percentage'].astype(str)" + "\n\n"
        res = res + "df = df.drop(columns=['patient_share_count', 'total_p_count', "
        res = res + "'patient_share_percentage'])\n\n"
        res = res + "dict_convert(df)\n\n"
        return res

    def regimen_drugs_share(self, analysis_id, col_name, start_date, end_date, line_number,
                            *argv):  # lot_dict={}, combomono='mono'):
        res = ''
        res = res + "if 'lot' not in df_" + str(analysis_id) + ".columns:\n"
        res = res + "\tdf_" + str(analysis_id) + "['lot'] = np.select([(df_" + str(analysis_id) + "['"
        res = res + str(col_name) + "']==1), (df_" + str(analysis_id) + "['"
        res = res + str(col_name) + "']==2), (df_" + str(analysis_id) + "['"
        res = res + str(col_name) + "']==3), (df_" + str(analysis_id) + "['"
        res = res + str(col_name) + "']==4)], [1, 2, 3, 4], default = 5).astype(int)" + "\n\n"



        res = res + PatientFlow().range_filter(analysis_id, start_date, end_date)

        col_name = 'lot'

        res = res + "if 'mono_combo' not in df_" + str(analysis_id) + ".columns:\n"
        res = res + "\tdf_" + str(analysis_id) + "['mono_combo'] = df_" + str(analysis_id) + ".loc[mask]"
        res = res + ".apply(lambda x: 'mono' if(len(x['regimen'].split(','))==1)"
        res = res + " else 'combo', axis=1)\n\n"

        res = res + "patient_ids = df_" + str(analysis_id) + ".loc[mask]"
        if bool(argv[0]) == True:
            res = res + ".loc["
            for key, val in argv[0].items():
                res = res + "(df_" + str(analysis_id) + "['" + str(key) + "'] == '" + str(val) + "') & "
            result = list(res)
            del result[-3:-1]
            result.insert(-1, "]")
            res = ''.join(result)

        res = res + "['patient_id'].unique().tolist()\n\n"

        res = res + "df = df_" + str(analysis_id) + ".loc[df_" + str(analysis_id)
        res = res + "['patient_id'].isin(patient_ids)]" + "\n\n"

        res = res + "df = df.loc[df_" + str(analysis_id) + "['" + str(col_name)
        res = res + "'] == " + str(line_number) + "].loc[df_" + str(analysis_id) + "['mono_combo'] == '"
        res = res + str(argv[1]) + "'].groupby(['regimen']).agg({'patient_id':"
        res = res + " 'nunique'}).reset_index().rename(columns={'patient_id':"
        res = res + "'regimen_share'})\n\n"

        res = res + "df['total_regimen_count'] = df['regimen_share'].sum()\n\n"
        res = res + "df['regimen_share_percentage'] = (df['regimen_share']/df['"
        res = res + "total_regimen_count'])*100" + "\n\n"
        res = res + "df = df.drop(columns=['total_regimen_count'])"
        res = res + ".sort_values(['regimen_share'], ascending=False)\n\n"
        res = res + "df = df.iloc[:10]\n\n"
        res = res + "dict_convert(df)\n\n"
        return res

    def average_treatment_days(self, analysis_id, col_name, start_date, end_date, lot_dict={}):
        res = ''
        res = res + "if 'lot' not in df_" + str(analysis_id) + ".columns:\n"
        res = res + "\tdf_" + str(analysis_id) + "['lot'] = np.select([(df_" + str(analysis_id) + "['"
        res = res + str(col_name) + "']==1), (df_" + str(analysis_id) + "['"
        res = res + str(col_name) + "']==2), (df_" + str(analysis_id) + "['"
        res = res + str(col_name) + "']==3), (df_" + str(analysis_id) + "['"
        res = res + str(col_name) + "']==4)], [1, 2, 3, 4], default = 5).astype(int)" + "\n\n"

        col_name = 'lot'

        res = res + PatientFlow().range_filter(analysis_id, start_date, end_date)
        res = res + "patient_ids = df_" + str(analysis_id) + ".loc[mask]"

        if bool(lot_dict) == True:
            res = res + ".loc["
            for key, val in lot_dict.items():
                res = res + "(df_" + str(analysis_id) + "['" + str(key) + "'] == '" + str(val) + "') & "
            result = list(res)
            del result[-3:-1]
            result.insert(-1, "]")
            res = ''.join(result)

        res = res + "['patient_id'].unique().tolist()\n\n"

        res = res + "df = df_" + str(analysis_id) + ".loc[df_" + str(analysis_id)
        res = res + "['patient_id'].isin(patient_ids)]" + "\n\n"

        res = res + "df = df.groupby(['patient_id', 'regimen', '"
        res = res + str(col_name) + "']).agg({'index_date': 'min', 'activity_date': 'max'})"
        res = res + ".reset_index()" + "\n\n"

        res = res + "df['days_on_therapy'] = (df['activity_date']-df['index_date'])"
        res = res + ".dt.days" + "\n\n"

        res = res + "df = df.groupby(['" + str(col_name) + "', 'regimen'])"
        res = res + ".agg({'days_on_therapy':'sum', 'patient_id':'nunique'})"
        res = res + ".reset_index().rename(columns={'days_on_therapy':"
        res = res + "'total_dot', 'patient_id':'patient_count'})" + "\n\n"

        res = res + "df['avg_days_on_therapy'] = (df['total_dot']/df['patient_count'])\n\n"

        res = res + "df['row_num'] = df.groupby(['" + str(col_name)
        res = res + "'])['patient_count'].rank(method = 'first', ascending = False)\n"
        res = res + "df.sort_values(by= ['" + str(col_name)
        res = res + "', 'patient_count', 'avg_days_on_therapy'], inplace = True)\n"
        res = res + "df = df.reset_index(drop = True)\n\n"

        res = res + "mask_1 = (df['row_num'] <= 10)\n"
        res = res + "df = df.loc[mask_1]\n\n"
        
        res = res + "output_dict = dict_convert(df[['" + str(col_name) + "', 'regimen', 'patient_count', "
        res = res + "'avg_days_on_therapy']].sort_values(by=['" + str(col_name)
        res = res + "', 'patient_count', 'avg_days_on_therapy'], ascending=False))\n\n"
        res = res + "final_result = dict()\n"
        res = res + "for line,value in output_dict.items():\n"
        res = res + "\ttemp_dict_1 = dict()\n"
        res = res + "\tfor regimen,regimen_value in value.items():\n"
        res = res + "\t\ttemp_dict_2 = dict()\n"
        res = res + "\t\tfor patient_count,avg_days in regimen_value.items():\n"
        res = res + "\t\t\ttemp_dict_2['patient_count'] = patient_count\n"
        res = res + "\t\t\ttemp_dict_2['avg_days'] = avg_days\n"
        res = res + "\t\ttemp_dict_1[regimen] = temp_dict_2\n"
        res = res + "\tfinal_result[line] = temp_dict_1\n\n"
        res = res + "final_result"
        return res


class Compliance:
    """
    Class contains all the functions related to Compliance Attribute
    """

    def __init__(self):
        """ Initialize all class level variables """
        pass
    

    def generate_compliance(self):
        res = ''
        res = res + "def generate_compliance(data_1, compliance_threshold, "
        res = res + "product_list, op, index_start_date, index_end_date, look_forward_days, flag1, "
        res = res + "flag2, dos_value, df_new = None):\n"

        res = res + "\t# product filter\n"
        res = res + "\tdata_1 = data_1.loc[data_1['product'].isin(product_list)]\n"
        res = res + "\t# Converting nan and 0's to null in days_supply column\n"
        res = res + "\tdata_1['days_supply'] = data_1['days_supply'].astype('float')\n"
        res = res + "\tdata_1['days_supply'] = data_1['days_supply'].replace(0, np.nan)\n\n"

        res = res + "\t# days of supply handling\n"
        res = res + "\tif flag1 == 'filter':\n"
        res = res + "\t\t# Filtering out the data in case Days Of Supply is null or 0 days\n"
        res = res + "\t\tdata_1 = data_1.dropna(subset = ['days_supply'])\n"
        res = res + "\telif flag1 == 'assign':\n"
        res = res + "\t\tif flag2 == 'all':\n"
        res = res + "\t\t\t# Assign user specified value to Days of supply for all the data considered for the analysis\n"
        res = res + "\t\t\tdata_1['days_supply'] = dos_value\n"
        res = res + "\t\telse:\n"
        res = res + "\t\t\t# Assign values to Days of supply in case of nulls or 0 values\n"
        res = res + "\t\t\tdata_1.fillna(value = {'days_supply': dos_value}, inplace = True)\n"
        res = res + "\telse:\n"
        res = res + "\t\t# getting mean_dos_value and default_value for the respective source_value\n"
        res = res + "\t\t# converting them to numeric data\n"
        res = res + "\t\tdata_1 = pd.merge(data_1, df_new, on=['source_value'], how = 'left')\n"
        res = res + "\t\tdata_1['mean_dos_value'] = data_1['mean_dos_value'].astype('float')\n"
        res = res + "\t\tdata_1['default_dos_value'] = pd.to_numeric(data_1['default_dos_value'],"
        res = res + " errors = 'coerce')\n"
        res = res + "\t\tif flag2 == 'mean':\n"
        res = res + "\t\t\t# if priority1 is mean\n"
        res = res + "\t\t\t# Then if the days_supply is null , then assign mean_dos_value\n"
        res = res + "\t\t\t# Then after if mean_dos_value is null , then assign default_dos_value\n"
        res = res + "\t\t\tdata_1['mean_dos_value'] = data_1['mean_dos_value'].replace(0, np.nan)\n"
        res = res + "\t\t\tdata_1['default_dos_value'] = data_1['default_dos_value'].replace(np.nan, 0)\n"
        res = res + "\t\t\tdata_1['days_supply'] = data_1['days_supply']"
        res = res + ".fillna(data_1['mean_dos_value']).fillna(data_1['default_dos_value'])\n"
        res = res + "\t\telse:\n"
        res = res + "\t\t\t# if priority1 is default\n"
        res = res + "\t\t\t# Then if the days_supply is null , then assign default_dos_value\n"
        res = res + "\t\t\t# Then after if default_dos_value is null , then assign mean_dos_value\n"
        res = res + "\t\t\tdata_1['default_dos_value'] = data_1['default_dos_value'].replace(0, np.nan)\n"
        res = res + "\t\t\tdata_1['mean_dos_value'] = data_1['mean_dos_value'].replace(np.nan, 0)\n"
        res = res + "\t\t\tdata_1['days_supply'] = data_1['days_supply']"
        res = res + ".fillna(data_1['default_dos_value']).fillna(data_1['mean_dos_value'])\n"
        res = res + "\t\tdata_1 = data_1.drop(['mean_dos_value', 'default_dos_value'], axis = 1)\n\n"
        res = res + "\tdata_1 = data_1.drop(['source_value'], axis = 1)\n"
        res = res + "\t# dropping duplicates as in some cases days_supply is changed\n"
        res = res + "\tdata_1 = data_1.drop_duplicates()\n\n"

        res = res + "\t# Index date calculation\n"
        res = res + "\tmin_date = data_1.groupby(['patient_id', 'product'])"
        res = res + ".agg(index_date=('activity_date', 'min')).reset_index()\n"
        res = res + "\tdata_1 = pd.merge(data_1, min_date, on = ['patient_id','product'], how = 'left')"
        res = res + ".sort_values(by = ['patient_id', 'product', 'activity_date'])\n\n"
        res = res + "\t# index date filter\n"
        res = res + "\tif op == 'lte':\n"
        res = res + "\t\tmask1 = (data_1['index_date'] <= index_start_date)\n"
        res = res + "\telif op == 'lt':\n"
        res = res + "\t\tmask1 = (data_1['index_date'] < index_start_date)\n"
        res = res + "\telif op == 'gte':\n"
        res = res + "\t\tmask1 = (data_1['index_date'] >= index_start_date)\n"
        res = res + "\telif op == 'gt':\n"
        res = res + "\t\tmask1 = (data_1['index_date'] > index_start_date)\n"
        res = res + "\telif op == 'eq':\n"
        res = res + "\t\tmask1 = (data_1['index_date'] == index_start_date)\n"
        res = res + "\telse:\n"
        res = res + "\t\tmask1 = (data_1['index_date'] >= index_start_date)"
        res = res + " & (data_1['index_date'] <= index_end_date)\n"
        res = res + "\tdata_1 = data_1.loc[mask1]\n\n"

        res = res + "\t# Raising Exception if data frame is empty\n"
        res = res + "\ttry:\n"
        res = res + "\t\tif len(data_1) == 0:\n"
        res = res + "\t\t\traise Exception\n"
        res = res + "\texcept:\n"
        res = res + "\t\traise Exception('No records available to calculate compliance metric.')\n\n"

        res = res + "\t# calculating days_difference between activity_date and the index_date\n"
        res = res + "\tdata_1['date_diff'] = (data_1['activity_date'] - data_1['index_date']).dt.days\n"
        res = res + "\t# Restricting the data based on look_forward_days\n"
        res = res + "\tdata_1 = data_1.loc[data_1['date_diff'] <= look_forward_days]\n"

        res = res + "\t# Counting the distinct activity_date at patient_id and product level\n"
        res = res + "\tdff = data_1.groupby(['patient_id','product'])"
        res = res + ".agg(count_ = ('activity_date', 'nunique')).reset_index()\n"
        res = res + "\tdata_1 = pd.merge(data_1, dff, on = ['patient_id', 'product']"
        res = res + ", how='inner')\n\n"

        res = res + "\t# Flaging the Single_dispensed and multiple_dispensed\n"
        res = res + "\tdata_1['patient_type'] = data_1['count_'].transform(lambda x: "
        res = res + "'Single_dispensed' if x == 1 else 'Multiple_dispensed')\n"
        res = res + "\tdata_1 = data_1.drop(['count_'], axis = 1)\n\n"
        
        res = res + "\t# Sorting is done to get the last_dos as max value\n"
        res = res + "\tdata_1.sort_values(by= ['patient_id', 'product', "
        res = res + "'activity_date', 'days_supply'], inplace = True)\n\n"

        res = res + "\t# Calculating sum_dos, calendar_days and last_dos\n"
        res = res + "\tdff = data_1.groupby(['patient_id', 'product'])"
        res = res + ".agg(sum_dos = ('days_supply', 'sum'), calendar_days = ('date_diff', 'max'),"
        res = res + " last_dos = ('days_supply', 'last')).reset_index()\n"
        res = res + "\tdata_1 = pd.merge(data_1, dff, on = ['patient_id', 'product'], how='left')\n"
        res = res + "\tdata_1 = data_1.drop(['activity_date', 'days_supply', 'date_diff'], axis = 1)\n"
        res = res + "\tdata_1 = data_1.drop_duplicates()\n\n"

        res = res + "\t# Calulating compliance value\n"
        res = res + "\tdata_1['comp_deno'] = data_1['calendar_days'] + data_1['last_dos']\n"
        res = res + "\tdata_1['compliance_rate'] = data_1['sum_dos'] / data_1['comp_deno']\n"
        res = res + "\tdata_1['compliance_percent'] = data_1['compliance_rate']*100\n"

        res = res + "\t# Rounding to 0 decimal and converting to int datatype\n"
        res = res + "\tdata_1 = data_1.round({'compliance_percent' : 0})"
        res = res + ".astype({'compliance_percent': 'int'})\n\n"
        res = res + "\tdata_1 = data_1.drop(['comp_deno', 'compliance_rate'], axis = 1, errors = 'ignore')\n\n"

        res = res + "\t# Compliance flaging (Single_dispensed, Compliant, Non-Compliant)\n"
        res = res + "\tdef compliance_flag_finder(patient_type, compliance_percent):\n"
        res = res + "\t\tif patient_type == 'Single_dispensed':\n"
        res = res + "\t\t\treturn patient_type\n"
        res = res + "\t\telse:\n"
        res = res + "\t\t\tif compliance_percent >= compliance_threshold:\n"
        res = res + "\t\t\t\treturn 'Compliant'\n"
        res = res + "\t\t\telse:\n"
        res = res + "\t\t\t\treturn 'Non-Compliant'\n"
        res = res + "\tdata_1['compliance_flag'] = data_1.apply(lambda x: "
        res = res + "compliance_flag_finder(x.patient_type, x.compliance_percent), axis = 1)\n\n"

        res = res + "\t# Based on compliance percentage flaging(greater than 150%, less than 150%)\n"
        res = res + "\tdata_1['flag'] = data_1['compliance_percent'].transform(lambda x: 'lte_150'"
        res = res + " if x <= 150 else 'gt_150')\n"
        res = res + "\treturn data_1"
        return res


    def compliance_metric(self, analysis_id, data, start_date, end_date, unwanted_columns):
        res = ''
        res = res + self.generate_compliance()
        res = res + "\n\n\n"
        res = res + "# Getting the required columns required for compliance calculation\n"
        res = res + "if 'regimen' in df_" + str(analysis_id) + ".columns:\n"
        res = res + "\tdf = df_" + str(analysis_id) + ".drop(['start_date', 'claim_id', 'quantity', 'specialty_code', 'specialty_desc', "
        res = res + "'npi_number', 'index_date', 'case_flag', 'regimen','HCP_name','mono_combo'], axis =1, "
        res = res + "errors='ignore')\n"
        res = res + "\tlot_ = [i for i in df.columns if i[:3] == 'lot' or i[8:13] == 'level']\n"
        res = res + "\tdf = df.drop(lot_, axis=1, errors='ignore')\n"
        res = res + "else:\n"
        res = res + "\tdf = df_" + str(analysis_id) + ".drop(['start_date', 'claim_id', 'quantity', 'specialty_code', 'specialty_desc', "
        res = res + "'npi_number','HCP_name','mono_combo'], axis =1, errors='ignore')\n\n"
        res = res + "df = df.drop_duplicates()\n\n"

        res = res + "# Analysis period filter\n"
        res = res + "mask = (df['activity_date'] >= '" + str(start_date) 
        res = res + "') & (df['activity_date'] <= '" + str(end_date) + "')\n"
        res = res + "df = df.loc[mask]\n\n"

        res = res + "# Filtering Treatment and Procedure records\n"
        res = res + "df = df.loc[df['code_flag'].isin(['Rx', 'Px'])]\n"
        res = res + "df = df.drop('code_flag', axis =1).drop_duplicates()\n\n"

        res = res + "df_list = []\n"
        for i,item in enumerate(data):
            res = res + "df = df.rename(columns = {'" + str(item['Product_Selection']['Drug_Class'])
            res = res + "' : 'product'})\n\n"
            res = res + "df = df.drop(" + str(unwanted_columns) + ", axis =1 , errors='ignore').drop_duplicates()\n\n"
            flag_1 = ''
            flag_2 = ''
            dos_value = 0
            if item['Days_of_Supply_Type'] == 'Filter':
                flag_1 = 'filter'
            elif item['Days_of_Supply_Type'] == 'Assign':
                flag_1 = 'assign'
                if item['Days_of_Supply_Details']['Flag'] == 'zero_or_null':
                    flag_2 = 'zero_or_null'
                    dos_value = item['Days_of_Supply_Details']['Days']
                else:
                    flag_2 = 'all'
                    dos_value = item['Days_of_Supply_Details']['Days']
            else:
                flag_1 = 'treating_dos'
                if item['Days_of_Supply_Details']['Priority1'] == 'Mean':
                    flag_2 = 'mean'
                else:
                    flag_2 = 'default'
    
            
            if item['Days_of_Supply_Type'] == 'Treating_dos':
                res = res + "mean_dos_json = " + str(item['Days_of_Supply_Details']['Treat_dos_json']) + "\n"
                res = res + "# Converting json to dataframe\n"
                res = res + "df_mean_dos = pd.DataFrame(mean_dos_json)\n"
                
            res = res + "# Generating compliance\n"
            res = res + "data_" + str(i+1)
            res = res + " = generate_compliance(df, "
            res = res + str(item['Threshold']) + ", "
            res = res + str(item['Product_Selection']['Drug_Class_Values']) + ", '"
            res = res + str(item['Time_period']['OP']) +"', '"
            res = res + str(item['Time_period']['Value']) + "', '"
             
            if item.get('Time_period').get('Extent'):
                res = res + str(item['Time_period']['Extent'])
            
            res = res + "', " + str(item['Time_period']['Look_Forward_Days'])
            res = res + ", '" + str(flag_1) + "', '" + str(flag_2) + "', "+ str(dos_value)

            if item['Days_of_Supply_Type'] == 'Treating_dos':
                res = res + ", df_mean_dos"
            
            res = res  + ")\n"
            res = res + "df_list.append(data_"+ str(i+1) + ")\n\n"
        res = res + "# Union of all the generated compliances\n"
        res = res + "df_compliance_" + str(analysis_id) + " = pd.concat(df_list)\n"
        res = res + "df_compliance_" + str(analysis_id) + "= df_compliance_" 
        res = res + str(analysis_id) + ".drop_duplicates()\n"
        res = res + "df_compliance_" + str(analysis_id)
        res = res + ".sort_values(by= ['patient_id','product'], inplace = True)\n"
        res = res + "del(df, df_list"
        for i in range(len(data)):
            res = res + ", data_" + str(i+1)
        res = res + ")"
        return res


    def create_mean_dos_table(self, analysis_id, drug_col, product_list, start_date, end_date):
        res = ''
        res = res + "# selecting the required columns and making a copy of that\n"
        res = res + "df = df_" + str(analysis_id) + "[['source_value', '"
        res = res + str(drug_col) + "', 'patient_id', 'activity_date',"
        res = res + " 'days_supply']].rename(columns = {'"
        res = res + str(drug_col) + "':'product'})\n\n"
        res = res + "# Analysis period filter\n"
        res = res + "mask = (df['activity_date'] >= '" + str(start_date) 
        res = res + "') & (df['activity_date'] <= '" + str(end_date) + "')\n"
        res = res + "df = df.loc[mask]\n\n"
        res = res + "# product filter\n"
        res = res + "product_list = " + str(product_list) + "\n"
        res = res + "df = df.loc[df['product'].isin(product_list)]\n"
        res = res + "# dropping duplicates if any\n"
        res = res + "df = df.drop_duplicates()\n\n"
        res = res + "# Converting the days_supply from nan to null if any\n"
        res = res + "df['days_supply'] = df['days_supply'].astype('float')\n\n"
        res = res + "# filtering the source_value(ndc_codes) where atleast one of its days_supply"
        res = res + " as 0 or null\n"
        res = res + "mask = (df['days_supply'] == 0) | (df['days_supply'].isnull())\n"
        res = res + "source_value_list = df.loc[mask]['source_value'].unique().tolist()\n"
        res = res + "df = df.loc[df['source_value'].isin(source_value_list)]\n\n"
        res = res + "if len(df) == 0:\n"
        res = res + "\tfinal_result = []\n"
        res = res + "else:\n"
        res = res + "\t# Calculating the mean_dos_value for the source_value having "
        res = res + "atleast a record has days_supply greater than 0\n"
        res = res + "\tdf_mean = df.groupby(['source_value','patient_id','activity_date'])"
        res = res + ".agg(days_supply = ('days_supply', 'max')).reset_index()\n"
        res = res + "\tdf_mean['days_supply'] = df_mean['days_supply'].replace(0, np.nan)\n"
        res = res + "\tdf_mean = df_mean.dropna(subset = ['days_supply'])\n"
        res = res + "\tdf_mean = df_mean.groupby(['source_value'])"
        res = res + ".agg(mean_dos_value = ('days_supply', 'mean')).reset_index()\n\n"
        res = res + "\t# Getting all the products and source_values having atleast one of its "
        res = res + "days_supply as 0 or null\n"
        res = res + "\tdf = df[['product', 'source_value']].drop_duplicates()\n"
        res = res + "\tdf = pd.merge(df, df_mean, on = ['source_value'], how = 'left')\n\n"
        res = res + "\t# There may be possibility that for a particular source_value will have all "
        res = res + "its days_supply as null or nan\n"
        res = res + "\t# Those sholud be replaced by 0\n"
        res = res + "\tdf.fillna(value = {'mean_dos_value' : 0}, inplace = True)\n\n"
        res = res + "\t# Rounding to 0 decimal and converting to int datatype\n"
        res = res + "\tdf = df.round({'mean_dos_value' : 0})\n"
        res = res + "\tdf['mean_dos_value'] = df['mean_dos_value'].astype(int)\n\n"
        res = res + "\t# Converting the dataframe to json how UI needs\n"
        res = res + "\toutput_dict = dict_convert(df.sort_values(by=['product']))\n"
        res = res + "\tfinal_result = []\n"
        res = res + "\tfor product,value1 in output_dict.items():\n"
        res = res + "\t\tfor source_value,mean_dos_value in value1.items():\n"
        res = res + "\t\t\ttemp_dict = dict()\n"
        res = res + "\t\t\ttemp_dict['product_name'] = product\n"
        res = res + "\t\t\ttemp_dict['source_value'] = source_value\n"
        res = res + "\t\t\ttemp_dict['mean_dos_value'] = mean_dos_value\n"
        res = res + "\t\t\tfinal_result.append(temp_dict)\n\n"
        res = res + "final_result"
        return res


    def save_to_s3(self, project_id, analysis_id, attribute_id):
        res = ''
        res = res + "df_compliance_" + str(analysis_id)
        res = res + ".to_parquet('s3://{}/PFA_COMPLIANCE/prj_".format(cs.S3_PROJECT_PATH)
        res = res + str(project_id) + "_" + str(analysis_id)
        res = res + "_" + str(attribute_id) + "')\n"
        return (res)


    def read_file(self, project_id, analysis_id, attribute_id):
        res = ''
        res = res + "df_compliance_" + str(analysis_id) + " = "
        res = res + "pd.read_parquet('s3://{}/PFA_COMPLIANCE/prj_".format(cs.S3_PROJECT_PATH)
        res = res + str(project_id) + "_" + str(analysis_id)
        res = res + "_" + str(attribute_id) + "')\n"
        return (res)

    
    def drop_attribute(self, key):
        res = ''
        res = res + "s3 = boto3.client('s3')\n"
        res = res + "response = s3.delete_object(Bucket='{}', Key='".format(cs.S3_BUCKET)
        res = res + str(key) + "')"
        return res
    
    
    def drop_column(self, analysis_id, col_name, project_id, attribute_id):
        res = self.read_file(str(project_id), str(analysis_id), str(attribute_id)) + "\n"
        res = res + "df_compliance_" + str(analysis_id) + " = df_compliance_" + str(analysis_id)
        res = res + ".drop(['{}'], axis=1)\n\n".format(col_name)
        res = res + self.save_to_s3(str(project_id), str(analysis_id), str(attribute_id))
        return (res)


    def age_range_attribute(self, analysis_id, splits, labels, col_name, project_id, attribute_id):
        res = self.read_file(str(project_id), str(analysis_id), str(attribute_id)) + "\n"
        splits.pop() 
        splits = map(str, splits)
        splits = ', '.join(splits)
        splt_str = "[float('-inf'), float('inf')]"
        for i in range(len(splt_str)):
            if splt_str[i] == ',':
                lst = list(splt_str)
                lst.insert(i, ', ' + splits)
                lst = ''.join(lst)
        res = res + "splits = " + str(lst) + "\n"
        res = res + "labels = " + str(labels) + "\n"
        res = res + "df_compliance_" + str(analysis_id) + "['" + str(col_name) + "'] = pd.cut(x=df_compliance_"
        res = res + str(analysis_id) + "['age'], bins=splits, labels=labels)\n"
        res = res + "df_compliance_" + str(analysis_id) + "['" + str(col_name) + "'] = df_compliance_"
        res = res + str(analysis_id) + "['" + str(col_name) + "'].astype(str)\n\n"

        res = res + self.save_to_s3(str(project_id), str(analysis_id), str(attribute_id))
        return (res)


    def gender_attribute(self, analysis_id, map_lst, dim_lst, col_name, project_id, attribute_id):
        res = self.read_file(str(project_id), str(analysis_id), str(attribute_id)) + "\n"
        res = res + "df_compliance_" + str(analysis_id) + "['" + str(col_name) + "'] = "
        res = res + "np.select(["
        if (len(dim_lst) >= 1):
            for i in range(len(dim_lst)):
                res = res + "(df_compliance_" + str(analysis_id)
                res = res + "['gender'].isin(['" + str(map_lst[i]) + "'])), "

            result = list(res)
            del result[-2:-1]
            res = ''.join(result)
            res = res + "], " + str(dim_lst) + ", default = 'others')\n\n"
            res = res + self.save_to_s3(str(project_id), str(analysis_id), str(attribute_id))
            return (res)
        else:
            return ("Invalid Gender Mappings")


    def average_compliance(self, analysis_id, compliance_dict = {}):
        res = ''
        
        if bool(compliance_dict) == True:
            res = res + "df_compliance_" + str(analysis_id) + " = df_compliance_" + str(analysis_id)
            res = res + ".loc["
            for key, val in compliance_dict.items():
                res = res + "(df_compliance_" + str(analysis_id) + "['" + str(key) + "'] == '" + str(val) + "') & "
            result = list(res)
            del result[-3:-1]
            result.insert(-1, "]")
            res = ''.join(result)

        res = res + "\n\n# filter out single dispensed patients\n"
        res = res + "df = df_compliance_" + str(analysis_id) + ".loc[df_compliance_"
        res = res + str(analysis_id) + "['patient_type'] != 'Single_dispensed']\n"
        res = res + "df = df[['patient_id', 'product', 'compliance_percent','flag']]\n\n"
        res = res + "gt_150_patient_count = df.loc[df['flag'] == 'gt_150']['patient_id'].nunique()\n\n"
        res = res + "df = df.loc[df['flag'] == 'lte_150']\n\n"
        res = res + "# Calculating average compliance\n"
        res = res + "df = df.groupby(['product']).agg(avg_compliance = ('compliance_percent', 'mean'),"
        res = res + " patient_count = ('patient_id', 'nunique')).reset_index()\n"
        res = res + "df = df.sort_values(by = ['patient_count'], ascending = False)\n\n"
        res = res + "# Rounding to 0 decimal and converting to int datatype\n"
        res = res + "df = df.round({'avg_compliance' : 0})\n"
        res = res + "df['avg_compliance'] = df['avg_compliance'].astype(int)\n\n"
        res = res + "# Converting the dataframe to json how UI needs\n"
        res = res + "final_dict = dict()\n"
        res = res + "final_dict['gt_150_patients'] = gt_150_patient_count\n"
        res = res + "final_dict['graph'] = df.to_dict('records')\n"
        res = res + "final_dict"
        return res

    def years_list_1b(self, analysis_id, product):
        res = ''
        res = res + "# Filter\n"
        res = res + "mask = ((df_compliance_" + str(analysis_id)
        res = res + "['product'] == '" + str(product) + "') & (df_compliance_" + str(analysis_id)
        res = res + "['patient_type'] != 'Single_dispensed') & (df_compliance_" + str(analysis_id)
        res = res + "['flag'] == 'lte_150'))\n"
        res = res + "df = df_compliance_" + str(analysis_id) + ".loc[mask].copy()\n"
        res = res + "# index date year\n"
        res = res + "df['year'] = df['index_date'].dt.year.astype('str')\n"
        res = res + "year_list = df['year'].unique().tolist()\n"
        res = res + "year_list.sort(reverse = True)\n"
        res = res + "year_list"
        return res


    def years_list_2b(self, analysis_id, product):
        res = ''
        res = res + "# Filter\n"
        res = res + "mask = (df_compliance_" + str(analysis_id)
        res = res + "['product'] == '" + str(product) + "')\n"
        res = res + "df = df_compliance_" + str(analysis_id) + ".loc[mask].copy()\n"
        res = res + "# index date year\n"
        res = res + "df['year'] = df['index_date'].dt.year.astype('str')\n"
        res = res + "year_list = df['year'].unique().tolist()\n"
        res = res + "year_list.sort(reverse = True)\n"
        res = res + "year_list"
        return res


    def average_compliance_over_time(self, analysis_id, product, year_list, time_period):
        res = ''
        res = res + "# filter out single dispensed patients\n"
        res = res + "df = df_compliance_" + str(analysis_id) + ".loc[df_compliance_"
        res = res + str(analysis_id) + "['patient_type'] != 'Single_dispensed']\n"
        res = res + "# Product filter\n"
        res = res + "df = df.loc[df['product'] == '" + str(product) + "']\n"
        res = res + "df = df[['patient_id','index_date', 'compliance_percent','flag']]\n\n"
        res = res + "# index date year and month\n"
        res = res + "df['year'] = df['index_date'].dt.year.astype('str')\n"
        res = res + "df['index_month'] = df['index_date'].dt.month\n\n"

        res = res + "# Year filter\n"
        res = res + "df = df.loc[df['year'].isin(["
        for i,year in enumerate(year_list):
            res = res + "'" + str(year) + "'"
            if i != len(year_list)-1:
                res = res + ","
        res = res + "])]\n\n"

        res = res + "gt_150_patient_count = df.loc[df['flag'] == 'gt_150']['patient_id'].nunique()\n\n"
        res = res + "df = df.loc[df['flag'] == 'lte_150']\n\n"
        res = res + "patient_count_by_year = df['patient_id'].nunique()\n\n"
        res = res + "# finding " + str(time_period)+ " based on index date\n"
        if time_period == 'month':
            res = res + "df['month'] = df['year'] + '-' + df['index_month'].astype(str).str.zfill(2)\n\n"
        elif time_period == 'quarter':
            res = res + "df['quarter'] = df['year'] + '-Q' + (((df['index_month'] -1)// 3) +1)"
            res = res + ".astype(str)\n\n"
        else:
            res = res + "df['semester'] = df['year'] + '-S' + (((df['index_month'] -1)// 6) +1)"
            res = res + ".astype(str)\n\n"
        
        res = res + "# Calculating average compliance at " + str(time_period) + " level\n"
        res = res + "df = df.groupby(['" + str(time_period) + "']).agg(avg_compliance = "
        res = res + "('compliance_percent', 'mean'), patient_count = ('patient_id', 'nunique'))"
        res = res + ".reset_index()\n\n"
        res = res + "# Rounding to 0 decimal and converting to int datatype\n"
        res = res + "df = df.round({'avg_compliance' : 0})\n"
        res = res + "df['avg_compliance'] = df['avg_compliance'].astype(int)\n"
        res = res + "df = df.rename(columns = {'" + str(time_period) + "': 'time_period'})\n\n"
        res = res + "# Converting the dataframe to json how UI needs\n"
        res = res + "final_dict = dict()\n"
        res = res + "final_dict['gt_150_patients'] = gt_150_patient_count\n"
        res = res + "final_dict['patient_count_by_year'] = patient_count_by_year\n"
        res = res + "final_dict['graph'] = df.to_dict('records')\n"
        res = res + "final_dict"
        return res


    def compliant_patients(self, analysis_id):
        res = ''
        res = res + "df = df_compliance_" + str(analysis_id)
        res = res + "[['product', 'patient_id', 'compliance_flag']]\n\n"
        res = res + "# Total Patient count at product level\n"
        res = res + "df1 = df.groupby(['product']).agg(total_patients = "
        res = res + "('patient_id', 'nunique')).reset_index()\n\n"
        res = res + "# Patient count at product and compliance_flag level\n"
        res = res + "df = df.groupby(['product', 'compliance_flag']).agg(patient_count= "
        res = res + "('patient_id', 'nunique')).reset_index()\n"
        res = res + "# Transposing\n"
        res = res + "df = df.pivot(index = 'product', columns = 'compliance_flag', "
        res = res + "values = 'patient_count').reset_index()\n"
        res = res + "df = pd.merge(df1, df, on = ['product'], how = 'left')\n\n"
        res = res + "df = df.fillna(0)\n\n"
        res = res + "# Adding columns if not present\n"
        res = res + "if 'Compliant' not in df.columns.tolist():\n"
        res = res + "\tdf['Compliant'] = 0\n"
        res = res + "if 'Non-Compliant' not in df.columns.tolist():\n"
        res = res + "\tdf['Non-Compliant'] = 0\n"
        res = res + "if 'Single_dispensed' not in df.columns.tolist():\n"
        res = res + "\tdf['Single_dispensed'] = 0\n\n"
        res = res + "# Renaming column names\n"
        res = res + "df = df.rename({'Compliant': 'compliant_patients', 'Non-Compliant': "
        res = res + "'non_compliant_patients', 'Single_dispensed': 'single_dispensed_patients'}"
        res = res + ", axis = 1)\n\n"
        res = res + "# Calculating compliant, non-compliant and single dispensed percentage\n"
        res = res + "df['compliant_percent'] = "
        res = res + "(df['compliant_patients']/df['total_patients']) *100\n"
        res = res + "df['non_compliant_percent'] = "
        res = res + "(df['non_compliant_patients']/df['total_patients']) *100\n"
        res = res + "df['single_dispensed_percent'] = "
        res = res + "(df['single_dispensed_patients']/df['total_patients']) *100\n\n"
        res = res + "df = df.sort_values(by=['total_patients'], ascending=False)\n"
        res = res + "# Repalcing nulls with 0\n"
        res = res + "df = df.fillna(0)\n\n"
        res = res + "# Rounding to 1 decimal and converting to int datatype\n"
        res = res + "df = df.round({'compliant_percent' : 1, 'non_compliant_percent': 1, "
        res = res + "'single_dispensed_percent': 1})\n\n"
        
        res = res + "df = df.astype({'compliant_patients': 'int', "
        res = res + "'non_compliant_patients': 'int', 'single_dispensed_patients': 'int'})\n\n"
        res = res + "# Converting the dataframe to json how UI needs\n"
        res = res + "df.to_dict('records')"
        return res

    def compliant_patients_over_time(self, analysis_id, product, year_list, time_period):
        res = ''
        res = res + "df = df_compliance_" + str(analysis_id)
        res = res + "[['product', 'index_date', 'patient_id', 'compliance_flag']]\n"
        res = res + "# product filter\n"
        res = res + "df = df.loc[df['product'] == '" + str(product) + "']\n\n"
        res = res + "# index date year and month\n"
        res = res + "df['year'] = df['index_date'].dt.year.astype('str')\n"
        res = res + "df['index_month'] = df['index_date'].dt.month\n\n"

        res = res + "# Year filter\n"
        res = res + "df = df.loc[df['year'].isin(["
        for i,year in enumerate(year_list):
            res = res + "'" + str(year) + "'"
            if i != len(year_list)-1:
                res = res + ","
        res = res + "])]\n\n"

        res = res + "patient_count_by_year = df['patient_id'].nunique()\n\n"

        res = res + "# finding " + str(time_period)+ " based on index date\n"
        if time_period == 'month':
            res = res + "df['month'] = df['year'] + '-' + df['index_month'].astype(str).str.zfill(2)\n\n"
        elif time_period == 'quarter':
            res = res + "df['quarter'] = df['year'] + '-Q' + (((df['index_month'] -1)// 3) +1)"
            res = res + ".astype(str)\n\n"
        else:
            res = res + "df['semester'] = df['year'] + '-S' + (((df['index_month'] -1)// 6) +1)"
            res = res + ".astype(str)\n\n"
    
        res = res + "# Total Patient count at " + str(time_period) + " level\n"
        res = res + "df1 = df.groupby(['" + str(time_period) + "']).agg(total_patients = "
        res = res + "('patient_id', 'nunique')).reset_index()\n\n"
        res = res + "# Patient count at " + str(time_period) + " and compliance_flag level\n"
        res = res + "df = df.groupby(['" + str(time_period) + "', 'compliance_flag']).agg(patient_count= "
        res = res + "('patient_id', 'nunique')).reset_index()\n"
        res = res + "# Transposing\n"
        res = res + "df = df.pivot(index = '" + str(time_period) + "', columns = 'compliance_flag', "
        res = res + "values = 'patient_count').reset_index()\n"
        res = res + "df = pd.merge(df1, df, on = ['" + str(time_period) + "'], how = 'left')\n\n"
        res = res + "df = df.fillna(0)\n\n"
        res = res + "# Adding columns if not present\n"
        res = res + "if 'Compliant' not in df.columns.tolist():\n"
        res = res + "\tdf['Compliant'] = 0\n"
        res = res + "if 'Non-Compliant' not in df.columns.tolist():\n"
        res = res + "\tdf['Non-Compliant'] = 0\n"
        res = res + "if 'Single_dispensed' not in df.columns.tolist():\n"
        res = res + "\tdf['Single_dispensed'] = 0\n\n"
        res = res + "# Renaming column names\n"
        res = res + "df = df.rename({'Compliant': 'compliant_patients', 'Non-Compliant': "
        res = res + "'non_compliant_patients', 'Single_dispensed': 'single_dispensed_patients'}"
        res = res + ", axis = 1)\n\n"
        res = res + "# Calculating compliant, non-compliant and single dispensed percentage\n"
        res = res + "df['compliant_percent'] = "
        res = res + "(df['compliant_patients']/df['total_patients']) *100\n"
        res = res + "df['non_compliant_percent'] = "
        res = res + "(df['non_compliant_patients']/df['total_patients']) *100\n"
        res = res + "df['single_dispensed_percent'] = "
        res = res + "(df['single_dispensed_patients']/df['total_patients']) *100\n\n"
        res = res + "# Repalcing nulls with 0\n"
        res = res + "df = df.fillna(0)\n\n"
        res = res + "# Rounding to 1 decimal and converting to int datatype\n"
        res = res + "df = df.round({'compliant_percent' : 1, 'non_compliant_percent': 1, "
        res = res + "'single_dispensed_percent': 1})\n\n"
        
        res = res + "df = df.astype({'compliant_patients': 'int', "
        res = res + "'non_compliant_patients': 'int', 'single_dispensed_patients': 'int'})\n\n"

        res = res + "df = df.rename(columns = {'" + str(time_period) + "': 'time_period'})\n\n"
        res = res + "df = df.sort_values(by=['time_period'], ascending = True)\n"
        res = res + "# Converting the dataframe to json how UI needs\n"
        res = res + "final_dict = dict()\n"
        res = res + "final_dict['patient_count_by_year'] = patient_count_by_year\n"
        res = res + "final_dict['graph'] = df.to_dict('records')\n"
        res = res + "final_dict"
        return res
    
        
    def percentage_patient_distribution(self, analysis_id):
        res = ''
        res = res + "# filter out single dispensed patients\n"
        res = res + "df = df_compliance_" + str(analysis_id) + ".loc[df_compliance_"
        res = res + str(analysis_id) + "['patient_type'] != 'Single_dispensed']\n"
        res = res + "df = df[['product', 'patient_id', 'compliance_percent','flag']]\n\n"
        res = res + "gt_150_patient_count = df.loc[df['flag'] == 'gt_150']['patient_id'].nunique()\n\n"
        res = res + "df = df.loc[df['flag'] == 'lte_150']\n\n"
        res = res + "# splits and labels for bucketing the data\n"
        res = res + "splits = [float('-inf')]\n"
        res = res + "labels= []\n\n"
        res = res + "max_percent = df.compliance_percent.max()\n"
        res = res + "for i in range(1,math.ceil(max_percent/10)+1):\n"
        res = res + "\tsplits.append(i*10)\n"
        res = res + "\tlabel = str((i-1)*10) + '-' + str(i*10)\n"
        res = res + "\tlabels.append(label)\n\n"
        res = res + "# Bucketing the compliance percentage\n"
        res = res + "df['bucket'] = pd.cut(x=df['compliance_percent'], bins=splits, labels=labels)\n"
        res = res + "# Total Patient count at product level\n"
        res = res + "df1 = df.groupby(['product']).agg(total_patient_count = ('patient_id', 'nunique'))"
        res = res + ".reset_index()\n\n"
        res = res + "# Patient count at product and compliance_bucket level\n"
        res = res + "df = df.groupby(['product','bucket']).agg(patient_count = "
        res = res + "('patient_id', 'nunique')).reset_index()\n"
        res = res + "df = pd.merge(df, df1, on=['product'], how='left')\n\n"
        res = res + "df['patient_percentage'] = (df['patient_count']/df['total_patient_count'])*100\n\n"
        res = res + "# Rounding to 2 decimal\n"
        res = res + "df = df.round({'patient_percentage' : 2})\n\n"
        res = res + "df = df.sort_values(by= ['total_patient_count','bucket'], ascending= [False,True])\n\n"
        res = res + "# Getting list of columns(product names) based on total patients at product level\n"
        res = res + "df1 = df[['product','total_patient_count']].drop_duplicates()\n"
        res = res + "columns_list = df1['product'].unique().tolist()\n"
        res = res + "columns_list.insert(0, 'bucket')\n\n"
        res = res + "# Transposing\n"
        res = res + "df = df.pivot(index = 'bucket', columns='product', values='patient_percentage')"
        res = res + ".reset_index()\n\n"
        res = res + "df = df.replace(np.nan, 0)\n"
        res = res + "df = df[columns_list]\n\n"
        res = res + "# Converting the dataframe to json how UI needs\n"
        res = res + "final_dict = dict()\n"
        res = res + "final_dict['gt_150_patients'] = gt_150_patient_count\n"
        res = res + "final_dict['graph'] = df.to_dict('records')\n"
        res = res + "final_dict"
        return res


    def patient_distribution(self, analysis_id, product):
        res = ''
        res = res + "# filter out single dispensed patients\n"
        res = res + "df = df_compliance_" + str(analysis_id) + ".loc[df_compliance_"
        res = res + str(analysis_id) + "['patient_type'] != 'Single_dispensed']\n"
        res = res + "df = df[['product', 'patient_id', 'compliance_percent', 'flag']]\n\n"
        res = res + "# product filter\n"
        res = res + "df = df.loc[df['product'] == '"+ str(product) + "']\n\n"
        res = res + "gt_150_patient_count = df.loc[df['flag'] == 'gt_150']['patient_id'].nunique()\n\n"
        res = res + "df = df.loc[df['flag'] == 'lte_150'].drop_duplicates()\n\n"
        res = res + "compliance_percent_list = df['compliance_percent'].tolist()\n\n"
        res = res + "# Converting the dataframe to json how UI needs\n"
        res = res + "final_dict = dict()\n"
        res = res + "final_dict['gt_150_patients'] = gt_150_patient_count\n"
        res = res + "final_dict['graph'] = compliance_percent_list\n"
        res = res + "final_dict"
        return res


class Persistence:
    """
    Class contains all the functions related to Persistence Attribute
    """

    def __init__(self):
        """ Initialize all class level variables """
        pass

    def generate_persistence(self):
        res = ''
        res = res + "def generate_persistence(data_1, "
        res = res + "product_list, op, index_start_date, index_end_date, look_forward_days, flag1, "
        res = res + "flag2, dos_value,grace_flag, grace_period,single_dispensed_value,"
        res = res + " df_grace, df_new = None):\n"

        res = res + "\t# product filter\n"
        res = res + "\tdata_1 = data_1.loc[data_1['product'].isin(product_list)]\n"
        res = res + "\t# Converting nan and 0's to null in days_supply column\n"
        res = res + "\tdata_1['days_supply'] = data_1['days_supply'].astype('float')\n"
        res = res + "\tdata_1['days_supply'] = data_1['days_supply'].replace(0, np.nan)\n\n"

        res = res + "\t# days of supply handling\n"
        res = res + "\tif flag1 == 'filter':\n"
        res = res + "\t\t# Filtering out the data in case Days Of Supply is null or 0 days\n"
        res = res + "\t\tdata_1 = data_1.dropna(subset = ['days_supply'])\n"
        res = res + "\telif flag1 == 'assign':\n"
        res = res + "\t\tif flag2 == 'all':\n"
        res = res + "\t\t\t# Assign user specified value to Days of supply for all the data considered for the analysis\n"
        res = res + "\t\t\tdata_1['days_supply'] = dos_value\n"
        res = res + "\t\telse:\n"
        res = res + "\t\t\t# Assign values to Days of supply in case of nulls or 0 values\n"
        res = res + "\t\t\tdata_1.fillna(value = {'days_supply': dos_value}, inplace = True)\n"
        res = res + "\telse:\n"
        res = res + "\t\t# getting mean_dos_value and default_value for the respective source_value\n"
        res = res + "\t\t# converting them to numeric data\n"
        res = res + "\t\tdata_1 = pd.merge(data_1, df_new, on=['source_value'], how = 'left')\n"
        res = res + "\t\tdata_1['mean_dos_value'] = data_1['mean_dos_value'].astype('float')\n"
        res = res + "\t\tdata_1['default_dos_value'] = pd.to_numeric(data_1['default_dos_value'],"
        res = res + " errors = 'coerce')\n"
        res = res + "\t\tif flag2 == 'mean':\n"
        res = res + "\t\t\t# if priority1 is mean\n"
        res = res + "\t\t\t# Then if the days_supply is null , then assign mean_dos_value\n"
        res = res + "\t\t\t# Then after if mean_dos_value is null , then assign default_dos_value\n"
        res = res + "\t\t\tdata_1['mean_dos_value'] = data_1['mean_dos_value'].replace(0, np.nan)\n"
        res = res + "\t\t\tdata_1['default_dos_value'] = data_1['default_dos_value'].replace(np.nan, 0)\n"
        res = res + "\t\t\tdata_1['days_supply'] = data_1['days_supply']"
        res = res + ".fillna(data_1['mean_dos_value']).fillna(data_1['default_dos_value'])\n"
        res = res + "\t\telse:\n"
        res = res + "\t\t\t# if priority1 is default\n"
        res = res + "\t\t\t# Then if the days_supply is null , then assign default_dos_value\n"
        res = res + "\t\t\t# Then after if default_dos_value is null , then assign mean_dos_value\n"
        res = res + "\t\t\tdata_1['default_dos_value'] = data_1['default_dos_value'].replace(0, np.nan)\n"
        res = res + "\t\t\tdata_1['mean_dos_value'] = data_1['mean_dos_value'].replace(np.nan, 0)\n"
        res = res + "\t\t\tdata_1['days_supply'] = data_1['days_supply']"
        res = res + ".fillna(data_1['default_dos_value']).fillna(data_1['mean_dos_value'])\n"
        res = res + "\t\tdata_1 = data_1.drop(['mean_dos_value', 'default_dos_value'], axis = 1)\n\n"
        res = res + "\tdata_1 = data_1.drop(['source_value'], axis = 1)\n"
        res = res + "\t# dropping duplicates as in some cases days_supply is changed\n"
        res = res + "\tdata_1 = data_1.drop_duplicates()\n\n"

        res = res + "\t#Grace Period Handling\n"
        res = res + "\tif grace_flag == 'assign_all':\n"
        res = res + "\t\tdata_1['grace_period'] = grace_period\n"
        res = res + "\telse:\n"
        res = res + "\t\t# Product-wise grace period assignment\n"
        res = res + "\t\tdata_1 = pd.merge(data_1, df_grace, on=['product'], how='left', suffixes=('', '_DROP'))"
        res = res + ".filter(regex='^(?!.*_DROP)')\n\n"

        res = res + "\t# Filtering out Single-dispensed claims\n"
        res = res + "\t# Days of supply must be greater than user-defined value\n"
        res = res + "\tdata_1 = data_1[data_1.days_supply > single_dispensed_value].drop_duplicates()\n\n"
        
        res = res + "\t# index date for each patient at product level\n"
        res = res + "\tdff = data_1.groupby(['patient_id', 'product']).agg(index_date = ('activity_date','min'))"
        res = res + ".reset_index()\n"
        res = res + "\tdata_1 = pd.merge(data_1, dff, on = ['patient_id', 'product'], how = 'left')"
        res = res + ".sort_values(by = ['patient_id', 'product', 'activity_date'])\n\n"

        res = res + "\t# Index date constraint\n"
        res = res + "\tif op == 'lte':\n"
        res = res + "\t\tmask1 = (data_1['index_date'] <= index_start_date)\n"
        res = res + "\telif op == 'lt':\n"
        res = res + "\t\tmask1 = (data_1['index_date'] < index_start_date)\n"
        res = res + "\telif op == 'gte':\n"
        res = res + "\t\tmask1 = (data_1['index_date'] >= index_start_date)\n"
        res = res + "\telif op == 'gt':\n"
        res = res + "\t\tmask1 = (data_1['index_date'] > index_start_date)\n"
        res = res + "\telif op == 'eq':\n"
        res = res + "\t\tmask1 = (data_1['index_date'] == index_start_date)\n"
        res = res + "\telse:\n"
        res = res + "\t\tmask1 = (data_1['index_date'] >= index_start_date)"
        res = res + " & (data_1['index_date'] <= index_end_date)\n"
        res = res + "\tdata_1 = data_1.loc[mask1]\n\n"

        res = res + "\t# Raising Exception if data frame is empty\n"
        res = res + "\ttry:\n"
        res = res + "\t\tif len(data_1) == 0:\n"
        res = res + "\t\t\traise Exception\n"
        res = res + "\texcept:\n"
        res = res + "\t\traise Exception('No records available to calculate Persistency metric.')\n\n"

        res = res + "\t# Updating activity date according to end date\n"
        res = res + "\tprev_patient_id = str(' ')\n"
        res = res + "\tprev_product = str(' ')\n"
        res = res + "\tfor i, row in data_1.iterrows():\n"
        res = res + "\t\tif (prev_patient_id != row['patient_id']) or "
        res = res + "(prev_product != row['product']):\n"
        res = res + "\t\t\tnew_activity_date = row['activity_date']\n"
        res = res + "\t\t\tnew_end_date = new_activity_date + pd.to_timedelta(row['days_supply'], unit='D')\n"
        res = res + "\t\telse:\n"
        res = res + "\t\t\tnew_activity_date = max([prev_new_end_date, row['activity_date']])\n"
        res = res + "\t\t\tnew_end_date = new_activity_date + pd.to_timedelta(row['days_supply'], unit='D')\n"
        res = res + "\t\tprev_patient_id = row['patient_id']\n"
        res = res + "\t\tprev_product = row['product']\n"
        res = res + "\t\tprev_new_activity_date = new_activity_date\n"
        res = res + "\t\tprev_new_end_date = new_end_date\n"
        res = res + "\t\tdata_1.at[i, 'new_activity_date'] = new_activity_date\n"
        res = res + "\t\tdata_1.at[i, 'new_end_date'] = new_end_date\n\n"

        res = res + "\t# Restricting the data based on look_forward_days\n"
        res = res + "\tmask1 = (((data_1['new_activity_date'] - data_1['index_date']).dt.days) <= look_forward_days)\n"
        res = res + "\tdata_1 = data_1.loc[mask1]\n\n"

        res = res + "\t# Grace period handling\n"
        res = res + "\tdata_1['prev_end_date'] = data_1.sort_values(by = ['new_activity_date'], ascending = True)"
        res = res + ".groupby(['product', 'patient_id'])['new_end_date'].shift(1)\n\n"
        res = res + "\tdata_1['next_diff'] = (data_1['new_activity_date'] - data_1['prev_end_date']).dt.days\n"
        res = res + "\tdata_1['next_diff'] = data_1['next_diff'].fillna(0)\n"
        res = res + "\tdata_1['next_diff'] = data_1['next_diff'].astype(int)\n"
        res = res + "\tdata_1['grace'] = data_1.apply(lambda x: x.next_diff "
        res = res + "if x.next_diff <= x.grace_period else 0, axis = 1)\n\n"
        
        res = res + "\t# Episode Creation\n"
        res = res + "\tdata_1['episode_flag'] = np.where((data_1['next_diff'] > data_1['grace_period']), 1, 0)"
        res = res + ".astype(int)\n"
        res = res + "\tdata_1['episode_number'] = data_1.groupby(['patient_id','product'])"
        res = res + "['episode_flag'].cumsum()\n"
        res = res + "\tdata_1['episode_num'] = data_1['episode_number'] + 1" + "\n"
        res = res + "\tdata_1 = data_1.drop(['episode_flag','episode_number'], axis = 1).drop_duplicates()\n\n"

        res = res + "\t# Filtering for episode 1\n"
        res = res + "\tdata_1 = data_1.loc[data_1['episode_num'] == 1]\n"

        res = res + "\t# Episode index date\n"
        res = res + "\tdff = data_1.groupby(['patient_id', 'product', 'episode_num']).agg(ep_index_date = "
        res = res + "('new_activity_date', 'min')).reset_index()\n"
        res = res + "\tdata_1 = pd.merge(data_1, dff, on = ['patient_id', 'product', 'episode_num'], how = 'left')"
        res = res + ".sort_values(by = ['patient_id', 'product', 'new_activity_date'])\n\n"
        
        res = res + "\t# Start and end months\n"
        res = res + "\tdata_1['st_mo'] = ((((data_1['new_activity_date'] - pd.to_timedelta(data_1['grace'], unit='D'))"
        res = res + " - data_1['ep_index_date']).dt.days)/30.0).apply(np.ceil).astype(int)\n\n"
        res = res + "\tdata_1['end_mo'] = ((((data_1['new_end_date'] + pd.to_timedelta(data_1['grace'], unit='D'))"
        res = res + " - data_1['ep_index_date']).dt.days)/30.0).apply(np.ceil).astype(int)\n"
        res = res + "\tdata_1['st_mo'] = data_1['st_mo'].replace(0, 1)\n\n"

        res = res + "\tdata_1['end_date_with_grace'] = (data_1['new_end_date'] + pd.to_timedelta(data_1['grace'], "
        res = res + "unit='D'))\n"
        res = res + "\tdff = data_1.groupby(['patient_id', 'product']).agg(min_activity_date = "
        res = res + "('new_activity_date', 'min'), max_activity_date = ('new_activity_date', 'max'),"
        res = res + " end_date = ('end_date_with_grace', 'last')).reset_index()\n\n"

        res = res + "\t# Droping unwanted columns\n"
        res = res + "\tdata_1 = data_1.drop(['row_number','prev_end_date','next_diff', 'episode_number',"
        res = res + "'episode_flag','activity_date','new_activity_date', 'days_supply', 'grace_period', "
        res = res + "'new_end_date', 'grace','ep_index_date', 'end_date_with_grace', 'index_date'], "
        res = res + "axis = 1, errors='ignore')\n\n"

        res = res + "\t# Maximum end month\n"
        res = res + "\tmax_end_month = data_1['end_mo'].max() + 1\n\n"

        res = res + "\t# creating flags for each month the patient is present\n"
        res = res + "\tnlevel_dict = dict()\n"
        res = res + "\tnlevel_list = []\n"
        res = res + "\tfor i in range(1, max_end_month):\n"
        res = res + "\t\tcolumn_name = 'n' + str(i)\n"
        res = res + "\t\tdata_1[column_name] = np.where((data_1['st_mo'] <= i) & (data_1['end_mo'] >= i), 1, 0)\n"
        res = res + "\t\tnlevel_dict[column_name] = 'sum'\n"
        res = res + "\t\tnlevel_list.append(column_name)\n\n"

        res = res + "\tdata_1 = data_1.drop(['st_mo','end_mo'], axis =1)\n"
        res = res + "\tcols = [i for i in data_1.columns if i not in nlevel_list]\n\n"

        res = res + "\t# Taking the sum of each flag and bringing it to patient episode level\n"
        res = res + "\tdata_1 = data_1.groupby(cols, observed=True).agg(nlevel_dict).reset_index()\n\n"
        res = res + "\tdata_1 = pd.merge(data_1, dff, on = ['patient_id', 'product'], how = 'inner')\n\n"

        res = res + "\t# Flagging months having atleast 1 occurrence\n"
        res = res + "\tnlevel_list = []\n"
        res = res + "\tfor i in range(1, max_end_month):\n"
        res = res + "\t\tcolumn_p = 'month' + str(i)\n"
        res = res + "\t\tcolumn_n = 'n' + str(i)\n"
        res = res + "\t\tdata_1[column_p] = np.where((data_1[column_n] > 0), 1, 0)\n"
        res = res + "\t\tnlevel_list.append(column_n)\n\n"

        res = res + "\tdata_1.drop(nlevel_list, axis = 1, inplace = True)\n"
        res = res + "\tdel(df_new, dff, df_grace)\n"
        res = res + "\treturn data_1\n"
        return res


    def persistence_metric(self,  analysis_id, data, start_date, end_date, unwanted_columns):
        res = ''
        res = res + self.generate_persistence()
        res = res + "\n\n\n"
        res = res + "# Getting the required columns required for compliance calculation\n"
        res = res + "if 'regimen' in df_" + str(analysis_id) + ".columns:\n"
        res = res + "\tdf = df_" + str(analysis_id) + ".drop(['start_date', 'claim_id', 'quantity', 'specialty_code', 'specialty_desc', "
        res = res + "'npi_number', 'index_date', 'case_flag', 'regimen','HCP_name','mono_combo'], axis =1, "
        res = res + "errors='ignore')\n"
        res = res + "\tlot_ = [i for i in df.columns if i[:3] == 'lot' or i[8:13] == 'level']\n"
        res = res + "\tdf = df.drop(lot_, axis=1, errors='ignore')\n"
        res = res + "else:\n"
        res = res + "\tdf = df_" + str(analysis_id) + ".drop(['start_date', 'claim_id', 'quantity', 'specialty_code', 'specialty_desc', "
        res = res + "'npi_number','HCP_name','mono_combo'], axis =1, errors='ignore')\n\n"
        res = res + "df = df.drop_duplicates()\n\n"
    
        res = res + "# Analysis period filter\n"
        res = res + "mask = (df['activity_date'] >= '" + str(start_date) 
        res = res + "') & (df['activity_date'] <= '" + str(end_date) + "')\n"
        res = res + "df = df.loc[mask]\n\n"

        res = res + "# Filtering Treatment and Procedure records\n"
        res = res + "df = df.loc[df['code_flag'].isin(['Rx', 'Px'])]\n"
        res = res + "df = df.drop('code_flag', axis =1).drop_duplicates()\n\n"

        res = res + "df_list = []\n"
        for i,item in enumerate(data):
            res = res + "df = df.rename(columns = {'" + str(item['Product_Selection']['Drug_Class'])
            res = res + "' : 'product'})\n\n"
            res = res + "df = df.drop(" + str(unwanted_columns) + ", axis =1 , errors='ignore').drop_duplicates()\n\n"
            # Generating flags for handling days of supply
            flag_1 = ''
            flag_2 = ''
            dos_value = 0
            if item['Days_of_Supply_Type'] == 'Filter':
                flag_1 = 'filter'
            elif item['Days_of_Supply_Type'] == 'Assign':
                flag_1 = 'assign'
                if item['Days_of_Supply_Details']['Flag'] == 'zero_or_null':
                    flag_2 = 'zero_or_null'
                    dos_value = item['Days_of_Supply_Details']['Days']
                else:
                    flag_2 = 'all'
                    dos_value = item['Days_of_Supply_Details']['Days']
            else:
                flag_1 = 'treating_dos'
                if item['Days_of_Supply_Details']['Priority1'] == 'Mean':
                    flag_2 = 'mean'
                else:
                    flag_2 = 'default'
            
            # generating flags for grace period handling
            grace_flag = ''
            grace_period = 0
            if item['Grace_Period_Type'] == 'Assign':
                grace_flag = 'assign_all'
                grace_period = item['Grace_Period_Details']['Days']
            else:
                grace_flag = 'product_level'
            
            if item['Days_of_Supply_Type'] == 'Treating_dos':
                res = res + "# Mean dos json at ndc level from UI\n"
                res = res + "# These are the ndc's having one or more records having days supply as zero\n"
                res = res + "mean_dos_json = " + str(item['Days_of_Supply_Details']['Treat_dos_json']) + "\n\n"
                res = res + "# Converting json to dataframe\n"
                res = res + "df_mean_dos = pd.DataFrame(mean_dos_json)\n\n"

            if item['Grace_Period_Type'] == 'Product':
                res = res + "# Product level grace period json from UI\n"
                res = res + "product_lvl_grace_prd_json = "
                res = res + str(item['Grace_Period_Details']['Attribute_Json']) + "\n\n"
                res = res + "# Converting json to dataframe\n"
                res = res + "df_product_lvl_grace = pd.DataFrame(product_lvl_grace_prd_json)"
                res = res + ".rename({'Grace_Period_value': 'grace_period'}, axis = 1)\n\n"
            else:
                res = res + "df_product_lvl_grace = pd.DataFrame({})\n\n"

            res = res + "# Generating persistence\n"
            res = res + "data_" + str(i+1)
            res = res + " = generate_persistence(df, "
            res = res + str(item['Product_Selection']['Drug_Class_Values']) + ", '"
            res = res + str(item['Time_period']['OP']) +"', '"
            res = res + str(item['Time_period']['Value']) + "', '"
             
            if item.get('Time_period').get('Extent'):
                res = res + str(item['Time_period']['Extent'])
            
            res = res + "', " + str(item['Time_period']['Look_Forward_Days'])
            res = res + ", '" + str(flag_1) + "', '" + str(flag_2) + "', "+ str(dos_value)
            res = res + ", '" + str(grace_flag) + "', " + str(grace_period)
            res = res + ", " + str(item['Single_Dispensed_Value'])
            res = res + ", df_product_lvl_grace"
            if item['Days_of_Supply_Type'] == 'Treating_dos':
                res = res + ", df_mean_dos"
            
            res = res  + ")\n"

            res = res + "df_list.append(data_"+ str(i+1) + ")\n\n"
        res = res + "# Union of all the generated persistence\n"
        res = res + "df_persistence_" + str(analysis_id) + " = pd.concat(df_list)\n"
        res = res + "df_persistence_" + str(analysis_id) + "= df_persistence_" 
        res = res + str(analysis_id) + ".drop_duplicates()\n"
        res = res + "df_persistence_" + str(analysis_id)
        res = res + ".sort_values(by= ['patient_id','product'], inplace = True)\n\n"
        res = res + "del(df, df_list, df_product_lvl_grace"
        for i in range(len(data)):
            res = res + ", data_" + str(i+1)
        res = res + ")\n"
        return res


    def patient_level_graph(self, analysis_id, persistence_dict = {}):
        res = ''

        if bool(persistence_dict) == True:
            res = res + "df_persistence_" + str(analysis_id) + " = df_persistence_" + str(analysis_id)
            res = res + ".loc["
            for key, val in persistence_dict.items():
                res = res + "(df_persistence_" + str(analysis_id) + "['" + str(key) + "'] == '" + str(val) + "') & "
            result = list(res)
            del result[-3:-1]
            result.insert(-1, "]")
            res = ''.join(result)

        res = res + "\n\n# Persistence Graph code\n"
        res = res + "df = df_persistence_" + str(analysis_id) + "\n"

        res = res + "cols_list= ['patient_id','product','episode_num']\n\n"
        res = res + "maxm_end_month = 0\n"
        res = res + "for colum in df.columns:\n"
        res = res + "\tif colum[0:5]=='month' and colum[5:].isnumeric():\n"
        res = res + "\t\tcols_list.append(colum)\n"
        res = res + "\t\tmaxm_end_month = maxm_end_month + 1\n\n"

        res = res + "df = df[cols_list].drop_duplicates()\n\n"

        res = res + "plevel_dict = dict()\n"
        res = res + "for i in range(1, maxm_end_month):\n"
        res = res + "\tcolumn_p = 'month' + str(i)\n"
        res = res + "\tplevel_dict[column_p] = 'sum'\n\n"

        res = res + "# Patient level persistence\n"
        res = res + "df = df.groupby(['product']).agg(plevel_dict).reset_index()\n\n"
        
        res = res + "# Summary\n"
        res = res + "plevel_list = []\n"
        res = res + "for i in range(1, maxm_end_month):\n"
        res = res + "\tcolumn_p = 'month' + str(i)\n"
        res = res + "\tcolumn_m = 'M' + str(i)\n"
        res = res + "\tdf[column_m] = (df[column_p]*100/df['month1']).round(2)\n"
        res = res + "\tplevel_list.append(column_p)\n\n"
        res = res + "df.drop(plevel_list, axis = 1, inplace = True)\n"
        res = res + "df = df.set_index('product').T.reset_index().rename(columns={'index': 'month'})\n\n"
        res = res + "df.to_dict('records')"
        return res


    def save_to_s3(self, project_id, analysis_id, attribute_id):
        res = ''
        res = res + "df_persistence_" + str(analysis_id)
        res = res + ".to_parquet('s3://{}/PFA_PERSISTENCE/prj_".format(cs.S3_PROJECT_PATH)
        res = res + str(project_id) + "_" + str(analysis_id)
        res = res + "_" + str(attribute_id) + "')\n"
        return (res)

    def read_file(self, project_id, analysis_id, attribute_id):
        res = ''
        res = res + "df_persistence_" + str(analysis_id) + " = "
        res = res + "pd.read_parquet('s3://{}/PFA_PERSISTENCE/prj_".format(cs.S3_PROJECT_PATH)
        res = res + str(project_id) + "_" + str(analysis_id)
        res = res + "_" + str(attribute_id) + "')\n"
        return (res)
    

    def drop_column(self, analysis_id, col_name, project_id, attribute_id):
        res = self.read_file(str(project_id), str(analysis_id), str(attribute_id)) + "\n"
        res = res + "df_persistence_" + str(analysis_id) + " = df_persistence_" + str(analysis_id)
        res = res + ".drop(['{}'], axis=1)\n\n".format(col_name)
        res = res + self.save_to_s3(str(project_id), str(analysis_id), str(attribute_id))
        return (res)


    def age_range_attribute(self, analysis_id, splits, labels, col_name, project_id, attribute_id):
        res = self.read_file(str(project_id), str(analysis_id), str(attribute_id)) + "\n"
        splits.pop() 
        splits = map(str, splits)
        splits = ', '.join(splits)
        splt_str = "[float('-inf'), float('inf')]"
        for i in range(len(splt_str)):
            if splt_str[i] == ',':
                lst = list(splt_str)
                lst.insert(i, ', ' + splits)
                lst = ''.join(lst)
        res = res + "splits = " + str(lst) + "\n"
        res = res + "labels = " + str(labels) + "\n"
        res = res + "df_persistence_" + str(analysis_id) + "['" + str(col_name) + "'] = pd.cut(x=df_persistence_"
        res = res + str(analysis_id) + "['age'], bins=splits, labels=labels)\n"
        res = res + "df_persistence_" + str(analysis_id) + "['" + str(col_name) + "'] = df_persistence_"
        res = res + str(analysis_id) + "['" + str(col_name) + "'].astype(str)\n\n"

        res = res + self.save_to_s3(str(project_id), str(analysis_id), str(attribute_id))
        return (res)


    def gender_attribute(self, analysis_id, map_lst, dim_lst, col_name, project_id, attribute_id):
        res = self.read_file(str(project_id), str(analysis_id), str(attribute_id)) + "\n"
        res = res + "df_persistence_" + str(analysis_id) + "['" + str(col_name) + "'] = "
        res = res + "np.select(["
        if (len(dim_lst) >= 1):
            for i in range(len(dim_lst)):
                res = res + "(df_persistence_" + str(analysis_id)
                res = res + "['gender'].isin(['" + str(map_lst[i]) + "'])), "

            result = list(res)
            del result[-2:-1]
            res = ''.join(result)
            res = res + "], " + str(dim_lst) + ", default = 'others')\n\n"
            res = res + self.save_to_s3(str(project_id), str(analysis_id), str(attribute_id))
            return (res)
        else:
            return ("Invalid Gender Mappings")