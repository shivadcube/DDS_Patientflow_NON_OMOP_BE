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
        res = res + "import os" + "\n"
        res = res + "os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'" + "\n\n"
        res = res + "import logging" + "\n"
        res = res + "logging.getLogger().setLevel(logging.ERROR)" + "\n\n"
        res = res + "from pyspark.sql.functions import *" + "\n"
        res = res + "from pyspark.ml.feature import Bucketizer" + "\n"
        res = res + "from pyspark.sql.window import Window" + "\n"
        res = res + "import pyspark.sql.functions as F\n"
        res = res + "import pandas as pd" + "\n"
        res = res + "import numpy as np" + "\n"
        res = res + "import boto3" + "\n"
        res = res + "import math" + "\n"
        res = res + "from collections import defaultdict\n"
        res = res + "from datetime import datetime, timedelta\n"
        res = res + "from pyspark.sql.types import *\n\n"
        res = res + "import databricks.koalas as ks" + "\n"
        res = res + "import warnings" + "\n"
        res = res + "warnings.filterwarnings('ignore')\n"
        res = res + "ks.set_option('compute.default_index_type', 'distributed-sequence')" + "\n\n"
        res = res + """def dict_convert_v2(df):
    levels = len(df.columns)-1
    dicts = [{} for i in range(levels)] 
    last_index = None
    for row in df.collect():
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
    return(dicts[-1])""" + "\n\n"
        return (res)


    def read_table(self, analysis_id, db, tbl):
        res = self.dict_convert()
        res = res + "df_" + str(analysis_id) + """ = spark.sql("select * from """
        res = res + str(db) + "." + str(tbl) + " where (code_flag='Rx') or "
        res = res + "(code_flag='Px' and source_value like 'Q%') or (code_flag='Px' "
        res = res + """and source_value like 'J%')").cache()""" + "\n"

        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('start_date', col('start_date')"
        res = res + ".cast(TimestampType()))\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('activity_date', col('activity_date')"
        res = res + ".cast(TimestampType()))\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('age', col('age').cast(IntegerType()))\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('days_supply', col('days_supply')"
        res = res + ".cast(StringType()))\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('quantity', col('quantity')"
        res = res + ".cast(IntegerType()))\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('npi_number', col('npi_number')"
        res = res + ".cast(StringType())).cache()\n\n"

        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".na.fill(value=0,subset="
        res = res + "['quantity', 'age'])" + "\n\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".na.fill(value='',subset="
        res = res + "['days_supply', 'npi_number', 'specialty_desc', 'specialty_code'])" + "\n\n"

        res = res + "try:\n"
        res = res + "\tflag = {}\n"
        res = res + "\tlen_df = len(df_" + str(analysis_id) + ".head(1))" + "\n"
        res = res + "\tif len_df == 0:" + "\n"
        res = res + "\t\t raise Exception" + "\n"
        res = res + "\telse:" + "\n"
        res = res + "\t\tflag['record_count'] = len_df\n"
        res = res + "\t\tprint(flag)" + "\n"
        res = res + "except:" + "\n"
        res = res + "\tflag['record_count'] = 0" + "\n"
        res = res + "\tprint(flag)" + "\n"
        return(res)


    def read_data(self, analysis_id, cohort_id, history_id):
        res = self.dict_convert()
        res = res + "df_" + str(analysis_id) + " = spark.read.parquet('"
        res = res + "s3://{}/{}/cohort_{}_{}_analytics_data').cache()\n\n".format(\
            cs.CB_OUTPUT_BUCKET, cs.CB_OUTPUT_PATH, cohort_id, history_id)

        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('start_date', col('start_date')"
        res = res + ".cast(TimestampType()))\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('activity_date', col('activity_date')"
        res = res + ".cast(TimestampType()))\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('age', col('age').cast(IntegerType()))\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('days_supply', col('days_supply')"
        res = res + ".cast(StringType()))\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('quantity', col('quantity')"
        res = res + ".cast(IntegerType()))\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('npi_number', col('npi_number')"
        res = res + ".cast(StringType())).cache()\n\n"

        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".na.fill(value=0,subset="
        res = res + "['quantity', 'age'])" + "\n\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".na.fill(value='',subset="
        res = res + "['days_supply', 'npi_number', 'specialty_desc', 'specialty_code'])" + "\n\n"
        
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
        res = res + "df_" + str(analysis_id) + " = spark.read.format('com.databricks.spark.csv')"
        res = res + ".option('header', 'true').load('{}').cache()\n".format(path)

        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('start_date', col('start_date')"
        res = res + ".cast(TimestampType()))\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('activity_date', col('activity_date')"
        res = res + ".cast(TimestampType()))\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('age', col('age').cast(IntegerType()))\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('days_supply', col('days_supply')"
        res = res + ".cast(StringType()))\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('quantity', col('quantity')"
        res = res + ".cast(IntegerType()))\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('npi_number', col('npi_number')"
        res = res + ".cast(StringType())).cache()\n\n"

        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".na.fill(value=0,subset="
        res = res + "['quantity', 'age'])" + "\n\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".na.fill(value='',subset="
        res = res + "['days_supply', 'npi_number', 'specialty_desc', 'specialty_code'])" + "\n\n"

        res = res + "try:\n"
        res = res + "\tflag = {}\n"
        res = res + "\tlen_df = len(df_" + str(analysis_id) + ".head(1))" + "\n"
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
        res = res + "df_" + str(analysis_id) + " = spark.read.format('delta')"
        res = res + ".load('{}').cache()\n".format(path)
        return (res)


    def save_to_s3(self, project_id, analysis_id):
        res = ''
        res = res + "df_" + str(analysis_id) + ".drop(*('lot', 'mono_combo'))"
        res = res + ".write.format('delta').option('overwriteSchema', 'true')"
        res = res + ".mode('overwrite').save('s3://{}/prj_".format(cs.S3_PROJECT_PATH)
        res = res + str(project_id) + "/prj_" + str(project_id) + "_" + str(analysis_id) + "')\n"
        return (res)


    def date_range(self, analysis_id):
        res = "date_periods = {'analysis_period':{'start_date':'','end_date':''}"
        res = res + ",'look_forward':{'start_date':'','end_date':''},'look_back'"
        res = res + ":{'start_date':'','end_date':''}}\n\n"
        res = res + "date_periods['analysis_period']['start_date'] = str(df_"
        res = res + str(analysis_id) + ".select(min(col('activity_date')))"
        res = res + ".collect()[0][0])\n\n"
        res = res + "date_periods['analysis_period']['end_date'] = str(df_"
        res = res + str(analysis_id) + ".select(max(col('activity_date')))"
        res = res + ".collect()[0][0])\n\n"
        res = res + "date_periods\n"
        return (res)


    def drop_attribute(self, analysis_id, col_name):
        res = "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".drop('{}')".format(col_name)
        return (res)


    def age_range_attribute(self, analysis_id, splits, labels, col_name):
        splits = [x+1 for x in splits]
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
        res = res + "bucketizer = Bucketizer(splits=splits, inputCol='age', outputCol='split')" + "\n"
        res = res + "with_split = bucketizer.transform(df_" + str(analysis_id) + ")" + "\n"
        res = res + "label_array = array(*(lit(label) for label in labels))" + "\n"
        res = res + "df_" + str(analysis_id) + " = with_split.withColumn('" + str(col_name)
        res = res + "', label_array.getItem(col('split').cast('integer')))" + "\n"
        res = res + self.drop_attribute(str(analysis_id), 'split') + "\n"
        return (res)


    def gender_attribute(self, analysis_id, map_lst, dim_lst, col_name):
        res = "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('{}', ".format(col_name)
        if (len(dim_lst) >= 1):
            for i in range(len(dim_lst)):
                res = res + "when(col('gender') == '" + str(map_lst[i]) + "', '" + str(dim_lst[i]) + "')."
            res = res + "otherwise(col('gender')))\n"
            return (res)
        else:
            return ("Invalid Gender Mappings")


    def generate_drug_class(self, analysis_id, drug_class_lst, drug_class_codes, col_name):
        res = "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('{}', ".format(col_name)
        if (len(drug_class_lst) >= 1):
            for i in range(len(drug_class_lst)):
                res = res + "when(col('source_value').isin(" + str(drug_class_codes[i]) + "), '" + str(
                    drug_class_lst[i]) + "')."
            res = res + "otherwise('other_drug_class'))"
            return (res)
        else:
            return ("Invalid Drug Class Mappings")


    def generate_provider_specialty(self, analysis_id, provider_specialty_lst,
                                    provider_specialty_codes, col_name):

        res = "df_" + str(analysis_id) + " = df_" + str(analysis_id) + ".withColumn('{}', ".format(col_name)
        if (len(provider_specialty_lst) >= 1):
            for i in range(len(provider_specialty_lst)):
                res = res + "when(col('specialty_code').isin(" + str(provider_specialty_codes[i]) + "), '" + str(
                    provider_specialty_lst[i]) + "')."
            res = res + "otherwise('other_specialty'))"
            return (res)
        else:
            return ("Invalid Provider Specialty Mappings")


    def get_data(self, *argv):
        res = "[row.asDict() for row in df.groupBy(" + str(
            list(argv)) + ").agg(countDistinct('patient_id').alias('count')).collect()]\n"
        return (res)


    def cross_tab(self, *argv):
        res = "dict(df.groupBy(" + str(
            argv) + ").agg(countDistinct('patient_id').alias('count')).rdd.groupBy(lambda x:x[0]).map(lambda x:(x[0],{y[1]:y[2] for y in x[1]})).collect())\n"
        return (res)

    def range_filter(self, analysis_id, start_date, end_date):
        res = "mask = (col('activity_date') >= '"
        res = res + str(start_date) + "') & (col" 
        res = res + "('activity_date') <= '" + str(end_date) + "')\n"
        return res


    def get_attribute_data(self, analysis_id, attribute_list, start_date, end_date):
        res = "ndf = df_" + str(analysis_id) + ".filter(col('activity_date')"
        res = res + ".between('" + str(start_date) + "', '" + str(end_date)
        res = res + "')).groupBy(" + str(attribute_list) + ").agg(countDistinct"
        res = res + "('patient_id').alias('count')).orderBy(" + str(attribute_list)
        res = res + ").na.fill('others')\n" + "dict_convert_v2(ndf)\n"
        return (res)


    def analysis_summary(self, analysis_id, attribute_list, start_date, end_date):
        res = ''
        col_list = []
        res = res + "summary = dict()\n"
        res = res + self.range_filter(analysis_id, start_date, end_date)
        res = res + "df = df_" + str(analysis_id) + ".filter(mask)" + "\n"
        res = res + "summary['Treated'] = df.select('patient_id').distinct().count()\n"
        attribute_df = []
        if len(attribute_list) > 1:
            for i in range(len(attribute_list)):
                res = res + "ndf" + str(i + 1) + "= df.groupBy("
                res = res + str(attribute_list[0:i + 1]) + ")"
                res = res + ".agg(countDistinct(col('patient_id')).alias('count" + str(i+1) + "'))" + "\n\n"
                attribute_df.append("ndf" + str(i + 1))
                col_list.extend([attribute_list[0:i + 1][-1], "count" + str(i+1)])
            res = res + "ndf = ndf1"
            for j in range(len(attribute_df) - 1):
                res = res + ".join(" + str(attribute_df[j + 1])
                res = res + ", on=" + str(attribute_list[0:j + 1]) + ", how='left')"
            res = res + ".select(" + str(col_list) + ")" + "\n\n"
        else:
            res = res + "ndf = df.groupBy(" + str(attribute_list) + ")"
            res = res + ".agg(countDistinct(col('patient_id')).alias('count'))" + "\n\n"
        
        res = res + "summary['children'] = dict_convert_v2(ndf)\n\n"
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
        res = res + self.age_range_attribute(analysis_id, [10, 20, 30, 40, 50, 60],
                                             ['0-10', '11-20', '21-30', '31-40', '41-50', '51-60', '60+'],
                                             "default_age_range")

        res = res + "ndf = df_" + str(analysis_id) + ".filter(col('activity_date')"
        res = res + ".between('" + str(start_date) + "', '" + str(end_date)
        res = res + "')).groupBy(['gender', 'default_age_range']).agg(countDistinct"
        res = res + "('patient_id').alias('count')).orderBy(['gender', "
        res = res + "'default_age_range']).na.fill('Undefined').cache()\n"

        res = res + "adf = ndf.groupby(['default_age_range'])"
        res = res + ".agg(sum('count').alias('count'))\n"
        res = res + "sum_dict['age'] = dict_convert_v2(adf)\n\n"

        res = res + "d1 = {i:0 for i in labels}" + "\n\n"
        res = res + "sum_dict['age'] = {**d1, **sum_dict['age']}" + "\n\n"

        res = res + "gdf = ndf.groupby(['gender'])"
        res = res + ".agg(sum('count').alias('count'))\n"
        res = res + "sum_dict['gender'] = dict_convert_v2(gdf)\n\n"

        res = res + "sum_dict['ndc_codes'] = list(set([row."
        res = res + "source_value for row in df_" + str(analysis_id)
        res = res + ".select('source_value').collect()]))\n\n"

        res = res + "sum_dict['speciality'] = list(set([row."
        res = res + "specialty_code for row in df_" + str(analysis_id)
        res = res + ".select('specialty_code').collect()]))\n\n"

        res = res + "specialties = [{row.specialty_code:row.specialty_desc} "
        res = res + "for row in df_" + str(analysis_id) + ".select('specialty_code', "
        res = res + "'specialty_desc').collect() if row.specialty_code ]\n"

        res = res + "sum_dict['specialty_list'] = list({frozenset(item.items()):item "
        res = res + "for item in specialties}.values())\n\n"

        res = res + "sum_dict['Total'] = ndf.agg(sum('count').alias('count')).collect()[0]['count']\n\n"

        res = res + self.drop_attribute(str(analysis_id), 'default_age_range') + "\n\n"
        res = res + "sum_dict"
        return (res)


class LOT:
    """
    Class contains all the functions related to LOT Attribute
    """

    def __init__(self):
        """ Initialize all class level variables """
        pass


    def get_record_count(self, analysis_id):
        res = ''
        res = res + "if not 'rc_" + str(analysis_id) + "' in globals():" + "\n"
        res = res + "\trc_" + str(analysis_id) + " = df_" + str(analysis_id) + ".count()" + "\n\n"
        res = res + "print(rc_" + str(analysis_id) + ")"
        return res


    def convert_to_pandas(self, analysis_id):
        res = ''
        res = res + "df_" + str(analysis_id) + "_pd = df_"
        res = res + str(analysis_id) + ".toPandas()" + "\n\n"
        return res


    def get_days_supply(self, data):
        if (data['Reg/Line']['OW_Period'] == ''):
            return ("days_of_supply = row['days_of_supply']\n")
        else:
            return ('')

    def create_episode_regimen(self, analysis_id, data, drug_col, col_name, start_date, end_date):
        res = ''
        res = res + "sc.setCheckpointDir('s3://{0}/checkpoints/{1}')".format(str(cs.S3_PROJECT_PATH), str(drug_col)) + "\n\n"
        res = res + PatientFlow().range_filter(analysis_id, start_date, end_date)
        res = res + "data_" + str(analysis_id) + " = df_" + str(analysis_id)
        res = res + ".filter(mask).drop(*('" + str(col_name) + "', "
        res = res + "'index_date', 'regimen', 'case_flag'))" + "\n\n"

        res = res + "data_" + str(analysis_id) + " = data_" + str(
            analysis_id) + ".withColumn('end_date', date_add(col('activity_date'), "
        res = res + str(data['Grace_Period']['Drop_Off_Period']) + "))\n"

        res = res + "days = lambda i: i * 86400" + "\n"

        res = res + "w1 = (Window().partitionBy(col('patient_id')).orderBy(col('activity_date')"
        res = res + ".cast('timestamp').cast('long')).rangeBetween("
        if (data['Grace_Period'].get('Drop_Off_Period') == str(0)): 
            res = res + str(1)
        else:
            res = res + str(0)
        res = res + ", days(" + str(data['Grace_Period']['Drop_Off_Period']) + ")))" + "\n\n"

        res = res + "data_" + str(analysis_id) + " = data_" + str(analysis_id) + ".withColumn"
        res = res + "('drop_period_regimen', array_sort(array_distinct(collect_list('" 
        res = res + str(drug_col) + "').over(w1))))" + "\n\n"

        res = res + "sameDaydrugs_maxDayssupply = data_" + str(analysis_id)
        res = res + ".groupBy(['patient_id', 'activity_date']).agg(collect_set('"
        res = res + str(drug_col) + "').alias('same_day_drugs'), max('days_supply')"
        res = res + ".alias('days_of_supply'))" + "\n\n"

        res = res + "data_" + str(analysis_id) + " = data_" + str(analysis_id)
        res = res + ".join(sameDaydrugs_maxDayssupply, on=['patient_id',"
        res = res + "'activity_date'], how='left')" + "\n\n"

        res = res + "data_" + str(analysis_id) + " = data_" + str(analysis_id)
        res = res + ".withColumn('same_drugs', split(col('" + str(drug_col)
        res = res + "'), ','))" + "\n\n"

        res = res + "data_" + str(analysis_id) + " = data_" + str(analysis_id)
        res = res + ".orderBy(['patient_id', 'activity_date', col('" + str(drug_col) 
        res = res + "').desc()]).checkpoint()" + "\n\n"

        res = res + "data_" + str(analysis_id) + " = ks.DataFrame(data_"
        res = res + str(analysis_id) + ").sort_values(by=['patient_id', 'activity_date', '" + str(drug_col)
        res = res + "'], ascending=[True, True, False])\n\n"
        
        res = res + "first_row = data_" + str(analysis_id) + ".head(1)" + "\n"
        res = res + "prev_regimen = first_row['same_drugs'].to_numpy()[0]" + "\n"
        res = res + "episode_date = first_row['activity_date']"
        res = res + ".to_numpy().astype('datetime64[D]')[0].astype('M8[ms]').astype('O')" + "\n"
        res = res + "prev_pid = first_row['patient_id'].to_numpy()[0]" + "\n"
        res = res + "episode_dates = []\n"

        if (len(data['Reg/Line']['Exception_List']) != 0):
            res = res + "drugs_lst_ = " + str([i["Drug_Class_Values"] for i in data['Reg/Line']['Exception_List']]) + "\n\n"

        res = res + "for i, row in data_" + str(analysis_id) + ".iterrows():" + "\n"

        if (data['Reg/Line']['OW_Period'] == ''):
            res = res + "\twindow_opt = (row['activity_date']-episode_date)"
            res = res + ".days <= days_of_supply" + "\n"
        else:
            res = res + "\twindow_opt = (row['activity_date']-episode_date)"
            res = res + ".days <= " + str(data['Reg/Line']['OW_Period']) + "\n"
        res = res + "\tdrugNotInRegimen = not set(row['same_day_drugs']).issubset(set(prev_regimen))" + "\n"
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
        res = res + "\t\telif window_opt==False and drugNotInRegimen==True:" + "\n"
        res = res + "\t\t\tprev_regimen = row['same_drugs']" + "\n"
        res = res + "\t\t\tepisode_date = row['activity_date']" + "\n"
        res = res + "\telse:" + "\n"
        res = res + "\t\tprev_regimen = row['same_drugs']" + "\n"
        res = res + "\t\tepisode_date = row['activity_date']" + "\n"
        res = res + "\tepisode_dates.append(episode_date)" + "\n"
        res = res + "\tprev_pid = row['patient_id']" + "\n"

        res = res + "data_" + str(analysis_id) + "['episode_date'] = episode_dates" + "\n\n"

        res = res + "del(episode_dates)" + "\n\n"
        res = res + "df_regimens = data_{0}.groupby(['patient_id','episode_date'])".format(analysis_id)
        res = res + "['{0}'].unique().reset_index().rename(columns={{'{0}':'ep_regimen'}})".format(drug_col) + "\n\n"
        res = res + "data_{0} = ks.merge(data_{0}, df_regimens, on=['patient_id', 'episode_date'], how='left')".format(analysis_id)
        res = res + ".sort_values(by=['patient_id','activity_date'])" + "\n\n"

        res = res + "df_regimens = data_{0}.groupby(['patient_id','episode_date'])".format(str(analysis_id))
        res = res + ".agg({'drop_period_regimen':'first', 'ep_regimen':'last'}).reset_index()" + "\n"
        res = res + "df_regimens['episode_regimen'] = df_regimens['ep_regimen']" + "\n"
        res = res + "data_{0} = data_{0}.rename(columns={{'ep_regimen':'old_regimen', ".format(str(analysis_id))
        res = res + "'drop_period_regimen':'old_drop_period_regimen'})" + "\n"
        res = res + "data_{0} = ks.merge(data_{0}, df_regimens, on=['patient_id','episode_date'], how='left')".format(str(analysis_id)) + "\n"
        res = res + "data_{0} = data_{0}.drop(['ep_regimen', 'old_regimen', 'old_drop_period_regimen'], axis = 1)".format(str(analysis_id))
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
                                                                                'Drug_Class_Values']) + "\n\n"
            return res
        return res

    def data_prep(self, analysis_id, data, drug_col):
        res = ''
        res = res + "data_" + str(analysis_id) + " = data_"
        res = res + str(analysis_id) + ".sort_values(by=['patient_id', "
        res = res + "'activity_date', '" + str(drug_col) + "'], ascending=[True, True, False])\n\n"

        res = res + "first_row = data_" + str(analysis_id) + ".head(1)" + "\n"
        res = res + "prev_regimen = first_row['same_drugs'].to_numpy()[0]" + "\n"
        res = res + "index_date = first_row['activity_date']"
        res = res + ".to_numpy().astype('datetime64[D]')[0].astype('M8[ms]').astype('O')" + "\n"
        res = res + "prev_pid = first_row['patient_id'].to_numpy()[0]\n"
        res = res + "prev_date = first_row['activity_date'].to_numpy()"
        res = res + ".astype('datetime64[D]')[0].astype('M8[ms]').astype('O')\n"
        res = res + "lot = 1\n"
        res = res + "case_flag = ''" + "\n"
        res = res + "lot_lst = []" + "\n"
        res = res + "regimen_lst = []" + "\n"
        res = res + "index_date_lst = []" + "\n"
        res = res + "case_flag_lst = []" + "\n"
        res = res + "index_lst = []" + "\n"

        if (data['Reg/Line']['OW_Period'] == ''):
            res = res + "days_of_supply = data_" + str(analysis_id) + ".head(1)['days_of_supply'].to_numpy()[0]" + "\n"

        res = res + self.line_except(analysis_id, data, drug_col)

        res = res + self.add_sub_except(analysis_id, data, drug_col)

        res = res + "for i, row in data_" + str(analysis_id) + ".iterrows():" + "\n"

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
                res = res + "\t\t\tdays_of_supply = ['days_of_supply']\n"

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
            res = res + "\tdrop_off_2 = set(row['episode_regimen'])"
            res = res + ".issubset(set(prev_regimen))" + "\n\n"
            res = res + "\tdrop_off_3 = set(prev_regimen).issubset(set("
            res = res + "row['drop_period_regimen']))" + "\n\n"
            return res

    def create_regimen(self, analysis_id, data, col_name, drug_col, start_date, end_date):
        res = self.create_episode_regimen(analysis_id, data, drug_col, col_name, start_date, end_date) + "\n\n"
        res = res + self.data_prep(analysis_id, data, drug_col)
        if (data['Reg/Line']['OW_Period'] == ''):
            res = res + "\twindow_opt = ((row['activity_date']-index_date)"
            res = res + ".days <= days_of_supply" + "\n\n"
        else:
            res = res + "\twindow_opt = (row['activity_date']-index_date).days <= "
            res = res + str(data['Reg/Line']['OW_Period']) + "\n\n"

        res = res + "\tdrugNotInRegimen = set(row['same_day_drugs'])"
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
                res = res + " - index_date).days <= "
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
        res = res + "\t\t\tcase_flag = 'First_Line_Started'\n"

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
        
        res = res + "\tlot_lst.append(lot)" + "\n"
        res = res + "\tregimen_lst.append(sorted(prev_regimen))" + "\n"
        res = res + "\tindex_date_lst.append(index_date)" + "\n"
        res = res + "\tcase_flag_lst.append(case_flag)" + "\n"
        res = res + "\tindex_lst.append(i)" + "\n"
        res = res + "\tprev_pid = row['patient_id']\n"
        res = res + "\tprev_date = row['activity_date']\n\n"


        res = res + "data_" + str(analysis_id) + " = data_" + str(analysis_id)
        res = res + ".sort_values(by=['patient_id', 'activity_date', '" + str(drug_col)
        res = res + "'], ascending=[True, True, False])\n\n"

        res = res + "temp_data = ks.DataFrame({'" + str(col_name) + "':lot_lst, "
        res = res + "'index_date':index_date_lst, 'regimen':regimen_lst, 'case_flag':case_flag_lst}, "
        res = res + "columns = ['" + str(col_name) + "', 'index_date', 'regimen', 'case_flag'], index=index_lst)" + "\n"

        res = res + "data_" + str(analysis_id) + " = data_" + str(analysis_id)
        res = res + ".merge(temp_data, left_index=True, right_index=True, how='left')" + "\n\n"

        res = res + "del(temp_data)" + "\n\n"

        res = res + "del(lot_lst)" + "\n"
        res = res + "del(index_date_lst)" + "\n"
        res = res + "del(regimen_lst)" + "\n"
        res = res + "del(case_flag_lst)" + "\n\n"

        res = res + "data_" + str(analysis_id) + " = data_" + str(analysis_id)
        res = res + ".to_spark()" + "\n\n"

        res = res + "win = Window().partitionBy(['patient_id','"
        res = res + str(col_name) + "','index_date']).orderBy(size(col('regimen'))"
        res = res + ".desc())" + "\n\n"

        res = res + "df_regimen = data_" + str(analysis_id) + ".withColumn('rn', row_number()"
        res = res + ".over(win)).filter(col('rn') == lit(1))" + "\n\n"

        res = res + "df_regimen = df_regimen.select('patient_id','"
        res = res + str(col_name) + "','index_date', concat_ws(',', 'regimen')"
        res = res + ".alias('regimen'))" + "\n\n"
        
        res = res + "data_" + str(analysis_id) + " = data_" + str(analysis_id)
        res = res + ".drop(*('regimen', 'rn'))" + "\n\n"
        res = res + "data_" + str(analysis_id) + " = data_" + str(analysis_id)
        res = res + ".join(df_regimen, on=['patient_id','" + str(col_name) + "','index_date'], how='left')" + "\n\n"
        
        res = res + "data_" + str(analysis_id) + " = data_" + str(
            analysis_id) + ".drop(*('same_day_drugs', 'same_drugs',"
        res = res + " 'drop_period_regimen', 'episode_date', "
        res = res + "'episode_regimen', 'end_date', 'days_of_supply'))\n\n"

        res = res + "df_columns = list(df_" + str(analysis_id) + ".columns)" + "\n\n"
        res = res + "unwanted_cols = {'" + str(col_name) + "', 'regimen', "
        res = res + "'index_date', 'case_flag'}" + "\n\n"
        res = res + "df_columns = [ele for ele in df_columns if ele not in "
        res = res + "unwanted_cols]" + "\n\n"

        res = res + "data_columns = list(data_" + str(analysis_id) + ".columns)" + "\n\n"

        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id)
        res = res + ".select(df_columns).fillna(np.nan).join(data_" + str(analysis_id)
        res = res + ".select(data_columns).fillna(np.nan), on=df_columns, how='left').cache()" + "\n\n"

        res = res + "del(data_" + str(analysis_id) + ")" + "\n\n"
        return (res)


    def patient_share_graph(self, analysis_id, col_name, start_date, end_date, lot_dict={}):
        res = ''
        res = res + "######################## Start of Patient Share Across Line of Therapy ########################" + "\n"
        
        res = res + "df = df_main.groupBy(['"
        res = res + str(col_name) + "']).agg(countDistinct('patient_id')"
        res = res + ".alias('total_p_count'))\n\n"
        res = res + "df_max = df.agg(F.max(col('total_p_count'))).first()[0]"+ "\n\n"
        res = res + "df = df.withColumn('total_percentage', (col('total_p_count')"
        res = res + "/df_max)*100)" + "\n\n"
        res = res + "######################## End of Patient Share Across Line of Therapy ########################" + "\n\n"
        return res


    def patient_share_table(self, analysis_id, col_name, start_date, end_date, lot_dict={}):
        res = ''
        res = res + "######################## Start of Patient Split Within Line of Therapy ########################" + "\n"
    
        res = res + "df_total = df_main.groupBy('" + str(col_name)
        res = res + "').agg(countDistinct('patient_id').alias('total_patients'))" + "\n\n"

        res = res + "df_cont = df_main.filter(col('min_activity_date') "
        res = res + "< '" + str(start_date) + "').groupBy('" + str(col_name) + "')"
        res = res + ".agg(countDistinct('patient_id').alias('Continued_patients'))" + "\n\n"

        res = res + "df_new = df_main.filter((col('min_activity_date') "
        res = res + ">= '" + str(start_date) + "'))"# & (col('min_activity_date') <= '" + str(end_date) + "'))"
        res = res + ".groupBy('" + str(col_name) + "').agg(countDistinct('patient_id')"
        res = res + ".alias('New_patients'))" + "\n\n"

        res = res + "df = df_total.join(df_cont, on = ['" + str(col_name) + "'], how = 'left')"
        res = res + ".join(df_new, on = ['" + str(col_name) + "'], how = 'left')" + "\n\n"

        res = res + "win = Window().partitionBy().orderBy(col('" + str(col_name) + "'))\n\n"
        res = res + "df_a = df.withColumn('Progressed', lead(col('total_patients'), 1)"
        res = res + ".over(win))" + "\n\n"

        res = res + "df_drop = df_main.withColumn('end_date', "
        res = res + "date_add(to_timestamp(lit('" + str(end_date) + "'),'yyyy-MM-dd HH:mm:ss'), "
        res = res + "-90)).withColumn('row', row_number().over(Window().partitionBy(col('patient_id'))"
        res = res + ".orderBy(col('activity_date').desc()))).filter(col('row') == 1)"
        res = res + ".filter(col('max_activity_date') < col('end_date'))"
        res = res + ".groupBy('lot').count().withColumnRenamed('count', 'Dropped_patients')" + "\n\n"

        res = res + "df = df_a.join(df_drop, on = ['" + str(col_name) + "'], how = 'left')"
        res = res + ".sort(col('lot')).fillna(0)" + "\n\n"

        res = res + "df = df.withColumn('percentage_continued', (df"
        res = res + ".Continued_patients/df.total_patients)*100)" + "\n\n"

        res = res + "df = df.withColumn('percentage_New', (df."
        res = res + "New_patients/df.total_patients)*100)" + "\n\n"

        res = res + "df = df.withColumn('percentage_Progressed', (df."
        res = res + "Progressed/df.total_patients)*100)" + "\n\n"

        res = res + "df = df.withColumn('percentage_dropoff', (df."
        res = res + "Dropped_patients/df.total_patients)*100)" + "\n\n"

        res = res + "table = []" + "\n\n"
        res = res + "for row in df.collect():" + "\n\n"
        res = res + "\td_dict = dict()" + "\n"
        res = res + "\tc_dict = dict()" + "\n"
        res = res + "\tn_dict = dict()" + "\n"
        res = res + "\tdr_dict = dict()" + "\n"
        res = res + "\tp_dict = dict()" + "\n"
        res = res + "\td_dict['Line'] = row['" + str(col_name) + "']\n"
        res = res + "\td_dict['Total'] = row['total_patients']" + "\n"
        res = res + "\tc_dict['Person_count'] = row['Continued_patients']" + "\n"
        res = res + "\tc_dict['Percentage'] = row['percentage_continued']" + "\n"
        res = res + "\td_dict['Continued'] = c_dict" + "\n"
        res = res + "\tn_dict['Person_count'] = row['New_patients']" + "\n"
        res = res + "\tn_dict['Percentage'] = row['percentage_New']" + "\n"
        res = res + "\td_dict['New'] = n_dict" + "\n"
        res = res + "\tdr_dict['Person_count'] = row['Dropped_patients']" + "\n"
        res = res + "\tdr_dict['Percentage'] = row['percentage_dropoff']" + "\n"
        res = res + "\td_dict['DroppedOff'] = dr_dict" + "\n"
        res = res + "\tp_dict['Person_count'] = row['Progressed']" + "\n"
        res = res + "\tp_dict['Percentage'] = row['percentage_Progressed']" + "\n"
        res = res + "\td_dict['Progressed'] = p_dict" + "\n"
        res = res + "\ttable.append(d_dict)" + "\n"
        return (res)


    def patient_share(self, analysis_id, col_name, start_date, end_date, lot_dict={}):
        res = ''
        res = res + "######################## Start of Patient Share Across and Patient Split Within Line of Therapy ########################" + "\n"
        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id)
        res = res + ".withColumn('lot', when(col('"
        res = res + str(col_name) + "')==1, lit(1)).when(col('" 
        res = res + str(col_name) + "')==2, lit(2)).when(col('" 
        res = res + str(col_name) + "')==3, lit(3)).when(col('"
        res = res + str(col_name) + "')==4, lit(4)).otherwise(lit(5))).cache()" + "\n\n"
        
        col_name = 'lot'

        res = res + PatientFlow().range_filter(analysis_id, start_date, end_date)

        res = res + "patient_ids = df_" + str(analysis_id) + ".filter(mask)"
        if bool(lot_dict) == True:
            res = res + ".filter("
            for key, val in lot_dict.items():
                res = res + "(col('" + str(key) + "') == '" + str(val) + "') & "
            result = list(res)
            del result[-3:-1]
            result.insert(-1, ")")
            res = ''.join(result)
        res = res + ".select('patient_id')" + "\n\n"
        res = res + "df_main = df_" + str(analysis_id) + ".join(patient_ids, "
        res = res + "on = ['patient_id'], how='leftsemi').select(df_"
        res = res + str(analysis_id) + "['*']).cache()" + "\n\n"

        res = res + "df = df_main.groupBy('patient_id').agg(min('activity_date')"
        res = res + ".alias('min_activity_date'),max('activity_date').alias('max_activity_date'))" + "\n\n"

        res = res + "df_main = df_main.join(df, on =['patient_id'], how = 'left')" + "\n\n"

        res = res + "result = dict()\n"
        res = res + self. patient_share_graph(analysis_id, col_name, start_date, end_date, lot_dict={})
        res = res + "result['graph'] = dict_convert_v2(df.orderBy('lot'))\n\n" 

        res = res + self.patient_share_table(analysis_id, col_name, start_date, end_date, lot_dict={})
        res = res + "result['table'] = table\n\n"

        res = res + "result"
        return res


    def regimen_reduction_graphs(self, col_name):
        res = "df1 = df.groupBy(['" + str(col_name) + "', 'regimen'])"
        res = res + ".agg(countDistinct('patient_id').alias('patient_count'), F"
        res = res + ".sum('days_on_therapy').alias('total_dot'))\n\n"

        res = res + "################### Stack chart ########################\n\n"

        res = res + "df1 = df1.groupBy(['" + str(col_name) + "', 'regimen'])"
        res = res + ".agg(F.sum('patient_count').alias('patient_count1'), F.sum(col('"
        res = res + "total_dot')).alias('total_dot_lot_reg1'))\n\n"

        res = res + "df1 = df1.withColumn('row_number', row_number()"
        res = res + ".over(Window.partitionBy('lot').orderBy(desc('patient_count1'))))" + "\n"
        res = res + "df1 = df1.withColumn('category', when(col('row_number') <=12, "
        res = res + "col('row_number')).otherwise(lit('Others')))" + "\n"
        res = res + "df1_new = df1.groupby(['lot', 'category']).agg(sum('patient_count1')"
        res = res + ".alias('patient_count'), sum('total_dot_lot_reg1').alias('total_dot_lot_reg'))" + "\n"
        res = res + "df1 = df1.withColumn('count_', row_number().over(Window.partitionBy('lot', 'category')"
        res = res + ".orderBy(desc('patient_count1'))))" + "\n"
        res = res + "df1 = df1.filter(col('count_') == 1)" + "\n"
        res = res + "df1 = df1.join(df1_new, on=['lot', 'category'], how = 'left')" + "\n"
        res = res + "df1 = df1.withColumn('regimen', when(col('category') == 'Others', lit('Others'))"
        res = res + ".otherwise(col('regimen')))" + "\n"
        res = res + "df1 = df1.drop('patient_count1', 'total_dot_lot_reg1', 'row_num', 'category', 'count_')" + "\n\n"

        res = res + "df1 = df1.withColumn('lot_reg_avg_days', col('total_dot_lot_reg')/"
        res = res + "col('patient_count'))\n\n"

        res = res + "df1_average = df1.groupBy('" + str(col_name) + "').agg(F"
        res = res + ".sum('patient_count').alias('total_p_count'))\n\n"

        res = res + "df1 = df1.join(df1_average , on = ['" + str(col_name)
        res = res + "'], how = 'left')" + "\n\n"

        res = res + "df1 = df1.withColumn('percentage', (col('patient_count')/"
        res = res + "col('total_p_count'))*100)" + "\n\n"

        res = res + "df1 = df1.withColumn('col2', concat(col('patient_count'), "
        res = res + "lit(','), col('lot_reg_avg_days'),lit(','), col('percentage')))" + "\n\n"

        res = res + "final_dict = dict()\n\n"

        res = res + "final_dict['Stack'] = dict_convert_v2(df1.select('" 
        res = res + str(col_name) + "','regimen', 'col2').orderBy('lot'))\n\n"

        res = res + "final_dict\n"
        return (res)

    def deep_dive_lot_regimen(self, analysis_id, col_name, start_date, end_date, *argv): #line_number=None, regimen=None, lot_dict={}):
        res = ''
        res = res + "if 'lot' not in df_" + str(analysis_id) + ".columns:\n"
        res = res + "\tdf_" + str(analysis_id) + " = df_" + str(analysis_id)
        res = res + ".withColumn('lot', when(col('"
        res = res + str(col_name) + "')==1, lit(1)).when(col('" 
        res = res + str(col_name) + "')==2, lit(2)).when(col('" 
        res = res + str(col_name) + "')==3, lit(3)).when(col('"
        res = res + str(col_name) + "')==4, lit(4)).otherwise(lit(5)))" + "\n\n"
        
        col_name = 'lot'

        res = res + PatientFlow().range_filter(analysis_id, start_date, end_date)

        if (argv[0] != None and argv[1] != None):
            res = res + "patient_ids = df_" + str(analysis_id) 
            
            if bool(argv[2]) == True:
                res = res + ".filter("
                for key, val in argv[2].items():
                    res = res + "(col('" + str(key) + "') == '" + str(val) + "') & "
                result = list(res)
                del result[-3:-1]
                result.insert(-1, ")")
                res = ''.join(result)
            
            res = res + ".filter((col('regimen') == '" + str(argv[1])
            res = res + "') & (col('" + str(col_name) 
            res = res + "') == " + str(argv[0]) + "))"
            res = res + ".select('patient_id')" + "\n\n"#.toPandas()['patient_id']))\n\n"

            res = res + "df = df_" + str(analysis_id) + ".filter(mask).join(patient_ids, "
            res = res + "on = ['patient_id'], how='leftsemi').select(df_"
            res = res + str(analysis_id) + "['*'])"

            res = res + ".groupby(['patient_id', "
            res = res + "'regimen', '" + str(col_name) + "'])"
            res = res + ".agg(datediff(max('activity_date'), min('index_date'))"
            res = res + ".alias('days_on_therapy')).cache()\n\n"

            res = res + "df_selected_line = df.filter((col('regimen') == '"
            res = res + str(argv[1]) + "') & (col('lot') == " + str(argv[0]) + "))" + "\n\n"
            res = res + "df_remaining = df.filter(col('" + str(col_name) + "') != " + str(argv[0]) + ")" + "\n\n"
            res = res + "df = df_selected_line.union(df_remaining).distinct()" + "\n\n"
        
        else:

            res = res + "patient_ids = df_" + str(analysis_id)
            res = res + ".filter(mask)"

            if bool(argv[2]) == True:
                res = res + ".filter("
                for key, val in argv[2].items():
                    res = res + "(col('" + str(key) + "') == '" + str(val) + "') & "
                result = list(res)
                del result[-3:-1]
                result.insert(-1, ")")
                res = ''.join(result)
            
            res = res + ".select('patient_id')" + "\n\n"

            res = res + "df = df_" + str(analysis_id) + ".join(patient_ids, "
            res = res + "on = ['patient_id'], how='leftsemi').select(df_"
            res = res + str(analysis_id) + "['*'])" + "\n\n"

            res = res + "df = df.groupBy(['patient_id', 'regimen', '" + str(col_name) + "'])"
            res = res + ".agg(datediff(max('activity_date'), min('index_date'))"
            res = res + ".alias('days_on_therapy'))\n\n"

        res = res + self.regimen_reduction_graphs(col_name)
        return (res)


    def regimen_combomono_share(self, analysis_id, col_name, start_date, end_date, lot_dict={}):
        res = ''
        res = res + "if 'lot' not in df_" + str(analysis_id) + ".columns:\n"
        res = res + "\tdf_" + str(analysis_id) + " = df_" + str(analysis_id)
        res = res + ".withColumn('lot', when(col('"
        res = res + str(col_name) + "')==1, lit(1)).when(col('" 
        res = res + str(col_name) + "')==2, lit(2)).when(col('" 
        res = res + str(col_name) + "')==3, lit(3)).when(col('"
        res = res + str(col_name) + "')==4, lit(4)).otherwise(lit(5)))" + "\n\n"
        
        res = res + PatientFlow().range_filter(analysis_id, start_date, end_date)
        col_name = 'lot'

        res = res + "df_" + str(analysis_id) + " = df_" + str(analysis_id)
        res = res + ".filter(mask).withColumn('mono_combo', when(size(F"
        res = res + ".split(col('regimen'), ',')) == 1, lit('mono'))"
        res = res + ".otherwise(lit('combo'))).cache()" + "\n\n"

        res = res + "patient_ids = df_" + str(analysis_id) + ".filter(mask)"
        if bool(lot_dict) == True:
            res = res + ".filter("
            for key, val in lot_dict.items():
                res = res + "(col('" + str(key) + "') == '" + str(val) + "') & "
            result = list(res)
            del result[-3:-1]
            result.insert(-1, ")")
            res = ''.join(result)

        res = res + ".select('patient_id')" + "\n\n"

        res = res + "df = df_" + str(analysis_id) + ".join(patient_ids, "
        res = res + "on = ['patient_id'], how='leftsemi').select(df_"
        res = res + str(analysis_id) + "['*'])" + "\n\n"

        res = res + "df = df.filter(mask)" + "\n\n"
        res = res + "graph_combo = df.groupby(['"
        res = res + str(col_name) + "', 'mono_combo']).agg(countDistinct('patient_id')"
        res = res + ".alias('patient_share_count'))" + "\n\n"
        res = res + "graph_p_count = graph_combo.groupby(['" + str(col_name) + "'])"
        res = res + ".agg(sum('patient_share_count').alias('total_p_count'))" + "\n\n"
        res = res + "df = graph_combo.join(graph_p_count, on=['" + str(col_name) 
        res = res + "'], how='left')\n\n"
        res = res + "df = df.withColumn('patient_share_percentage', (col('patient_share_count')"
        res = res + "/col('total_p_count'))*100)" + "\n\n"
        res = res + "df = df.withColumn('col1',  concat(col('patient_share_count'), lit(', ')"
        res = res + ", col('patient_share_percentage')))" + "\n\n"
        res = res + "df = df.drop('patient_share_count', 'total_p_count', "
        res = res + "'patient_share_percentage')\n\n"
        res = res + "dict_convert_v2(df.orderBy('lot'))\n\n"
        return res

    def regimen_drugs_share(self, analysis_id, col_name, start_date, end_date, line_number, *argv):# lot_dict={}, combomono='mono'):
        res = ''
        res = res + "if 'lot' not in df_" + str(analysis_id) + ".columns:\n"
        res = res + "\tdf_" + str(analysis_id) + " = df_" + str(analysis_id)
        res = res + ".withColumn('lot', when(col('"
        res = res + str(col_name) + "')==1, lit(1)).when(col('" 
        res = res + str(col_name) + "')==2, lit(2)).when(col('" 
        res = res + str(col_name) + "')==3, lit(3)).when(col('"
        res = res + str(col_name) + "')==4, lit(4)).otherwise(lit(5)))" + "\n\n"
        
        res = res + PatientFlow().range_filter(analysis_id, start_date, end_date)
        
        col_name = 'lot'

        res = res + "if 'mono_combo' not in df_" + str(analysis_id) + ".columns:\n"
        res = res + "\tdf_" + str(analysis_id) + " = df_" + str(analysis_id)
        res = res + ".filter(mask).withColumn('mono_combo', when(size(F"
        res = res + ".split(col('regimen'), ',')) == 1, lit('mono'))"
        res = res + ".otherwise(lit('combo')))" + "\n\n"

        res = res + "patient_ids = df_" + str(analysis_id) + ".filter(mask)"
        if bool(argv[0]) == True:
            res = res + ".filter("
            for key, val in argv[0].items():
                res = res + "(col('" + str(key) + "') == '" + str(val) + "') & "
            result = list(res)
            del result[-3:-1]
            result.insert(-1, ")")
            res = ''.join(result)

        res = res + ".select('patient_id')" + "\n\n"

        res = res + "df = df_" + str(analysis_id) + ".join(patient_ids, "
        res = res + "on = ['patient_id'], how='leftsemi').select(df_"
        res = res + str(analysis_id) + "['*'])" + "\n\n"

        res = res + "df = df.filter(col('" + str(col_name)
        res = res + "') == " + str(line_number) + ").filter(col('mono_combo') == '"
        res = res + str(argv[1]) + "').groupby(['regimen']).agg(countDistinct"
        res = res + "('patient_id').alias('regimen_share'))" + "\n\n"

        res = res + "total_regimen_count = df.agg(F.sum('regimen_share')).first()[0]\n\n"
        res = res + "df = df.withColumn('regimen_share_percentage', (col('regimen_share')/"
        res = res + "total_regimen_count)*100)" + "\n\n"
        res = res + "df = df.drop('total_regimen_count')"
        res = res + ".orderBy(col('regimen_share').desc())\n\n"
        res = res + "df = df.limit(10)" + "\n\n"
        res = res + "dict_convert_v2(df)\n\n"
        return res

    def average_treatment_days(self, analysis_id, col_name, start_date, end_date, lot_dict={}):
        res = ''
        res = res + "if 'lot' not in df_" + str(analysis_id) + ".columns:\n"
        res = res + "\tdf_" + str(analysis_id) + " = df_" + str(analysis_id)
        res = res + ".withColumn('lot', when(col('"
        res = res + str(col_name) + "')==1, lit(1)).when(col('" 
        res = res + str(col_name) + "')==2, lit(2)).when(col('" 
        res = res + str(col_name) + "')==3, lit(3)).when(col('"
        res = res + str(col_name) + "')==4, lit(4)).otherwise(lit(5)))" + "\n\n"
        
        col_name = 'lot'
        
        res = res + PatientFlow().range_filter(analysis_id, start_date, end_date)

        res = res + "patient_ids = df_" + str(analysis_id) + ".filter(mask)"
        if bool(lot_dict) == True:
            res = res + ".filter("
            for key, val in lot_dict.items():
                res = res + "(col('" + str(key) + "') == '" + str(val) + "') & "
            result = list(res)
            del result[-3:-1]
            result.insert(-1, ")")
            res = ''.join(result)

        res = res + ".select('patient_id')" + "\n\n"

        res = res + "df = df_" + str(analysis_id) + ".join(patient_ids, "
        res = res + "on = ['patient_id'], how='leftsemi').select(df_"
        res = res + str(analysis_id) + "['*'])" + "\n\n"

        res = res + "df = df.groupBy(['patient_id', "
        res = res + "'regimen', '" + str(col_name) + "'])"
        res = res + ".agg(datediff(max('activity_date'), min('index_date'))"
        res = res + ".alias('days_on_therapy'))\n\n"

        res = res + "df = df.groupby(['" + str(col_name) + "', 'regimen'])"
        res = res + ".agg(F.sum('days_on_therapy').alias('total_dot'), "
        res = res + "countDistinct('patient_id').alias('patient_count'))" + "\n\n"

        res = res + "df = df.withColumn('avg_days_on_therapy', col('total_dot')/col"
        res = res + "('patient_count'))\n\n"

        res = res + "windowSpec = Window.partitionBy(['" + str(col_name)
        res = res + "']).orderBy(col('patient_count').desc())" + "\n\n"
        res = res + "df = df.withColumn('row_num', rank().over(windowSpec))" + "\n\n"

        res = res + "mask_1 = (df['row_num'] <= 10)" + "\n\n"
        res = res + "df = df.filter(mask_1)" + " \n\n"

        res = res + "output_dict = dict_convert_v2(df.select('" + str(col_name) + "', 'regimen', "
        res = res + "'patient_count', 'avg_days_on_therapy').orderBy(col('" + str(col_name)
        res = res + "').desc(), col('patient_count').desc(), col('avg_days_on_therapy').desc()))" + "\n\n"

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

    def create_mean_dos_table(self, analysis_id, drug_col, product_list, start_date, end_date):
        res = ''
        res = res + "# selecting the required columns and making a copy of that\n"
        res = res + "df = df_" + str(analysis_id) + ".select('source_value', col('"
        res = res + str(drug_col) + "').alias('product'), 'patient_id', 'activity_date',"
        res = res + " col('days_supply').cast('int'))\n\n"
        res = res + "# Analysis period filter\n"
        res = res + "mask = (col('activity_date') >= '" + str(start_date)
        res = res + "') & (col('activity_date') <= '" + str(end_date) + "')\n"
        res = res + "df = df.filter(mask)\n\n"
        res = res + "# product filter\n"
        res = res + "product_list = " + str(product_list) + "\n"
        res = res + "df = df.filter(col('product').isin(product_list))\n"
        res = res + "# dropping duplicates if any\n"
        res = res + "df = df.dropDuplicates()\n\n"
        res = res + "# filtering the source_value(ndc_codes) where atleast one of its days_supply as 0 or null\n"
        res = res + "mask = (df.days_supply == 0) | (df.days_supply.isNull())\n"
        res = res + "source_value_list = [x.source_value for x in df.filter(mask)"
        res = res + ".select('source_value').distinct().collect()]\n"
        res = res + "df = df.filter(col('source_value').isin(source_value_list))\n\n"

        res = res + "if df.rdd.isEmpty():\n"
        res = res + "\tfinal_result = []\n"
        res = res + "else:\n"

        res = res + "\t# Calculating the mean_dos_value for the source_value having atleast a record "
        res = res + "has days_supply greater than 0\n"
        res = res + "\tdf1 = df.groupby(['source_value','patient_id','activity_date'])"
        res = res + ".agg(max(col('days_supply')).alias('days_supply'))\n"
        res = res + "\tdf1 = df1.filter((col('days_supply') !=0) & col('days_supply').isNotNull())\n"
        res = res + "\tdf1 = df1.groupby(['source_value'])"
        res = res + ".agg(mean(col('days_supply')).alias('mean_dos_value'))\n\n"
        res = res + "\t# Getting all the products and source_values having atleast one of its "
        res = res + "days_supply as 0 or null\n"
        res = res + "\tdf = df.select('product', 'source_value').dropDuplicates()\n"
        res = res + "\tdf = df.join(df1, on = ['source_value'], how = 'left')\n\n"
        res = res + "\t# There may be possibility that for a particular source_value will have "
        res = res + "all its days_supply as null or nan\n"
        res = res + "\t# Those sholud be replaced by 0\n"
        res = res + "\tdf = df.na.fill(value=0,subset=['mean_dos_value'])\n\n"
        res = res + "\t# Rounding to 0 decimal and converting to int datatype\n"
        res = res + "\tdf = df.select('product', 'source_value', round(col('mean_dos_value'))"
        res = res + ".cast('int').alias('mean_dos_value'))\n\n"
        res = res + "\t# Converting the dataframe to json how UI needs\n"
        res = res + "\toutput_dict = dict_convert_v2(df.orderBy('product'))\n"
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


    def compliance_metric(self, analysis_id, data, start_date, end_date, unwanted_columns):
        data = data[0]
        res = ''
        res = res + "sc.setCheckpointDir('s3://{0}/checkpoints/Compliance')".format(str(cs.S3_PROJECT_PATH)) + "\n\n"
        res = res + "# Getting the required columns required for compliance calculation\n"
        res = res + "if 'regimen' in df_" + str(analysis_id) + ".columns:\n"
        res = res + "\tdf = df_" + str(analysis_id) + ".drop(*('start_date', 'claim_id', 'quantity', "
        res = res + "'specialty_code', 'specialty_desc', 'npi_number', 'index_date', 'case_flag', "
        res = res + "'regimen', 'HCP_name', 'mono_combo'))\n"
        res = res + "\tlot_ = tuple([i for i in df.columns if i[:3] == 'lot' or i[8:13] == 'level'])\n"
        res = res + "\tdf = df.drop(*lot_)\n"
        res = res + "else:\n"
        res = res + "\tdf = df_" + str(analysis_id) + ".drop(*('start_date', 'claim_id', 'quantity', "
        res = res + "'specialty_code', 'specialty_desc', 'npi_number', 'HCP_name', 'mono_combo'))\n\n"

        res = res +"df = df.withColumnRenamed('"+ str(data['Product_Selection']['Drug_Class'])
        res = res + "', 'product')\n"
        res = res + "df = df.withColumn('days_supply', col('days_supply').cast('int'))\n\n"

        res = res + "df = df.drop(*tuple(" + str(unwanted_columns) + ")).dropDuplicates().checkpoint()\n\n"

        res = res + "# Analysis period filter\n"
        res = res + "mask = (col('activity_date') >= '" + str(start_date)
        res = res + "') & (col('activity_date') <= '" + str(end_date) + "')\n"
        res = res + "df = df.filter(mask)\n\n"
        res = res + "# filtering Treatment and Procedure records\n"
        res = res + "df = df.filter(col('code_flag').isin(['Rx', 'Px'])).drop('code_flag')\n"
        res = res + "# filtering the products\n"
        res = res + "product_list = " + str(data['Product_Selection']['Drug_Class_Values']) + "\n"
        res = res + "df = df.filter(col('product').isin(product_list))\n\n"
        
        if data['Days_of_Supply_Type'] == 'Filter':
            res = res + "# Filtering out the data in case Days Of Supply is null or 0 days\n"
            res = res + "df = df.filter((col('days_supply') != 0) & (col('days_supply').isNotNull()))\n"
            res = res + "df = df.drop('source_value').dropDuplicates()\n\n"
        elif data['Days_of_Supply_Type'] == 'Assign':
            res = res + "assign_days = " + str(data['Days_of_Supply_Details']['Days']) + "\n"
            if data['Days_of_Supply_Details']['Flag'] == 'zero_or_null':
                res = res + "# Assign values to Days of supply in case of nulls or 0 values\n"
                res = res + "df = df.withColumn('days_supply', when((df.days_supply == 0) | "
                res = res + "(df.days_supply.isNull()), assign_days).otherwise(df.days_supply))\n\n"
            else:
                res = res + "# Assign user specified value to Days of supply for all the data "
                res = res + "considered for the analysis\n"
                res = res + "df = df.withColumn('days_supply', lit(assign_days))\n"
            res = res + "df = df.drop('source_value').dropDuplicates()\n\n"
        else:
            res = res + "# mean dos json from UI\n"
            res = res + "mean_dos_json = " + str(data['Days_of_Supply_Details']['Treat_dos_json']) + "\n"
            res = res + "schema = StructType([StructField('default_dos_value',StringType(),True), "
            res = res + "StructField('mean_dos_value',IntegerType(),True), "
            res = res + "StructField('source_value',StringType(),True)])\n\n"

            res = res + "# Converting json to dataframe\n"
            res = res + "dff = spark.createDataFrame(data = mean_dos_json, schema= schema).select("
            res = res + "'source_value', 'mean_dos_value', 'default_dos_value')\n\n"
            res = res + "# getting mean_dos_value and default_value for the respective source_value\n"
            res = res + "# converting them to numeric data\n"
            res = res + "df = df.join(dff, on=['source_value'], how='left')\n"
            res = res + "df = df.withColumn('mean_dos_value', df.mean_dos_value.cast('int'))"
            res = res + ".withColumn('default_dos_value', df.default_dos_value.cast('int'))\n\n"
            res = res + "df = df.withColumn('days_supply', when(df.days_supply == 0, lit(None))"
            res = res + ".otherwise(df.days_supply))\n\n"
            if data['Days_of_Supply_Details']['Priority1'] == 'Mean':
                res = res + "# Priority1 is mean\n"
                res = res + "# if the days_supply is null , then assign mean_dos_value\n"
                res = res + "# Then after if mean_dos_value is null , then assign default_dos_value\n"
                res = res + "df = df.withColumn('mean_dos_value', when(df.mean_dos_value == 0, lit(None))"
                res = res + ".otherwise(df.mean_dos_value))\n"
                res = res + "df = df.withColumn('days_supply', coalesce(col('days_supply'), "
                res = res + "col('mean_dos_value'), col('default_dos_value'), lit(0)))\n"
            else:
                res = res + "# Priority1 is default\n"
                res = res + "# if the days_supply is null , then assign default_dos_value\n"
                res = res + "# Then after if default_dos_value is null , then assign mean_dos_value\n"
                res = res + "df = df.withColumn('default_dos_value', when(df.default_dos_value == 0, "
                res = res + "lit(None)).otherwise(df.default_dos_value))\n"
                res = res + "df = df.withColumn('days_supply', coalesce(col('days_supply'), "
                res = res + "col('default_dos_value'), col('mean_dos_value'), lit(0)))\n"
            res = res + "df = df.drop('mean_dos_value','default_dos_value', 'source_value')"
            res = res + ".dropDuplicates()\n\n"
        res = res + "# Index date\n"
        res = res + "w = Window.partitionBy('patient_id', 'product')\n"
        res = res + "df = df.withColumn('index_date', min('activity_date').over(w))"
        res = res + ".orderBy('patient_id', 'product', 'activity_date').checkpoint()\n\n"

        if data['Time_period']['OP'] == 'lte':
            res = res + "mask1 = (col('index_date') <= '" + str(data['Time_period']['Value']) + "')\n"
        elif data['Time_period']['OP'] == 'lt':
            res = res + "mask1 = (col('index_date') < '" + str(data['Time_period']['Value']) + "')\n"
        elif data['Time_period']['OP'] == 'gte':
            res = res + "mask1 = (col('index_date') >= '" + str(data['Time_period']['Value']) + "')\n"
        elif data['Time_period']['OP'] == 'gt':
            res = res + "mask1 = (col('index_date') > '" + str(data['Time_period']['Value']) + "')\n"
        elif data['Time_period']['OP'] == 'eq':
            res = res + "mask1 = (col('index_date') == '" + str(data['Time_period']['Value']) + "')\n"
        else:
            res = res + "mask1 = (col('index_date') >= '" + str(data['Time_period']['Value'])
            res = res + "') & (col('index_date') <= '" + str(data['Time_period']['Extent']) + "')\n"
        
        res = res + "df = df.filter(mask1)\n\n"

        res = res + "try:\n"
        res = res + "\tif df.rdd.isEmpty():\n"
        res = res + "\t\traise Exception\n"
        res = res + "except:\n"
        res = res + "\traise Exception('No records available to calculate compliance metric.')\n\n"

        res = res + "# calculating days_difference between activity_date and the index_date\n"
        res = res + "df = df.withColumn('date_diff', datediff(df.activity_date, df.index_date))\n\n"

        res = res + "# Restricting the data based on look_forward_days\n"
        res = res + "look_forward_days = " + str(data['Time_period']['Look_Forward_Days']) + "\n"
        res = res + "df = df.filter(col('date_diff') <= look_forward_days)\n\n"

        res = res + "# Counting the distinct activity_date at patient_id and product level\n"
        res = res + "dff = df.groupby(['patient_id','product'])"
        res = res + ".agg(countDistinct('activity_date').alias('count_'))\n"
        res = res + "df = df.join(dff, on = ['patient_id', 'product'], how='inner')"
        res = res + ".orderBy('patient_id', 'product', 'activity_date').checkpoint()\n\n"

        res = res + "# Flaging the Single_dispensed and multiple_dispensed\n"
        res = res + "df = df.withColumn('patient_type', when(df.count_ == 1, lit('Single_dispensed'))"
        res = res + ".otherwise(lit('Multiple_dispensed'))).drop('count_')\n\n"

        res = res + "# Calculating sum_dos, calendar_days and last_dos\n"
        res = res + "# OrderBy is done to get the last_dos as max value\n"
        res = res + "w = Window.partitionBy('patient_id', 'product')"
        res = res + ".orderBy('activity_date', 'days_supply').rangeBetween("
        res = res + "Window.unboundedPreceding, Window.unboundedFollowing)\n"
        res = res + "df = df.withColumn('sum_dos', sum('days_supply').over(w))"
        res = res + ".withColumn('calendar_days', max('date_diff').over(w))"
        res = res + ".withColumn('last_dos', last('days_supply').over(w))\n\n"

        res = res + "df = df.drop('activity_date', 'days_supply', 'date_diff').dropDuplicates().checkpoint()\n\n"

        res = res + "# Calulating compliance value\n"
        res = res + "df = df.withColumn('comp_deno', col('calendar_days')+ col('last_dos'))\n"
        res = res + "df = df.withColumn('compliance_rate', col('sum_dos')/col('comp_deno'))\n"
        res = res + "df = df.withColumn('compliance_percent', round(col('compliance_rate')*100).cast('int'))\n"
        res = res + "df = df.drop('comp_deno', 'compliance_rate').checkpoint()\n\n"


        res = res + "# Compliance flaging (Single_dispensed, Compliant, Non-Compliant)\n"
        res = res + "compliance_threshold = " + str(data['Threshold']) + "\n"
        res = res + "df = df.withColumn('compliance_flag', "
        res = res + "when(col('patient_type') == 'Single_dispensed', 'Single_dispensed')"
        res = res + ".when(col('compliance_percent') >= compliance_threshold , 'Compliant')"
        res = res + ".otherwise('Non-Compliant'))\n\n"
        res = res + "df = df.withColumn('flag', when(col('compliance_percent') <= 150, "
        res = res + "lit('lte_150')).otherwise(lit('gt_150')))\n\n"
        res = res + "df = df.dropDuplicates()\n"
        res = res + "df_compliance_" + str(analysis_id) + " = df.orderBy('patient_id','product')\n\n"
        res = res + "del(df, dff)\n"
        res = res + "rc_compliance_" + str(analysis_id) + " = df_compliance_" + str(analysis_id) + ".count()\n\n"
        return res
        

    def save_to_s3(self, project_id, analysis_id, attribute_id):
        res = "df_compliance_" + str(analysis_id) + ".write.format('delta')"
        res = res + ".option('overwriteSchema', 'true').mode('overwrite')"
        res = res + ".save('s3://{}/PFA_COMPLIANCE/prj_".format(cs.S3_PROJECT_PATH)
        res = res + str(project_id) + "_" + str(analysis_id)
        res = res + "_" + str(attribute_id) + "')\n"
        return (res)

    def read_file(self, project_id, analysis_id, attribute_id):
        res = "df_compliance_" + str(analysis_id) + " = spark.read.format('delta')"
        res = res + ".load('s3://{}/PFA_COMPLIANCE/prj_".format(cs.S3_PROJECT_PATH)
        res = res + str(project_id) + "_" + str(analysis_id)
        res = res + "_" + str(attribute_id) + "')\n"
        return (res)
    

    def get_record_count(self, analysis_id):
        res = "if not 'rc_compliance_" + str(analysis_id) + "' in globals():\n"
        res = res + "\trc_compliance_" + str(analysis_id) + " = df_compliance_" + str(analysis_id) + ".count()\n\n"
        res = res + "print(rc_compliance_" + str(analysis_id) + ")"
        return res


    def convert_to_pandas(self, analysis_id):
        res = ''
        res = res + "df_compliance_" + str(analysis_id) + " = df_compliance_" + str(analysis_id) + ".toPandas()\n\n"
        return res


    def drop_attribute(self, key):
        res = "# Deleting the attribute\n"
        res = res + "s3 = boto3.resource('s3')\n"
        res = res + "bucket = s3.Bucket('{}')\n".format(cs.S3_BUCKET)
        res = res + "response = bucket.objects.filter(Prefix='" + str(key) + "').delete()"
        return res
    

    def drop_column(self, analysis_id, col_name, project_id, attribute_id):
        res = self.read_file(str(project_id), str(analysis_id), str(attribute_id)) + "\n"
        res = res + "df_compliance_" + str(analysis_id) + " = df_compliance_" + str(analysis_id) + ".drop('{}')\n\n".format(col_name)
        res = res + self.save_to_s3(str(project_id), str(analysis_id), str(attribute_id))
        return (res)

    def age_range_attribute(self, analysis_id, splits, labels, col_name, project_id, attribute_id):
        res = self.read_file(project_id, analysis_id, attribute_id) + "\n"

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
        res = res + "bucketizer = Bucketizer(splits=splits, inputCol='age', outputCol='split')" + "\n"
        res = res + "with_split = bucketizer.transform(df_compliance_" + str(analysis_id) + ")" + "\n"
        res = res + "label_array = array(*(lit(label) for label in labels))" + "\n"
        res = res + "df_compliance_" + str(analysis_id) + " = with_split.withColumn('" + str(col_name)
        res = res + "', label_array.getItem(col('split').cast('integer')))" + "\n"
        res = res + "df_compliance_" + str(analysis_id) + " = df_compliance_" + str(analysis_id) + ".drop('split')\n\n"
        res = res + self.save_to_s3(str(project_id), str(analysis_id), str(attribute_id))
        return (res)


    def gender_attribute(self, analysis_id, map_lst, dim_lst, col_name, project_id, attribute_id):
        res = self.read_file(str(project_id), str(analysis_id), str(attribute_id)) + "\n"

        res = res + "df_compliance_" + str(analysis_id) + " = df_compliance_" + str(analysis_id) + ".withColumn('{}', ".format(col_name)
        if (len(dim_lst) >= 1):
            for i in range(len(dim_lst)):
                res = res + "when(col('gender') == '" + str(map_lst[i]) + "', '" + str(dim_lst[i]) + "')."
            res = res + "otherwise(col('gender')))\n"
            res = res + self.save_to_s3(str(project_id), str(analysis_id), str(attribute_id))
            return (res)
        else:
            return ("Invalid Gender Mappings")
    

    def average_compliance(self, analysis_id, compliance_dict = {}):
        res = ''
        
        if bool(compliance_dict) == True:
            res = res + "df_compliance_" + str(analysis_id) + " = df_compliance_" + str(analysis_id)
            res = res + ".filter("
            for key, val in compliance_dict.items():
                res = res + "(col('" + str(key) + "') == '" + str(val) + "') & "
            result = list(res)
            del result[-3:-1]
            result.insert(-1, ")")
            res = ''.join(result)

        res = res + "\n# filter out single dispensed patients\n"
        res = res + "df = df_compliance_" + str(analysis_id)
        res = res + ".filter(col('patient_type') != 'Single_dispensed')"
        res = res + ".select('patient_id', 'product', 'compliance_percent', 'flag').dropDuplicates()\n\n"
        res = res + "gt_150_patient_count = df.filter(col('flag') == 'gt_150').select('patient_id')"
        res = res + ".distinct().count()\n\n"
        res = res + "df = df.filter(col('flag') == 'lte_150')\n\n"
        res = res + "# Calculating average compliance\n"
        res = res + "df = df.groupby(['product']).agg("
        res = res + "mean(col('compliance_percent')).alias('avg_compliance'), "
        res = res + "countDistinct(col('patient_id')).alias('patient_count'))\n"
        res = res + "df = df.orderBy(desc('patient_count'))\n\n"
        res = res + "df = df.select('product', round(col('avg_compliance')).cast('int')"
        res = res + ".alias('avg_compliance'), 'patient_count')\n\n"
        res = res + "# Converting the dataframe to json how UI needs\n"
        res = res + "final_dict = dict()\n"
        res = res + "final_dict['gt_150_patients'] = gt_150_patient_count\n"
        res = res + "final_dict['graph'] = list(map(lambda row: row.asDict(True), df.collect()))\n"
        res = res + "final_dict"
        return res
    

    def years_list_1b(self, analysis_id, product):
        res = ''
        res = res + "# Product filter\n"
        res = res + "df = df_compliance_" + str(analysis_id) + ".filter((col('product') == '" + str(product)
        res = res + "') & (col('patient_type') != 'Single_dispensed') & (col('flag') == 'lte_150'))\n\n"
        res = res + "# index date year\n"
        res = res + "df = df.withColumn('year', year(col('index_date')).cast('string'))\n"
        res = res + "year_list = [x.year for x in df.select('year').distinct().collect()]\n"
        res = res + "year_list.sort(reverse = True)\n"
        res = res + "year_list"
        return res


    def years_list_2b(self, analysis_id, product):
        res = ''
        res = res + "# Product filter\n"
        res = res + "df = df_compliance_" + str(analysis_id) + ".filter(col('product') == '" + str(product) + "')\n"
        res = res + "# index date year\n"
        res = res + "df = df.withColumn('year', year(col('index_date')).cast('string'))\n"
        res = res + "year_list = [x.year for x in df.select('year').distinct().collect()]\n"
        res = res + "year_list.sort(reverse = True)\n"
        res = res + "year_list"
        return res

    
    def years_list(self, analysis_id, product):
        res = ''
        res = res + "# Product filter\n"
        res = res + "df = df_compliance_" + str(analysis_id) + ".filter(col('product') == '" + str(product) + "')\n"
        res = res + "# index date year\n"
        res = res + "df = df.withColumn('year', year(col('index_date')).cast('string'))\n"
        res = res + "year_list = [x.year for x in df.select('year').distinct().collect()]\n"
        res = res + "year_list.sort(reverse = True)\n"
        res = res + "year_list"
        return res


    def average_compliance_over_time(self, analysis_id, product, year_list, time_period):
        res = ''
        res = res + "# filter out single dispensed patients\n"
        res = res + "df = df_compliance_" + str(analysis_id)
        res = res + ".filter(col('patient_type') != 'Single_dispensed')\n\n"

        res = res + "# Product filter\n"
        res = res + "df = df.filter(col('product') == '" + str(product)
        res = res + "').select('patient_id','index_date', 'compliance_percent','flag').dropDuplicates()\n\n"
        res = res + "# index date year and month\n"
        res = res + "df = df.withColumn('year', year(col('index_date')).cast('string'))\n"
        res = res + "df = df.withColumn('index_month', month(col('index_date')))\n\n"

        res = res + "# Year filter\n"
        res = res + "df = df.filter(col('year').isin(["
        for i,year in enumerate(year_list):
            res = res + "'" + str(year) + "'"
            if i != len(year_list)-1:
                res = res + ","
        res = res + "]))\n\n"

        res = res + "gt_150_patient_count = df.filter(col('flag') == 'gt_150').select('patient_id')"
        res = res + ".distinct().count()\n\n"
        res = res + "df = df.filter(col('flag') == 'lte_150')\n\n"
        res = res + "patient_count_by_year = df.select('patient_id').distinct().count()\n\n"
        res = res + "# finding " + str(time_period)+ " based on index date\n"
        if time_period == 'month':
            res = res + "df = df.withColumn('month', concat(col('year'), lit('-'), "
            res = res + "lpad(col('index_month'), 2, '0')))\n\n"
        elif time_period == 'quarter':
            res = res + "df = df.withColumn('quarter', concat(col('year'), lit('-Q'), "
            res = res + "quarter(col('index_date'))))\n\n"
        else:
            res = res + "df = df.withColumn('semester', concat(col('year'), lit('-S'),"
            res = res + "expr('((index_month-1)/6)+1').cast('int')))\n\n"

        res = res + "# Calculating average compliance at " + str(time_period) + " level\n"
        res = res + "df = df.groupby(['" + str(time_period) + "']).agg(mean(col('compliance_percent'))"
        res = res + ".alias('avg_compliance'), countDistinct(col('patient_id')).alias('patient_count'))\n"
        res = res + "df = df.withColumn('avg_compliance', round(col('avg_compliance')).cast('int'))\n"
        res = res + "df = df.withColumnRenamed('" + str(time_period) + "','time_period')\n"
        res = res + "df = df.orderBy('" + str(time_period) + "')\n\n"

        res = res + "# Converting the dataframe to json how UI needs\n"
        res = res + "final_dict = dict()\n"
        res = res + "final_dict['gt_150_patients'] = gt_150_patient_count\n"
        res = res + "final_dict['patient_count_by_year'] = patient_count_by_year\n"
        res = res + "final_dict['graph'] = list(map(lambda row: row.asDict(True), df.collect()))\n"
        res = res + "final_dict"
        return res
        
    
    def compliant_patients(self, analysis_id):
        res = ''
        res = res + "df = df_compliance_" + str(analysis_id)
        res = res + ".select('product', 'patient_id', 'compliance_flag').dropDuplicates()\n\n"
        res = res + "# Total Patient count at product level\n"
        res = res + "df1 = df.groupby(['product'])"
        res = res + ".agg(countDistinct(col('patient_id')).alias('total_patients'))\n\n"
        res = res + "# Patient count at product and compliance_flag level\n"
        res = res + "df = df.groupby(['product', 'compliance_flag'])"
        res = res + ".agg(countDistinct(col('patient_id')).alias('patient_count'))\n\n"
        res = res + "# Transposing\n"
        res = res + "df = df.groupby('product').pivot('compliance_flag').agg(sum('patient_count'))\n\n"
        res = res + "df = df1.join(df, on=['product'], how='left')\n\n"

        res = res + "df = df.fillna(0)\n"
        res = res + "# Adding columns if not present\n"
        res = res + "if 'Compliant' not in df.columns:\n"
        res = res + "\tdf = df.withColumn('Compliant', lit(0))\n"
        res = res + "if 'Non-Compliant' not in df.columns:\n"
        res = res + "\tdf = df.withColumn('Non-Compliant', lit(0))\n"
        res = res + "if 'Single_dispensed' not in df.columns:\n"
        res = res + "\tdf = df.withColumn('Single_dispensed', lit(0))\n\n"

        res = res + "# Renaming column names\n"
        res = res + "df = df.withColumnRenamed('Compliant', 'compliant_patients')"
        res = res + ".withColumnRenamed('Non-Compliant', 'non_compliant_patients')"
        res = res + ".withColumnRenamed('Single_dispensed', 'single_dispensed_patients')\n\n"

        res = res + "# Calculating compliant, non-compliant and single dispensed percentage\n"
        res = res + "df = df.withColumn('compliant_percent', round((col('compliant_patients')/"
        res = res + "col('total_patients'))*100,1))\n"
        res = res + "df = df.withColumn('non_compliant_percent', round((col('non_compliant_patients')"
        res = res + "/col('total_patients'))*100,1))\n"
        res = res + "df = df.withColumn('single_dispensed_percent', "
        res = res + "round((col('single_dispensed_patients')/col('total_patients'))*100,1))\n\n"

        res = res + "df = df.orderBy(desc('total_patients'))\n\n"
        res = res + "# Replacing nulls with 0\n"
        res = res + "df = df.fillna(0)\n\n"
        res = res + "# Converting the dataframe to json how UI needs\n"
        res = res + "final_dict = list(map(lambda row: row.asDict(True), df.collect()))\n"
        res = res + "final_dict"
        return res


    def compliant_patients_over_time(self, analysis_id, product, year_list, time_period):
        res = ''
        res = res + "df = df_compliance_" + str(analysis_id)
        res = res + ".select('patient_id', 'product', 'index_date', 'compliance_flag').dropDuplicates()\n\n"
        res = res + "# product filter\n"
        res = res + "df = df.filter(col('product') == '" + str(product) +"')\n\n"
        res = res + "# index date year and month\n"
        res = res + "df = df.withColumn('year', year(col('index_date')).cast('string'))\n"
        res = res + "df = df.withColumn('index_month', month(col('index_date')))\n\n"

        res = res + "# Year filter\n"
        res = res + "df = df.filter(col('year').isin(["
        for i,year in enumerate(year_list):
            res = res + "'" + str(year) + "'"
            if i != len(year_list)-1:
                res = res + ","
        res = res + "]))\n\n"

        res = res + "patient_count_by_year = df.select('patient_id').distinct().count()\n\n"
        
        res = res + "# finding " + str(time_period)+ " based on index date\n"
        if time_period == 'month':
            res = res + "df = df.withColumn('month', concat(col('year'), lit('-'), "
            res = res + "lpad(col('index_month'), 2, '0')))\n\n"
        elif time_period == 'quarter':
            res = res + "df = df.withColumn('quarter', concat(col('year'), lit('-Q'), "
            res = res + "quarter(col('index_date'))))\n\n"
        else:
            res = res + "df = df.withColumn('semester', concat(col('year'), lit('-S'),"
            res = res + "expr('((index_month-1)/6)+1').cast('int')))\n\n"
        
        res = res + "# Total Patient count at " + str(time_period)+ " level\n"
        res = res + "df1 = df.groupby(['" + str(time_period)+ "']).agg(countDistinct(col('patient_id'))"
        res = res + ".alias('total_patients'))\n\n"
        res = res + "# Patient count at " + str(time_period)+ " and compliance_flag level\n"
        res = res + "df = df.groupby(['" + str(time_period)+ "', 'compliance_flag'])"
        res = res + ".agg(countDistinct(col('patient_id')).alias('patient_count'))\n\n"
        res = res + "# Transposing\n"
        res = res + "df = df.groupby('" + str(time_period)+ "').pivot('compliance_flag')"
        res = res + ".agg(sum('patient_count'))\n\n"
        res = res + "df = df1.join(df, on=['" + str(time_period)+ "'], how='left')\n"

        res = res + "df = df.fillna(0)\n"
        res = res + "# Adding columns if not present\n"
        res = res + "if 'Compliant' not in df.columns:\n"
        res = res + "\tdf = df.withColumn('Compliant', lit(0))\n"
        res = res + "if 'Non-Compliant' not in df.columns:\n"
        res = res + "\tdf = df.withColumn('Non-Compliant', lit(0))\n"
        res = res + "if 'Single_dispensed' not in df.columns:\n"
        res = res + "\tdf = df.withColumn('Single_dispensed', lit(0))\n\n"

        res = res + "# Renaming column names\n"
        res = res + "df = df.withColumnRenamed('Compliant', 'compliant_patients')"
        res = res + ".withColumnRenamed('Non-Compliant', 'non_compliant_patients')"
        res = res + ".withColumnRenamed('Single_dispensed', 'single_dispensed_patients')\n\n"

        res = res + "# Calculating compliant, non-compliant and single dispensed percentage\n"
        res = res + "df = df.withColumn('compliant_percent', round((col('compliant_patients')/"
        res = res + "col('total_patients'))*100,1))\n"
        res = res + "df = df.withColumn('non_compliant_percent', round((col('non_compliant_patients')"
        res = res + "/col('total_patients'))*100,1))\n"
        res = res + "df = df.withColumn('single_dispensed_percent', "
        res = res + "round((col('single_dispensed_patients')/col('total_patients'))*100,1))\n\n"

        res = res + "df = df.withColumnRenamed('" + str(time_period)+ "','time_period')\n"
        res = res + "df = df.orderBy('time_period')\n\n"

        res = res + "# Converting the dataframe to json how UI needs\n"
        res = res + "final_dict = dict()\n"
        res = res + "final_dict['patient_count_by_year'] = patient_count_by_year\n"
        res = res + "final_dict['graph'] = list(map(lambda row: row.asDict(True), df.collect()))\n"
        res = res + "final_dict"
        return res


    def percentage_patient_distribution(self, analysis_id):
        res = ''
        res = res + "# filter out single dispensed patients\n"
        res = res + "df = df_compliance_" + str(analysis_id)
        res = res + ".filter(col('patient_type') != 'Single_dispensed')\n"
        res = res + "df = df.select('product', 'patient_id', 'compliance_percent','flag').dropDuplicates()\n\n"
        res = res + "gt_150_patient_count = df.filter(col('flag') == 'gt_150').select('patient_id')"
        res = res + ".distinct().count()\n\n"
        res = res + "df = df.filter(col('flag') == 'lte_150')\n\n"
        res = res + "# splits and labels for bucketing the data\n"
        res = res + "splits = [float('-inf')]\n"
        res = res + "labels= []\n\n"
        res = res + "max_percent = df.agg({'compliance_percent': 'max'}).collect()[0][0]\n"
        res = res + "for i in range(1,math.ceil(max_percent/10)+1):\n"
        res = res + "\tsplits.append((i*10)+0.0001)\n"
        res = res + "\tlabel = str((i-1)*10) + '-' + str(i*10)\n"
        res = res + "\tlabels.append(label)\n\n"
        res = res + "# Bucketing the compliance percentage\n"
        res = res + "bucketizer = Bucketizer(splits=splits, inputCol='compliance_percent', "
        res = res + "outputCol='split')\n"
        res = res + "with_split = bucketizer.transform(df)\n"
        res = res + "label_array = array(*(lit(label) for label in labels))\n"
        res = res + "df = with_split.withColumn('bucket', label_array.getItem(col('split').cast('integer')))\n"
        res = res + "df = df.drop('split')\n\n"

        res = res + "# Patient count at product and compliance_bucket level\n"
        res = res + "df = df.groupby(['product','bucket']).agg(countDistinct(col('patient_id'))"
        res = res + ".alias('patient_count'))\n"
        res = res + "w = Window.partitionBy('product').rangeBetween(Window.unboundedPreceding, "
        res = res + "Window.unboundedFollowing)\n"
        res = res + "df = df.withColumn('total_patient_count', sum('patient_count').over(w))\n"
        res = res + "df = df.withColumn('patient_percent', round((col('patient_count')/"
        res = res + "col('total_patient_count'))*100,2))\n\n"
        res = res + "# Getting list of columns(product names) based on total patients at product level\n"
        res = res + "df1 = df.select('product','total_patient_count').dropDuplicates()\n"
        res = res + "df1 = df1.orderBy(desc('total_patient_count'))\n\n"
        res = res + "columns_list = [x.product for x in df1.collect()]\n"
        res = res + "columns_list.insert(0,'bucket')\n\n"
        res = res + "# Transposing\n"
        res = res + "df = df.groupby('bucket').pivot('product').agg(sum('patient_percent'))\n\n"
        res = res + "# Make the static class values \n"
        res = res + "df1 = spark.createDataFrame(data = labels, schema = StringType())"
        res = res + ".withColumnRenamed('value', 'bucket')\n"
        res = res + "df = df1.join(df, on=['bucket'] , how = 'left')\n\n"
        res = res + "df = df.fillna(0).select(columns_list)\n\n"
        res = res + "df = df.withColumn('compliance_bucket_value', when((length(col('bucket'))== 4), "
        res = res + "col('bucket').substr(1,1).cast('int')).when((length(col('bucket'))<= 6), "
        res = res + "col('bucket').substr(1,2).cast('int')).otherwise(col('bucket').substr(1,3)"
        res = res + ".cast('int')))\n"
        res = res + "df = df.orderBy('compliance_bucket_value').drop('compliance_bucket_value')\n\n"
        res = res + "# Converting the dataframe to json how UI needs\n"
        res = res + "final_dict = dict()\n"
        res = res + "final_dict['gt_150_patients'] = gt_150_patient_count\n"
        res = res + "final_dict['graph'] = list(map(lambda row: row.asDict(True), df.collect()))\n"
        res = res + "final_dict"
        return res


    def patient_distribution(self, analysis_id, product):
        res = ''
        res = res + "# filter out single dispensed patients\n"
        res = res + "df = df_compliance_" + str(analysis_id)
        res = res + ".filter(col('patient_type') != 'Single_dispensed')\n"
        res = res + "df = df.select('product', 'patient_id', 'compliance_percent','flag').dropDuplicates()\n\n"
        res = res + "# product filter\n"
        res = res + "df = df.filter(col('product') == '" + str(product) + "')\n\n"

        res = res + "gt_150_patient_count = df.filter(col('flag') == 'gt_150').select('patient_id')"
        res = res + ".distinct().count()\n\n"
        res = res + "df = df.filter(col('flag') == 'lte_150').dropDuplicates()\n\n"

        res = res + "compliant_percent_list = [x.compliance_percent for x in df.collect()]\n\n"

        res = res + "# Converting the dataframe to json how UI needs\n"
        res = res + "final_dict = dict()\n"
        res = res + "final_dict['gt_150_patients'] = gt_150_patient_count\n"
        res = res + "final_dict['graph'] = compliant_percent_list\n"
        res = res + "final_dict"
        return res


class Persistence:
    """
    Class contains all the functions related to Persistence Attribute
    """

    def __init__(self):
        """ Initialize all class level variables """
        pass

    def persistence_metric(self, analysis_id, data, start_date, end_date, unwanted_columns):
        data = data[0]
        res = ''
        res = res + "sc.setCheckpointDir('s3://{0}/checkpoints/Persistence')".format(str(cs.S3_PROJECT_PATH)) + "\n\n"
        res = res + "# Getting the required columns required for persistence calculation\n"
        res = res + "if 'regimen' in df_" + str(analysis_id) + ".columns:\n"
        res = res + "\tdf = df_" + str(analysis_id) + ".drop(*('start_date', 'claim_id', 'quantity', "
        res = res + "'specialty_code', 'specialty_desc', 'npi_number', 'index_date', 'case_flag', "
        res = res + "'regimen','HCP_name', 'mono_combo'))\n"
        res = res + "\tlot_ = tuple([i for i in df.columns if i[:3] == 'lot' or i[8:13] == 'level'])\n"
        res = res + "\tdf = df.drop(*lot_)\n"
        res = res + "else:\n"
        res = res + "\tdf = df_" + str(analysis_id) + ".drop(*('start_date', 'claim_id', 'quantity', "
        res = res + "'specialty_code', 'specialty_desc', 'npi_number', 'HCP_name', 'mono_combo'))\n\n"

        res = res +"df = df.withColumnRenamed('"+ str(data['Product_Selection']['Drug_Class'])
        res = res + "', 'product')\n"
        res = res + "df = df.withColumn('days_supply', col('days_supply').cast('int'))\n\n"

        res = res + "df = df.drop(*tuple(" + str(unwanted_columns) + ")).dropDuplicates().checkpoint()\n\n"

        res = res + "# Analysis period filter\n"
        res = res + "mask = (col('activity_date') >= '" + str(start_date)
        res = res + "') & (col('activity_date') <= '" + str(end_date) + "')\n"
        res = res + "df = df.filter(mask)\n\n"
        res = res + "# filtering Treatment and Procedure records\n"
        res = res + "df = df.filter(col('code_flag').isin(['Rx', 'Px'])).drop('code_flag')\n\n"
        res = res + "# filtering the products\n"
        res = res + "product_list = " + str(data['Product_Selection']['Drug_Class_Values']) + "\n"
        res = res + "df = df.filter(col('product').isin(product_list))\n\n"
        
        if data['Days_of_Supply_Type'] == 'Filter':
            res = res + "# Filtering out the data in case Days Of Supply is null or 0 days\n"
            res = res + "df = df.filter((col('days_supply') != 0) & (col('days_supply').isNotNull()))\n"
            res = res + "df = df.drop('source_value').dropDuplicates()\n\n"
        elif data['Days_of_Supply_Type'] == 'Assign':
            res = res + "assign_days = " + str(data['Days_of_Supply_Details']['Days']) + "\n"
            if data['Days_of_Supply_Details']['Flag'] == 'zero_or_null':
                res = res + "# Assign values to Days of supply in case of nulls or 0 values\n"
                res = res + "df = df.withColumn('days_supply', when((df.days_supply == 0) | "
                res = res + "(df.days_supply.isNull()), assign_days).otherwise(df.days_supply))\n\n"
            else:
                res = res + "# Assign user specified value to Days of supply for all the data "
                res = res + "considered for the analysis\n"
                res = res + "df = df.withColumn('days_supply', lit(assign_days))\n"
            res = res + "df = df.drop('source_value').dropDuplicates()\n\n"
        else:
            res = res + "# mean dos json from UI\n"
            res = res + "mean_dos_json = " + str(data['Days_of_Supply_Details']['Treat_dos_json']) + "\n"
            res = res + "schema = StructType([StructField('default_dos_value',StringType(),True), "
            res = res + "StructField('mean_dos_value',IntegerType(),True), "
            res = res + "StructField('source_value',StringType(),True)])\n\n"
            res = res + "# Converting json to dataframe\n"
            res = res + "dff = spark.createDataFrame(data = mean_dos_json, schema= schema).select("
            res = res + "'source_value', 'mean_dos_value', 'default_dos_value')\n\n"
            res = res + "# getting mean_dos_value and default_value for the respective source_value\n"
            res = res + "# converting them to numeric data\n"
            res = res + "df = df.join(dff, on=['source_value'], how='left')\n"
            res = res + "df = df.withColumn('mean_dos_value', df.mean_dos_value.cast('int'))"
            res = res + ".withColumn('default_dos_value', df.default_dos_value.cast('int'))\n\n"
            res = res + "df = df.withColumn('days_supply', when(df.days_supply == 0, lit(None))"
            res = res + ".otherwise(df.days_supply))\n\n"
            if data['Days_of_Supply_Details']['Priority1'] == 'Mean':
                res = res + "# Priority1 is mean\n"
                res = res + "# if the days_supply is null , then assign mean_dos_value\n"
                res = res + "# Then after if mean_dos_value is null , then assign default_dos_value\n"
                res = res + "df = df.withColumn('mean_dos_value', when(df.mean_dos_value == 0, lit(None))"
                res = res + ".otherwise(df.mean_dos_value))\n"
                res = res + "df = df.withColumn('days_supply', coalesce(col('days_supply'), "
                res = res + "col('mean_dos_value'), col('default_dos_value'), lit(0)))\n"
            else:
                res = res + "# Priority1 is default\n"
                res = res + "# if the days_supply is null , then assign default_dos_value\n"
                res = res + "# Then after if default_dos_value is null , then assign mean_dos_value\n"
                res = res + "df = df.withColumn('default_dos_value', when(df.default_dos_value == 0, "
                res = res + "lit(None)).otherwise(df.default_dos_value))\n"
                res = res + "df = df.withColumn('days_supply', coalesce(col('days_supply'), "
                res = res + "col('default_dos_value'), col('mean_dos_value'), lit(0)))\n"
            res = res + "df = df.drop('mean_dos_value','default_dos_value', 'source_value')"
            res = res + ".dropDuplicates()\n\n"
        res = res + "#Grace Period Handling\n"
        if data['Grace_Period_Type'] == 'Assign':
            res = res + "# Assigning " + str(data['Grace_Period_Details']['Days'])
            res = res + " days as grace period for every record\n"
            res = res + "df = df.withColumn('grace_period', lit("
            res = res + str(data['Grace_Period_Details']['Days']) + "))\n\n"
        else:
            res = res + "# Product-wise grace period assignment\n"
            res = res + "# Json from UI\n"
            res = res + "att_lvl_grace_prd_json = "
            res = res + str(data['Grace_Period_Details']['Attribute_Json']) + "\n"
            res = res + "# Converting json to dataframe\n"
            res = res + "dff = spark.createDataFrame(data = att_lvl_grace_prd_json).select("
            res = res + "'product', col('Grace_Period_value').cast('int').alias('grace_period'))\n\n"
            res = res + "df = df.join(dff, on=['product'], how='left')\n"
        
        res = res + "# Filtering out Single-dispensed claims\n"
        res = res + "# Days of supply must be greater than user-defined value\n"
        res = res + "df = df.filter(col('days_supply')> " + str(data['Single_Dispensed_Value']) + ").dropDuplicates().checkpoint()\n\n"

        res = res + "# index date for each patient at product level\n"
        res = res + "w = Window.partitionBy('patient_id', 'product')\n"
        res = res + "df = df.withColumn('index_date', min('activity_date').over(w))"
        res = res + ".orderBy('patient_id', 'product', 'activity_date')\n\n"

        res = res + "# index date constraint\n"
        if data['Time_period']['OP'] == 'lte':
            res = res + "mask1 = (col('index_date') <= '" + str(data['Time_period']['Value']) + "')\n"
        elif data['Time_period']['OP'] == 'lt':
            res = res + "mask1 = (col('index_date') < '" + str(data['Time_period']['Value']) + "')\n"
        elif data['Time_period']['OP'] == 'gte':
            res = res + "mask1 = (col('index_date') >= '" + str(data['Time_period']['Value']) + "')\n"
        elif data['Time_period']['OP'] == 'gt':
            res = res + "mask1 = (col('index_date') > '" + str(data['Time_period']['Value']) + "')\n"
        elif data['Time_period']['OP'] == 'eq':
            res = res + "mask1 = (col('index_date') == '" + str(data['Time_period']['Value']) + "')\n"
        else:
            res = res + "mask1 = (col('index_date') >= '" + str(data['Time_period']['Value'])
            res = res + "') & (col('index_date') <= '" + str(data['Time_period']['Extent']) + "')\n"
        
        res = res + "df = df.filter(mask1)\n\n"
        res = res + "try:\n"
        res = res + "\tif df.rdd.isEmpty():\n"
        res = res + "\t\traise Exception\n"
        res = res + "except:\n"
        res = res + "\traise Exception('No records available to calculate persistency metric.')\n\n"
        res = res + "df = df.orderBy(['patient_id','product', 'activity_date']).checkpoint()\n\n"
        
        res = res + "# Updating activity date\n"
        res = res + "w = Window().orderBy(['patient_id','product', 'activity_date'])\n"
        res = res + "df = df.withColumn('row_number', row_number().over(w))\n\n"
        res = res + "prev_patient_id = str(' ')\n"
        res = res + "prev_product = str(' ')\n"
        res = res + "cols = ['row_number','new_activity_date', 'new_end_date']\n"
        res = res + "for j in range(1,int(df.count()/500000)+2):\n"
        res = res + "\tnew_list = []\n"
        res = res + "\tdff = df.filter(df.row_number.between((500000*(j-1))+1, (500000*j)))\n"
        res = res + "\tfor row in dff.collect():\n"
        res = res + "\t\tif (prev_patient_id != row['patient_id']) or "
        res = res + "(prev_product != row['product']):\n"
        res = res + "\t\t\tnew_activity_date = row['activity_date']\n"
        res = res + "\t\t\tday = row['days_supply']\n"
        res = res + "\t\t\tnew_end_date = new_activity_date + timedelta(days= day)\n"
        res = res + "\t\telse:\n"
        res = res + "\t\t\tprev_new_end_date_1 = (int(str(prev_new_end_date.year)+str(prev_new_end_date.month)"
        res = res + ".zfill(2)+str(prev_new_end_date.day).zfill(2)))\n"
        res = res + "\t\t\tcurrent_activity_date = (int(str(row['activity_date'].year)"
        res = res + "+str(row['activity_date'].month).zfill(2)+str(row['activity_date'].day).zfill(2)))\n"
        res = res + "\t\t\tif prev_new_end_date_1 >= current_activity_date:\n"
        res = res + "\t\t\t\tnew_activity_date = prev_new_end_date\n"
        res = res + "\t\t\telse:\n"
        res = res + "\t\t\t\tnew_activity_date = row['activity_date']\n"
        res = res + "\t\t\tday = row['days_supply']\n"
        res = res + "\t\t\tnew_end_date = new_activity_date + timedelta(days= day)\n"
        res = res + "\t\tprev_patient_id = row['patient_id']\n"
        res = res + "\t\tprev_product = row['product']\n"
        res = res + "\t\tprev_new_activity_date = new_activity_date\n"
        res = res + "\t\tprev_new_end_date = new_end_date\n"
        res = res + "\t\tnew_list.append((row['row_number'], new_activity_date, new_end_date))\n"
        res = res + "\tif(j==1):\n"
        res = res + "\t\tdf1 = spark.createDataFrame(data = new_list , schema = cols)\n"
        res = res + "\telse:\n"
        res = res + "\t\tdf2 = spark.createDataFrame(data = new_list , schema = cols)\n"
        res = res + "\t\tdf1 = df1.union(df2)\n\n"
        res = res + "df = df.join(df1, df.row_number == df1.row_number, 'inner').drop(df1.row_number)\n\n"
        
        res = res + "# look forward filter\n"
        res = res + "look_forward_days = " + str(data['Time_period']['Look_Forward_Days']) + "\n"
        res = res + "df = df.filter(datediff(col('new_activity_date'), col('index_date')) <= "
        res = res + "look_forward_days).checkpoint()\n\n"
        
        res = res + "# Grace period handling\n"
        res = res + "prev_end_date  = Window().partitionBy('patient_id', 'product')"
        res = res + ".orderBy('new_activity_date')\n"
        res = res + "df = df.withColumn('prev_end_date', lag(col('new_end_date')).over(prev_end_date))\n"
        res = res + "df = df.withColumn('next_diff', datediff(col('new_activity_date'), col('prev_end_date'))"
        res = res + ".cast(IntegerType()))\n"
        res = res + "df = df.fillna(value=0,subset=['next_diff'])\n"
        res = res + "df = df.withColumn('grace', when(col('next_diff') <= col('grace_period'), "
        res = res + "col('next_diff')).otherwise(lit(0))).checkpoint()\n\n"
        
        res = res + "# Episode creation\n"
        res = res + "episode_number = Window().partitionBy('patient_id', 'product')"
        res = res + ".orderBy('new_activity_date').rangeBetween(Window.unboundedPreceding, 0)\n"
        res = res + "df = df.withColumn('episode_flag', when(col('next_diff') > col('grace_period'), 1)"
        res = res + ".otherwise(lit(0)))\n"
        res = res + "df = df.withColumn('episode_number', sum('episode_flag')"
        res = res + ".over(episode_number).cast(IntegerType()))\n"
        res = res + "df = df.withColumn('episode_num', (col('episode_number') + 1).cast(IntegerType())).checkpoint()\n\n"
        
        res = res + "# Filtering for episode 1\n"
        res = res + "df = df.filter(col('episode_num') == 1)\n\n"

        res = res + "# Episode index date\n"
        res = res + "df1 = df.groupby(col('patient_id'), col('product'), col('episode_num'))"
        res = res + ".agg(min('new_activity_date').alias('ep_index_date'))\n"
        res = res + "df = df.join(df1, on=['patient_id','product','episode_num'], how='left')\n"
        res = res + "df = df.orderBy(col('patient_id'),col('product'),col('new_activity_date')).checkpoint()\n\n"

        res = res + "# Start and end months of therapy\n"
        res = res + "df = df.withColumn('st_mo', "
        res = res + "((expr('datediff(date_sub(new_activity_date, grace),ep_index_date)'))/30))\n"
        res = res + "df = df.withColumn('end_mo', "
        res = res + "((expr('datediff(date_add(new_end_date, grace),ep_index_date)'))/30))\n"
        res = res + "df = df.replace(0, value=1, subset=['st_mo']).checkpoint()\n\n"

        res = res + "# min and max of Activity date, end date(max_activity_date + dos + grace) at patient and product level\n"
        res = res + "df = df.withColumn('end_date_with_grace', expr('date_add(new_end_date, grace)').cast(TimestampType()))\n"
        res = res + "df1 = df.groupBy(['patient_id', 'product']).agg(min(col('new_activity_date'))"
        res = res + ".alias('min_activity_date'), max(col('new_activity_date')).alias('max_activity_date'),"
        res = res + " last(col('end_date_with_grace')).alias('end_date'))\n\n"

        res = res + "#Droping unwanted columns\n"
        res = res + "df = df.drop('row_number','prev_end_date','next_diff', 'episode_number',"
        res = res + "'episode_flag','activity_date','new_activity_date', 'days_supply', 'grace_period',"
        res = res + " 'new_end_date', 'grace','ep_index_date', 'index_date', 'end_date_with_grace')\n\n"

        res = res + "df = df.withColumn('st_mo', ceil('st_mo').cast(IntegerType()))\n"
        res = res + "df = df.withColumn('end_mo', ceil('end_mo').cast(IntegerType())).checkpoint()\n\n"

        res = res + "#maximum end month\n"
        res = res + "max_end_month = (df.agg({'end_mo': 'max'}).collect()[0][0]) + 1\n\n"
        res = res + "# creating flags for each month the patient is present\n"
        res = res + "nlevel_dict = dict()\n"
        res = res + "nlevel_list = []\n"
        res = res + "for i in range(1, max_end_month):\n"
        res = res + "\tcolumn_name = 'n' + str(i)\n"
        res = res + "\tdf = df.withColumn(column_name, when((col('st_mo') <= i) & (col('end_mo') >= i), 1)"
        res = res + ".otherwise(0))\n"
        res = res + "\tnlevel_dict[column_name] = 'sum'\n"
        res = res + "\tnlevel_list.append(column_name)\n\n"
        res = res + "df = df.drop('st_mo','end_mo')\n"
        res = res + "cols = [i for i in df.columns if i not in nlevel_list]\n\n"

        res = res + "# Taking the sum of each flag and bringing it to patient episode level\n"
        res = res + "df = df.groupby(cols).agg(nlevel_dict)\n\n"
        
        res = res + "df = df.join(df1, on=['patient_id', 'product'], how='inner')\n\n"

        res = res + "# Flagging months having atleast 1 occurrence\n"
        res = res + "nlevel_list = []\n"
        res = res + "for i in range(1, max_end_month):\n"
        res = res + "\tcolumn_p = 'month' + str(i)\n"
        res = res + "\tcolumn_n = 'sum(n' + str(i) + ')'\n"
        res = res + "\tdf = df.withColumn(column_p, when((col(column_n) > 0), lit(1)).otherwise(lit(0)))\n"
        res = res + "\tnlevel_list.append(column_n)\n\n"
        
        res = res + "df_persistence_" + str(analysis_id)
        res = res + " = df.drop(*nlevel_list).dropDuplicates()\n\n"
        res = res + "del(df1, df)\n"
        return res


    def save_to_s3(self, project_id, analysis_id, attribute_id):
        res = "df_persistence_" + str(analysis_id) + ".write.format('delta')"
        res = res + ".option('overwriteSchema', 'true').mode('overwrite')"
        res = res + ".save('s3://{}/PFA_PERSISTENCE/prj_".format(cs.S3_PROJECT_PATH)
        res = res + str(project_id) + "_" + str(analysis_id)
        res = res + "_" + str(attribute_id) + "')\n"
        return (res)


    def read_file(self, project_id, analysis_id, attribute_id):
        res = "df_persistence_" + str(analysis_id) + " = spark.read.format('delta')"
        res = res + ".load('s3://{}/PFA_PERSISTENCE/prj_".format(cs.S3_PROJECT_PATH)
        res = res + str(project_id) + "_" + str(analysis_id)
        res = res + "_" + str(attribute_id) + "')\n"
        return (res)

    
    def drop_column(self, analysis_id, col_name, project_id, attribute_id):
        res = self.read_file(str(project_id), str(analysis_id), str(attribute_id)) + "\n"
        res = res + "df_persistence_" + str(analysis_id) + " = df_persistence_" + str(analysis_id) + ".drop('{}')\n\n".format(col_name)
        res = res + self.save_to_s3(str(project_id), str(analysis_id), str(attribute_id))
        return (res)


    def age_range_attribute(self, analysis_id, splits, labels, col_name, project_id, attribute_id):
        res = self.read_file(project_id, analysis_id, attribute_id) + "\n"

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
        res = res + "bucketizer = Bucketizer(splits=splits, inputCol='age', outputCol='split')" + "\n"
        res = res + "with_split = bucketizer.transform(df_persistence_" + str(analysis_id) + ")" + "\n"
        res = res + "label_array = array(*(lit(label) for label in labels))" + "\n"
        res = res + "df_persistence_" + str(analysis_id) + " = with_split.withColumn('" + str(col_name)
        res = res + "', label_array.getItem(col('split').cast('integer')))" + "\n"
        res = res + "df_persistence_" + str(analysis_id) + " = df_persistence_" + str(analysis_id) + ".drop('split')\n\n"
        res = res + self.save_to_s3(str(project_id), str(analysis_id), str(attribute_id))
        return (res)


    def gender_attribute(self, analysis_id, map_lst, dim_lst, col_name, project_id, attribute_id):
        res = self.read_file(str(project_id), str(analysis_id), str(attribute_id)) + "\n"

        res = res + "df_persistence_" + str(analysis_id) + " = df_persistence_" + str(analysis_id) + ".withColumn('{}', ".format(col_name)
        if (len(dim_lst) >= 1):
            for i in range(len(dim_lst)):
                res = res + "when(col('gender') == '" + str(map_lst[i]) + "', '" + str(dim_lst[i]) + "')."
            res = res + "otherwise(col('gender')))\n"
            res = res + self.save_to_s3(str(project_id), str(analysis_id), str(attribute_id))
            return (res)
        else:
            return ("Invalid Gender Mappings")


    def patient_level_graph(self, analysis_id, persistence_dict = {}):
        res = ''
        
        if bool(persistence_dict) == True:
            res = res + "df_persistence_" + str(analysis_id) + " = df_persistence_" + str(analysis_id)
            res = res + ".filter("
            for key, val in persistence_dict.items():
                res = res + "(col('" + str(key) + "') == '" + str(val) + "') & "
            result = list(res)
            del result[-3:-1]
            result.insert(-1, ")")
            res = ''.join(result)

        res = res + "\n# Persistence Graph code\n"
        res = res + "df = df_persistence_" + str(analysis_id) + "\n"
        res = res + "cols_list= ['patient_id','product','episode_num']\n"
        res = res + "maxm_end_month = 0\n"
        res = res + "for colum in df.columns:\n"
        res = res + "\tif colum[0:5]=='month' and colum[5:].isnumeric():\n"
        res = res + "\t\tcols_list.append(colum)\n"
        res = res + "\t\tmaxm_end_month = maxm_end_month + 1\n\n"
        res = res + "df = df.select(cols_list).dropDuplicates()\n\n"
        res = res + "plevel_dict = dict()\n"
        res = res + "for i in range(1, maxm_end_month):\n"
        res = res + "\tcolumn_p = 'month' + str(i)\n"
        res = res + "\tplevel_dict[column_p] = 'sum'\n\n"
        res = res + "# Patient level persistence\n"
        res = res + "df = df.groupby(['product']).agg(plevel_dict)\n"
        res = res + "df = df.toDF(*(c.replace(')', '') for c in df.columns))\n"
        res = res + "df = df.toDF(*(c.replace('sum(', '') for c in df.columns))\n\n"
        res = res + "# Summary\n"
        res = res + "plevel_list = []\n"
        res = res + "mlevel_list = []\n"
        res = res + "for i in range(1, maxm_end_month):\n"
        res = res + "\tcolumn_p = 'month' + str(i)\n"
        res = res + "\tcolumn_m = 'M' + str(i)\n"
        res = res + "\tdf = df.withColumn(column_m, round((col(column_p)*100/col('month1')), 2))\n"
        res = res + "\tplevel_list.append(column_p)\n"
        res = res + "\tmlevel_list.append(column_m)\n\n"
        res = res + "df = df.drop(*plevel_list)\n\n"

        res = res + "columns_value = list(map(lambda x: str(\"'\") + str(x) + str(\"',\")  + str(x), mlevel_list))\n"
        res = res + "stack_cols = ','.join(x for x in columns_value)\n"
        res = res + "df = df.selectExpr('product', 'stack(' + str(len(mlevel_list)) + ',' + stack_cols + ')')"
        res = res + ".select('product', 'col0', 'col1')\n"
        res = res + "df = df.groupBy(col('col0')).pivot('product').agg(collect_list(col('col1'))[0])"
        res = res + ".withColumnRenamed('col0', 'month')\n\n"
        res = res + "df = df.withColumn('length_', (length(col('month'))).cast('int'))\n"
        res = res + "df = df.orderBy('length_','month').drop('length_')\n\n"
        res = res + "final_dict = list(map(lambda row: row.asDict(True), df.collect()))\n"
        res = res + "final_dict"
        return res