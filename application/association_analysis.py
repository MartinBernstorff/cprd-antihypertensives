#!/usr/bin/env python
# %%
import os
import sys

from cprd_antihypertensives.code_extractors.get_antihypertensive_product_codes import (
    get_codes,
)
from cprd_antihypertensives.cprd.config.spark import read_parquet, spark_init
from cprd_antihypertensives.cprd.functions import tables
from cprd_antihypertensives.cprd.functions.cohort_select_causal import CohortSoftCut
from cprd_antihypertensives.cprd.functions.MedicalDictionary import (
    MedicalDictionaryRiskPrediction,
)
from cprd_antihypertensives.cprd.functions.Prediction import OutcomePrediction
from cprd_antihypertensives.globals import PROJECT_ROOT
from cprd_antihypertensives.utils.load_config import load_config
from cprd_antihypertensives.utils.save_cohort import save_cohort

sys.path.insert(0, "/home/mbernstorff/cprd-antihypertensives")

config = load_config(
    config_path=PROJECT_ROOT / "application" / "config" / "config.yaml"
)

# %%
file_paths = config["file_path"]
cprd_params = config["params"]


# %%
# medical dict can give us both exposures and outcomes codes - e.g. diabetes as outcomes or antihyyp as exposurea
medical_dict = MedicalDictionaryRiskPrediction(file_paths)

antihypertensive_codes = get_codes(term="antihy", output_type="medication", merge=True)
bp_measurements = get_codes(term="sbp", output_type="measurement")


# Exposure selection
cohort_pt_1 = CohortSoftCut(
    least_year_register_gp=1,
    least_age=60,
    greatest_age=61,
    exposure=antihypertensive_codes,
    imdReq=False,
    linkage=False,
    practiceLink=True,
)

# %%
# no mapping as you don't want to drop the prodcodes which are not mapped...
# medications = retrieve_medications(file, spark, mapping='none', duration=(2009, 2010), demographics=cohort, practiceLink=True) # noqa: ERA001
# medications.write.parquet('/home/shared/shishir/AurumOut/rawDat/meds_nomapping_2009_2010_association_example.parquet') # noqa: ERA001

# %%
os.environ["JAVA_HOME"] = "/home/mbernstorff/miniconda3/envs/antihypertensives-39/"
spark_instance = spark_init(config["pyspark"])

medications = read_parquet(
    spark_instance.sqlContext,
    "/home/shared/shishir/AurumOut/rawDat/meds_nomapping_2009_2010_association_example.parquet",
)
# medications.select('patid').dropDuplicates().count()  - 208603 patients have meds in the time period
# medications.select('patid').count() - 11764962 number of records


# # pipeline() function has 3 components:  # noqa: ERA001s
# 1) Demo extract gets eligible patients between age 60 and 61 ^ defined above and years 2009 and 2010
# 2) Extraction of the exposure of interest -  set  baseline as the date of the exposure
# 3) For those without exposure (i.e. control patients), set up baseline as randomised baseline
#

# %%
cohort_df_1 = cohort_pt_1.pipeline(
    file=file_paths,
    spark=spark_instance,
    duration=("2009-01-01", "2010-01-01"),
    randomNeg=True,
    sourceT=medications,
    sourceCol="prodcode",
    rollingTW=-1,
)

column_names = [
    "patid",
    "gender",
    "dob",
    "study_entry",
    "startdate",
    "enddate",
    "exp_label",
]

cohort_df_1 = cohort_df_1.select(column_names)


# %%
# label codes phenotyping from medical dict - ie maybe ischaemic conditions
ischaemic_codes = get_codes(term="ischaem", output_type="disease", merge=True)

ischaemic_codes_combined = (
    ischaemic_codes["medcode"] + ischaemic_codes["ICD10"] + ischaemic_codes["OPCS"]
)


# %%
# split diags into icd and nonicd(medcode) and re-union as "code"
def new_func(spark_instance):
    all_diag_timestamps = read_parquet(
        spark_instance.sqlContext,
        "/home/shared/shishir/AurumOut/rawDat/diagGP_med2sno2icd_HESAPC_praclinkage_1985_2021.parquet",
    )
    gp_diag_timestamps = all_diag_timestamps[all_diag_timestamps.source == "CPRD"]
    gp_diag_timestamps = gp_diag_timestamps.select(
        ["patid", "eventdate", "medcode"]
    ).withColumnRenamed(
        "medcode",
        "code",
    )
    hes_diag_timestamps = all_diag_timestamps[all_diag_timestamps.source == "HES"]
    hes_diag_timestamps = hes_diag_timestamps.select(
        ["patid", "eventdate", "ICD"]
    ).withColumnRenamed(
        "ICD",
        "code",
    )
    combined_diag_timestamps = gp_diag_timestamps.union(hes_diag_timestamps)
    return combined_diag_timestamps


combined_diag_timestamps = new_func(spark_instance)

# read death registry as death is an important data source for looking for outcome
death = tables.retrieve_death(dir=file_paths["death"], spark=spark_instance)

# %%
diabetes_codes = get_codes(term="diabetes", output_type="disease")
risk_pred_generator = OutcomePrediction(
    label_condition=ischaemic_codes_combined,
    exclusion_codes=diabetes_codes,
    duration=(2008, 2020),
    follow_up_duration_month=24,
    time_to_event_mark_default=-1,
)

# %%
risk_cohort = risk_pred_generator.pipeline(
    demographics=cohort_df_1,
    source=combined_diag_timestamps,
    exclusion_source=False,
    check_death=True,
    death=death,
    column_condition="code",
    incidence=True,
    prevalent_conditions=None,
)

# %%
save_cohort(risk_cohort, output_type="spark")

# %%
save_cohort(risk_cohort, output_type="pandas")

# %%
