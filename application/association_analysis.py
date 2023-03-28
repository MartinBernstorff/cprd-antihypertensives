#!/usr/bin/env python
# %%
# Autoreload imports on cell execution
# %load_ext autoreload
# %autoreload 2

# %%
import os
import sys

from cprd_antihypertensives.cprd.config.spark import read_parquet, spark_init
from cprd_antihypertensives.cprd.functions import tables
from cprd_antihypertensives.cprd.functions.cohort_select_causal import CohortSoftCut
from cprd_antihypertensives.cprd.functions.MedicalDictionary import (
    MedicalDictionaryRiskPrediction,
)
from cprd_antihypertensives.cprd.functions.Prediction import OutcomePrediction
from cprd_antihypertensives.cprd.utils.yaml_act import yaml_load
from cprd_antihypertensives.globals import COHORTS, PROJECT_ROOT

sys.path.insert(0, "/home/mbernstorff/cprd-antihypertensives")
os.environ["JAVA_HOME"] = "/home/mbernstorff/miniconda3/envs/antihypertensives-39/"
os.environ["PYSPARK_PYTHON"] = "/home/mbernstorff/miniconda3/envs/antihypertensives-39/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/mbernstorff/miniconda3/envs/antihypertensives-39/bin/python"


class dotdict(dict):
    """dot.notation access to dictionary attributes"""

    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


config = yaml_load(
    dotdict({"params": PROJECT_ROOT / "application" / "config" / "config.yaml"}).params,  # type: ignore
)

# %%
config["pyspark"]["pyspark_env"]
pyspark_config = config["pyspark"]
spark_instance = spark_init(pyspark_config)

# %%
file_paths = config["file_path"]
current_params = config["params"]


# # Cohort selection
# ### Effect of antihypertensives on ischaemic conditions
#
# - Cohort selection: Age between 60 and 61 years in years between 2009 and 2010
#
# - Baseline: First initiation of any antihypertensive
#
# - Take random baseline for those without any antihypertensive

# %%
# medical dict can give us both exposures and outcomes codes - e.g. diabetes as outcomes or antihyyp as exposurea
md = MedicalDictionaryRiskPrediction(file_paths)
antihypertensive_product_codes = md.queryMedication(md.findItem("antihy"), merge=True)[
    "merged"
]
expcodes = {"prodcode": antihypertensive_product_codes}


# Exposure selection

# %%
# CohortSoftCut from the causal cohort selection package has everything to select cohort and baseline
# specifically the baseline for those WITH the exposure is date of exposure, and for those WITHOUT exp of interest is random sampling of baseline
cohortSelector = CohortSoftCut(
    least_year_register_gp=1,
    least_age=60,
    greatest_age=61,
    exposure=expcodes,
    imdReq=False,
    linkage=False,
    practiceLink=True,
)


# %%
# no mapping as you don't want to drop the prodcodes which are not mapped...
# medications = retrieve_medications(file, spark, mapping='none', duration=(2009, 2010), demographics=cohort, practiceLink=True) # noqa: ERA001
# medications.write.parquet('/home/shared/shishir/AurumOut/rawDat/meds_nomapping_2009_2010_association_example.parquet') # noqa: ERA001

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
output_path = "/home/mbernstorff/data/cprd-antihypertensives/raw/cohort_association_example.parquet"

cohort = cohortSelector.pipeline(
    file=file_paths,
    spark=spark_instance,
    duration=("2009-01-01", "2010-01-01"),
    randomNeg=True,
    sourceT=medications,
    sourceCol="prodcode",
    rollingTW=-1,
)

cohort.write.parquet(output_path)


# %%
cohort = read_parquet(
    spark_instance.sqlContext,
    output_path,
)
cohort.count()
# 259345 pats


# outcome selection

# %%
cohort = read_parquet(
    spark_instance.sqlContext,
    output_path,
)

necessaryColumns = [
    "patid",
    "gender",
    "dob",
    "study_entry",
    "startdate",
    "enddate",
    "exp_label",
]

cohort = cohort.select(necessaryColumns)


# %%
# label codes phenotyping from medical dict - ie maybe ischaemic conditions
labelcodes = md.queryDisease(md.findItem("ischaem"), merge=True)["merged"]
allIschaemiaCodes = labelcodes["medcode"] + labelcodes["ICD10"] + labelcodes["OPCS"]


# %%
# split diags into icd and nonicd(medcode) and re-union as "code"
allDiag = read_parquet(
    spark_instance.sqlContext,
    "/home/shared/shishir/AurumOut/rawDat/diagGP_med2sno2icd_HESAPC_praclinkage_1985_2021.parquet",
)
GPdiags = allDiag[allDiag.source == "CPRD"]
GPdiags = GPdiags.select(["patid", "eventdate", "medcode"]).withColumnRenamed(
    "medcode",
    "code",
)
HESdiags = allDiag[allDiag.source == "HES"]
HESdiags = HESdiags.select(["patid", "eventdate", "ICD"]).withColumnRenamed(
    "ICD",
    "code",
)
allDiag = GPdiags.union(HESdiags)

# read death registry as death is an important data source for looking for outcome
death = tables.retrieve_death(dir=file_paths["death"], spark=spark_instance)

# %%
# now we use the risk prediction label capture class
# basically with baseline we can capture 1) outcome, 2) time to outcome

# the exclusion_codes is those we should exclude based on condition - i.e., exclude those with prior cancers
# the duration is time we should consider the records in the outcome space (maybe from 2008 since earliest baseline is 2009 and end is 2020)
# the follow_up_duration_month is number of months for the followup
# the time_to_event_mark_default is mark as -1 if no event and lasts till end of follow-up
# more information in package
risk_pred_generator = OutcomePrediction(
    label_condition=allIschaemiaCodes,
    exclusion_codes=None,
    duration=(2008, 2020),
    follow_up_duration_month=24,
    time_to_event_mark_default=-1,
)


# %%
# demographics is the cohort file with sutdy entry etc
# source is the diag table
# source_col is column that has the diags
# exclusion_source is True if we want to exclude based on past diags

# check_death is true if we are to check death
# column_condition is column that has the diags or meds or whatever modality we are wanting to look for label
# %%
# prevalent_conditions is if incidence is false, then what are some prevalent conditions we are allowing to look for (a subset of the labels)
# more information in package

risk_cohort = risk_pred_generator.pipeline(
    demographics=cohort,
    source=allDiag,
    exclusion_source=False,
    check_death=True,
    death=death,
    column_condition="code",
    incidence=True,
    prevalent_conditions=None,
)


risk_cohort.write.parquet(str(COHORTS / "test.parquet"))

risk_cohort.toPandas().to_parquet(str(COHORTS / "test_pandas.parquet"))

# %%
