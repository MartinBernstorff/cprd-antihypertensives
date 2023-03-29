# %%
import os

from cprd_antihypertensives.code_extractors.get_antihypertensive_product_codes import (
    get_codes,
)
from cprd_antihypertensives.cprd.config.spark import spark_init
from cprd_antihypertensives.cprd.functions.MedicalDictionary import (
    MedicalDictionaryRiskPrediction,
)
from cprd_antihypertensives.globals import PROJECT_ROOT, RAW_DATA
from cprd_antihypertensives.loaders.load_demographics import load_demographics
from cprd_antihypertensives.loaders.load_diagnoses import get_all_diagnoses
from cprd_antihypertensives.utils.load_config import load_config

config = load_config(
    config_path=PROJECT_ROOT / "application" / "config" / "config.yaml",
)
file_paths = config["file_path"]
medical_dict = MedicalDictionaryRiskPrediction(file_paths)

# %%
os.environ["JAVA_HOME"] = "/home/mbernstorff/miniconda3/envs/antihypertensives-39/"
spark_instance = spark_init(config["pyspark"])

# %%
all_diagnoses = get_all_diagnoses()

# %%
# Save all_diagnoses to parquet
all_diagnoses.write_parquet(RAW_DATA / "all_diagnoses.parquet")

# %%
diabetes_codes = get_codes(term="diabetes", output_type="disease")

# %%
demographics_df = load_demographics()


# %%
antihypertensive_codes = get_codes(term="antihy", output_type="medication", merge=True)
bp_measurements = get_codes(term="sbp", output_type="measurement")
