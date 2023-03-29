# %%
from datetime import date, datetime

from cprd_antihypertensives.code_extractors.get_antihypertensive_product_codes import (
    get_codes,
)
from cprd_antihypertensives.filters.diagnoses.get_diagnoses_matching_codes import (
    get_diagnoses_matching_codes,
)
from cprd_antihypertensives.loaders.load_diagnoses import (
    get_all_diagnoses,
)

# %%
date_interval = (
    date(year=1994, month=1, day=1),
    date(year=1995, month=1, day=1),
)
df = get_all_diagnoses(date_interval=date_interval)

# %%
diabetes_codes = get_codes(term="diabetes", output_type="disease")

all_diabetes = get_diagnoses_matching_codes(df=df, codes=diabetes_codes)
# %%
collected_df = all_diabetes.collect()

# %%
