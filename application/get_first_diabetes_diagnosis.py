# %%
from cprd_antihypertensives.loaders.load_diagnoses import get_first_diabetes_diagnosis

# %%
df = get_first_diabetes_diagnosis().collect()
# %%
