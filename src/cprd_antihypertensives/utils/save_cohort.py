# %%
import datetime
from pathlib import Path
from typing import Literal

from cprd_antihypertensives.globals import COHORTS


def save_cohort(risk_cohort, output_type: Literal["spark", "pandas"] = "spark") -> None:
    now_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    cohort_dir = Path(COHORTS / now_timestamp)
    cohort_dir.mkdir(parents=True, exist_ok=True)

    if output_type == "spark":
        risk_cohort.write.parquet(f"{cohort_dir}/antihypertensives_in_diabetes")
    elif output_type == "pandas":
        risk_cohort.toPandas().to_parquet(
            f"{cohort_dir}/antihypertensives_in_diabetes.parquet"
        )
    else:
        raise ValueError("Disallowed output_type")
