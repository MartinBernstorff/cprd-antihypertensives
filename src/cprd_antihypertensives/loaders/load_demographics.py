from pathlib import Path

import polars as pl

from cprd_antihypertensives.globals import COHORTS


def load_demographics() -> pl.DataFrame:
    demographics_path = Path(
        COHORTS / "20230329_095042" / "antihypertensives_in_diabetes.parquet"
    )
    demographics_df = pl.read_parquet(demographics_path)
    return demographics_df
