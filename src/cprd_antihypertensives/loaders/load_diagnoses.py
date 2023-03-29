# split diags into icd and nonicd(medcode) and re-union as "code"
from pathlib import Path

import polars as pl

from cprd_antihypertensives.code_extractors.get_antihypertensive_product_codes import (
    get_codes,
)


def get_all_diagnoses() -> pl.LazyFrame:
    diag_dir = Path(
        "/home/shared/shishir/AurumOut/rawDat/diagGP_med2sno2icd_HESAPC_praclinkage_1985_2021.parquet/",
    )

    # Get paths for all fiels in directory
    paths = diag_dir.glob("*.parquet")
    dfs = [pl.scan_parquet(path) for path in paths]
    concatenated_df = pl.concat(dfs)

    return concatenated_df


def get_diabetes_diagnoses() -> pl.DataFrame:
    get_all_diagnoses()
    get_codes(term="diabetes", output_type="disease")

    pass


if __name__ == "__main__":
    get_diabetes_diagnoses()
