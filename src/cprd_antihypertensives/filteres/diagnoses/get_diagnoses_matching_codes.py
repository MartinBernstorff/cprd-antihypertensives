from typing import Sequence

import polars as pl


def get_rows_matching_icd(df: pl.LazyFrame, icd_codes: Sequence[str]) -> pl.LazyFrame:
    output_df = df.filter(pl.col("ICD").is_in(icd_codes))

    return output_df
