import typing
from typing import List, Sequence

import polars as pl


class Codes(typing.TypedDict):
    ICD10: Sequence[str]
    prodcode: Sequence[str]
    medcode: Sequence[str]
    OPCS: Sequence[str]


def get_rows_matching_values(
    df: pl.LazyFrame, column_name: str, values: Sequence[str]
) -> pl.LazyFrame:
    output_df = df.filter(pl.col(column_name).is_in(values))

    return output_df


def get_diagnoses_matching_codes(df: pl.LazyFrame, codes: Codes) -> pl.LazyFrame:
    icd_10 = get_rows_matching_values(df=df, column_name="ICD", values=codes["ICD10"])
    medcode = get_rows_matching_values(
        df=df, column_name="medcode", values=codes["medcode"]
    )
    return pl.concat([icd_10, medcode])
