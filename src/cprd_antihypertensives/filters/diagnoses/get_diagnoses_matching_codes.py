import typing
from typing import Sequence

import polars as pl


class Codes(typing.TypedDict):
    ICD: Sequence[str]
    prodcode: Sequence[str]
    medcode: Sequence[str]
    OPCS: Sequence[str]


def get_rows_matching_values(
    df: pl.LazyFrame, column_name: str, values: Sequence[str]
) -> pl.LazyFrame:
    output_df = df.filter(pl.col(column_name).is_in(values))

    return output_df


def get_rows_matching_codes(df: pl.LazyFrame, codes: Codes) -> pl.LazyFrame:
    df = (
        df.pipe(
            get_rows_matching_values,
            column_name="ICD",
            values=codes["ICD"],
        )
        .pipe(
            get_rows_matching_values,
            column_name="prodcode",
            values=codes["prodcode"],
        )
        .pipe(
            get_rows_matching_values,
            column_name="medcode",
            values=codes["medcode"],
        )
        .pipe(
            get_rows_matching_values,
            column_name="OPCS",
            values=codes["OPCS"],
        )
    )

    return df
