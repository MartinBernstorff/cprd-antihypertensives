import typing
from typing import List, Sequence

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
    icd = get_rows_matching_values(df=df, column_name="ICD", values=codes["ICD"])
    predcode = get_rows_matching_values(
        df=df, column_name="prodcode", values=codes["prodcode"]
    )

    dfs: list[pl.LazyFrame] = []

    for code_source in ["ICD", "prodcode", "medcode", "OPCS"]:
        source_df = get_rows_matching_values(
            df=df, column_name=code_source, values=codes[code_source]
        )
        dfs.append(source_df)

    return pl.concat(dfs)
