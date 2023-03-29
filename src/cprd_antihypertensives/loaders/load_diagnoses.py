# split diags into icd and nonicd(medcode) and re-union as "code"
from datetime import date, datetime
from pathlib import Path
from typing import Union

import polars as pl
from numpy import concatenate

from cprd_antihypertensives.code_extractors.get_antihypertensive_product_codes import (
    get_codes,
)
from cprd_antihypertensives.cprd.config.spark import read_parquet
from cprd_antihypertensives.cprd.functions.MedicalDictionary import (
    MedicalDictionaryRiskPrediction,
)
from cprd_antihypertensives.filters.diagnoses.get_diagnoses_matching_codes import (
    get_diagnoses_matching_codes,
)


def get_all_diagnoses(
    date_interval: tuple[Union[datetime, date], Union[datetime, date]]
) -> pl.LazyFrame:
    diag_dir = Path(
        "/home/shared/shishir/AurumOut/rawDat/diagGP_med2sno2icd_HESAPC_praclinkage_1985_2021___wLongICDmap.parquet/"
    )

    # Get paths for all fiels in directory
    paths = diag_dir.glob("*.parquet")
    dfs = [pl.scan_parquet(path) for path in paths]
    concatenated_df = pl.concat(dfs)

    # Filter by date interval
    df = concatenated_df.filter(
        (pl.col("eventdate") >= date_interval[0])
        & (pl.col("eventdate") <= date_interval[1])
    )

    return df


def get_diabetes_diagnoses(
    date_interval: tuple[Union[datetime, date], Union[datetime, date]]
) -> pl.LazyFrame:
    all_diagnoses = get_all_diagnoses(date_interval=date_interval)
    diabetes_codes = get_codes(term="diabetes", output_type="disease")

    diabetes_diagnoses = get_diagnoses_matching_codes(
        df=all_diagnoses, codes=diabetes_codes
    )

    return diabetes_diagnoses


def get_first_row_by_patient(
    df: pl.LazyFrame, datetime_column_name: str, patient_id_column_name: str
) -> pl.LazyFrame:
    df = df.with_columns(
        pl.col(datetime_column_name).min().over(patient_id_column_name).alias("row_max")
    ).filter(pl.col(datetime_column_name) == pl.col("row_max"))

    first_row = df.groupby(patient_id_column_name).head(1)

    return first_row


def get_first_diabetes_diagnosis(
    date_interval: tuple[Union[datetime, date], Union[datetime, date]]
) -> pl.LazyFrame:
    all_diabetes = get_diabetes_diagnoses(date_interval=date_interval)

    first_diabetes = get_first_row_by_patient(
        df=all_diabetes,
        datetime_column_name="eventdate",
        patient_id_column_name="patid",
    )

    return first_diabetes
