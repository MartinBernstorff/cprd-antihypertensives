import pytest
from cprd_antihypertensives.cprd.utils.test_utils import str_to_df
from cprd_antihypertensives.filters.diagnoses.get_diagnoses_matching_codes import (
    Codes,
    get_rows_matching_codes,
    get_rows_matching_values,
)
from cprd_antihypertensives.loaders.load_diagnoses import get_first_row_by_patient


def test_get_rows_matching_values():
    input_df = """patid,eventdate,ICD
    1,2019-01-01,N832,
    1,2019-01-01,N83,
    2,2014-01-01,O080,
    """

    pl_df = str_to_df(input_df)

    diabetes_rows = get_rows_matching_values(
        df=pl_df, column_name="ICD", values=["N832"]
    )

    assert diabetes_rows.collect().shape[0] == 1


def test_get_rows_matching_codes():
    input_df = str_to_df(
        """patid,eventdate,ICD,prodcode,medcode,OPCS
    1,2019-01-01,N832,0,0,0, # Kept by ICD
    2,2019-01-01,N83,1,0,0, # Kept by prodcode
    3,2014-01-01,O080,0,1,0, # Kept by medcode
    4,2014-01-01,O080,0,0,1, # Kept by OPCS
    5,2014-01-01,O080,0,0,0, # Dropped
    """
    )

    codes: Codes = {
        "ICD": ["N832"],
        "prodcode": ["1"],
        "medcode": ["1"],
        "OPCS": ["1"],
    }

    df = get_rows_matching_codes(df=input_df, codes=codes).collect()

    assert df.shape[0] == 4
    assert df["patid"][0] == "1"
    assert df["patid"][1] == "2"
    assert df["patid"][2] == "3"
    assert df["patid"][3] == "4"


def test_get_first_row_for_each_patient():
    input_df = str_to_df(
        """patid,date,val,keep,
                        1,2019-01-10,1,Drop,
                        1,2019-01-02,2,Keep: first date for patient,
                        2,2019-01-01,3,Keep: first date for patient,
                        2,2019-01-03,4,Drop,
                        2,2019-01-03,5,Drop: Duplicate on date,
                        """
    )

    df = get_first_row_by_patient(
        df=input_df,
        datetime_column_name="date",
        patient_id_column_name="patid",
    ).collect()

    assert df.shape[0] == 2

    for val in df["keep"]:
        assert "Keep" in val
