import pytest
from cprd_antihypertensives.cprd.utils.test_utils import str_to_df
from cprd_antihypertensives.filteres.diagnoses.get_diagnoses_matching_codes import (
    get_rows_matching_icd,
)


def test_get_rows_matching_codes():
    input_df = """patid,eventdate,ICD
    1,2019-01-01,N832,
    1,2019-01-01,N83,
    2,2014-01-01,O080,
    """

    pl_df = str_to_df(input_df)

    diabetes_rows = get_rows_matching_icd(df=pl_df, icd_codes=["N832"])

    assert diabetes_rows.collect().shape[0] == 1
