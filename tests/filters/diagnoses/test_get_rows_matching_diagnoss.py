import pytest
from cprd_antihypertensives.cprd.utils.test_utils import str_to_df
from cprd_antihypertensives.filters.diagnoses.get_diagnoses_matching_codes import (
    get_rows_matching_codes,
    get_rows_matching_values,
)


def test_get_rows_matching_values():
    input_df = """patid,eventdate,ICD
    1,2019-01-01,N832,
    1,2019-01-01,N83,
    2,2014-01-01,O080,
    """

    pl_df = str_to_df(input_df)

    diabetes_rows = get_rows_matching_values(df=pl_df, column_name="ICD" ,values=["N832"])

    assert diabetes_rows.collect().shape[0] == 1

def test_get_rows_matching_codes():
    input_df = str_to_df("""patid,eventdate,ICD,prodcode,medcode,OPCS
    1,2019-01-01,N832,0,0,0, # Kept by ICD
    2,2019-01-01,N83,1,0,0, # Kept by prodcode
    3,2014-01-01,O080,0,1,0, # Kept by medcode
    4,2014-01-01,O080,0,0,1, # Kept by OPCS
    5,2014-01-01,O080,0,0,0, # Dropped
    """)
    
    codes = {
        "ICD": ["N832"],
        "prodcode": ["1"],
        "medcode": ["1"],
        "OPCS": ["1"],
    }
    
    df = get_rows_matching_codes(df=input_df, codes=codes)
    
    