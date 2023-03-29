import pytest
from cprd_antihypertensives.cprd.utils.test_utils import str_to_df


def test_get_rows_matching_codes():
    input_df = """patid,eventdate,ICD
    1,2019-01-01,N832,
    2,2014-01-01,O080,
    """

    pl_df = str_to_df(input_df)

    pass
