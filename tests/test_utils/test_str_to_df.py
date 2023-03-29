import polars as pl
import pytest
from cprd_antihypertensives.cprd.utils.test_utils import str_to_df

dates = """patid,date,ICD
    1,2019-01-01,N832,
    2,2014-01-01,O080,
    """

datetimes = """patid,datetime,ICD
    1,2019-01-01 00:00:00,N832,
    2,2014-01-01 00:00:00,O080,
    """


@pytest.mark.parametrize(
    "input_df, column_name, column_dtype",
    [(dates, "date", pl.Date), (datetimes, "datetime", pl.Datetime)],
)
def test_get_rows_matching_codes(input_df, column_name, column_dtype):
    pl_df = str_to_df(input_df).collect()

    # Check that the column is of the correct type
    assert pl_df[column_name].dtype == column_dtype
