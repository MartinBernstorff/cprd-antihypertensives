import pandas as pd
from cprd_antihypertensives.globals import COHORTS

if __name__ == "__main__":
    df = pd.read_parquet(COHORTS / "test_pandas.parquet")

    pass
