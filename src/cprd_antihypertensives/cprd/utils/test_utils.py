import polars as pl


def str_to_df(
    input_str: str,
    convert_timestamp_to_datetime: bool = True,
    convert_str_to_float: bool = False,
) -> pl.LazyFrame:
    """Convert a string representation of a dataframe to a polars lazyframe."""

    lines = [line.strip() for line in input_str.split("\n") if line.strip()]

    col_names = lines[0].split(",")

    output_dict = {}

    for i, col_name in enumerate(col_names):
        col_vals = [line.split(",")[i] for line in lines[1:]]
        output_dict[col_name] = col_vals

    df = pl.DataFrame(output_dict)

    for col in col_names:
        if convert_timestamp_to_datetime:
            if "date" in col:
                if "time" in col:
                    df = df.with_columns(
                        pl.col(col).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
                    )
                else:
                    df = df.with_columns(pl.col(col).str.strptime(pl.Date, "%Y-%m-%d"))

        if convert_str_to_float:
            # Check if all strings in column can be converted to float
            if all([isinstance(x, float) for x in df[col].to_numpy()]):
                df = df.with_columns(pl.col(col).cast(pl.Float64))

    return df.lazy()
