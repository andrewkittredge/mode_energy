import pandera.pandas as pa
import pandas as pd


class ERCOT_BESS_Owners_Schema(pa.DataFrameModel):
    date: pa.typing.Series[pd.Timestamp] = pa.Field()
    total_energy_capacity_mwh: pa.typing.Series[float] = pa.Field()
    total_rated_power_mw: pa.typing.Series[float] = pa.Field()
    owner: pa.typing.Series[str] = pa.Field()
