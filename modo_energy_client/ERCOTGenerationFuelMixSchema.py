from typing_extensions import Annotated
import pandas as pd
import pandera.pandas as pa
from pandera.typing import Index


class ERCOTGenerationFuelMixSchema(pa.DataFrameModel):
    coalAndLignite: pa.typing.Series[float] = pa.Field()
    hydro: pa.typing.Series[float] = pa.Field()
    nuclear: pa.typing.Series[float] = pa.Field()
    other: pa.typing.Series[float] = pa.Field()
    powerStorage: pa.typing.Series[float] = pa.Field()
    solar: pa.typing.Series[float] = pa.Field()
    wind: pa.typing.Series[float] = pa.Field()
    naturalGas: pa.typing.Series[float] = pa.Field()
    settlementType: pa.typing.Series[str] = pa.Field()
    repeated_hour_flag: pa.typing.Series[bool] = pa.Field()
    timestamp: Index[Annotated[pd.DatetimeTZDtype, "us", "UTC"]] = pa.Field()
