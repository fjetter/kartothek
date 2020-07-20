import pickle

import pandas as pd
import pytest

from kartothek.io.testing.read import *  # noqa
from kartothek.io_components.metapartition import SINGLE_TABLE


@pytest.fixture()
def output_type():
    return "table"


def _read_dataset(
    dataset_uuid,
    store,
    factory=None,
    categoricals=None,
    tables=None,
    dataset_has_index=False,
    **kwargs,
):
    table = tables or SINGLE_TABLE
    if categoricals:
        categoricals = categoricals[table]
    from kartothek.io.intake import KartothekDatasetSource

    ds = KartothekDatasetSource(
        dataset_uuid or factory.uuid,
        store or factory.store_factory,
        categoricals=categoricals,
        **kwargs,
    )
    ddf = ds.to_dask()

    if categoricals:
        assert ddf._meta.dtypes["P"] == pd.api.types.CategoricalDtype(
            categories=["__UNKNOWN_CATEGORIES__"], ordered=False
        )
        if dataset_has_index:
            assert ddf._meta.dtypes["L"] == pd.api.types.CategoricalDtype(
                categories=[1, 2], ordered=False
            )
        else:
            assert ddf._meta.dtypes["L"] == pd.api.types.CategoricalDtype(
                categories=["__UNKNOWN_CATEGORIES__"], ordered=False
            )

    s = pickle.dumps(ddf, pickle.HIGHEST_PROTOCOL)
    ddf = pickle.loads(s)

    ddf = ddf.compute().reset_index(drop=True)

    def extract_dataframe(ix):
        df = ddf.iloc[[ix]].copy()
        for col in df.columns:
            if pd.api.types.is_categorical(df[col]):
                df[col] = df[col].cat.remove_unused_categories()
        return df.reset_index(drop=True)

    return [extract_dataframe(ix) for ix in ddf.index]


@pytest.fixture()
def bound_load_dataframes():
    return _read_dataset


@pytest.fixture()
def backend_identifier():
    return "dask_intake"
