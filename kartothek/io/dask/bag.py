# -*- coding: utf-8 -*-
from collections import defaultdict
from functools import partial
from typing import Any, Callable, Dict, List, Optional

import dask.bag as db
import pandas as pd

from kartothek.core import naming
from kartothek.core.docs import default_docs
from kartothek.core.factory import DatasetFactory, _ensure_factory
from kartothek.core.types import PredicatesType, StoreFactory
from kartothek.core.utils import _check_callable
from kartothek.core.uuid import gen_uuid
from kartothek.io.dask._utils import (
    _cast_categorical_to_index_cat,
    _get_data,
    _identity,
    _maybe_get_categoricals_from_index,
)
from kartothek.io_components.index import update_indices_from_partitions
from kartothek.io_components.metapartition import (
    MetaPartition,
    parse_input_to_metapartition,
)
from kartothek.io_components.read import dispatch_metapartitions_from_factory
from kartothek.io_components.utils import normalize_args
from kartothek.io_components.write import (
    raise_if_dataset_exists,
    store_dataset_from_partitions,
)
from kartothek.serialization import DataFrameSerializer


def _store_dataset_from_partitions_flat(mps, *args, **kwargs):
    return store_dataset_from_partitions(
        [mp for sublist in mps for mp in sublist], *args, **kwargs
    )


def _load_and_concat_metapartitions_inner(mps, *args, **kwargs):
    return MetaPartition.concat_metapartitions(
        [mp.load_dataframes(*args, **kwargs) for mp in mps]
    )


@default_docs
def read_dataset_as_metapartitions_bag(
    dataset_uuid: Optional[str] = None,
    store: Optional[StoreFactory] = None,
    tables: Optional[str] = None,
    columns: Optional[List[str]] = None,
    concat_partitions_on_primary_index: bool = False,
    predicate_pushdown_to_io: bool = True,
    categoricals: Optional[Dict[str, List[str]]] = None,
    label_filter: Optional[Callable] = None,
    dates_as_object: bool = False,
    load_dataset_metadata: bool = False,
    predicates: PredicatesType = None,
    factory: Optional[DatasetFactory] = None,
    dispatch_by: Optional[List[str]] = None,
    partition_size: Optional[int] = None,
) -> db.Bag:
    """
    Retrieve dataset as `dask.bag` of `MetaPartition` objects.

    Parameters
    ----------

    Returns
    -------
    A dask.bag object containing the metapartions.
    """
    ds_factory = _ensure_factory(
        dataset_uuid=dataset_uuid,
        store=store,
        factory=factory,
        load_dataset_metadata=load_dataset_metadata,
    )
    store = ds_factory.store_factory
    mps = dispatch_metapartitions_from_factory(
        dataset_factory=ds_factory,
        concat_partitions_on_primary_index=concat_partitions_on_primary_index,
        label_filter=label_filter,
        predicates=predicates,
        dispatch_by=dispatch_by,
    )
    mp_bag = db.from_sequence(mps, partition_size=partition_size)
    del mps

    if concat_partitions_on_primary_index or dispatch_by:
        mp_bag = mp_bag.map(
            _load_and_concat_metapartitions_inner,
            store=store,
            tables=tables,
            columns=columns,
            categoricals=categoricals,
            predicate_pushdown_to_io=predicate_pushdown_to_io,
            dates_as_object=dates_as_object,
            predicates=predicates,
        )
    else:
        mp_bag = mp_bag.map(
            MetaPartition.load_dataframes,
            store=store,
            tables=tables,
            columns=columns,
            categoricals=categoricals,
            predicate_pushdown_to_io=predicate_pushdown_to_io,
            dates_as_object=dates_as_object,
            predicates=predicates,
        )

    categoricals_from_index = _maybe_get_categoricals_from_index(
        ds_factory, categoricals
    )

    if categoricals_from_index:
        func_dict: Dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = defaultdict(
            _identity
        )
        func_dict.update(
            {
                table: partial(_cast_categorical_to_index_cat, categories=cats)
                for table, cats in categoricals_from_index.items()
            }
        )
        mp_bag = mp_bag.map(MetaPartition.apply, func_dict, type_safe=True)
    return mp_bag


@default_docs
def read_dataset_as_dataframe_bag(
    dataset_uuid: Optional[str] = None,
    store: Optional[StoreFactory] = None,
    tables: Optional[str] = None,
    columns: Optional[List[str]] = None,
    concat_partitions_on_primary_index: bool = False,
    predicate_pushdown_to_io: bool = True,
    categoricals: Optional[List[str]] = None,
    label_filter: Optional[Callable] = None,
    dates_as_object: bool = False,
    predicates: PredicatesType = None,
    factory: Optional[DatasetFactory] = None,
    dispatch_by: Optional[List[str]] = None,
    partition_size: Optional[int] = None,
) -> db.Bag:
    """
    Retrieve data as dataframe from a `dask.bag` of `MetaPartition` objects

    Parameters
    ----------

    Returns
    -------
    dask.bag
        A dask.bag which contains the metapartitions and mapped to a function for retrieving the data.
    """
    mps = read_dataset_as_metapartitions_bag(
        dataset_uuid=dataset_uuid,
        store=store,
        factory=factory,
        tables=tables,
        columns=columns,
        concat_partitions_on_primary_index=concat_partitions_on_primary_index,
        predicate_pushdown_to_io=predicate_pushdown_to_io,
        categoricals=categoricals,
        label_filter=label_filter,
        dates_as_object=dates_as_object,
        load_dataset_metadata=False,
        predicates=predicates,
        dispatch_by=dispatch_by,
        partition_size=partition_size,
    )
    return mps.map(_get_data)


@default_docs
@normalize_args
def store_bag_as_dataset(
    bag: db.Bag,
    store: Callable,
    dataset_uuid: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    df_serializer: Optional[DataFrameSerializer] = None,
    overwrite: bool = False,
    metadata_merger: Callable = None,
    metadata_version: int = naming.DEFAULT_METADATA_VERSION,
    partition_on: Optional[List[str]] = None,
    metadata_storage_format: str = naming.DEFAULT_METADATA_STORAGE_FORMAT,
    secondary_indices: Optional[List[str]] = None,
) -> db.Bag:
    """
    Transform and store a dask.bag of dictionaries containing
    dataframes to a kartothek dataset in store.

    This is the dask.bag-equivalent of
    :func:`store_delayed_as_dataset`. See there
    for more detailed documentation on the different possible input types.

    Parameters
    ----------
    bag: dask.bag
        A dask bag containing dictionaries of dataframes or dataframes.

    Returns
    -------
    A dask.bag.Item dataset object.
    """
    _check_callable(store)
    if dataset_uuid is None:
        dataset_uuid = gen_uuid()

    if not overwrite:
        raise_if_dataset_exists(dataset_uuid=dataset_uuid, store=store)

    input_to_mps = partial(
        parse_input_to_metapartition, metadata_version=metadata_version
    )
    mps = bag.map(input_to_mps)

    if partition_on:
        mps = mps.map(MetaPartition.partition_on, partition_on=partition_on)

    if secondary_indices:
        mps = mps.map(MetaPartition.build_indices, columns=secondary_indices)

    mps = mps.map(
        MetaPartition.store_dataframes,
        store=store,
        df_serializer=df_serializer,
        dataset_uuid=dataset_uuid,
    )

    aggregate = partial(
        _store_dataset_from_partitions_flat,
        dataset_uuid=dataset_uuid,
        store=store,
        dataset_metadata=metadata,
        metadata_merger=metadata_merger,
        metadata_storage_format=metadata_storage_format,
    )

    return mps.reduction(perpartition=list, aggregate=aggregate, split_every=False)


@default_docs
def build_dataset_indices__bag(
    store: StoreFactory,
    dataset_uuid: str,
    columns: List[str],
    partition_size: Optional[int] = None,
    factory: Optional[DatasetFactory] = None,
) -> db.Bag:
    """
    Function which builds a :class:`~kartothek.core.index.ExplicitSecondaryIndex`.

    This function loads the dataset, computes the requested indices and writes
    the indices to the dataset. The dataset partitions itself are not mutated.

    Parameters
    ----------

    Returns
    -------
    A dask.delayed computation object.
    """
    ds_factory = _ensure_factory(
        dataset_uuid=dataset_uuid,
        store=store,
        factory=factory,
        load_dataset_metadata=False,
    )

    cols_to_load = {
        table: set(columns) & set(meta.names)
        for table, meta in ds_factory.table_meta.items()
    }
    cols_to_load = {table: cols for table, cols in cols_to_load.items() if cols}

    mps = dispatch_metapartitions_from_factory(ds_factory)

    return (
        db.from_sequence(seq=mps, partition_size=partition_size)
        .map(
            MetaPartition.load_dataframes,
            store=ds_factory.store_factory,
            tables=list(cols_to_load.keys()),
            columns=cols_to_load,
        )
        .map(MetaPartition.build_indices, columns=columns)
        .map(MetaPartition.remove_dataframes)
        .reduction(list, list, split_every=False, out_type=db.Bag)
        .flatten()
        .map_partitions(list)
        .map_partitions(
            update_indices_from_partitions, dataset_metadata_factory=ds_factory
        )
    )
