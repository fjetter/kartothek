from typing import Callable, Tuple, Union

from intake.source.base import DataSource, Schema
from pydantic import SecretStr

import kartothek
from kartothek.core.factory import DatasetFactory
from kartothek.core.utils import LazyStore
from kartothek.io.dask.dataframe import read_dataset_as_ddf
from kartothek.io.iter import read_dataset_as_dataframes__iterator


class KartothekDatasetSource(DataSource):
    name = "kartothek"
    version = kartothek.__version__
    container = "dataframe"
    partition_access = True

    def __init__(
        self,
        dataset_uuid: str,
        store_url: Union[str, SecretStr, Callable],
        table="table",
        metadata=None,
        mask_store_url=False,
        **kwargs,
    ):
        """
        This implements a ``intake.source.base.DataSource`` to read kartothek datasets.


        .. warning::

            This is an experimental feature. The entire intake module might be dropped or changed without further notice.

        Parameters
        ----------
        dataset_uuid:
            The dataset UUID
        store_url:
            A storefact store URL
        table:
            The kartothek in-dataset table
        mask_store_url:
            Whether or not the store url shall be masked
        """
        super().__init__(metadata=metadata)
        self._store_fact: Union[LazyStore, Callable]
        if isinstance(store_url, str) or isinstance(store_url, SecretStr):
            self._store_fact = LazyStore(store_url, mask=mask_store_url)
            if mask_store_url:
                # intake actually inspects the stack to capture the original arguments. Nasty.
                self._captured_init_args: Tuple
                captured_args = list(self._captured_init_args)
                captured_args[1] = repr(self._store_fact._store_url)
                self._captured_init_args = tuple(captured_args)
        elif callable(store_url):
            # This is mainly to not have to rewrite the test suite and is
            # discouraged usage. We do not advertise this in the docs
            self._store_fact = store_url
        else:
            raise NotImplementedError()
        self.table = table
        self.dataset_uuid = dataset_uuid
        self.kwargs = kwargs
        self._ds_factory = None
        self.dataframe = None
        self._schema = None

    def arrow_schema(self):
        self._load_ds_factory()
        return self._ds_factory.table_meta[self.table].internal().remove_metadata()

    def _load_ds_factory(self):
        if self._ds_factory is None:
            self._ds_factory = DatasetFactory(
                dataset_uuid=self.dataset_uuid, store_factory=self._store_fact
            )

    def _load_dask_dataframe(self):
        self._load_ds_factory()
        if self.dataframe is None:
            self.dataframe = read_dataset_as_ddf(
                factory=self._ds_factory, **self.kwargs
            )

    def _get_schema(self):
        self._load_dask_dataframe()
        self.shape = (None, len(self.dataframe.columns))
        self.dtype = self.dataframe.dtypes.to_dict()
        self.npartitions = self.dataframe.npartitions
        if self._schema is None:
            self._schema = Schema(
                npartitions=self.npartitions,
                extra_metadata=self.metadata,
                dtype=self.dtype,
                shape=self.shape,
            )
        return self._schema

    def read_chunked(self):
        ds_iter = read_dataset_as_dataframes__iterator(
            factory=self._ds_factory, **self.kwargs
        )
        return map(lambda x: x[self.table], ds_iter)

    def _get_partition(self, i):
        self._get_schema()
        return self.dataframe.partitions[i].compute()

    def to_dask(self):
        self._get_schema()
        return self.dataframe

    def read(self):
        self._get_schema()
        return self.dataframe.compute()
