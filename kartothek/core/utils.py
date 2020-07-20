from typing import Union

from pydantic import SecretStr
from storefact import get_store_from_url

from kartothek.core.naming import MAX_METADATA_VERSION, MIN_METADATA_VERSION


def _check_callable(store_factory, obj_type="store"):
    if not callable(store_factory):
        raise TypeError("{} must be a factory function".format(obj_type))


def _verify_metadata_version(metadata_version):
    """
    This is factored out to be an easier target for mocking
    """
    if metadata_version < MIN_METADATA_VERSION:
        raise NotImplementedError(
            "Minimal supported metadata version is 4. You requested {metadata_version} instead.".format(
                metadata_version=metadata_version
            )
        )
    elif metadata_version > MAX_METADATA_VERSION:
        raise NotImplementedError(
            "Future metadata version `{}` encountered.".format(metadata_version)
        )


def verify_metadata_version(*args, **kwargs):
    return _verify_metadata_version(*args, **kwargs)


def ensure_string_type(obj):
    """
    Parse object passed to the function to `str`.

    If the object is of type `bytes`, it is decoded, otherwise a generic string representation of the object is
    returned.

    Parameters
    ----------
    obj: Any
        object which is to be parsed to `str`

    Returns
    -------
    str_obj: String
    """
    if isinstance(obj, bytes):
        return obj.decode()
    else:
        return str(obj)


class LazyStore:
    def __init__(self, store_url: Union[str, SecretStr], mask=True):
        if isinstance(store_url, str) and mask:
            store_url = SecretStr(store_url)
        self.mask = mask
        self._store_url = store_url

    def __call__(self):
        if self.mask:
            return get_store_from_url(self._store_url.get_secret_value())
        else:
            return get_store_from_url(self._store_url)
