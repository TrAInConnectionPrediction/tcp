from cityhash import CityHash64


def hash64(to_hash: str) -> int:
    """
    Create int64 hash from input. Compatible with postgres BIGINT as the hash is signed.

    Parameters
    ----------
    to_hash : str
        String to hash

    Returns
    -------
    int
        Hashed input as int64
    """
    return CityHash64(to_hash) - ((2 ** 63) - 1)
