import time
from functools import lru_cache, partial, update_wrapper


def lru_cache_time(time_to_last, maxsize=128):
    """
    Adds time aware caching to lru_cache
    """
    def wrapper(func):
        # Lazy function that makes sure the lru_cache() invalidate after X secs
        ttl_hash = time.time() // time_to_last
        
        @lru_cache(maxsize)
        def time_aware(__ttl, *args, **kwargs):
            """
            Main wrapper, note that the first argument ttl is not passed down. 
            This is because no function should bother to know this that 
            this is here.
            """
            def wrapping(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapping(*args, **kwargs)
        return update_wrapper(partial(time_aware, ttl_hash), func)
    return wrapper
