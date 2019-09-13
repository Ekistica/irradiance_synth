from pandas import date_range, Series, DatetimeIndex, concat
from pandas.core.generic import NDFrame
from pandas.tseries.frequencies import to_offset
from numpy import array
from numpy.random import choice, seed

from irradiance_synth.ts_bootstrap.stitch import stitch
from irradiance_synth.ts_bootstrap.pool_selector import NullPoolSelector

def ts_bootstrap(data, index, chunk_size='D', pool_selector=None, random_seed=None, stitch_boundaries=False):
    """Sample from chunks of a timeseries or timedataframe to produce a new series or dataframe with a given index.

    The new data is assembled in chunks of a fixed `chunk_size` (a pandas offset string).

    Parameters
    ----------
    data : pandas.NDFrame
        The timeseries data to utilise
    index : pandas.DateTimeIndex
        The "destination" index that we want to produce data onto.
        Note that `index` must have a freq attribute defined i.e. it must be a fixed frequency index
    chunk_size : str
        A pandas date offset string (e.g. 'W' for week, 'A' for year, 'M' for month)
        This defines the size of chunks to be sampled; the output series is assembled in chunks of this size.

    pool_selector: 

    random_seed : Number
        The random seed to pass to numpy when sampling. If left as None, resampling
        will produce non-deterministic samples. Passing any other value will ensure
        that the same "random" sample is always produced for the same inputs.

    TODO
    ----
    * allow a user-defined aggregation/interpolation method if the source data needs resampling
    """
    if pool_selector is None:
        pool_selector = NullPoolSelector()

    if not isinstance(data, NDFrame):
        raise Exception(f"expected series :: pandas.NDFrame, got {type(data)}")

    if not isinstance(index, DatetimeIndex):
        raise Exception(f"expected index :: pandas.DatetimeIndex, got {type(index)}")

    if index.freq is None:
        raise Exception("index must have a fixed freq attribute.")

    if random_seed is not None:
        seed(random_seed)

    # the keys for the chunks of the new index that we need to fill
    dest_keys = date_range(index[0], index[-1], freq=chunk_size)

    # resample the input data so that it is in the same frequency as the target index
    # TODO: aggregation function should be customisable
    resampled_input = data.resample(index.freq).mean().dropna()

    # use resample again to split our input data into chunks that we can sample from
    resampler = resampled_input.resample(chunk_size)

    def set_index(chunk, bin_id):
        # given a chunk and a target key, try to assign the index
        if chunk is None:
            return None
        chunk.index = date_range(start=bin_id, periods=len(chunk), freq=index.freq)
        return chunk

    # iterate over the destination keys, yielding a pool of source chunks
    chunk_pools = (pool_selector.get_pool(list(resampler.groups.keys()), key) for key in dest_keys)

    # iterate over the pools, and select a random chunk from each
    chunks = (
        resampler.get_group(choice(chunk_pool))
        for chunk_pool in chunk_pools
    )

    # reindex each chunk using the destination keys
    reindexed_chunks = (set_index(chunk, key) for chunk, key in zip(chunks, dest_keys))

    
    # concat all the chunks into a pandas series
    out = concat(reindexed_chunks)

    if stitch_boundaries:
        # TODO: pass in window size for stitching
        return stitch(out, dest_keys[1:])
    else:
        return out
