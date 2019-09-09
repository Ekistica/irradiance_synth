from pandas.tseries.frequencies import to_offset
from pandas import concat, DataFrame, Series
import statsmodels.api as sm
import logging
log = logging.getLogger(__name__)

# TODO: Implement some other smoothers. Linear trend, for example.

def stitch(data, boundaries, error_model='additive', window_size='1H'):
    if isinstance(data, Series):
        return stitch_series(data, boundaries, error_model, window_size)
    elif isinstance(data, DataFrame):
        return concat([
            stitch_series(data[col], boundaries, error_model, window_size).rename(col)
            for col in data.columns
        ], axis=1)
    else:
        raise TypeError("`data` must be a pandas Series or DataFrame")


def stitch_series(series, boundaries, error_model='additive', window_size='1H'):
    """Stitch discontinuities in a pandas Series.

    Given a series of data that contains discontinuities, this function uses
    scatterplot smoothing to re-trend the data and remove the dicontinuities.
    
    The process works as follows:

    1) Fit two trend curves for each boundary point, one for the section of
       data just before the boundary, and another for the section of data just
       after the boundary.

    2) Using the two trends, decompose the original data to produce an an error
       signal. This is done using either an additive or multiplicative model,
       so that `trend + error == original`, or `trend * error == original`.

    3) Fit a third trend curve using the entire window around the boundary
       point.

    4) Add/multiply the error signals from step 2 back onto the trend signal
       from step 3.

    This process is applied for each boundary point found in the `boundaries`
    series.

    Parameters
    ----------
    series : pandas.Series
        The timeseries data containing discontinuities
    boundaries : pandas.DateTimeIndex
        The locations of the discontinuities
    error_model : str
        The error model for the smoothers, which must be either 'additive' or
        'multiplicative'.
    window_size : str
        A pandas date offset string (e.g "2H", or "3D") indicating the size of
        the window around each boundary to apply the smoothing
    """
    error_models = ('additive', 'multiplicative')

    offset = to_offset(window_size)
    ix = series.index 
    out = series.copy()

    for b in boundaries[1:-1]:
        orig = series[(ix >= (b - offset)) & (ix < (b + offset))]

        lr_noise = []
        for side in (orig[orig.index < b], orig[orig.index >= b]):
            trend = Series(
                sm.nonparametric.lowess(side.values, side.index, return_sorted=False),
                index=side.index
            )

            if error_model == 'additive':
                lr_noise.append(side - trend)
            elif error_model == 'multiplicative':
                lr_noise.append(side / trend)
            else:
                raise NotImplementedError("`error_model` must be one of", error_models)

        noise = concat(lr_noise)

        new_trend = Series(
            sm.nonparametric.lowess(orig, orig.index, return_sorted=False),
            index=orig.index
        )
    
        if error_model == 'additive':
            stitched = new_trend + noise
        elif error_model == 'multiplicative':
            stitched = new_trend * noise
        else:
            raise NotImplementedError("`error_model` must be one of", error_models)

        out[stitched.index] = stitched

    return out
