import numpy as np
import pandas as pd
import pvlib

import logging
log = logging.getLogger(__name__)

import irradiance_synth.ts_bootstrap as ts_bootstrap
from irradiance_synth import IrradianceDataset

from importlib import reload

def default_feature_space(data, chunk_size):
    if chunk_size == 'D':
        test_data = data.replace(1.0, np.nan)
    else:
        test_data = data
    out = pd.DataFrame(test_data.resample(chunk_size).agg([np.mean]))
    return out

class IrradianceSynthesizer:
    """Synthesize high-res irradiance data by clear-sky decomposition and weighter sampling"""

    def __init__(self, source_irradiance):
        self.source = source_irradiance
        
            
    def synthesize(self, target_irradiance, chunk_size='D', feature_space=None, sampling_method='weighted'):
        if feature_space is None:
            feature_space = default_feature_space

        target = target_irradiance.k_star.ghi
        # target.index = target.index.tz_localize(None)

        source = self.source.k_star.ghi
        # source.index = source.index.tz_localize(None)

        out_ix = pd.date_range(
            target.index[0],
            target.index[-1],
            freq=source.index.freq,
            tz=target.index.tz
        )

        log.info("Generating feature space")
        target_features = feature_space(target, chunk_size)
        source_features = feature_space(source.resample(target.index.freq).mean(), chunk_size)

        if sampling_method == 'weighted':
            selector = ts_bootstrap.WeightedRandomPoolSelector(source_features, target_features)
        elif sampling_method == 'nearest':
            selector = ts_bootstrap.KNNPoolSelector(source_features, target_features, k=1)
        else:
            raise ValueError("Sampling method must be one of 'weighted' or 'nearest'.")

        log.info("Generating high res clearness index samples")

        # Drop all of these columns from the samples
        sp_cols = list(filter(lambda c: c[:3] == 'sp_', self.source.columns))
        clear_cols = list(filter(lambda c: c[:6] == 'clear_', self.source.columns))
        k_cols = list(filter(lambda c: c[:2] == 'k_', self.source.columns))
        irrad_cols = list(filter(lambda c: c in ('ghi', 'dhi', 'dni'), self.source.columns))
        drop_cols = sp_cols + clear_cols + irrad_cols + k_cols

        k_star = self.source.k_star
        k_star.columns = [f'k_{col}' for col in k_star.columns]

        src = self.source.drop(columns=drop_cols)
        if len(src.columns):
            src = pd.concat([src, k_star], axis=1)
        else:
            src = k_star

        out = ts_bootstrap.ts_bootstrap(
            src,
            out_ix,
            chunk_size=chunk_size,
            pool_selector=selector,
            stitch_boundaries=False
        )

        out = out.asfreq(self.source.index.freq)
        # out.index = out.index.tz_localize(target_irradiance.location.tz)

        return IrradianceDataset(out, location=target_irradiance.location)

