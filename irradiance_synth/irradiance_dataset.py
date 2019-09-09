import pandas as pd
import numpy as np

import pvlib

import logging
log = logging.getLogger(__name__)

class IrradianceDataset(pd.DataFrame):
    _metadata = ['location', 'k_star_sensitivity', 'k_star_angle'] 

    @classmethod
    def _internal_ctor(cls, *args, **kwargs):
        kwargs['location'] = None
        return cls(*args, **kwargs)

    def __init__(self, data, location=None, index=None, columns=None, dtype=None, copy=True):
        super(IrradianceDataset, self).__init__(data=data,
                                          index=index,
                                          columns=columns,
                                          dtype=dtype,
                                          copy=copy)
        self.location = location
        self.k_star_sensitivity = 50
        self.k_star_angle = 7 

    @property
    def _constructor(self):
        return IrradianceDataset

    @property
    def _constructor_sliced(self):
        return IrradianceDatasetSeries

    @property
    def is_complete(self):
        """Returns true if this dataset has all three of ghi, dni, and dhi"""
        return {'ghi', 'dhi', 'dni'} <= set(self.columns) 
    
    @property
    def is_ghi_only(self):
        """Returns true if this dataset has only ghi"""
        c = self.columns
        return 'ghi' in c and 'dhi' not in c and 'dni' not in c

    def complete_irradiance(self):
        """This code adapted from pvlib."""
        cols = set(self.columns)

        if {'ghi', 'dhi'} <= cols and 'dni' not in cols:
            log.info("Completing DNI irradiance")
            self.loc[:, 'dni'] = pvlib.irradiance.dni(
                    self['ghi'],
                    self['dhi'],
                    self.sp.zenith,
                    clearsky_dni=self.clear.dni,
                    clearsky_tolerance=1.1)
            

        elif {'dni', 'dhi'} <= cols and 'ghi' not in cols:
            log.info("Completing GHI irradiance")
            self.loc[:, 'ghi'] = (
                self['dni'] * np.cos(np.radians(self.sp.zenith) +
                self['dhi']))

        elif {'dni', 'ghi'} <= cols and 'dhi' not in cols:
            log.info("Completing DHI irradiance")
            self.loc[:, 'dhi'] = (
                self['ghi'] - self['dni'] * np.cos(np.radians(self.sp.zenith))
            )
        return self
    
    @property
    def g(self):
        if len({'ghi', 'dni', 'dhi'} & set(self.columns)) == 0:
            log.info("No irradiance data. Trying to build from clearness index.")
            g = self.clear * self.k_star
            for col in g:
                self.loc[:, col] = g[col]
        
        if not (self.is_complete or self.is_ghi_only):
            self.complete_irradiance()

        return pd.DataFrame(
            self[[col for col in self.columns if col in ('ghi', 'dni', 'dhi')]]
        )

    @property
    def sp(self):
        sp_cols = list(filter(lambda c: c[:3] == 'sp_', self.columns))

        if len(sp_cols) == 0:
            log.info("Calculating solar position")
            sp = self.location.get_solarposition(self.index)
            sp_cols = [f'sp_{col}' for col in sp.columns]
            for old, new in zip(sp.columns, sp_cols):
                self.loc[:, new] = sp[old]
            
        return pd.DataFrame(
            self[sp_cols]
        ).rename(columns={col:col[3:] for col in sp_cols})

    @property
    def clear(self):
        clear_cols = list(filter(lambda c: c[:6] == 'clear_', self.columns))

        if len(clear_cols) == 0:
            # TODO add a check that the clear cols correspond to the g cols
            log.info("Calculating clear sky irradiance")
            clear = self.location.get_clearsky(self.index, solar_position=self.sp)
            clear_cols = [f'clear_{col}' for col in clear.columns if col in self.columns or f'k_{col}' in self.columns]
            for old, new in zip(clear.columns, clear_cols):
                self.loc[:, new] = clear[old]
            
        return pd.DataFrame(
            self[clear_cols]
        ).rename(columns={col:col[6:] for col in clear_cols})
    

    @property
    def k(self):
        k_cols = list(filter(lambda c: c[:2] == 'k_', self.columns))

        if len(k_cols) == 0:
            log.info("Calculating clearness index")
            k = self.g / self.clear
            k_cols = [f'k_{col}' for col in k.columns]
            for old, new in zip(k.columns, k_cols):
                self.loc[:, new] = k[old]
            
        return pd.DataFrame(
            self[k_cols]
        ).rename(columns={col:col[2:] for col in k_cols})
        return self

    @property
    def k_star(self):
        z = self.sp.zenith * np.pi / 180
        angle_above_cutoff = z - np.pi / 2 + np.pi * self.k_star_angle / 180
        is_above = 1/(1 + np.exp(self.k_star_sensitivity*angle_above_cutoff))
        return (self.k.mul(is_above, axis=0)
                    .replace(-np.inf, 0)
                    .replace(np.inf, 0)
                    .fillna(0)
                    .add(1 - is_above, axis=0))


class IrradianceDatasetSeries(pd.Series):
    _metadata = ['location', 'k_star_sensitivity', 'k_star_angle'] 

    @property
    def _constructor(self):
        return IrradianceDatasetSeries

    @property
    def _constructor_expanddim(self):
        return IrradianceDataset
