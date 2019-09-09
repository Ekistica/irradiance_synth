from numpy.linalg import norm
from numpy.random import choice
from numpy import nan_to_num, ones
from pandas import Series

import logging
log = logging.getLogger(__name__)

class PoolSelector:
    def get_pool(self, input_keys, target_key):
        pass

class NullPoolSelector(PoolSelector):
    def get_pool(self, input_keys, target_key):
        return input_keys

class FunctionPoolSelector(PoolSelector):
    def __init__(self, f):
        self.f = f

    def get_pool(self, input_keys, target_key):
        return lambda target_key: [
                k for k in input_keys
                if self.f(k, target_key)
        ]
    
class WeightedRandomPoolSelector(PoolSelector):
    def __init__(self, input_vectors, target_vectors, norm_ord=2):
        self.input_vectors = input_vectors
        self.target_vectors = target_vectors
        self.norm_ord = norm_ord

    def get_pool(self, input_keys, target_key):
        target_vector = self.target_vectors.loc[target_key, :].values
        # TODO: Normalise this vect_diff i.e. divide by std and subtract mean?
        # Needs some consideration
        vect_diff = self.input_vectors.loc[input_keys, :] - target_vector

        weights = 1/(0.00001 + norm(vect_diff, ord=self.norm_ord, axis=1) ** 2)
        weights = nan_to_num(weights, 0)
        if weights.sum() == 0:
            log.warn("Warning, bad target vector. Using uniform random sampling")
            weights = ones(len(weights))
        p = weights / weights.sum()
        return list(vect_diff.sample(1, weights=p).index)


class KNNPoolSelector(PoolSelector):
    def __init__(self, input_vectors, target_vectors, k=1, norm_ord=2):
        self.input_vectors = input_vectors
        self.target_vectors = target_vectors
        self.k = k
        self.norm_ord = norm_ord

    def get_pool(self, input_keys, target_key):
        target_vector = self.target_vectors.loc[target_key, :].values
        vect_diff = self.input_vectors - target_vector
        norms = Series(norm(vect_diff, ord=self.norm_ord, axis=1), index=vect_diff.index)
        sorted_norms = norms.sort_values(ascending=True)
        return list(sorted_norms.head(self.k).index)

class SeasonalPoolSelector(PoolSelector):
    def __init__(self, k=1):
        pass

