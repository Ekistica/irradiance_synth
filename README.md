# Irradiance Data Synthesis

This package contains an example implementation of a technique for generating high resolution irradiance datasets by sampling clearness index measurements from a secondary location. The code and image blow illusatrate the overall technique.


```python
import irradiance_synth

# a script for loading some useful example datasets is included
import datasets
hawaii = datasets.load_hawaii_3s()
alice = datasets.load_alice_15m()

synth = irradiance_synth.IrradianceSynthesizer(source=hawaii)
output = synth.synthesize(target=alice['2017-12'], chunk_size='D')
```

![Rainbow example](https://github.com/Ekistica/irradiance_synth/raw/master/figures/rainbow_example.png?s=500)

## Datasets

This repository contains some routines for reading publically available irradiance datasets. However, the datasets themselves are not included. See [here](https://github.com/Ekistica/irradiance_synth/blob/master/datasets/README.md) for more detail.
