from setuptools import setup, find_packages

setup(
    name='Irradiance Synthesis',
    version='0.0.1',
    description='Proof-of-concept code for irradiance sampling synthesis',
    python_requires='>=3.5',
    packages=find_packages(exclude=['tests', 'figures', 'datasets']),
    install_requires=['numpy', 'pandas', 'pvlib', 'statsmodels']
)
