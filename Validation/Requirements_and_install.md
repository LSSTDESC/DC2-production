# Requirements and install instructions

## DC2_matching_and_depth

In order to use this notebook you will need:

* The DM stack (in particular `lsst.daf.persistence` you can check [here](pipelines.lsst.io).
* `scikit-learn` (`pip install scikit-learn` or `conda install scikit-learn`).
* `glob` (`pip install glob`).
* The LSST sims stack (in particular `lsst.sims.utils` you can check [here](pipelines.lsst.io)).
* `astropy` (`pip install astropy` or `conda install astropy`).
* `healpy` (`pip install healpy`). 
* `GCRCatalogs` (check [here](https://github.com/LSSTDESC/gcr-catalogs) for instructions).

## DC2_rough_angular_power

In order to use this notebook you will need:

* `healpy` (`pip install healpy`)
* `astropy` (`pip install astropy` or `conda install astropy`)
* `flatmaps` (included in this directory, no install needed)
* `pymaster` (follow instructions [here](https://github.com/damonge/NaMaster/NERSC_installation.md))
* `h5py` (`pip install h5py`)
* `CCL` (follow instructions [here](https://github.com/LSSTDESC/CCL))

For questions please ping `@fjaviersanchez` on Slack or open an issue in this repo.
