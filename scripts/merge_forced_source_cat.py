import os
import sys

import numpy as np
import pandas as pd

from lsst.daf.persistence import Butler
from lsst.daf.persistence.butlerExceptions import NoResults

import GCRCatalogs
from GCRCatalogs.dc2_forced_source import DC2ForcedSourceCatalog


class DummyDC2ForcedSourceCatalog(GCRCatalogs.BaseGenericCatalog):
    """
    A dummy reader class that can be used to generate all native quantities
    required for the DPDD columns in DC2 Source Catalog
    """
    def __init__(self, schema_version=None):
        self._quantity_modifiers = DC2ForcedSourceCatalog._generate_modifiers(dm_schema_version=schema_version)

    @property
    def required_native_quantities(self):
        """
        the set of native quantities that are required by the quantity modifiers
        """
        return set(self._translate_quantities(self.list_all_quantities()))


def extract_and_save_visit(repo, visit, filename, object_table=None,
                           dm_schema_version=3,
                           overwrite=True, verbose=False, **kwargs):
    """Save catalogs to Parquet from visit-level forced source catalogs.

    Parameters
    --
    repo: str
        File location of Butler repository+rerun to load.
    visit: int
        Visit to process
    filename: str
        Filename for HDF file.
    overwrite: bool
        Overwrite an existing parquet file.
    """
    butler = Butler(repo)

    data_refs = butler.subset('forced_src', dataId={'visit': visit})

    columns_to_keep = list(DummyDC2ForcedSourceCatalog(dm_schema_version).required_native_quantities)

    collected_cats = pd.DataFrame()
    for dr in data_refs:
        if not dr.datasetExists():
            if verbose:
                print("Skipping non-existent dataset: ", dr.dataId)
            continue

        if verbose:
            print("Processing ", dr.dataId)
        src_cat = load_detector(dr, object_table=object_table,
                                columns_to_keep=columns_to_keep,
                                verbose=verbose, **kwargs)
        if len(src_cat) == 0:
            if verbose:
                print("  No good entries for ", dr.dataId)
            continue
        collected_cats = collected_cats.append(src_cat)

    if overwrite:
        if os.path.exists(filename):
            os.remove(filename)

    if len(collected_cats) == 0:
        if verbose:
            print("No sources collected from ", data_refs.dataId)
            return

    collected_cats.to_parquet(filename)


def load_detector(data_ref, object_table=None, matching_radius=1,
                  columns_to_keep=None, debug=False, **kwargs):
    """Load detector source catalog and associate sources with Object Table.

    Parameters
    --
    data_ref:
        Butler data_ref
    object_table:  AFW Table, Pandas DataFrame, or AstroPy Table
        Table of at least ('id', 'ra', 'dec') for spatial matching to assign
        the associated Object Id.
    matching_radius:  float [arcsec]
    columns_to_keep:  iterable
        If None, then keep all columns

    Returns
    --
    Pandas DataFrame of all sources for a visit in one catalog
    with photometric calibration and associated Object
    """
    cat = data_ref.get(datasetType='src')

    flux_field_names_per_schema_version = {
        1: {'psf_flux': 'base_PsfFlux_flux', 'psf_flux_err': 'base_PsfFlux_fluxSigma'},
        2: {'psf_flux': 'base_PsfFlux_flux', 'psf_flux_err': 'base_PsfFlux_fluxErr'},
        3: {'psf_flux': 'base_PsfFlux_instFlux', 'psf_flux_err': 'base_PsfFlux_instFluxErr'}
    }

    if debug:
        print("AFW photometry catalog schema version: {}".format(cat.schema.VERSION))
    flux_names = flux_field_names_per_schema_version[cat.schema.VERSION]

    # Get a Pandas DataFrame out that we can more easily manipulate:
    cat = cat.asAstropy().to_pandas()

    # Add visit, filter information as columns.
    # There's no separate metadata field so we redundantly include here
    # In the Parquet storage these columns will be categoricals and not take up much disk space.
    # Pandas automatically broadcasts a scalar to be the same shape as the DataFrame.
    cat['visit'] = data_ref.dataId['visit']
    cat['detector'] = data_ref.dataId['detector']
    cat['filter'] = data_ref.dataId['filter']

    # Calibrate magnitudes and fluxes
    calib = data_ref.get('calexp_calib')
    calib.setThrowOnNegativeFlux(False)

    mag, mag_err = calib.getMagnitude(cat[flux_names['psf_flux']].values,
                                      cat[flux_names['psf_flux_err']].values)

    cat['mag'] = mag
    cat['mag_err'] = mag_err
    cat['SNR'] = np.abs(cat[flux_names['psf_flux']] /
                        cat[flux_names['psf_flux_err']])

    # TODO Need to calibrate fluxes as well
    # For now we'll just save this information in an additional column
    # to be available further downstream.
    flux_mag0, flux_mag0_err = calib.getFluxMag0()
    cat['fluxmag0'] = flux_mag0

    # Restrict to columns that we need
    cat = cat[columns_to_keep]

    return cat


if __name__ == '__main__':
    from argparse import ArgumentParser, RawTextHelpFormatter
    usage = """
    Extract individual visit forced source table photometry
    """
    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument('repo', type=str,
                        help='Filepath to LSST DM Stack Butler repository.')
    parser.add_argument('--visits', type=int, nargs='+',
                        help='Visit IDs to process.')
    parser.add_argument('--visit_file', type=str, default=None,
                        help="""
A file of visit IDs to process.  One visit ID per line.
If both --visits and --visit_file are specified, then the entries in
visit_file are appended to the list specified in visits.
""")
    parser.add_argument('--name', default='src',
                        help='Base name of files: <name>_visit_0235062.parquet')
    parser.add_argument('--output_dir', default='./',
                        help='Output directory.  (default: %(default)s)')
    parser.add_argument('--verbose', dest='verbose', default=True,
                        action='store_true', help='Verbose mode.')
    parser.add_argument('--silent', dest='verbose', action='store_false',
                        help='Turn off verbosity.')
    parser.add_argument('--dm_schema_version', default=3,
                        help="""
The schema version of the DM tables.
v1: '_flux', '_fluxSigma'
v2: '_flux', '_fluxError'
v3: '_instFlux', '_instFluxError'
""")

    args = parser.parse_args(sys.argv[1:])

    if args.visit_file:
        if not args.visits:
            args.visits = []

        visits_from_file = np.loadtxt(args.visit_file, dtype=int)
        # Numpy 'int' defaults to 'numpy.int64' but then we have to
        # Explicitly force a conversion from 'numpy.int64' to 'int'
        # For reasons I don't fully understand,
        # the former doesn't work with the butler.subset call
        args.visits.extend([int(v) for v in visits_from_file])

    for visit in args.visits:
        filebase = '{:s}_visit_{:d}'.format(args.name, visit)
        filename = os.path.join(args.output_dir, filebase + '.parquet')
        extract_and_save_visit(args.repo, visit, filename,
                               dm_schema_version=args.dm_schema_version,
                               verbose=args.verbose)
