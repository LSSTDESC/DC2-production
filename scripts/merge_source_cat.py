import os
import re
import sys

import numpy as np
import pandas as pd

from lsst.daf.persistence import Butler
from lsst.daf.persistence.butlerExceptions import NoResults

from astropy.coordinates import SkyCoord, matching
import astropy.units as u

import GCRCatalogs
from GCRCatalogs.dc2_source import DC2SourceCatalog


class DummyDC2SourceCatalog(GCRCatalogs.BaseGenericCatalog):
    """
    A dummy reader class that can be used to generate all native quantities
    required for the DPDD columns in DC2 Source Catalog
    """
    def __init__(self, schema_version=None):
        self._quantity_modifiers = DC2SourceCatalog._generate_modifiers(dm_schema_version=schema_version)

    @property
    def required_native_quantities(self):
        """
        the set of native quantities that are required by the quantity modifiers
        """
        return set(self._translate_quantities(self.list_all_quantities()))


def extract_and_save_visit(repo, visit, filename, object_table,
                           dm_schema_version=3,
                           overwrite=True, verbose=False, **kwargs):
    """Save catalogs to Parquet from visit-level source catalogs.

    Associates an ObjectID.
    Iterates through detectors, saving each in append mode to the save Parquet file.

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

    data_refs = butler.subset('src', dataId={'visit': visit})

    columns_to_keep = list(DummyDC2SourceCatalog(dm_schema_version).required_native_quantities)

    collected_cats = pd.DataFrame()
    for dr in data_refs:
        if not dr.datasetExists:
            if verbose:
                print("Skipping non-existent dataset: ", dr.dataId)
            continue

        if verbose:
            print("Processing ", dr.dataId)
        src_cat = load_detector(dr, object_table,
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

    collected_cats.to_parquet(filename)


def load_detector(data_ref, object_table, matching_radius=1,
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

    # Associate with closest
    object_id = associate_object_ids(cat, object_table, matching_radius=matching_radius)
    cat['objectId'] = object_id

    # Restrict to columns that we need
    cat = cat[columns_to_keep]

    return cat


def associate_object_ids(cat, object_table, matching_radius=1, verbose=True):
    """Return object ID associated with each entry in cat.

    Takes closest match within matching radius.
    Entries with no match will be -1.  (the index entries are integers so we can't use NaN as signaling value)

    Currently does purely position based matching above an implicit SNR
    Does not do a flux-based matching.

    Parameters
    --
    cat:  AFW table SourceCatalog
    object_table:
        AFW tabe SimpleCatalog
    matching_radius: float [arcsec]

    Notes
    --
    cat is assumed to have RA, Dec units of rad

    TODO
    --
    Testing rewriting this to use deepCoadd_ref AFW tables instead of Object Table.
    """
    # Let's first try the stupid way:
    # Explicitly iterate through cat
    # Construct a subset of object

    min_ra, max_ra = np.min(cat['coord_ra']), np.max(cat['coord_ra'])
    min_dec, max_dec = np.min(cat['coord_dec']), np.max(cat['coord_dec'])
    edge_buffer = np.deg2rad(10 * matching_radius / 3600)
    # Note this is wrong.  There's no cos(dec)
    ra_range = (np.rad2deg(min_ra - edge_buffer), np.rad2deg(max_ra + edge_buffer))
    dec_range = (np.rad2deg(min_dec - edge_buffer), np.rad2deg(max_dec + edge_buffer))

    if verbose:
        print("Extract objects from Object Table with RA, Dec in ranges:")
        print(ra_range, dec_range)
    in_source_catalog_area = (ra_range[0] < object_table['ra']) & \
                             (object_table['ra'] < ra_range[1]) & \
                             (dec_range[0] < object_table['dec']) & \
                             (object_table['dec'] < dec_range[1])
    this_object_table = object_table[in_source_catalog_area]
    if verbose:
        print("Found %d objects in Object Table in region" % len(this_object_table))

    this_object_table_skyCoor = SkyCoord(this_object_table['ra'], this_object_table['dec'], unit=(u.deg, u.deg))
    cat_skyCoor = SkyCoord(cat['coord_ra'], cat['coord_dec'], unit=(u.rad, u.rad))

    idx, sep2d, _ = matching.match_coordinates_sky(cat_skyCoor, this_object_table_skyCoor)
    associated_ids = np.asarray(this_object_table.iloc[idx]['id'], dtype=int)

    # Remove the associations that were too far away.
    w = sep2d > matching_radius * u.arcsec
    # sentinel value for no match.
    # Assumes input catalog uses only non-negative numbers for object Ids
    associated_ids[w] = -1

    return associated_ids


def prefix_columns(cat, filt, fields_to_skip=()):
    """Prefix the columns of an Pandas DataFrame with the filter name.

    >>> import pandas as pd
    >>> tab = pd.DataFrame({'letter': ['a', 'b'], 'number': [1, 2]})
    >>> prefix_columns(tab, 'filter')
    >>> print(tab)
    filter_letter filter_number
    ------------- -------------
                a             1
                b             2

    """
    old_colnames = list(cat.columns)
    for field in fields_to_skip:
        field_idx = old_colnames.index(field)
        old_colnames.pop(field_idx)

    transformation = {col: '%s_%s' % (filt, col) for col in old_colnames}
    cat.rename(index=str, columns=transformation, inplace=True)


if __name__ == '__main__':
    from argparse import ArgumentParser, RawTextHelpFormatter
    usage = """
    Generate merged source table photometry
    (based on individual visit photometry matched to deepCoadd object ID)
    """
    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument('repo', type=str,
                        help='Filepath to LSST DM Stack Butler repository.')
    parser.add_argument('reader', type=str,
                        help='Name of Object Table reader')
    parser.add_argument('--visits', type=int, nargs='+',
                        help='Visit IDs to process.')
    parser.add_argument('--name', default='src',
                        help='Base name of files: <name>_visit_0235062.hdf5')
    parser.add_argument('--output_dir', default='./',
                        help='Output directory.  (default: %(default)s)')
    parser.add_argument('--verbose', dest='verbose', default=True,
                        action='store_true', help='Verbose mode.')
    parser.add_argument('--silent', dest='verbose', action='store_false',
                        help='Turn off verbosity.')
    parser.add_argument('--hsc', dest='hsc', action='store_true',
                        help='Uses HSC filters')
    parser.add_argument('--dm_schema_version', default=3,
                        help="""
The schema version of the DM tables.
v1: '_flux', '_fluxSigma'
v2: '_flux', '_fluxError'
v3: '_instFlux', '_instFluxError'
""")

    args = parser.parse_args(sys.argv[1:])

    if args.hsc:
        filters = {'u': 'HSC-U', 'g': 'HSC-G', 'r': 'HSC-R', 'i': 'HSC-I',
                   'z': 'HSC-Z', 'y': 'HSC-Y'}
    else:
        filters = {'u': 'u', 'g': 'g', 'r': 'r', 'i': 'i', 'z': 'z', 'y': 'y'}

    cat = GCRCatalogs.load_catalog(args.reader)
    object_table = pd.DataFrame(cat.get_quantities(['id', 'ra', 'dec']))
    for visit in args.visits:
        filebase = '{:s}_visit_{:d}'.format(args.name, visit)
        filename = os.path.join(args.output_dir, filebase + '.parquet')
        extract_and_save_visit(args.repo, visit, filename, object_table,
                               dm_schema_version=args.dm_schema_version,
                               verbose=args.verbose,
                               filters=filters)
