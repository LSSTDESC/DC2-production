import os
import sys

import numpy as np
import pandas as pd

from lsst.geom import radians
try:
    from lsst.geom import SpherePoint
except ImportError:
    from lsst.afw.geom.spherePoint import SpherePoint

import lsst.daf.base
from lsst.daf.persistence import Butler
from lsst.daf.persistence.butlerExceptions import NoResults

from astropy.coordinates import SkyCoord, matching
import astropy.units as u

import GCRCatalogs
from GCRCatalogs.dc2_source import DC2SourceCatalog
from GCRCatalogs.dc2_dia_source import DC2DiaSourceCatalog


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


class DummyDC2DiaSourceCatalog(GCRCatalogs.BaseGenericCatalog):
    """
    A dummy reader class that can be used to generate all native quantities
    required for the DPDD columns in DC2 Source Catalog
    """
    def __init__(self, schema_version=None):
        self._quantity_modifiers = DC2DiaSourceCatalog._generate_modifiers(dm_schema_version=schema_version)

    @property
    def required_native_quantities(self):
        """
        the set of native quantities that are required by the quantity modifiers
        """
        return set(self._translate_quantities(self.list_all_quantities()))


def extract_and_save_visit(butler, visit, filename,
                           dataset='src',
                           object_table=None,
                           object_dataset=None,
                           dm_schema_version=3,
                           overwrite=True, verbose=False, debug=False,
                           **kwargs):
    """Save catalogs to Parquet from visit-level source catalogs.

    Associates an ObjectID.
    Iterates through detectors, then saves all to a Parquet file.

    Parameters
    --
    butler:  Butler object to use to load
    visit: int
        Visit to process
    filename: str
        Filename for output Parquet file.
    overwrite: bool
        Overwrite an existing parquet file.
    """
    visit = str(visit)
    data_refs = butler.subset(dataset, dataId={'visit': visit})
    if debug:
        print("DATA_REFS: ", data_refs)

    if dataset == 'deepDiff_diaSrc':
        dummy_catalog = DummyDC2DiaSourceCatalog(dm_schema_version)
    else:
        dummy_catalog = DummyDC2SourceCatalog(dm_schema_version)

    columns_to_keep = list(dummy_catalog.required_native_quantities)

    collected_cats = pd.DataFrame()
    for dr in data_refs:
        if not dr.datasetExists():
            if verbose:
                print("Skipping non-existent dataset: ", dr.dataId)
            continue

        if verbose:
            print("Processing ", dr.dataId)
        src_cat = load_detector(dr,
                                dataset=dataset,
                                object_table=object_table,
                                object_dataset=object_dataset,
                                columns_to_keep=columns_to_keep,
                                verbose=verbose, debug=debug, **kwargs)
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


def load_detector(data_ref,
                  dataset='src',
                  object_table=None,
                  object_dataset=None,
                  matching_radius=1,
                  columns_to_keep=None,
                  debug=False,
                  **kwargs):
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
    cat = data_ref.get(datasetType=dataset)
    # Here we get the calexp soley to get the MJD
    calexp_visitInfo = data_ref.get(datasetType='calexp_visitInfo')

    flux_field_names_per_schema_version = {
        1: {'psf_flux': 'base_PsfFlux_flux', 'psf_flux_err': 'base_PsfFlux_fluxSigma'},
        2: {'psf_flux': 'base_PsfFlux_flux', 'psf_flux_err': 'base_PsfFlux_fluxErr'},
        3: {'psf_flux': 'base_PsfFlux_instFlux', 'psf_flux_err': 'base_PsfFlux_instFluxErr'},
        4: {'psf_flux': 'base_PsfFlux_instFlux', 'psf_flux_err': 'base_PsfFlux_instFluxErr'},
    }

    if debug:
        print("AFW photometry catalog schema version: {}".format(cat.schema.VERSION))
    flux_names = flux_field_names_per_schema_version[cat.schema.VERSION]

    # Get a Pandas DataFrame out that we can more easily manipulate:
    cat = cat.asAstropy().to_pandas()
    if debug:
        print("Looking at {} entries".format(len(cat)))

    # Add visit, filter information as columns.
    # There's no separate metadata field so we redundantly include here
    # In the Parquet storage these columns will be categoricals and not take up much disk space.
    # Pandas automatically broadcasts a scalar to be the same shape as the DataFrame.
    cat['visit'] = data_ref.dataId['visit']
    cat['detector'] = data_ref.dataId['detector']
    cat['filter'] = data_ref.dataId['filter']

    mjd = calexp_visitInfo.getDate().get(lsst.daf.base.DateTime.MJD)  # TAI MJD
    cat['mjd'] = mjd

    # Calibrate magnitudes and fluxes
    calib_dataset_map = {'src': 'calexp', 'deepDiff_diaSrc': 'deepDiff_differenceExp'}
    try:
        calib = data_ref.get(datasetType=calib_dataset_map[dataset]+'_photoCalib')
    except AttributeError:
        calib = data_ref.get(datasetType=calib_dataset_map[dataset]+'_calib')

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

    if object_table is not None or object_dataset is not None:
        # Associate with closest
        object_id = associate_object_ids(cat,
                                         data_ref=data_ref,
                                         object_table=object_table,
                                         object_dataset=object_dataset,
                                         matching_radius=matching_radius,
                                         **kwargs)
        cat['objectId'] = object_id

    # Restrict to columns that we need
    cat = cat[columns_to_keep]

    return cat


def associate_object_ids(cat, data_ref=None, object_table=None, object_dataset=None, **kwargs):
    """Wrapper for development to easily switch
    Object-Table based
    coadd file based

    associate_object_ids
    """
    if object_dataset is not None:
        associated_ids = associate_object_ids_to_coadd(cat,
                                                       data_ref=data_ref,
                                                       object_dataset=object_dataset,
                                                       **kwargs)
    elif object_table is not None:
        associated_ids = associate_object_ids_to_table(cat, object_table=object_table, **kwargs)

    return associated_ids


def associate_object_ids_to_coadd(cat, data_ref=None,
                                  object_dataset='deepCoadd_ref',
                                  verbose=True, **kwargs):
    """Load and match to deepCoadd or deepDiff_diaObject references."""

    skymap = data_ref.get(datasetType='deepCoadd_skyMap')

    # radians
    min_ra, max_ra = np.min(cat['coord_ra']), np.max(cat['coord_ra'])
    min_dec, max_dec = np.min(cat['coord_dec']), np.max(cat['coord_dec'])

    coord_list = [
        SpherePoint(min_ra * radians, min_dec * radians),
        SpherePoint(max_ra * radians, min_dec * radians),
        SpherePoint(max_ra * radians, max_dec * radians),
        SpherePoint(min_ra * radians, max_dec * radians),
    ]

    # This will be a list of tracts that overlap
    # Each tract entry will have its own list of patches that overlap
    tract_patches = skymap.findTractPatchList(coord_list)

    associated_id_table = []
    for tract, patches in tract_patches:
        for patch in patches:
            tract_patch_data_id = {}
            tract_patch_data_id['tract'] = tract.getId()
            # Yes, really, the following is the supported way to get the
            # patch ID in a way that can be queried.
            patch_str = '%d,%d' % patch.getIndex()
            tract_patch_data_id['patch'] = patch_str
            if verbose:
                print("Searching ", tract_patch_data_id)
            try:
                ref_table = data_ref.getButler().get(datasetType=object_dataset,
                                                     dataId=tract_patch_data_id)
            except NoResults:
                if verbose:
                    print("No results found for ", tract_patch_data_id)
                continue

            ref_table = ref_table.asAstropy().to_pandas()
            # Rename RA, Dec columns to match associate_object_ids_to_table
            # expectations
            ref_table['ra'] = np.rad2deg(ref_table['coord_ra'])
            ref_table['dec'] = np.rad2deg(ref_table['coord_dec'])
            # Restrict ref_table to isPrimary?

            # We get back and array of matching object IDs
            # Cat rows with no matches get a -1 entry.
            these_associated_ids = associate_object_ids_to_table(cat, object_table=ref_table,
                                                                 verbose=verbose, **kwargs)
            associated_id_table.append(these_associated_ids)

    # Now merge the lists together, taking the last non-negative entry
    # TODO Could consider checking for an inconsistent entry.
    associated_ids = np.zeros(len(cat), dtype=int) - 1
    for matches in associated_id_table:
        w, = np.where(matches >= 0)
        associated_ids[w] = matches[w]

    return associated_ids


def associate_object_ids_to_table(cat, object_table=None, matching_radius=1,
                                  verbose=True):
    """Return object ID associated with each entry in cat.

    Takes closest match within matching radius.
    Entries with no match will be -1.  (the index entries are integers so we can't use NaN as signaling value)

    Currently does purely position based matching above an implicit SNR
    Does not do a flux-based matching.

    Parameters
    --
    cat: pandas DataFrame
    object_table:  pandas DataFrame
    matching_radius: float [arcsec]

    Notes
    --
    cat and object_table are assumed to have RA, Dec units of rad

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

    if len(this_object_table) < 1:
        # Return an array of -1
        return np.zeros(len(cat)) - 1

    this_object_table_skyCoor = SkyCoord(this_object_table['ra'],
                                         this_object_table['dec'],
                                         unit=(u.deg, u.deg))
    cat_skyCoor = SkyCoord(cat['coord_ra'], cat['coord_dec'], unit=(u.rad, u.rad))

    idx, sep2d, _ = matching.match_coordinates_sky(cat_skyCoor, this_object_table_skyCoor)
    associated_ids = np.asarray(this_object_table.iloc[idx]['id'], dtype=int)

    # Remove the associations that were too far away.
    w = sep2d > matching_radius * u.arcsec
    # sentinel value for no match.
    # Assumes input catalog uses only non-negative numbers for object Ids
    associated_ids[w] = -1

    return associated_ids


def unique_in_order(possible_duplicates):
    """Remove duplicates from a list or array while maintaining order

    1. Get index of unique elements.  This will be sorted in element order
    2. Sort this index list.  That will return the list in index order.
    3. Define the new array based on this sorted index.

    If passed a list, then return a list, otherwise a numpy array.

    >>> foo = [0, 1, 2]
    >>> print(unique_in_order(foo))
    [0, 1, 2]

    >>> foo = [2, 0, 1, 1, 2, 1, 0]
    >>> print(repr(unique_in_order(foo)))
    [2, 0, 1]

    >>> foo = [2, 2, 0, 0, 1, 2]
    >>> print(repr(unique_in_order(foo)))
    [2, 0, 1]

    >>> foo = ['b', 'a', 'a', 'c', 'd', 'e', 'a']
    >>> print(repr(unique_in_order(foo)))
    ['b', 'a', 'c', 'd', 'e']

    >>> foo = np.array(['b', 'a', 'a', 'c', 'd', 'e', 'a'], dtype='<U1')
    >>> print(repr(unique_in_order(foo)))
    array(['b', 'a', 'c', 'd', 'e'], dtype='<U1')
    """
    if isinstance(possible_duplicates, list):
        return_type = 'list'
    else:
        return_type = 'array'

    possible_duplicates = np.asarray(possible_duplicates)
    _, idx = np.unique(possible_duplicates, return_index=True)
    no_duplicates = possible_duplicates[np.sort(idx)]
    if return_type == 'list':
        return list(no_duplicates)
    else:
        return no_duplicates


if __name__ == '__main__':
    from argparse import ArgumentParser, RawTextHelpFormatter
    usage = """
    Extract individual visit source table photometry

    The individual visit photometry is matched to deepCoadd object ID.

    The matching can be done either against an Object Table from a GCR reader
    or individually to each merged detection reference file from the coadd.
    The numbers will come out the same.

    * If an Object Table reader is provided through '--object_reader', then the catalog
    read by that Generic Catalog Reader will be used to match Object IDs.

    * If this reader is not provided, then source catalogs will be matched against
    the the merged detection reference coadds
    (which are what are processed to generate the Object Table).

    Comparison of approaches
    --
    The Object Table reader approach requires DPDD Object Table to be locally available
    It requires 2.5 GB per process to load the table into memory for Run 1.2.
    For larger catalogs, this memory usage will increase.
    It also requires the Object Table to have been generated.

    Running without the Object Table loaded into memory is more naturally parallel
    It can be run prior to the generation of an Object Table.
    However, matching directly to the merged coadd catalogs is ~5 times slower
    because of the slow performance in reading AFW tables from FITS files.
    Compounding this, each merged reference detection catalog is loaded multiple times
    as many visits different will cover a given tract+path on the sky.
    """
    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument('repo', type=str,
                        help='Filepath to LSST DM Stack Butler repository.')
    parser.add_argument('--dataset', type=str, default='src',
                        help="""
Butler catalog dataset type.
E.g., "src", "deepCoadd_diaSrc"(default: %(default)s)
""")
    parser.add_argument('--object_reader', type=str, default=None,
                        help='Name of Object Table reader.')
    parser.add_argument('--object_dataset', type=str, default=None,
                        help='Name of Object dataset type.  E.g., "deepCoadd", "deepDiff_diaObject".')
    parser.add_argument('--base_dir', default=None,
                        help='Override the base_dir setting of the reader.  This is motivated by the need to run on different file systems due to problems sometimes locking files for access from the compute nodes.')
    parser.add_argument('--visits', type=int, nargs='+',
                        help='Visit IDs to process.')
    parser.add_argument('--visit_file', type=str, default=None,
                        help="""
A file of visit IDs to process.  One visit ID per line.
If both --visits and --visit_file are specified, then the entries in
visit_file are appended to the list specified in visits.
""")
    parser.add_argument('--radius', default=1,
                        help="""
Matching radius for object association [arcsec].  (default: %(default)s'
""")
    parser.add_argument('--output_name', default='src',
                        help='Base name of files: <output_name>_visit_0235062.parquet')
    parser.add_argument('--output_dir', default='./',
                        help='Output directory.  (default: %(default)s)')
    parser.add_argument('--verbose', dest='verbose', default=True,
                        action='store_true', help='Verbose mode.')
    parser.add_argument('--silent', dest='verbose', action='store_false',
                        help='Turn off verbosity.')
    parser.add_argument('--debug', dest='debug', default=True,
                        action='store_true', help='Debug mode.')
    parser.add_argument('--dm_schema_version', default=3,
                        help="""
The schema version of the DM tables.
v1: '_flux', '_fluxSigma'
v2: '_flux', '_fluxError'
v3: '_instFlux', '_instFluxError'
""")

    args = parser.parse_args(sys.argv[1:])

    object_table = None
    if args.object_reader:
        config_override = {}
        if args.base_dir:
            config_override['base_dir'] = args.base_dir
        cat = GCRCatalogs.load_catalog(args.object_reader,
                                       config_overwrite=config_override)
        id_col = 'objectId'
        # Crude way to define ID column based on reader name.
        if args.object_reader.index('dia_object'):
            id_col = 'diaObjectId'

        object_table = pd.DataFrame(cat.get_quantities([id_col, 'ra', 'dec']))
        # Standardize name of ID column in DataFrame
        object_table = object_table.rename(index=str, columns={id_col: 'id'})

        del cat

    if args.visit_file:
        if not args.visits:
            args.visits = []

        visits_from_file = np.loadtxt(args.visit_file, dtype=int, ndmin=1)
        # Numpy 'int' defaults to 'numpy.int64' but then we have to
        # Explicitly force a conversion from 'numpy.int64' to 'int'
        # For reasons I don't fully understand,
        # the former doesn't work with the butler.subset call
        args.visits.extend([int(v) for v in visits_from_file])

    args.visits = unique_in_order(args.visits)

    butler = Butler(args.repo)
    for visit in args.visits:
        filebase = '{:s}_visit_{:d}'.format(args.output_name, visit)
        filename = os.path.join(args.output_dir, filebase + '.parquet')
        extract_and_save_visit(butler, visit, filename,
                               dataset=args.dataset,
                               object_dataset=args.object_dataset,
                               object_table=object_table,
                               matching_radius=args.radius,
                               dm_schema_version=args.dm_schema_version,
                               verbose=args.verbose, debug=args.debug)
