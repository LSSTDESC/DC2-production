import warnings
from collections import namedtuple
import numpy as np
import matplotlib.pyplot as plt
import lsst.afw.geom as afw_geom
import lsst.afw.table as afw_table
import lsst.daf.persistence as dp
import GCRCatalogs
with warnings.catch_warnings():
    warnings.filterwarnings('ignore')
    from desc.sims.GCRCatSimInterface import FieldRotator

plt.ion()

Coldef = namedtuple('Coldef', 'name type doc'.split())

def mag_cols(bands):
    return [Coldef('mag_{}'.format(x), float, '{}-magnitude'.format(x))
            for x in bands]

def make_SourceCatalog(new_cols):
    """Make a minimal SourceCatalog to contain positions, etc."""
    schema = afw_table.SourceTable.makeMinimalSchema()
    for coldef in new_cols:
        schema.addField(coldef.name, type=coldef.type, doc=coldef.doc)
    return afw_table.SourceCatalog(schema)

class PatchSelector:
    protoDC2_ra = 55.064
    protoDC2_dec = -29.783
    field_rotator = FieldRotator(0, 0, protoDC2_ra, protoDC2_dec)
    def __init__(self, butler, tract, patch_index):
        # Get the patch boundaries.
        skymap = butler.get('deepCoadd_skyMap')
        tractInfo = skymap[tract]
        wcs = tractInfo.getWcs()
        patchInfo = tractInfo.getPatchInfo(patch_index)
        patchBox = afw_geom.Box2D(patchInfo.getOuterBBox())
        patch_corners = patchBox.getCorners()
        ra_values, dec_values = [], []
        for corner in patch_corners:
            ra, dec = wcs.pixelToSky(corner)
            ra_values.append(ra.asDegrees())
            dec_values.append(dec.asDegrees())
        self.ra_range = min(ra_values), max(ra_values)
        self.dec_range = min(dec_values), max(dec_values)

    def __call__(self, gc, band, max_mag):
        # Retrieve the desired columns and filter on magnitude values.
        bandname = 'mag_true_{}_lsst'.format(band)
        filter_ = '{} < {}'.format(bandname, max_mag)
        print(filter_)
        gc_cols = gc.get_quantities(['galaxy_id', 'ra_true', 'dec_true',
                                     bandname], filters=[filter_])
        print(len(gc_cols[bandname]))
        # Rotate to the Run1.2p field.
        gc_ra_rot, gc_dec_rot \
            = self.field_rotator.transform(gc_cols['ra_true'],
                                           gc_cols['dec_true'])

        # Select the galaxies within the patch.
        index = np.where((gc_ra_rot > self.ra_range[0]) &
                         (gc_ra_rot < self.ra_range[1]) &
                         (gc_dec_rot > self.dec_range[0]) &
                         (gc_dec_rot < self.dec_range[1]))
        galaxy_id = gc_cols['galaxy_id'][index]
        gc_ra = gc_ra_rot[index]
        gc_dec = gc_dec_rot[index]
        gc_mag = gc_cols[bandname][index]
        print(len(galaxy_id))

        # Create a SourceCatalog with the galaxy_ids, coordinates, magnitudes
        galaxy_catalog = make_SourceCatalog(mag_cols((band,)))
        for id_, ra, dec, mag in zip(galaxy_id, gc_ra, gc_dec, gc_mag):
            record = galaxy_catalog.addNew()
            record.set('id', id_)
            record.set('coord_ra', afw_geom.Angle(ra, afw_geom.degrees))
            record.set('coord_dec', afw_geom.Angle(dec, afw_geom.degrees))
            record.set('mag_{}'.format(band), mag)
        return galaxy_catalog

# Create a data butler for the repo.
repo = '/global/projecta/projectdirs/lsst/global/in2p3/Run1.1-test2/output'
butler = dp.Butler(repo)

# Pick filter, tract, and patch.
filter_ = 'u'
mag_max = 24.5
#tract = 4850
#tract = 5063
tract = 4638
patch_index = 2, 2
patch_id = '{},{}'.format(*patch_index)

# Create function to down-select galaxy catalog entries to lie within
# a tract/patch.
patch_selector = PatchSelector(butler, tract, patch_index)

# Get the DRP catalog for a selected tract and patch
dataId = dict(tract=tract, patch=patch_id, filter=filter_)
coadd_catalog = butler.get('deepCoadd_meas', dataId=dataId)
coadd_calexp = butler.get('deepCoadd', dataId=dataId)
calib = coadd_calexp.getCalib()

# Select primary (deblended) DRP galaxies with mag < mag_max.
ext = coadd_catalog.get('base_ClassificationExtendedness_value')
model_flag = coadd_catalog.get('modelfit_CModel_flag')
model_flux = coadd_catalog.get('modelfit_CModel_flux')
is_primary = coadd_catalog.get('detect_isPrimary')
cat_temp = coadd_catalog.subset((ext == 1) &
                                (model_flag == False) &
                                (model_flux > 0) &
                                (is_primary == True))
mag = calib.getMagnitude(cat_temp['modelfit_CModel_flux'])
cat_temp = cat_temp.subset(mag < mag_max)

drp_catalog = make_SourceCatalog(mag_cols((filter_,)))
for record in cat_temp:
    new_rec = drp_catalog.addNew()
    for name in 'id coord_ra coord_dec parent'.split():
        new_rec.set(name, record[name])
    new_rec.set('mag_{}'.format(filter_),
                calib.getMagnitude(record['modelfit_CModel_flux']))

# Read in the galaxy catalog data.
with warnings.catch_warnings():
    warnings.filterwarnings('ignore')
    gc = GCRCatalogs.load_catalog('proto-dc2_v2.1.2_test')

# Create a SourceCatalog from the gc data, restricting to the
# tract/patch being considered.
galaxy_catalog = patch_selector(gc, band=filter_, max_mag=mag_max)

# Find positional matches within 100 milliarcseconds.
radius = afw_geom.Angle(0.1, afw_geom.arcseconds)
matches = afw_table.matchRaDec(drp_catalog, galaxy_catalog, radius)

# Compare magnitudes for matched objects.
drp_mag = np.zeros(len(matches), dtype=np.float)
gc_mag = np.zeros(len(matches), dtype=np.float)
sep = np.zeros(len(matches), dtype=np.float)
# Arrays for a quiver plot.
u = np.zeros(len(matches), dtype=np.float)
v = np.zeros(len(matches), dtype=np.float)
for i, match in enumerate(matches):
    drp_mag[i] = match.first['mag_{}'.format(filter_)]
    gc_mag[i] = match.second['mag_{}'.format(filter_)]
    sep[i] = np.degrees(match.distance)*3600.*1000.
    u[i] = match.first['coord_ra'] - match.second['coord_ra']
    v[i] = match.first['coord_dec'] - match.second['coord_dec']

title = 'Run1.1p, filter={}, tract={}, patch={}'.format(filter_, tract,
                                                        patch_id)
plt.rcParams['figure.figsize'] = 8, 8
fig = plt.figure()
frame_axes = fig.add_subplot(111, frameon=False)
frame_axes.set_title(title)
frame_axes.get_xaxis().set_ticks([])
frame_axes.get_yaxis().set_ticks([])

# Histogram of match separations.
fig.add_subplot(2, 2, 1)
plt.hist(sep, range=(0, 100), histtype='step', bins=40)
plt.xlabel('separation (marcsec)')
plt.ylabel('entries / bin')

# Quiver plot of (DRP - galaxy_catalog) positions on the sky.
fig.add_subplot(2, 2, 2)
plt.quiver(np.degrees(drp_catalog['coord_ra']),
           np.degrees(drp_catalog['coord_dec']),
           u, v)
plt.xlabel('RA (deg)')
plt.ylabel('Dec (deg)')

# Difference in magnitudes vs mag_gc.
fig.add_subplot(2, 2, 3)
plt.errorbar(gc_mag, gc_mag - drp_mag, fmt='.')
plt.xlabel('{}_gc'.format(filter_))
plt.ylabel('{0}_gc - {0}_drp'.format(filter_))

# Difference in magnitudes vs separation.
fig.add_subplot(2, 2, 4)
plt.errorbar(sep, gc_mag - drp_mag, fmt='.')
plt.xlabel('separation (marcsec)')
plt.ylabel('{0}_gc - {0}_drp'.format(filter_))

plt.tight_layout()
