from __future__ import with_statement
import argparse
import os
import numpy as np
import gzip

from lsst.sims.catUtils.exampleCatalogDefinitions import PhoSimCatalogPoint
from lsst.sims.catalogs.definitions import InstanceCatalog
from lsst.sims.catalogs.decorators import cached

from GCRCatSimInterface import PhoSimDESCQA, bulgeDESCQAObject, diskDESCQAObject

class MaskedPhoSimCatalogPoint(PhoSimCatalogPoint):

    min_mag = None

    column_outputs = ['prefix', 'uniqueId', 'raPhoSim', 'decPhoSim', 'maskedMagNorm', 'sedFilepath',
                      'redshift', 'gamma1', 'gamma2', 'kappa', 'raOffset', 'decOffset',
                      'spatialmodel', 'internalExtinctionModel',
                      'galacticExtinctionModel', 'galacticAv', 'galacticRv']

    @cached
    def get_maskedMagNorm(self):
        raw_norm = self.column_by_name('phoSimMagNorm')
        if self.min_mag is None:
            return raw_norm
        return np.where(raw_norm<self.min_mag, self.min_mag, raw_norm)


class BrightStarCatalog(PhoSimCatalogPoint):

    min_mag = None

    @cached
    def get_isBright(self):
        raw_norm = self.column_by_name('phoSimMagNorm')
        return np.where(raw_norm<self.min_mag, raw_norm, None)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Generate an InstanceCatalog')
    parser.add_argument('--db', type=str,
                        default='minion_1016_sqlite_new_dithers.db',
                        help='path to the OpSim database to query')
    parser.add_argument('--descqa_cat_file', type=str,
                        default='proto-dc2_v2.0',
                        help='path to DESCQA catalog file')
    parser.add_argument('--out', type=str,
                        default='.',
                        help='directory where output will be written')
    parser.add_argument('--id', type=int, nargs='+',
                        default=None,
                        help='obsHistID to generate InstanceCatalog for (a list)')
    parser.add_argument('--dither', type=str,
                        default='True',
                        help='whether or not to apply dithering (true/false; default true)')
    parser.add_argument('--min_mag', type=float, default=10.0,
                        help='the minimum magintude for stars')
    parser.add_argument('--fov', type=float, default=2.0,
                        help='field of view radius in degrees')
    args = parser.parse_args()

    obshistid_list = args.id
    opsimdb = args.db
    out_dir = args.out
    dither_switch = True
    if args.dither.lower()[0] == 'f':
        dither_switch = False

    from lsst.sims.catUtils.utils import ObservationMetaDataGenerator

    if not os.path.exists(opsimdb):
        raise RuntimeError('%s does not exist' % opsimdb)

    obs_generator = ObservationMetaDataGenerator(database=opsimdb, driver='sqlite')

    from lsst.sims.catUtils.exampleCatalogDefinitions import PhoSimCatalogSersic2D
    from lsst.sims.catUtils.exampleCatalogDefinitions import PhoSimCatalogZPoint
    from lsst.sims.catUtils.exampleCatalogDefinitions import DefaultPhoSimHeaderMap
    from lsst.sims.catUtils.baseCatalogModels import StarObj
    from lsst.sims.catUtils.baseCatalogModels import GalaxyBulgeObj, GalaxyDiskObj
    from lsst.sims.catUtils.baseCatalogModels import GalaxyAgnObj
    from lsst.sims.utils import _getRotSkyPos
    import copy

    star_db = StarObj(database='LSSTCATSIM', host='fatboy.phys.washington.edu',
                      port=1433, driver='mssql+pymssql')

    bulge_db = GalaxyBulgeObj(connection=star_db.connection)
    disk_db = GalaxyDiskObj(connection=star_db.connection)
    agn_db = GalaxyAgnObj(connection=star_db.connection)

    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    phosim_header_map = copy.deepcopy(DefaultPhoSimHeaderMap)
    phosim_header_map['nsnap'] = 1
    phosim_header_map['vistime'] = 30.0
    phosim_header_map['camconfig'] = 1
    for obshistid in obshistid_list:

        obs_list = obs_generator.getObservationMetaData(obsHistID=obshistid,
                                                        boundType='circle',
                                                        boundLength=args.fov)

        obs = obs_list[0]
        if dither_switch:
            print 'dithering'
            obs.pointingRA = np.degrees(obs.OpsimMetaData['randomDitherFieldPerVisitRA'])
            obs.pointingDec = np.degrees(obs.OpsimMetaData['randomDitherFieldPerVisitDec'])
            rotSky = _getRotSkyPos(obs._pointingRA, obs._pointingDec, obs,
                                   obs.OpsimMetaData['ditheredRotTelPos'])

            obs.rotSkyPos = np.degrees(rotSky)
            obs.OpsimMetaData['rotTelPos'] = obs.OpsimMetaData['ditheredRotTelPos']

        cat_name = os.path.join(out_dir,'phosim_cat_%d.txt' % obshistid)
        star_name = 'star_cat_%d.txt' % obshistid
        gal_name = 'gal_cat_%d.txt' % obshistid
        agn_name = 'agn_cat_%d.txt' % obshistid

        cat = PhoSimCatalogPoint(star_db, obs_metadata=obs)
        cat.phoSimHeaderMap = phosim_header_map
        with open(cat_name, 'w') as output:
            cat.write_header(output)
            output.write('includeobj %s.gz\n' % star_name)
            output.write('includeobj %s.gz\n' % gal_name)
            output.write('includeobj %s.gz\n' % agn_name)

        star_cat = MaskedPhoSimCatalogPoint(star_db, obs_metadata=obs)
        star_cat.phoSimHeaderMap = phosim_header_map
        bright_cat = BrightStarCatalog(star_db, obs_metadata=obs, cannot_be_null=['isBright'])
        star_cat.min_mag = args.min_mag
        bright_cat.min_mag = args.min_mag

        from lsst.sims.catalogs.definitions import parallelCatalogWriter
        cat_dict = {}
        cat_dict[os.path.join(out_dir, star_name)] = star_cat
        cat_dict[os.path.join(out_dir, 'bright_stars_%d.txt' % obshistid)] = bright_cat
        parallelCatalogWriter(cat_dict, chunk_size=100000, write_header=False)

        db_bulge = bulgeDESCQAObject(args.descqa_cat_file)
        cat = PhoSimDESCQA(db_bulge, obs_metadata=obs, cannot_be_null=['hasBulge'])
        cat.phoSimHeaderMap = DefaultPhoSimHeaderMap
        cat.write_catalog(os.path.join(out_dir, gal_name), chunk_size=100000,
                          write_header=False)

        db_disk = diskDESCQAOject(args.descqa_cat_file)
        cat = PhoSimDESCQA(db_disk, obs_metadata=obs, cannot_be_null=['hasDisk'])
        cat.write_catalog(os.path.join(out_dir, gal_name), chunk_size=100000,
                          write_mode='a', write_header=False)

        for orig_name in (star_name, gal_name, agn_name):
            full_name = os.path.join(out_dir, orig_name)
            with open(full_name, 'r') as input_file:
                with gzip.open(full_name+'.gz', 'w') as output_file:
                    output_file.writelines(input_file)
            os.unlink(full_name)
