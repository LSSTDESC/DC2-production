# need LSST MAF loaded
# also need set up beforehand: code from https://github.com/humnaawan/DC1-Regions

import numpy as np
import healpy as hp
import pandas as pd
import lsst.sims.maf.db as db
import lsst.sims.maf.slicers as slicers
import lsst.sims.maf.metrics as metrics
import lsst.sims.maf.metricBundles as metricBundles
import lsst.sims.maf.utils as mafUtils

from astropy.coordinates import SkyCoord
from astropy import units as u
import os

__all__= ['findDC2RegionPixels', 'getSurveyHEALPixRADec',
          'findDC2RegionVisitsInfo', 'getSimData',
         'getDC2VisitList']

def returnXYZ(ra, dec):
    """
    Input: ra, dec (in degrees).
    Returns: corresponding Cartesian coords, x,y,z.
    
    Needed to run hp.query_polygon.
    """
    ra, dec= np.radians(ra), np.radians(dec)
    c = SkyCoord(ra=ra*u.radian, dec=dec*u.radian)
    return c.cartesian.xyz

def getSurveyHEALPixRADec(bundle):
    """

    Get the RA, Dec (in radians) corresponding to each HEALPix pixel.
    Method returns a list of all pixel numbers, based on the HEALPpix
    grid in the bundle object.
    # Modified version of getSurveyHEALPixRADec in DC1code.

    Required Parameter
    ------------------
    * bundle: a metricBundle object.

    """
    # create a list of pixelNumbers.
    pixelNum= []
    for pix in range(len(bundle.slicer)):
        if not bundle.metricValues.mask[pix]:   # only consider the unmasked pixels
            pixelNum.append(pix)

    return pixelNum

def findDC2RegionPixels(nside, regionCorners):
    """
    Returns the HEALPix pixels numbers inside the region defined
    by regionCorners. nside dependent.
    """
    corners= np.zeros(shape=(4,3))
    for i in range(len(regionCorners)):
        corners[i,]= returnXYZ(regionCorners[i][0], regionCorners[i][1])
    return hp.query_polygon(nside, vertices= corners, inclusive= True) # HEALpixel numbers

def findDC2RegionVisitsInfo(regionCorners, simdata, pointingRACol, pointingDecCol, nside):
    """
    Get the visits that fall in the region specified by regionCorners.
    Returns: regionPixels, obsIDs, fIDs, bands
    """
    regionPixels= findDC2RegionPixels(nside, regionCorners)
    hpSlicer= slicers.HealpixSlicer(nside= nside, lonCol= pointingRACol,
                                    latCol= pointingDecCol)
    hpSlicer.setupSlicer(simdata)    # slice data: know which pixels are observed in which visit
    
    obsIDs, fIDs, bands= [], [], []
    for p in regionPixels:
        ind = hpSlicer._sliceSimData(p)            
        obsIDs+= list(simdata[ind['idxs']]['obsHistID'])   # obsIDs corresponding to pixel p
        fIDs+= list(simdata[ind['idxs']]['fieldID'])   # fieldIDs corresponding to pixel p
        bands+= list(simdata[ind['idxs']]['filter'])   # filter corresponding to pixel p
        
    # clean up: get rid of repeated entries; consolidate the data from unique observations.
    obsIDs, fIDs, bands= np.array(obsIDs), np.array(fIDs), np.array(bands)
    obsIDs, ind= np.unique(obsIDs, return_index= True)
    fIDs, bands= fIDs[ind], bands[ind]
    
    return regionPixels, obsIDs, fIDs, bands


def getSimData(dbpath, surveyRegionTag, pointingRACol, pointingDecCol):
    """
    Get OpSim data for surveyRegionTag (WFD, DD). All filters.
    Columns returned:
    'fieldID', 'fieldRA', 'fieldDec', 'filter', pointingRACol, pointingDecCol
    """
    if (surveyRegionTag!='WFD') and (surveyRegionTag!='DD'):
        raise ValueError('surveyRegionTag must be either WFD or DD. Not %s'%surveyRegionTag)
    # access the data
    opsdb = db.OpsimDatabase(dbpath)
    propIds, propTags = opsdb.fetchPropInfo()
    sqlconstraint  = mafUtils.createSQLWhere(surveyRegionTag, propTags)
    colnames = ['fieldID', 'fieldRA', 'fieldDec', 'filter', pointingRACol, pointingDecCol]
    simdata = opsdb.fetchMetricData(colnames, sqlconstraint)

    return simdata


def getDC2VisitList(dbpath, simDataTag, surveyRegionTag, pointingRACol, pointingDecCol,
                    outDir, nside, regionCorners,
                    filters= ['u', 'g', 'r', 'i', 'z', 'y'], outFileTag= None):
    """
    Get the list of visits that fall in the region specified by regionCorners.
    surveyRegionTag: either WFD or DD
    
    Saved the visit list and regionPixel as a csv files. 
    Filnames are printed.
    
    Returns: simdata, regionPixels, obsIDsList, fIDsList, bandList
    """
    if (surveyRegionTag!='WFD') and (surveyRegionTag!='DD'):
        raise ValueError('surveyRegionTag must be either WFD or DD. Not %s'%surveyRegionTag)
    
    # need to set up a HEALPIx map within MAF.
    opsdb = db.OpsimDatabase(dbpath)
    # no sql constraint: want the entire pixel grid
    sqlconstraint = None
    
    resultsDb = db.ResultsDb(outDir=outDir)
    slicer= slicers.HealpixSlicer(nside= nside, lonCol= pointingRACol,  latCol= pointingDecCol, useCache=False)
    metric= metrics.MeanMetric(col='fiveSigmaDepth')  # no reason to choose fiveSigmaDepth; just need something.
    bundle = metricBundles.MetricBundle(metric, slicer, sqlconstraint= sqlconstraint)
    bgroup = metricBundles.MetricBundleGroup({'pixelGrid': bundle}, opsdb, outDir=outDir,
                                         resultsDb=resultsDb, saveEarly= False)
    # run the bundle
    bgroup.runAll()
    
    # ------ ------  ------ ------  ------ ------  ------ ------ 
    # get the ra, dec of HEALpix pixel centers
    pixelNum= getSurveyHEALPixRADec(bundle)
    
    # ------ ------  ------ ------  ------ ------  ------ ------ 
    # find the fIDs, obsIDs, bands in th region.
    print('\nFinding the visit list.')
    simdata= getSimData(dbpath, surveyRegionTag, pointingRACol, pointingDecCol) 
    out= findDC2RegionVisitsInfo(regionCorners, simdata, pointingRACol, pointingDecCol, nside)
    regionPixels, obsIDsList, fIDsList, bandList= out
  
    # print out stuff
    print('\n##Total number of unique visits in the region (across all bands): %s'%(len(obsIDsList)))
    for band in filters:
        ind= np.where(bandList==band)[0]
        print('\nTotal number of unique visits in the region for %s band: %s'%(band, len(obsIDsList[ind])))
        print('fIDs: %s'%(np.unique(fIDsList[ind])))
    
    # save the data: visit list
    if outFileTag is None: outFileTag= ''
    else: outFileTag= '_'+outFileTag
        
    filename= 'DC2VisitList_%s_%svisits_nside%s%s.csv'%(simDataTag, surveyRegionTag, nside, outFileTag)
    DF= pd.DataFrame({'obsHistID': obsIDsList, 'fID': fIDsList, 'band': bandList})
    currentDir= os.getcwd()
    
    dataDir= outDir+'visitLists/'
    if not os.path.exists(dataDir):
        os.makedirs(dataDir)
    os.chdir(dataDir)
    
    DF.to_csv(filename, index= False)
    print('\nSaved data in %s.\nOutdir: %s.'%(filename, dataDir))
    
    # save the data: pixels. for plotting purposes.
    filename= 'DC2RegionPixels_%s_%svisits_nside%s%s.csv'%(simDataTag, surveyRegionTag, nside, outFileTag)
    DF= pd.DataFrame({'regionPixels': regionPixels})
    
    dataDir= outDir+'regionPixels/'
    if not os.path.exists(dataDir):
        os.makedirs(dataDir)
    os.chdir(dataDir)
    DF.to_csv(filename, index= False)
    print('\nSaved data in %s.\nOutdir: %s.'%(filename, dataDir))
    os.chdir(currentDir)
    
    return simdata, regionPixels, obsIDsList, fIDsList, bandList


