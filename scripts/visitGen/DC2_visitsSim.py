# needs set up beforehand: code from https://github.com/humnaawan/DC1-Regions
import os
import numpy as np
import healpy as hp
import imageio
import matplotlib.pyplot as plt
from astropy import units as u
from astropy.coordinates import SkyCoord

def pixNum2RaDec(nside, pix):
    """
    Convert from HEALpix pixels number to ra, dec.
    """
    theta, phi = hp.pix2ang(pix, nside)
    return phi, np.pi/2.0-theta

def DC2VisitsSim(outDir, dataTag, simdata, pointingRACol, pointingDecCol,
                 obsIDsList, fIDsList, bandList,
                 regionPixels_WFD, regionPixels_DD,
                 nside,
                 obsHistIndMin, obsHistIndMax, dpi= 40,
                 minRA= None, maxRA= None, minDec= None, maxDec= None):
    """
    Code to produce a GIF.
    obsHistIndMin, obsHistIndMax specify the indices on the obsIDsList
    to loop over. e.g.,
    obsHistIndMin, obsHistIndMax= 0, 10 will plot first ten visits.
    """
    # plot helpers
    kargs= {'fps': 2}
    fontsize= 14

    # set up output directory.
    if not os.path.exists(outDir):
        os.makedirs(outDir)
    os.chdir(outDir)

    # set up for circular FOV footprints.
    stepsize = np.pi/50.
    theta = np.arange(0, np.pi*2.+stepsize, stepsize)
    radius= 0.0305
    delx= radius*np.cos(theta)
    dely= radius*np.sin(theta)
    
    filenames= []  # keep track for imageio
    ###########################################################################
    # loop over histIDs
    for index, hid in enumerate(obsIDsList[obsHistIndMin:obsHistIndMax]):
        fig, axes= plt.subplots(1,1)
        # plot the full protoDc2 region
        DC2pixRA, DC2pixDec= pixNum2RaDec(regionPixels_WFD, nside)
        axes.plot(np.degrees(DC2pixRA), np.degrees(DC2pixDec), 'o',
                  color= 'g', alpha= 0.3, ms= 10, label= 'WFD footprint')
        # plot the uDDF region
        DC2pixRA, DC2pixDec= pixNum2RaDec(regionPixels_DD, nside)
        axes.plot(np.degrees(DC2pixRA), np.degrees(DC2pixDec), 'o',
                  color= 'b', alpha= 0.3, ms= 10, label= 'uDDF footprint')
        
        # ok. now deal with the individual visit.
        i= np.where(obsIDsList==hid)[0][0]

        # --
        # plot the undithered FOV with a circular boundary with an x at the center
        ind= np.where(simdata['obsHistID']==hid)[0] # all the entries corresponding to the objID.
        pointingRA= simdata[ind]['fieldRA'] # radians
        pointingDec= simdata[ind]['fieldDec'] # radians

        # plot the undithered FOV boundary/center.
        #axes.plot(np.degrees(pointingRA), np.degrees(pointingDec), 'x',
        #          alpha= 0.5,
        #          color= 'k',  mew= 2, ms= 10)
        axes.plot(np.degrees(pointingRA+delx/np.cos(pointingDec)),
                  np.degrees(pointingDec+dely), ':',
                  alpha= 0.5,
                  color= 'k',lw= 3, label= 'undithered FOV')
        # --
        
        # plot the dithered FOV pixels
        pointingRA= simdata[ind][pointingRACol] # radians
        pointingDec= simdata[ind][pointingDecCol] # radians
        c = SkyCoord(ra=pointingRA*u.radian, dec= pointingDec*u.radian)
        regionPixels= hp.query_disc(nside= nside, vec=c.cartesian.xyz, radius= radius)
        pixRA, pixDec= pixNum2RaDec(regionPixels, nside)
        axes.plot(np.degrees(pixRA), np.degrees(pixDec), '.',
                  color= 'k', ms= 10, alpha= 0.7, label='dithered FOV')

        # set up for title: include histIS, filter, rot angle
        title= '%s\nhID: %s ; fid: %s ; filter: %s'%(dataTag, obsIDsList[i], fIDsList[i], bandList[i])
        axes.set_title(title,  fontsize= fontsize, )

        # axes labels
        axes.set_xlabel('RA (deg)', fontsize= fontsize)
        axes.set_ylabel('Dec (deg)', fontsize= fontsize)

        # axes limits
        axes.set_xlim([minRA-1, maxRA+1])
        axes.set_ylim([minDec-1, maxDec+1])
        #axes.axis('equal')
        
        axes.invert_xaxis()
        axes.tick_params(axis='x', labelsize=fontsize)
        axes.tick_params(axis='y', labelsize=fontsize)

        axes.legend(loc= "lower left", fontsize= fontsize)
        fig.set_size_inches(10,10)   
        
        # save each png.
        filename= '%sth.png'%index
        plt.savefig(filename, format= 'png', bbox_inches = 'tight', dpi= dpi)
        filenames.append(filename)
        plt.close()

    # compile saved pngs; delete them after.
    images = []
    for filename in filenames:
        images.append(imageio.imread(filename))
        os.remove(filename)  # delete png file.
    
    if (obsHistIndMax==-1):  # asked to plot upto max index
        if (obsHistIndMin==0):
            movName= 'hIDs_All%s_%s_nside%s.gif'%(index+2, dataTag, nside)
        else:
            movName= 'hIDs_%s-%sIndex_%s_nside%s.gif'%(obsHistIndMin, index+1, dataTag, nside)
    else:
        movName= 'hIDs_%s-%sIndex_%s_nside%s.gif'%(obsHistIndMin, obsHistIndMax, dataTag, nside)
    imageio.mimsave(movName, images, **kargs)
    print('Saved ', movName)
    
    