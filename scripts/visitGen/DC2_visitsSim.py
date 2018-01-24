# needs set up beforehand: code from https://github.com/humnaawan/DC1-Regions

import os
import numpy as np
import healpy as hp
from DC1code.intermediates import findFOVPixels
import imageio
import matplotlib.pyplot as plt

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
                 obsHistIndMin, obsHistIndMax,
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

    # find pixels related to each FOV for plotting purposes.
    FOVpixs= {}
    for fid in np.unique(fIDsList):
        centralRA, centralDec, FOVpixs[fid]= findFOVPixels(fid, simdata, nside= nside,
                                                            FOV_radius= radius)    
    filenames= []  # keep track for imageio
    ###########################################################################
    # loop over histIDs
    for index, hid in enumerate(obsIDsList[obsHistIndMin:obsHistIndMax]):
        fig, axes= plt.subplots(1,1)
        # plot the full protoDc2 region
        DC2pixRA, DC2pixDec= pixNum2RaDec(regionPixels_WFD, nside)
        axes.plot(DC2pixRA, DC2pixDec, 'o', color= 'g', alpha= 0.3, ms= 10, label= 'WFD footprint')
        # plot the uDDF region
        DC2pixRA, DC2pixDec= pixNum2RaDec(regionPixels_DD, nside)
        axes.plot(DC2pixRA, DC2pixDec, 'o', color= 'b', alpha= 0.3, ms= 10, label= 'uDDF footprint')
        
        # ok. now deal with the individual visit.
        i= np.where(obsIDsList==hid)[0][0]

        # plot the undithered FOV pixels
        fid= fIDsList[i]
        pixRA, pixDec= pixNum2RaDec(FOVpixs[fid], nside)
        axes.plot(pixRA, pixDec, '.', color= 'k', ms= 10, alpha= 0.5, label='undithered FOV')

        # dithered FOV
        ind= np.where(simdata['obsHistID']==hid)[0] # all the entries corresponding to the objID.
        pointingRA= simdata[ind][pointingRACol] # radians
        pointingDec= simdata[ind][pointingDecCol] # radians

        # plot the FOV boundary/center.    
        axes.plot(pointingRA, pointingDec, 'x', color= 'k',  mew= 2, ms= 10)
        axes.plot(pointingRA+delx/np.cos(pointingDec), pointingDec+dely, color= 'k',lw= 3, label= 'dithered FOV')

        # set up for title: include histIS, filter, rot angle
        title= '%s\nhID: %s ; fid: %s ; filter: %s'%(dataTag, obsIDsList[i], fIDsList[i], bandList[i])
        axes.set_title(title,  fontsize= fontsize, )

        # axes labels
        axes.set_xlabel('RA (rad)', fontsize= fontsize)
        axes.set_ylabel('Dec (rad)', fontsize= fontsize)

        # axes limits
        axes.set_xlim([np.radians(minRA-1), np.radians(maxRA+1)])
        axes.set_ylim([np.radians(minDec-1), np.radians(maxDec+1)])
        #axes.axis('equal')
        
        axes.tick_params(axis='x', labelsize=fontsize)
        axes.tick_params(axis='y', labelsize=fontsize)

        axes.legend(loc= "upper right", fontsize= fontsize)
        fig.set_size_inches(10,10)   
        
        # save each png.
        filename= '%sth.png'%index
        plt.savefig(filename, format= 'png', bbox_inches = 'tight', dpi= 40)
        filenames.append(filename)
        plt.close()

    # compile saved pngs; delete them after.
    images = []
    for filename in filenames:
        images.append(imageio.imread(filename))
        os.remove(filename)  # delete png file.
    
    if (obsHistIndMax==-1):  # asked to plot upto max index
        if (obsHistIndMin==0):
            movName= 'hIDs_All%s_%s_nside%s.gif'%(index, dataTag, nside)
        else:
            movName= 'hIDs_%s-%sIndex_%s_nside%s.gif'%(obsHistIndMin, index, dataTag, nside)
    else:
        movName= 'hIDs_%s-%sIndex_%s_nside%s.gif'%(obsHistIndMin, obsHistIndMax, dataTag, nside)
    imageio.mimsave(movName, images, **kargs)
    print('Saved ', movName)
    
    