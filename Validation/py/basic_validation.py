from __future__ import print_function
import numpy as np
import matplotlib.pyplot as plt
import healpy as hp
from sklearn.neighbors import KDTree
from scipy.stats import binned_statistic
import astropy.wcs
import astropy.table
import astropy.units as u
import re
import sys
import lsst.daf.persistence
import matplotlib
matplotlib.rcParams.update({'font.size': 14})

### Utilities
def spatial_closest_mag_1band(ra_data,dec_data,mag_data,
                              ra_true,dec_true,mag_true,true_id,
                              rmax=3,max_deltamag=1.):
    """
    Function to return the closest match in magnitude within a user-defined radius within certain
    magnitude difference.
    
    ***Caveats***: This method uses small angle approximation sin(theta)
    ~ theta for the declination axis. This should be fine to find the closest
    neighbor. This method does not use any weighting.
    
    Args:
    -----
    
    ra_data: Right ascension of the measured objects (degrees).
    dec_data: Declination of the measured objects (degrees).
    mag_data: Measured magnitude of the objects.
    ra_true: Right ascension of the true catalog (degrees).
    dec_true: Declination of the true catalog (degrees).
    mag_true: True magnitude of the true catalog.
    true_id: Array of IDs in the true catalog.
    rmax: Maximum distance in number of pixels to perform the query.
    max_deltamag: Maximum magnitude difference for the match to be good.
    
    Returns:
    --------
    
    dist: Distance to the closest neighbor in the true catalog. If inputs are
    in degrees, the returned distance is in arcseconds.
    true_id: ID in the true catalog for the closest match.
    matched: True if matched, False if not matched.
    """
    X = np.zeros((len(ra_true),2))
    X[:,0] = ra_true
    X[:,1] = dec_true
    tree = KDTree(X,metric='euclidean')
    Y = np.zeros((len(ra_data),2))
    Y[:,0] = ra_data
    Y[:,1] = dec_data
    ind,dist= tree.query_radius(Y,r=rmax*0.2/3600,return_distance=True)
    matched = np.zeros(len(ind),dtype=bool)
    ids = np.zeros(len(ind),dtype=true_id.dtype)
    dist_out = np.zeros(len(ind))
    for i, ilist in enumerate(ind):
        if len(ilist)>0:
            dmag = np.fabs(mag_true[ilist]-mag_data[i])
            good_ind = np.argmin(dmag)
            ids[i]=true_id[ilist[good_ind]]
            dist_out[i]=dist[i][good_ind]
            if np.min(dmag)<max_deltamag:
                matched[i]=True
            else:
                matched[i]=False
        else:
            ids[i]=-99
            matched[i]=False
            dist_out[i]=-99.
    return dist_out*3600., ids,matched

def get_depth_map(ra, dec, mags, snr, nside=128, min_snr=4, max_snr=6):
    """ Routine to quickly compute the depth given position, magnitude and SNR
    
    Args:
    -----
    ra: ndarray (float),
        RA of the detected objects in degrees
    dec: ndarray (float),
        Dec of the detected objects in degrees
    mags: ndarray (float),
        Magnitude of the detected objects 
    snr: ndarray (float).
        Array containing the SNR of the detected objects
    
    """
    # Filter NaNs and select objects in the range of SNR that we care about to speed things up
    good = (~np.isnan(ra)) & (~np.isnan(dec)) & (snr >= min_snr) & (snr <= max_snr) 
    pix_nums = hp.ang2pix(nside,ra[good],dec[good], lonlat=True)
    map_out = np.zeros(12*nside**2)
    for px in np.unique(pix_nums):
        mask = px==pix_nums
        if np.count_nonzero(mask)>0:
            map_out[px]=np.nanmedian(mags[good][mask]) 
        else:
            map_out[px]=0.
    return map_out

def make_hp_map(ra,dec,nside=128):
    good = (~np.isnan(ra)) & (~np.isnan(dec))
    pix_nums = hp.ang2pix(nside,ra[good],dec[good], lonlat=True)
    pix_counts = np.bincount(pix_nums,minlength=12*nside**2)
    return pix_counts

def plot_magnitude_difference(mag_true, mag_meas, mag_range=(10,30), bins=50, savename='mag_diff.png'):
    delta_mag = mag_meas-mag_true # We check the magnitude difference
    mean_im, be, _ = binned_statistic(mag_true, delta_mag, range=mag_range, bins=bins, statistic='median')
    std_im, be, _ = binned_statistic(mag_true, delta_mag ,range=mag_range, bins=bins, statistic='std')
    n_im, be, _ = binned_statistic(mag_true, delta_mag, range=mag_range, bins=bins, statistic='count')
    n_true, be, _ = binned_statistic(mag_true, mag_true, range=mag_range, bins=bins, statistic='count')

    f, ax = plt.subplots(nrows=1, ncols=2)
    ax[0].errorbar(0.5*be[1:]+0.5*be[:-1], mean_im,std_im/np.sqrt(n_im), fmt='o', color='red')
    im = ax[0].hexbin(mag_true, delta_mag, gridsize=bins, extent=[14,26,-0.5,0.5])
    ax[0].set_xlabel('mag$_{true}$', fontsize=16)
    ax[0].set_ylabel('mag$_{PSF}$-mag$_{true}$', fontsize=16)
    plt.colorbar(im, label='Objects/bin')
    ax[0].grid()
    ax[0].set_ylim(-0.1,0.1)
    ax[0].set_xlim(14,26);
    ax[1].hist(delta_mag, range=(-0.1,0.1), bins=bins, histtype='step')
    ax[1].set_xlabel('mag$_{PSF}$-mag$_{true}$',fontsize=14)
    ax[1].set_ylabel('Number of objects',fontsize=14)
    #ax[2].errorbar(0.5*(be[1:]+be[:-1]),1.0*n_im/n_true,np.sqrt(n_im+n_true)/n_true,fmt='o')
    #ax[2].set_xlabel('mag$_{true}$',fontsize=12)
    #ax[2].set_ylabel('Detection efficiency (stars)',fontsize=12)
    #ax[2].grid()
    plt.tight_layout()
    f.savefig(savename, bbox_to_inches='tight')

def check_astrometry(ra_data, ra_true, dec_data, dec_true, savename):
    delta_ra = ra_data - ra_true
    delta_ra = 3600*1000*delta_ra # If RA in degrees, delta_ra in mas
    delta_dec = 3600*1000*(dec_data - dec_true) # delta_dec in mas
    f, ax = plt.subplots(1,1)
    ax.hist(np.cos(dec_data)*delta_ra, range=(-200,200), bins=100, label=r'$\Delta$ RA')
    ax.hist(delta_dec, range=(-200, 200), bins=100, label=r'$\Delta$ Dec')
    ax.set_xlabel(r'$\Delta X$ [mas]', fontsize=16)
    ax.set_ylabel(r'Objects/bin', fontsize=16)
    f.savefig(savename)
    print('Median astrometric deviation', np.median(np.sqrt(delta_ra**2+delta_dec**2)))
    print('LSST-SRD max = 50 mas')

def plot_PSF_size(T, star_mask, mag_true, mag_range=(10,30), bins=50, savename='PSF_T_test.png'):
    mean_im_t, be, _ = binned_statistic(mag_true[star_mask], T[star_mask], range=mag_range, bins=bins, statistic='median')
    std_im_t, be, _ = binned_statistic(mag_true[star_mask], T[star_mask], range=mag_range, bins=bins, statistic='std')
    n_im_t, be, _ = binned_statistic(mag_true[star_mask], T[star_mask], range=mag_range, bins=bins, statistic='count')
    f, ax = plt.subplots(nrows=1, ncols=2)
    ax[0].scatter(mag_true[star_mask], T[star_mask], c='b', s=0.4, alpha=0.2, label='stars')
    ax[0].scatter(mag_true[~star_mask][::10],T[~star_mask][::10], c='r', s=0.4, alpha=0.2, label='galaxies') # We only show 10% of the galaxies
    ax[0].errorbar(0.5*be[1:]+0.5*be[:-1], mean_im_t, std_im_t/np.sqrt(n_im_t), fmt='o', c='orange', label='stars median')
    ax[0].set_ylabel('$T$ [arcsec$^{2}$]',fontsize=16)
    ax[0].set_xlabel('mag$_{r,true}$',fontsize=16)
    ax[0].grid()
    ax[0].set_ylim(0.,1.)
    ax[0].set_xlim(14,24)
    ax[0].legend(loc='best')
    bc = 0.5*(be[1:]+be[:-1])
    ax[1].errorbar(bc,mean_im_t-np.nanmean(mean_im_t[(bc>20) & (bc<22)]),std_im_t/np.sqrt(n_im_t),fmt='o')
    ax[1].set_ylabel(r'$\Delta T$ [arcsec$^{2}$]',fontsize=16)
    ax[1].set_xlabel('mag$_{r,true}$',fontsize=16)
    ax[1].grid()
    ax[1].set_ylim(-0.01,0.01)
    plt.tight_layout()
    f.savefig(savename, bbox_to_inches='tight')

def check_astrometry(ra_data, dec_data, ra_true, dec_true, wcs, mjd, savename=None, use_seaborn=True, use_sigmaG=True, **kwargs):
    """
    Function to produce astrometry QA plots
    
    Args:
    -----
    
    ra_data: ndarray, measured right ascension (in degrees).
    dec_data: ndarray, measured declination (in degrees).
    ra_true: ndarray, input right ascension (in degrees).
    dec_true: ndarray, input declination (in degrees).
    wcs: astropy.wcs.WCS, WCS of one of the sensors in the visit to check
    kwargs: keyword arguments to pass to seaborn.jointplot or to matplotlib.pyplot.hexbin
    """
    # Compute astrometry residuals
    d_ra = np.cos(np.radians(dec_data))*(ra_data - ra_true)
    d_dec = dec_data - dec_true
    d_ra = 3600*1000*d_ra # To milliarcseconds
    d_dec = 3600*1000*d_dec # To milliarcseconds
    
    # Compute orientation of focal plane's axes
    
    xy_vec = np.array([[0,0],[0,300], [300,0]]) # We check 300 px up and to the right
    radec = wcs.all_pix2world(xy_vec,0)
    # Y axis
    dx_y = (radec[1,0]-radec[0,0])*np.cos(np.radians(radec[0,1]))*3600 # To make the arrow 60 mas long
    dy_y = (radec[1,1]-radec[0,1])*3600
    # X axis
    dx_x = (radec[2,0]-radec[0,0])*np.cos(np.radians(radec[0,1]))*3600 # To make the arrow 60 mas long 
    dy_x = (radec[2,1]-radec[0,1])*3600
    
    # Get zenith's orientation
    
    cerro_pachon = astropy.coordinates.EarthLocation.of_site('Cerro Pachon')
    aa_frame = astropy.coordinates.AltAz(obstime=astropy.time.Time(mjd, format='mjd'), location=cerro_pachon)
    sky_coord = astropy.coordinates.SkyCoord(alt=np.array([0,10])*u.deg, az=np.array([0,0])*u.deg, frame=aa_frame)
    radec = sky_coord.icrs
    dx_z = ((radec.ra[1]-radec.ra[0])*np.cos(radec.dec[1].to(u.rad)))/u.deg
    dy_z = (radec.dec[1]-radec.dec[0])/u.deg
    mod = np.sqrt(dx_z**2+dy_z**2) #length in deg
    dx_z = dx_z*50/mod # We want the arrow to be 50 mas long
    dy_z = dy_z*50/mod
    
    # Some stats
    if use_sigmaG:
        sigma_ra = 0.741*(np.percentile(d_ra,75)-np.percentile(d_ra,25))
        sigma_dec = 0.741*(np.percentile(d_dec,75)-np.percentile(d_dec,25))
    else:
        sigma_ra = np.std(d_ra)
        sigma_dec = np.std(d_dec)
    mean_ra = np.mean(d_ra)
    mean_dec = np.mean(d_dec)
    # Make plots
    if use_seaborn:
        d_ra = pd.Series(d_ra, name=r'$\Delta_{1} \equiv \Delta \rm{RA}$ $\cos{(\rm{Dec})}$ [mas]')
        d_dec = pd.Series(d_dec, name=r'$\Delta_{2} \equiv \Delta \rm{Dec}$ [mas]')
        p1 = sns.jointplot(d_ra, d_dec, **kwargs, marginal_kws={'hist':True, 'kde': True})
        p1.plot_joint(sns.kdeplot)
        ax = p1.ax_joint
        ax.annotate('+x', (dx_x+10, dy_x+10), color='r')
        ax.annotate('+y', (dx_y+10, dy_y+10), color='r')
        ax.annotate('Zenith', (dx_z-30, dy_z-20), color='g')
        ax.annotate(r'$\langle \Delta_{1} \rangle, \sigma_{\Delta_{1}} = (%.1f, %.1f$) [mas]' % (mean_ra, sigma_ra), (90,90), color='black')
        ax.annotate(r'$\langle \Delta_{2} \rangle, \sigma_{\Delta_{2}} = (%.1f, %.1f$) [mas]' % (mean_dec, sigma_dec), (90,70), color='black')

        ax.arrow(0,0,dx_y, dy_y, color='r', head_width=6)
        ax.arrow(0,0,dx_x, dy_x, color='r', head_width=6)
        ax.arrow(0,0,dx_z, dy_z, color='g', head_width=6)
        if savename is not None:
            p1.fig.savefig(savename)
            p1.fig.close()
    else:
        f, ax = plt.subplots(ncols=1,nrows=1)
        im = ax.hexbin(d_ra, d_dec, **kwargs)
        ax.set_xlabel(r'$\Delta_{1} \equiv \Delta \rm{RA}$ $\cos{(\rm{Dec})}$ [mas]')
        ax.set_ylabel(r'$\Delta_{2} \equiv \Delta \rm{Dec}$ [mas]')
        ax.annotate('+x', (dx_x+10, dy_x+10), color='white')
        ax.annotate('+y', (dx_y+10, dy_y+10), color='white')
        ax.annotate('Zenith', (dx_z-30, dy_z-20), color='white')
        ax.annotate(r'$\langle \Delta_{1} \rangle, \sigma_{\Delta_{1}} = (%.1f, %.1f$) [mas]' % (mean_ra, sigma_ra), (-90,90), color='white')
        ax.annotate(r'$\langle \Delta_{2} \rangle, \sigma_{\Delta_{2}} = (%.1f, %.1f$) [mas]' % (mean_dec, sigma_dec), (-90,70), color='white')

        ax.arrow(0,0,dx_y, dy_y, color='r', head_width=6)
        ax.arrow(0,0,dx_x,dy_x, color='r', head_width=6)
        ax.arrow(0,0,dx_z, dy_z, color='g', head_width=6)
        plt.colorbar(im, label='Objects/bin')
    #plt.show()
    if savename is not None:
        f.savefig(savename)
        f.close()

class SrcCat:
    def __init__(self, repo, visit, columns, filter_band=None, butler=None, filters=None):
        self.data = None
        self._calib = dict()
        self._wcs = None
        self._datarefs = None
        self.repo = repo
        self.visit = visit
        self.filter_band = filter_band
        self.columns = columns
        if butler is None:
            if self.filter_band is not None:
                gc = SingleVisitCatalog(repo=self.repo, filter_band=self.filter_band, visit=self.visit, detector=None) # We instantiate a SourceCatalog
                self.data = gc.get_quantities(self.columns, filters=filters)
            else:
                raise ValueError('When using SingleVisitCatalog the filter should be specified')
        else:
            self.data = dict()
            for key in self.columns:
                self.data[key] = []
            if self.filter_band is not None:
                self._datarefs = butler.subset('src', dataId={'visit':self.visit, 'filter':self.filter_band}).cache
            else:
                self._datarefs = butler.subset('src', dataId={'visit':self.visit}).cache
            for visitId in self._datarefs:
                try:
                    src = butler.get('src', visitId)
                    # Set up the mask/filter that we are going to pass to the arrays
                    mask = True
                    if filters is not None:
                        for fil in filters:
                            fil_string = re.split('(\W)', fil)
                            fil_string = np.array(fil_string)
                            fil_string = fil_string[~(fil_string=='') & ~(fil_string==' ')]
                            mask = (mask) & (eval('src.get("%s") %s %s' % (fil_string[0], fil_string[1], fil_string[2])))
                    for key in self.columns:
                        if 'mag' not in key.lower(): # If we don't request a magnitude, just get the column
                            self.data[key].append(src.get(key)[mask])
                        elif 'err' not in key.lower():
                            vis_key = '%d-%d' % (visitId['visit'], visitId['detector'])
                            if vis_key not in self._calib.keys():
                                self._calib[vis_key] = butler.get('calexp_photoCalib', visitId)
                            flux_type = key.split('mag')[0][:-1]
                            #print(flux_type)
                            aux_mag = self._calib[vis_key].instFluxToMagnitude(src, flux_type)
                            self.data[key].append(aux_mag[:,0][mask])
                            try:
                                self.data[key.replace('mag', 'magErr')].append(aux_mag[:,1][mask])
                            except KeyError: # The error has not been requested?
                                continue
                except lsst.daf.persistence.butlerExceptions.NoResults:
                    continue #Data not found for some reason
                self.columns = self.data.keys() # Adding the magnitude errors 
            self.data = {q: np.concatenate(self.data[q]).ravel() for q in self.columns}

class MultiSrcCat:
    def __init__(self, repo, columns, visits=None, filter_band=None, butler=None, filters=None):
        self.data = []
        self.visits = visits
        self.filter_band = filter_band
        self.butler = butler
        self.columns = columns
        self._calib = dict()
        self._datarefs = None
        self._wcs = None
        self.repo = repo  
        if (self.visits is None) and (self.filter_band is None):
            raise ValueError('At least the filter or the visit number should be specified')
        elif (self.visits is None) and (self.filter_band is not None):
            print('Will query all visits in %s-band' % filter_band)
            if self.butler is None:
                self.butler = lsst.daf.persistence.Butler(self.repo)
            self._datarefs = self.butler.subset('src', dataId={'filter':filter_band}).cache
            self.visits = np.array(Counter([visitId['visit'] for visitId in self._datarefs]).keys()).astype(int)
        try:
            for visit in self.visits:
                self.data.append(SrcCat(self.repo, visit, self.columns, filter_band=self.filter_band, butler=self.butler, filters=filters).data)
            self.data = {q: np.concatenate([data_this[q] for data_this in self.data]) for q in self.data[0]}
        except TypeError: # We are just querying 1 visit
            self.data = SrcCat(self.repo, self.visits, self.columns, filter_band=self.filter_band, butler=self.butler, filters=filters).data
     
#### options
vis = [246648]
filt_band = 'y'
use_butler = True
data_repo = '/global/cscratch1/sd/jchiang8/desc/Run2.1i/prod_run_2019-03-20/output/rerun/w_2019_16'
ref_cat_path = '/global/projecta/projectdirs/lsst/groups/SSim/DC2/reference_catalogs/merged_reference_2.0i_test.fits'

columns = ['coord_ra', # ra coordinate (in radians)
           'coord_dec', # dec coordinate (in radians)
           'calib_photometry_used', # Has this object been used for photometric calibrations?
           'calib_astrometry_used', # Has this object been used for astrometric calibrations?
           'base_ClassificationExtendedness_value', # This is a basic star/galaxy separation flag more on Bosch et al 2018
           'ext_shapeHSM_HsmShapeRegauss_e1', # e1 using GalSim's HSM
           'ext_shapeHSM_HsmShapeRegauss_e2', # e2 using GalSim's HSM
           'base_SdssShape_xx', # xx moment using SDSS algorithm
           'base_SdssShape_xy', # xy moment using SDSS algorithm
           'base_SdssShape_yy', # yy moment using SDSS algorithm
           'base_SdssShape_psf_xx', # xx moment of the PSF in the position of this object using SDSS algorithm
           'base_SdssShape_psf_xy', # xy as above
           'base_SdssShape_psf_yy', # yy as above
           'base_PsfFlux_instFlux', # PSF-flux
           'base_PsfFlux_instFluxErr', # PSF-flux error
           'ext_photometryKron_KronFlux_instFlux', # Kron flux
           'ext_photometryKron_KronFlux_instFluxErr', # Kron flux uncertainty
           'ext_photometryKron_KronFlux_mag', # Kron magnitude
           'ext_photometryKron_KronFlux_magErr', # Kron magnitude error
           'base_PsfFlux_mag', # magnitude computed in a PSF magnitude (derived from base_PsfFlux_instFlux with the corresponding zeropoint)
           'base_PsfFlux_magErr', # corresponding error to the above magnitude
]

#######
if use_butler: 
    butler = lsst.daf.persistence.Butler(data_repo)
else:
    butler = None
datarefs = butler.subset('src')
if vis is None: # Build a list of visits to check
    vis = Counter([visitId['visit'] for visitId in datarefs.cache]).keys()

data  = MultiSrcCat(data_repo, columns, vis, butler=butler, filters=['deblend_nChild < 1']).data
print('Catalog loaded with ', len(data[columns[0]]), ' entries')

ref_cat = astropy.table.Table.read(ref_cat_path)
print('Reference catalog loaded with', len(ref_cat), ' entries')
mag_true = ((ref_cat['%s_flux' % filt_band]*u.Jy).to(u.ABmag)).value
print(mag_true)
dist, ids, matched = spatial_closest_mag_1band(np.degrees(data['coord_ra']),np.degrees(data['coord_dec']),data['base_PsfFlux_mag'],
                              np.degrees(ref_cat['coord_ra']),np.degrees(ref_cat['coord_dec']),mag_true,np.arange(len(mag_true)))
print(len(ids), np.count_nonzero(matched), len(matched), len(data['base_PsfFlux_mag']), len(data['coord_ra']))
mag_true = mag_true[ids][matched]
mag_meas = data['base_PsfFlux_mag'][matched]
plot_magnitude_difference(mag_true, mag_meas, savename='%d_mag_diff.pdf' % vis[0])
T = data['base_SdssShape_xx']+data['base_SdssShape_yy']
T = T[matched]*0.2**2
star_mask = (data['base_ClassificationExtendedness_value'] < 0.5)
plot_PSF_size(T, star_mask[matched], mag_true, savename='%d_psf_size.pdf' % vis[0])
depth_mp = get_depth_map(np.degrees(data['coord_ra']),
                         np.degrees(data['coord_dec']),
                         data['base_PsfFlux_mag'],
                         data['base_PsfFlux_instFlux']/data['base_PsfFlux_instFluxErr'],
                         nside=2048)
hp.write_map('%d_depth_map.fits.gz' % vis[0], depth_mp)
f = plt.figure()
plt.hist(depth_mp[depth_mp>0], range=(20,30), bins=30)
f.savefig('%d_depth_hist.pdf' % vis[0])
obj_map = make_hp_map(np.degrees(data['coord_ra']),np.degrees(data['coord_dec']),
                      nside=2048)
print('Mean density [gal/sq-arcmin]:', 
       np.mean(obj_map[depth_mp>0])/(hp.nside2pixarea(2048, degrees=True)*3600))
print('Median 5-sigma depth', np.nanmedian(depth_mp[depth_mp>0]))

