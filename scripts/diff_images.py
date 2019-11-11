"""
Script to compute difference and summmed images for raw FITS files for
the test image datasets generated on Cori, Theta, and UKGrid.
"""
import os
from astropy.io import fits
test_dir = '/global/projecta/projectdirs/lsst/production/DC2_ImSim/Run2.2i/sim/tests'
if not os.path.islink('sim_tests'):
    os.symlink(test_dir, 'sim_tests')

with open('common_det_names.txt') as fd:
    det_names = [_.strip() for _ in fd]

for det_name in det_names:
    cori = f'sim_tests/00479028_cori/lsst_a_479028_{det_name}_i.fits'
    grid = f'sim_tests/00479028_grid/lsst_a_479028_{det_name}_i.fits'
    theta = f'sim_tests/00479028_theta/lsst_a_479028_{det_name}_i.fits'

    # compute pair-wise image differences
    with fits.open(cori) as cori_hdus, fits.open(grid) as grid_hdus:
        for amp in range(1, 17):
            cori_hdus[amp].data -= grid_hdus[amp].data
        cori_hdus.writeto(f'images/cori-grid_a_479028_{det_name}_i.fits')

    with fits.open(cori) as cori_hdus, fits.open(theta) as theta_hdus:
        for amp in range(1, 17):
            cori_hdus[amp].data -= theta_hdus[amp].data
        cori_hdus.writeto(f'images/cori-theta_a_479028_{det_name}_i.fits')

    with fits.open(grid) as grid_hdus, fits.open(theta) as theta_hdus:
        for amp in range(1, 17):
            grid_hdus[amp].data -= theta_hdus[amp].data
        grid_hdus.writeto(f'images/grid-theta_a_479028_{det_name}_i.fits')

    # sum the images for pull scaling
    with fits.open(cori) as cori_hdus, fits.open(grid) as grid_hdus:
        for amp in range(1, 17):
            cori_hdus[amp].data += grid_hdus[amp].data
        cori_hdus.writeto(f'images/cori+grid_a_479028_{det_name}_i.fits')

    with fits.open(cori) as cori_hdus, fits.open(theta) as theta_hdus:
        for amp in range(1, 17):
            cori_hdus[amp].data += theta_hdus[amp].data
        cori_hdus.writeto(f'images/cori+theta_a_479028_{det_name}_i.fits')

    with fits.open(grid) as grid_hdus, fits.open(theta) as theta_hdus:
        for amp in range(1, 17):
            grid_hdus[amp].data += theta_hdus[amp].data
        grid_hdus.writeto(f'images/grid+theta_a_479028_{det_name}_i.fits')
