from __future__ import print_function

import multiprocessing
import galsim
import numpy as np
import os
import time
try:
    import cPickle as pickle
except ImportError:
    import pickle

"""Script for validating GalSim atmospheric PSF"""

def get_atm(args):
    # Initiate some GalSim random number generators.
    rng = galsim.BaseDeviate(args.seed)
    u = galsim.UniformDeviate(rng)

    # The GalSim atmospheric simulation code describes turbulence in the 3D atmosphere as a series
    # of 2D turbulent screens.  The galsim.Atmosphere() helper function is useful for constructing
    # this screen list.

    # First, we estimate a weight for each screen, so that the turbulence is dominated by the lower
    # layers consistent with direct measurements.  The specific values we use are from SCIDAR
    # measurements on Cerro Pachon as part of the 1998 Gemini site selection process
    # (Ellerbroek 2002, JOSA Vol 19 No 9).

    Ellerbroek_alts = [0.0, 2.58, 5.16, 7.73, 12.89, 15.46]  # km
    Ellerbroek_weights = [0.652, 0.172, 0.055, 0.025, 0.074, 0.022]
    Ellerbroek_interp = galsim.LookupTable(Ellerbroek_alts, Ellerbroek_weights,
                                           interpolant='linear')

    # Use given number of uniformly spaced altitudes
    alts = np.max(Ellerbroek_alts)*np.arange(args.nlayers)/(args.nlayers-1)
    weights = Ellerbroek_interp(alts)  # interpolate the weights
    weights /= sum(weights)  # and renormalize

    # Each layer can have its own turbulence strength (roughly inversely proportional to the Fried
    # parameter r0), wind speed, wind direction, altitude, and even size and scale (though note that
    # the size of each screen is actually made infinite by "wrapping" the edges of the screen.)  The
    # galsim.Atmosphere helper function is useful for constructing this list, and requires lists of
    # parameters for the different layers.

    spd = []  # Wind speed in m/s
    dirn = [] # Wind direction in radians
    r0_500 = [] # Fried parameter in m at a wavelength of 500 nm.
    for i in range(args.nlayers):
        spd.append(u()*args.max_speed)  # Use a random speed between 0 and max_speed
        dirn.append(u()*360*galsim.degrees)  # And an isotropically distributed wind direction.
        # The turbulence strength of each layer is specified by through its Fried parameter r0_500,
        # which can be thought of as the diameter of a telescope for which atmospheric turbulence
        # and unaberrated diffraction contribute equally to image resolution (at a wavelength of
        # 500nm).  The weights above are for the refractive index structure function (similar to a
        # variance or covariance), however, so we need to use an appropriate scaling relation to
        # distribute the input "net" Fried parameter into a Fried parameter for each layer.  For
        # Kolmogorov turbulence, this is r0_500 ~ (structure function)**(-3/5):
        r0_500.append(args.r0_500*weights[i]**(-3./5))
        print("Adding layer at altitude {:5.2f} km with velocity ({:5.2f}, {:5.2f}) m/s, "
              "and r0_500 {:5.3f} m."
              .format(alts[i], spd[i]*dirn[i].cos(), spd[i]*dirn[i].sin(), r0_500[i]))

    # Additionally, we set the screen size and scale.
    atm = galsim.Atmosphere(r0_500=r0_500, speed=spd, direction=dirn, altitude=alts, rng=rng,
                            screen_size=args.screen_size, screen_scale=args.screen_scale)
    # `atm` is now an instance of a galsim.PhaseScreenList object.
    return atm


def getPSFImage(theta):
    # Use global atm, which should hopefully be in shared memory.
    theta = (theta[0]*galsim.arcmin, theta[1]*galsim.arcmin)
    psf = atm.makePSF(lam=args.lam, theta=theta, aper=aper, t0=0.0, exptime=args.exptime)
    psf_image = psf.drawImage(nx=args.nx, ny=args.nx, scale=args.scale)
    return(psf_image)


def makePSFImage(a):
    i, theta = a
    try:
        filename = "{}{:06d}-psf.fits".format(args.outprefix, i)
        if os.path.exists(filename) and not args.clobber:
            return False
        psf_image = getPSFImage(theta)
        galsim.fits.write(psf_image, filename)
    except:
        return False
    return True


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=1,
                        help="Random number seed for generating turbulence.  Default: 1")

    parser.add_argument("--npsf", type=int, default=16,
                        help="Number of PSFs to generate.")
    parser.add_argument("--field_size", type=float, default=13.6*3,  # one LSST raft
                        help="Size of region on sky to (uniformly) sample in arcmin.  "
                             "Default: 40.8")

    parser.add_argument("--nx", default=32, type=int,
                        help="Size of postage stamp in pixels.  Default: 32")
    parser.add_argument("--scale", default=0.2, type=float,
                        help="Size of pixel in arcsec.  Default: 0.2")

    parser.add_argument("--lam", type=float, default=700.0,
                        help="Wavelength in nanometers.  Default: 700.0")
    parser.add_argument("--r0_500", type=float, default=0.2,
                        help="Fried parameter at wavelength 500 nm in meters.  Default: 0.2")
    parser.add_argument("--nlayers", type=int, default=6,
                        help="Number of atmospheric layers.  Default: 6")
    parser.add_argument("--screen_size", type=float, default=819.2,
                        help="Size of atmospheric screen in meters.  Note that the screen wraps "
                             "with periodic boundary conditions.  Default: 819.2")
    parser.add_argument("--screen_scale", type=float, default=0.1,
                        help="Resolution of atmospheric screen in meters.  Default: 0.1")
    parser.add_argument("--max_speed", type=float, default=20.0,
                        help="Maximum wind speed in m/s.  Default: 20.0")

    parser.add_argument("--diam", type=float, default=8.36,
                        help="Size of circular telescope pupil in meters.  Default: 8.36")
    parser.add_argument("--obscuration", type=float, default=0.61,
                        help="Linear fractional obscuration of telescope pupil.  Default: 0.61")
    parser.add_argument("--nstruts", type=int, default=0,
                        help="Number of struts supporting secondary obscuration.  Default: 0")
    parser.add_argument("--strut_thick", type=float, default=0.05,
                        help="Thickness of struts as fraction of aperture diameter.  Default: 0.05")
    parser.add_argument("--strut_angle", type=float, default=0.0,
                        help="Starting angle of first strut in degrees.  Default: 0.0")

    parser.add_argument("--time_step", type=float, default=0.03,
                        help="Incremental time step for advancing phase screens and accumulating "
                             "instantaneous PSFs in seconds.  Default: 0.03")
    parser.add_argument("--exptime", type=float, default=30.0,
                        help="Total amount of time to integrate in seconds.  Default: 30.0")

    parser.add_argument("--pad_factor", type=float, default=1.0,
                        help="Factor by which to pad PSF InterpolatedImage to avoid aliasing. "
                             "Default: 1.0")
    parser.add_argument("--oversampling", type=float, default=1.0,
                        help="Factor by which to oversample the PSF InterpolatedImage. "
                             "Default: 1.0")

    parser.add_argument("--nproc", type=int, default=1,
                        help="Number of subprocesses to launch to create PSFs")

    parser.add_argument("--outprefix", type=str, default="output/",
                        help="Prefix to apply to output files.  Default: 'output/'")

    parser.add_argument("--clobber", action='store_true',
                        help="Clobber any existing output files.  Default: False")

    args = parser.parse_args()

    rng = galsim.BaseDeviate(args.seed+1)
    u = galsim.UniformDeviate(rng)

    thetas = [
        (i,
         (u()*args.field_size, u()*args.field_size))
        for i in range(args.npsf)
    ]

    metafilename = "{}meta.pkl".format(args.outprefix)
    # Make sure out directory exists
    dirname = os.path.dirname(metafilename)
    if not os.path.exists(dirname):
        os.mkdir(dirname)
    if not os.path.isdir(dirname):
        raise RuntimeError("Output directory is not a directory!")

    if os.path.exists(metafilename) and not args.clobber:
        raise RuntimeError("Meta file already exists")
    pickle.dump({"args":args, "thetas":thetas}, open(metafilename, 'wb'))

    # Create a global read-only hopefully shared memory atmosphere.
    t0 = time.time()
    atm = get_atm(args)
    t1 = time.time()
    print("Took {:6.1f} seconds to inflate atmosphere".format(t1-t0))

    # Construct an Aperture object for computing the PSF.  The Aperture object describes the
    # illumination pattern of the telescope pupil, and chooses good sampling size and resolution
    # for representing this pattern as an array.
    aper = galsim.Aperture(diam=args.diam, lam=args.lam, obscuration=args.obscuration,
                           nstruts=args.nstruts, strut_thick=args.strut_thick,
                           strut_angle=args.strut_angle*galsim.degrees,
                           screen_list=atm, pad_factor=args.pad_factor,
                           oversampling=args.oversampling)
    print("aper = {!r}".format(aper))
    print("aper.illuminated.shape = {}".format(aper.illuminated.shape))
    psf_diam = args.lam*1e-9*206265/aper.pupil_plane_scale
    print("max PSF diam = {:5.1f} arcsec".format(psf_diam))
    print("             = {:5.1f} pixels".format(psf_diam/args.scale))

    pool = multiprocessing.Pool(processes=args.nproc)
    t0 = time.time()
    successes = pool.map(makePSFImage, thetas)
    t1 = time.time()
    print("Took {:8.1f} seconds to draw PSFs".format(t1-t0))

    if not all(successes):
        raise RuntimeError("Failed to draw all PSFs")
