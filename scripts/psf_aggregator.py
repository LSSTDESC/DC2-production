import argparse
import pickle
import glob
import os
import galsim
import numpy as np


parser = argparse.ArgumentParser()
parser.add_argument("indir", type=str)
parser.add_argument("njobs", type=int)
parser.add_argument("--inprefix", type=str, default="output/")
args = parser.parse_args()

metafiles = [os.path.join(args.indir, "{}meta_{}_{}.pkl".format(args.inprefix, i, args.njobs))
             for i in range(args.njobs)]

runargs = pickle.load(open(metafiles[0], 'rb'))['args']
thetas = []
for metafile in metafiles:
    thetas.extend(pickle.load(open(metafile, 'rb'))['thetas'])

npsf = len(thetas)
stats = dict(
    x = np.empty(npsf, dtype=float),
    y = np.empty(npsf, dtype=float),
    Mxx = np.empty(npsf, dtype=float),
    Myy = np.empty(npsf, dtype=float),
    Mxy = np.empty(npsf, dtype=float),
    rsqr = np.empty(npsf, dtype=float),
    e1 = np.empty(npsf, dtype=float),
    e2 = np.empty(npsf, dtype=float),
    e = np.empty(npsf, dtype=float),
    beta = np.empty(npsf, dtype=float)
)

for i in range(npsf):
    theta = thetas[i][1]
    filename = os.path.join(args.indir, "{}{:06}-psf.fits".format(args.inprefix, i))
    img = galsim.fits.read(filename)
    mom = galsim.hsm.FindAdaptiveMom(img)
    stats['x'][i] = theta[0]
    stats['y'][i] = theta[1]
    stats['rsqr'][i] = mom.moments_sigma**2
    stats['e1'][i] = mom.observed_shape.e1
    stats['e2'][i] = mom.observed_shape.e2
    stats['e'][i] = mom.observed_shape.e
    stats['beta'][i] = mom.observed_shape.beta.rad
    stats['Mxx'][i] = 0.5 * (1 + mom.observed_shape.e1) * mom.moments_sigma**2
    stats['Myy'][i] = 0.5 * (1 - mom.observed_shape.e1) * mom.moments_sigma**2
    stats['Mxy'][i] = 0.5 * mom.moments_sigma**2 * mom.observed_shape.e2
    if (i%1000) == 0:
        print("{:7d} of {:7d}".format(i, npsf))

outfile = os.path.join(args.indir, args.inprefix, "psf_stats.pkl")
pickle.dump(stats, open(outfile, 'wb'))
