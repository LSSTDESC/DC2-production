import argparse
import pickle
import glob
import os
import galsim
import numpy as np
import time
from astropy.utils.console import ProgressBar
from collections import defaultdict
from multiprocessing import Pool

def nanfn():
    return float('nan')

def processFile(indir, inprefix, i):
    if i%200==0:
        print(i)
    filename = os.path.join(indir, "{}{:06}-psf.fits".format(inprefix, i))
    out = defaultdict(nanfn)
    try:
        img = galsim.fits.read(filename)
    except (KeyboardInterrupt, SystemExit):
        raise
    except FileNotFoundError:
        return out
    except Exception as e:
        print("Caught exception {} for i={}".format(e, i))
        return out
    out['img'] = img.array
    try:
        mom = galsim.hsm.FindAdaptiveMom(img)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        print("Caught exception {} for i={}".format(e, i))
        return out
    out['rsqr']=mom.moments_sigma**2
    out['e1']=mom.observed_shape.e1
    out['e2']=mom.observed_shape.e2
    out['e']=mom.observed_shape.e
    out['beta']=mom.observed_shape.beta.rad
    out['Mxx']=0.5*(1+mom.observed_shape.e1)*mom.moments_sigma**2
    out['Myy']=0.5*(1-mom.observed_shape.e1)*mom.moments_sigma**2
    out['Mxy']=0.5*mom.observed_shape.e2*mom.moments_sigma**2
    return out

parser = argparse.ArgumentParser()
parser.add_argument("indir", type=str)
parser.add_argument("njobs", type=int)
parser.add_argument("--inprefix", type=str, default="output/")
parser.add_argument("--nproc", type=int, default=32)
args = parser.parse_args()

print("Reading meta files")
thetas = []
with ProgressBar(args.njobs) as bar:
    for i in range(args.njobs):
        filename = os.path.join(
            args.indir,
            "{}meta_{}_{}.pkl".format(args.inprefix, i, args.njobs)
        )
        try:
            thetas.extend(pickle.load(open(filename, 'rb'))['thetas'])
        except FileNotFoundError:
            continue
        bar.update()
print("Processing FITS files")

npsf = len(thetas)
stats = dict(
    x = np.full(npsf, np.nan, dtype=float),
    y = np.full(npsf, np.nan, dtype=float),
    Mxx = np.full(npsf, np.nan, dtype=float),
    Myy = np.full(npsf, np.nan, dtype=float),
    Mxy = np.full(npsf, np.nan, dtype=float),
    rsqr = np.full(npsf, np.nan, dtype=float),
    e1 = np.full(npsf, np.nan, dtype=float),
    e2 = np.full(npsf, np.nan, dtype=float),
    e = np.full(npsf, np.nan, dtype=float),
    beta = np.full(npsf, np.nan, dtype=float),
    imgs = np.full((npsf, 32, 32), np.nan, dtype=float)
)
print("# of PSFS = {}".format(npsf))

t0 = time.time()
with Pool(args.nproc) as pool:
    results = pool.starmap(processFile, [(args.indir, args.inprefix, i) for i in range(npsf)])
for i, (result, (_, theta)) in enumerate(zip(results, thetas)):    
    stats['x'][i] = theta[0]
    stats['y'][i] = theta[1]
    for key in ['rsqr', 'e1', 'e2', 'e', 'beta', 'Mxx', 'Myy', 'Mxy']:
        stats[key][i] = result[key]
    stats['imgs'][i] = result['img']
t1 = time.time()
print("Took {} seconds".format(t1-t0))
print("Writing output")
outfile = os.path.join(args.indir, args.inprefix, "psf_stats.pkl")
pickle.dump(stats, open(outfile, 'wb'))
