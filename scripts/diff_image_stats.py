"""
Script to compute pull distributions for differenced and summed raw FITS files
for Run2.2i.  The files are generated on Cori, Theta, and UKGrid and
the comparisons are made between the three sets of pairs.
"""

import os
import pickle
from collections import defaultdict
import numpy as np
import matplotlib.pyplot as plt
from astropy.io import fits
plt.ion()

def get_det_names(infile='common_det_names.txt'):
    with open(infile) as fd:
        return [_.strip() for _ in fd]

def rebin_array(arr, binsize, use_mean=False):
    "See http://scipython.com/blog/binning-a-2d-array-in-numpy/"
    if binsize == 1:
        return arr
    shape = (arr.shape[0]//binsize, binsize, arr.shape[1]//binsize, binsize)
    if use_mean:
        result = arr.reshape(shape).mean(-1).mean(1)
    else:
        result = arr.reshape(shape).sum(-1).sum(1)
    return result

def compute_pulls(site_pairs, binsizes, gain=0.7, bias_level=1000, read_noise=7,
                  visit=479028, band='i'):

    det_names = get_det_names()
    pulls = {_: defaultdict(list) for _ in site_pairs}
    for site_pair in site_pairs:
        site1, site2 = site_pair
        print(site_pair)
        for det_name in det_names:
            print(det_name)
            diff_file = f'images/{site1}-{site2}_a_{visit}_{det_name}_{band}.fits'
            summ_file = f'images/{site1}+{site2}_a_{visit}_{det_name}_{band}.fits'
            with fits.open(diff_file) as diff_image,\
                 fits.open(summ_file) as summ_image:
                # Convert to electrons and subtract off median to account for
                # sky background and bias levels.
                for amp in range(1, 17):
                    summ_image[amp].data = np.array(summ_image[amp].data,
                                                    dtype=np.float)
                    summ_image[amp].data -= 2*bias_level
                    summ_image[amp].data *= gain
                    median = np.median(summ_image[amp].data)
                    summ_image[amp].data -= median
                    #summ_image[amp].data -= gain**2*read_noise**2
                # Zero-out pixels with zero difference, masking them from the
                # analysis.
                for amp in range(1, 17):
                    index = np.where((diff_image[amp].data == 0) |
                                     (summ_image[amp].data < 0))
                    summ_image[amp].data[index] = 0
                # Rebin the data to reduce correlations due to the finite sizes
                # of rendered objects.
                for i, binsize in enumerate(binsizes):
                    diff_rebinned = []
                    summ_rebinned = []
                    for amp in range(1, 17):
                        # Restrict to imaging section of segment.
                        diff_rebinned.append(
                            rebin_array(diff_image[amp].data[0:2000,3:503],
                                        binsize))
                        summ_rebinned.append(
                            rebin_array(summ_image[amp].data[0:2000,3:503],
                                        binsize))
                    diff_values = np.concatenate([_.ravel() for _ in
                                                  diff_rebinned])
                    summ_values = np.concatenate([_.ravel() for _ in
                                                  summ_rebinned])
                    index = np.where((diff_values != 0) & (summ_values != 0))
                    pulls[site_pair][binsize].extend(
                        diff_values[index]/np.sqrt(summ_values[index]))
    return pulls

if __name__ == '__main__':
    site_pairs = (('cori', 'grid'), ('cori', 'theta'), ('grid', 'theta'))
    binsizes = np.array((10, 20, 25, 50, 100, 125, 250))
    pulls_file = 'image_stats.pickle'
    if not os.path.isfile(pulls_file):
        pulls = compute_pulls(site_pairs, binsizes)
        with open(pulls_file, 'wb') as output:
            pickle.dump(pulls, output)
    else:
        with open(pulls_file, 'rb') as fd:
            pulls = pickle.load(fd)

    for site_pair in site_pairs:
        fig = plt.figure()
        for binsize in binsizes:
            stdev = np.std(pulls[site_pair][binsize])
            label = f'{binsize}: {stdev:.2f}'
            plt.hist(pulls[site_pair][binsize], bins=40, range=(-4, 4),
                     density=True, histtype='step', label=label)
        plt.legend(fontsize='x-small')
        plt.xlabel('pull')
        plt.title(f'{site_pair[0]} - {site_pair[1]}')
        plt.savefig(f'diff_image_stats_{site_pair[0]}_{site_pair[1]}.png')
