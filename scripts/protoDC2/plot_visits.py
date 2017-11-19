import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import gzip

plt.ion()

def plot_galaxies(catalog, limit=1000000, alpha=0.01, color='grey'):
    ra, dec = [], []
    with gzip.open(catalog) as input_:
        for i, line in enumerate(input_):
            if i > limit:
                break
            tokens = line.split()
            ra_val = float(tokens[2])
            if ra_val > 180:
                ra_val -= 360.
            ra.append(ra_val)
            dec.append(float(tokens[3]))
    plt.errorbar(ra, dec, fmt='.', alpha=alpha, color=color)

def plot_visits(df, xname, yname, side=2.5, ms=2, alpha=1):
    for filt in 'ugrizy':
        band = df.query('filter=="%s"' % filt)
        label = '%s band, %i visits' % (filt, len(band))
        plt.errorbar(band[xname], band[yname], fmt='.', label=label, ms=ms,
                     alpha=alpha)
    plt.xlabel(xname)
    plt.ylabel(yname)
    plt.legend(fontsize='x-small', loc=0)
    plt.plot((-side, -side, side, side, -side),
             (-side, side, side, -side, -side))

df = pd.read_pickle('protoDC2_visits.pkl')

plt.figure()
plot_galaxies('gal_cat_138143.txt.gz')
plot_visits(df, 'randomDitherFieldPerVisitRA', 'randomDitherFieldPerVisitDec')

plt.title('minion_1016_sqlite_new_dithers.db, %i total visits' % len(df))
plt.savefig('protoDC2_visits.png')
