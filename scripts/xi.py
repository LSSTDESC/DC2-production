import pickle
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.figure import Figure
import treecorr


def xi(data, outprefix):
    x = data['x']
    y = data['y']
    e1 = data['e1']
    e2 = data['e2']
    e1 -= np.nanmean(e1)
    e2 -= np.nanmean(e2)
    cat = treecorr.Catalog(x=x, y=y, g1=e1, g2=e2, x_units='arcmin', y_units='arcmin')
    gg = treecorr.GGCorrelation(min_sep=0.5, max_sep=210.0, bin_size=0.1, sep_units='arcmin')
    gg.process(cat)
    xip = gg.xip
    xim = gg.xim
    r = np.exp(gg.meanlogr)
    sig = np.sqrt(gg.varxi)

    outfile = "{}_xi.pdf".format(outprefix)
    with PdfPages(outfile) as pdf:
        fig = Figure(figsize=(12, 10))
        ax = fig.add_subplot(111)
    
        ax.plot(r, xip, c='b')
        ax.plot(r, -xip, c='b', ls=':')
        ax.errorbar(r[xip>0], xip[xip>0], yerr=sig[xip>0], c='b', lw=0.1, ls='')
        ax.errorbar(r[xip<0], -xip[xip<0], yerr=sig[xip<0], c='b', lw=0.1, ls='')
        lp = ax.errorbar(-r, xip, yerr=sig, c='b')

        ax.plot(r, xim, c='g')
        ax.plot(r, -xim, c='g', ls=':')
        ax.errorbar(r[xim>0], xim[xim>0], yerr=sig[xim>0], c='g', lw=0.1, ls='')
        ax.errorbar(r[xim<0], -xim[xim<0], yerr=sig[xim<0], c='g', lw=0.1, ls='')
        lm = ax.errorbar(-r, xim, yerr=sig, c='g')

        ax.set_xscale('log')
        ax.set_yscale('log')
        ax.set_xlabel(r"$\theta$ (arcmin)")

        ax.legend([lp, lm], [r"$\xi_+(\theta)$", r"$\xi_-(\theta)$"])
        ax.set_xlim([0.5, 210.0])
        ax.set_ylim([1e-7, 1e-3])
        ax.set_ylabel(r"$\xi_{+/-}$")

        fig.tight_layout()
        pdf.savefig(fig, dpi=100)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("infile", type=str, help="input pickle file")
    parser.add_argument("outprefix", type=str, help="output pdf file prefix")
    args = parser.parse_args()
    data = pickle.load(open(args.infile, 'rb'))
    xi(data, args.outprefix)
