import pickle
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.figure import Figure
import treecorr


def get_xi(data):
    x = data['x']
    y = data['y']
    e1 = data['e1']
    e2 = data['e2']
    e1 -= np.nanmean(e1)
    e2 -= np.nanmean(e2)
    cat = treecorr.Catalog(x=x, y=y, g1=e1, g2=e2, x_units='arcmin', y_units='arcmin')
    gg = treecorr.GGCorrelation(min_sep=0.5, max_sep=210.0, bin_size=0.1, sep_units='arcmin')
    gg.process(cat)
    return gg


def compare_xi(data1, data2, outprefix):
    if len(data1['x']) != len(data2['x']):
        raise ValueError("len(data1) != len(data2)  !!!!")

    outfile = "{}_compare_xi.pdf".format(outprefix)
    with PdfPages(outfile) as pdf:
        fig = Figure(figsize=(12, 10))
        ax = fig.add_subplot(111)
    
        for data, lw in zip([data1, data2], [1.0, 2.0]):
            gg = get_xi(data)
            xip = gg.xip
            xim = gg.xim
            r = np.exp(gg.meanlogr)
            sig = np.sqrt(gg.varxi)

            ax.plot(r, xip, c='b', lw=lw)
            ax.plot(r, -xip, c='b', ls=':', lw=lw)
            ax.errorbar(r[xip>0], xip[xip>0], yerr=sig[xip>0], c='b', lw=0.1*lw, ls='')
            ax.errorbar(r[xip<0], -xip[xip<0], yerr=sig[xip<0], c='b', lw=0.1*lw, ls='')
            lp = ax.errorbar(-r, xip, yerr=sig, c='b', lw=lw)
            
            ax.plot(r, xim, c='g', lw=lw)
            ax.plot(r, -xim, c='g', ls=':', lw=lw)
            ax.errorbar(r[xim>0], xim[xim>0], yerr=sig[xim>0], c='g', lw=0.1*lw, ls='')
            ax.errorbar(r[xim<0], -xim[xim<0], yerr=sig[xim<0], c='g', lw=0.1*lw, ls='')
            lm = ax.errorbar(-r, xim, yerr=sig, c='g', lw=lw)

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
    parser.add_argument("infile1", type=str, help="input pickle file")
    parser.add_argument("infile2", type=str, help="input pickle file")
    parser.add_argument("outprefix", type=str, help="output pdf file prefix")
    args = parser.parse_args()
    data1 = pickle.load(open(args.infile1, 'rb'))
    data2 = pickle.load(open(args.infile2, 'rb'))
    compare_xi(data1, data2, args.outprefix)
