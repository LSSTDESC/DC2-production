import pickle
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.figure import Figure


def hexbins(data, outprefix):
    keys = list(data.keys())
    keys = ['e1', 'e2', 'e', 'beta', 'rsqr', 'Mxx', 'Myy', 'Mxy']
    x, y = data['x'], data['y']
    
    outfile = "{}_hexbins.pdf".format(outprefix)
    with PdfPages(outfile) as pdf:
        for key in keys:
            fig = Figure(figsize=(12, 10))
            ax = fig.add_subplot(111)
            hb = ax.hexbin(x, y, data[key], cmap='inferno')
            ax.set_title(key)
            ax.set_xlabel('x (arcmin)')
            ax.set_ylabel('y (arcmin)')
            fig.tight_layout()
            fig.colorbar(hb)
            pdf.savefig(fig, dpi=100)
        


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("infile", type=str, help="input pickle file")
    parser.add_argument("outprefix", type=str, help="output pdf file prefix")
    args = parser.parse_args()
    data = pickle.load(open(args.infile, 'rb'))
    hexbins(data, args.outprefix)
