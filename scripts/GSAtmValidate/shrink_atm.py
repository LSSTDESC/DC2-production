import galsim
import pickle

def shrunken_atm(atm):
    layers = atm._layers
    ret = galsim.PhaseScreenList.__new__(galsim.PhaseScreenList)
    ret._layers = [shrunken_layer(l) for l in layers]
    ret.rng = atm.rng
    ret.dynamic = atm.dynamic
    ret.reversible = atm.reversible
    ret._pending = atm._pending
    ret._update_time_heap = atm._update_time_heap
    return ret


def shrunken_layer(layer):
    tab2d = layer._tab2d
    orig = tab2d.f[:-1, :-1]

    new = orig[::2, ::2]

    ret = galsim.AtmosphericScreen.__new__(galsim.AtmosphericScreen)
    ret.npix = layer.npix//2
    ret.screen_scale = layer.screen_scale*2
    ret.screen_size = layer.screen_size
    ret.altitude = layer.altitude
    ret.time_step = layer.time_step
    ret.r0_500 = layer.r0_500
    ret.L0 = layer.L0
    ret.vx = layer.vx
    ret.vy = layer.vy
    ret.alpha = layer.alpha
    ret._time = layer._time
    ret._orig_rng = layer._orig_rng.duplicate()
    ret.dynamic = layer.dynamic
    ret.reversible = layer.reversible
    ret.rng = layer.rng.duplicate()
    ret._xs = layer._xs[::2]
    ret._ys = layer._ys[::2]
    ret._tab2d = galsim.LookupTable2D(
        ret._xs, ret._ys, new, interpolant='linear', edge_mode='wrap')

    return ret

from argparse import ArgumentParser
parser = ArgumentParser()
parser.add_argument("infile")
parser.add_argument("outfile")
args = parser.parse_args()

print("Loading atm")
atm = pickle.load(open(args.infile, 'rb'))
print("Shrinking atm")
new_atm = shrunken_atm(atm)
print("Dumping new atm")
pickle.dump(new_atm, open(args.outfile, 'wb'))

for ilayer, (layer, new_layer) in enumerate(zip(atm, new_atm)):
    print(ilayer)
    for k in layer.__dict__:
        if k in ['_xs', '_ys']:
            print(k, layer.__dict__[k][0:4], new_layer.__dict__[k][0:4])
            continue
        elif k == '_tab2d':
            continue
        print(k, layer.__dict__[k], new_layer.__dict__[k])


for k in atm.__dict__:
    if k == '_layers':
        continue
    print(k, atm.__dict__[k], new_atm.__dict__[k])
