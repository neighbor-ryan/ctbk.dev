from typing import Collection

def darken(c, f=0.5):
    if isinstance(c, str):
        return '#' + ''.join([ ('%02X' % int(f*int(hx, 16))) for hx in [ c[1:3], c[3:5], c[5:7] ] ])
    elif isinstance(c, Collection):
        return [ darken(e, f=f) for e in c ]
    else:
        raise ValueError(c)
