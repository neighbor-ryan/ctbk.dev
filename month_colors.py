month_colors = {
    2: '#78BCFF',
    4: '#52D208',
    6: '#FFE621',
    8: '#E41B05',
    10: '#E69315',
    12: '#B87D1D',
}
prv = None
nxt = None
it = iter(month_colors.items())
fk, fv = next(it)
pk, pv = fk, fv
nk, nv = next(it)
mc = month_colors.copy()
k = fk + 1
ik = k
while True:
    if ik == fk:
        break
    if ik == nk:
        pk, pv = nk, nv
        try:
            nk, nv = next(it)
        except StopIteration:
            nk, nv = fk+12, fv
    else:
        pc = [ int(hx, 16) for hx in [ pv[1:3], pv[3:5], pv[5:7] ] ]
        nc = [ int(hx, 16) for hx in [ nv[1:3], nv[3:5], nv[5:7] ] ]
        c = [ p + (n-p)*(k-pk)/(nk-pk) for p,n in zip(pc,nc) ]
        mc[ik] = '#' + ''.join(['%02X' % int(ch) for ch in c])

    k += 1
    ik += 1
    if ik > 12: ik = 1

month_colors = [ mc[i] for i in range(1, 13) ]
