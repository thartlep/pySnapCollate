#!/usr/bin/env python
####################################################
# FILE 'export_from_pencil_snapshot.py'
# formerly known as 'export_array_from_pencil_snapshot'
####################################################
# Written by
# Thomas Hartlep
# Bay Area Environmental Research Institute
# November/December 2024; August 2025
####################################################

import time, gc
import numpy as np

def export_pencil(varnames, varfile, data_directory, pvar=False, verbose=False):

    try:
        import pencil_old as pc
    except ModuleNotFoundError:
        print('Pencil code not found. Cannot import data.')
        exit()

    # We let pencil python code collect data (usually requires more memory)
    if not pvar:
        d = pc.read_var(varfile=varfile, datadir=data_directory+'data', trimall=True, quiet=(not verbose))
    else:
        d = pc.read_pvar(varfile=varfile, datadir=data_directory+'data', verbose=verbose)

    # Get variables and write out to file
    if len(varnames) == 0:
        print(f'Variables stored in snapshot file {varfile}: '+', '.join([att for att in dir(d) if not callable(getattr(d,att)) and att[:2]!='__']))

    for varname in varnames:
        try:
            data = getattr(d, varname)
        except AttributeError:
            print(f'Unknown varname {varname} !')
        else:                    
            # Write entire array to file
            if len(np.shape(data)) == 0:
                numpy_array_filename = 'exported__'+varname+'__'+varfile+'.txt'
                np.savetxt(numpy_array_filename, [data])
            else:
                numpy_array_filename = 'exported__'+varname+'__'+varfile+'.npy'
                np.save(numpy_array_filename, data)

varname_list = ['dx', 'dy', 'dz', 'np', 'rho', 'rhop', 't', 'ux', 'uy', 'uz', 'x', 'y', 'z']
pvarname_list = ['ipars', 'ivpx', 'ivpy', 'ivpz', 'ixp', 'iyp', 'izp', 'vpx', 'vpy', 'vpz', 'xp', 'yp', 'zp']

def main():

    # Parse command line argument
    import argparse
    parser = argparse.ArgumentParser(description = 'Export variables from pencil snapshot.')
    parser.add_argument('-n', '--varnames', nargs = "*", help = 'variable name(s) to export, leave empty to get list of available names (default: '+' '.join(sorted(varname_list))+')', default = varname_list)
    parser.add_argument('-f', '--varfiles', nargs = "*", help = 'name(s) of snapshot files (default: None)', default = [])
    parser.add_argument('-pn', '--pvarnames', nargs = "*", help = 'particle variable name(s) to export, leave empty to get list of available names (default: '+' '.join(sorted(pvarname_list))+')', default = pvarname_list)
    parser.add_argument('-pf', '--pvarfiles', nargs = "*", help = 'name(s) of particle snapshot files (default: None)', default = [])
    parser.add_argument('-d',  '--directory', help = 'directory of input data (default: .)', default = '.')
    parser.add_argument('-v', '--verbose', action = 'store_true', help = 'verbose output (default: False)', default = False)

    args = parser.parse_args()
    for varfile in args.varfiles:
    	export_pencil(varnames=args.varnames, varfile=varfile, data_directory=args.directory+'/', pvar=False, verbose=args.verbose)
    for pvarfile in args.pvarfiles:
        export_pencil(varnames=args.pvarnames, varfile=pvarfile, data_directory=args.directory+'/', pvar=True, verbose=args.verbose)

if __name__ == "__main__":
    main()

##################################################################
# End of file: export_from_pencil_snapshot.py                    #
##################################################################
