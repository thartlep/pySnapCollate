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
import sys
import os
import glob

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
default_auto_wait = 10

def main():

    # Parse command line argument
    import argparse
    parser = argparse.ArgumentParser(description = 'Export variables from pencil snapshot.')
    parser.add_argument('--varnames', nargs = "*", help = 'Variable name(s) to export, leave empty to get list of available names (default: '+' '.join(sorted(varname_list))+')', default = varname_list)
    parser.add_argument('--varfiles', nargs = "*", help = 'Name(s) of snapshot files, automatically find snapshots if none provided (default: None)', default = [])
    parser.add_argument('--pvarnames', nargs = "*", help = 'Particle variable name(s) to export, leave empty to get list of available names (default: '+' '.join(sorted(pvarname_list))+')', default = pvarname_list)
    parser.add_argument('--pvarfiles', nargs = "*", help = 'Name(s) of particle snapshot files, automatically find snapshots if none provided (default: None)', default = [])
    parser.add_argument('--directory', help = 'Directory of input data (default: .)', default = '.')
    parser.add_argument('--verbose', action = 'store_true', help = 'Verbose output (default: False)', default = False)
    parser.add_argument('--daemon_mode', action = 'store_true', help = 'Daemon mode, automatically restart code after set wait time (default: False)', default = False)
    parser.add_argument('--wait_time', help = 'Wait time when running in daemon mode (default: '+str(default_auto_wait)+')', default = default_auto_wait, type=int)

    args = parser.parse_args()

    # Check arguments are compatible
    if args.daemon_mode:
        if len(args.varfiles) > 0 or len(args.pvarfiles) > 0:
            print('pySnapCollate: Explicit snapshot names given, turning off daemon mode')
            args.daemon_mode = False
    if len(args.varfiles) == 0 and len(args.varnames) == 0:
        print('pySnapCollate: Must provide at least varnames or varfiles')
        sys.exit(1)
    if len(args.pvarfiles) == 0 and len(args.pvarnames) == 0:
        print('pySnapCollate: Must provide at least pvarnames or pvarfiles')
        sys.exit(1)

    # Run export routines at least once        
    while True:

        # Check if snapshot names are provided
        if len(args.varfiles) > 0:
            varfiles = args.varfiles
        else: # Automatically discover snapshots
            directory = os.path.expanduser(args.directory) # Source directory
            data_dir = os.path.join(directory, "data/proc0") # proc0 data directory
            varfile_pattern = os.path.join(data_dir, "VAR*") # varfile search pattern
            varfiles_full_path = glob.glob(varfile_pattern) # paths of varfiles for proc0
            varfiles = [os.path.basename(file_path) for file_path in varfiles_full_path] # only the varfile names
            needed_varfiles = []
            # Check if we need to process varfiles or if we already have all variables existig locally
            for varfile in varfiles:
                varfile_still_needed = False
                for varname in args.varnames:
                    existing_exported_files = glob.glob('exported__'+varname+'__'+varfile+'.*')
                    if len(existing_exported_files) == 0:
                        varfile_still_needed = True
                if varfile_still_needed:
                    needed_varfiles.append(varfile)
            varfiles = needed_varfiles

        # Check if snapshot names are provided
        if len(args.pvarfiles) > 0:
            pvarfiles = args.pvarfiles
        else: # Automatically discover snapshots
            directory = os.path.expanduser(args.directory) # Source directory
            data_dir = os.path.join(directory, "data/proc0") # proc0 data directory
            pvarfile_pattern = os.path.join(data_dir, "PVAR*") # pvarfile search pattern
            pvarfiles_full_path = glob.glob(pvarfile_pattern) # paths of pvarfiles for proc0
            pvarfiles = [os.path.basename(file_path) for file_path in pvarfiles_full_path] # only the pvarfile names
            needed_pvarfiles = []
            # Check if we need to process varfiles or if we already have all variables existig locally
            for pvarfile in pvarfiles:
                pvarfile_still_needed = False
                for pvarname in args.pvarnames:
                    existing_exported_files = glob.glob('exported__'+pvarname+'__'+pvarfile+'.*')
                    if len(existing_exported_files) == 0:
                        pvarfile_still_needed = True
                if pvarfile_still_needed:
                    needed_pvarfiles.append(pvarfile)
            pvarfiles = needed_pvarfiles

        # Export variables
        for varfile in varfiles:
            export_pencil(varnames=args.varnames, varfile=varfile, data_directory=args.directory+'/', pvar=False, verbose=args.verbose)

        # Export particle variables
        for pvarfile in pvarfiles:
            export_pencil(varnames=args.pvarnames, varfile=pvarfile, data_directory=args.directory+'/', pvar=True, verbose=args.verbose)

        # Keep on looping if in daemon mode
        if args.daemon_mode:  
            time.sleep(args.wait_time * 60)
        else: # otherwise break out
            break

if __name__ == "__main__":
    main()

##################################################################
# End of file: export_from_pencil_snapshot.py                    #
##################################################################
