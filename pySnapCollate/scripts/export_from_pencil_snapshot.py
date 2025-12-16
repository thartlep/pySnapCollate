#!/usr/bin/env python
####################################################
# FILE 'export_from_pencil_snapshot.py'
# formerly known as 'export_array_from_pencil_snapshot'
####################################################
# Written by
# Thomas Hartlep
# Bay Area Environmental Research Institute
# November/December 2024; August/September/December 2025
####################################################

import time, gc
import numpy as np
import sys
import os
import glob
import subprocess
from natsort import natsorted

# Export PENCIL snaphot to numpy 
def export_pencil(varnames, varfile, data_directory, pvar=False, verbose=False):

    try:
        import pencil_old as pc
    except ModuleNotFoundError:
        print('Pencil code not found. Cannot import data.')
        exit()

    # We let pencil python code collect data (usually requires more memory)
    try:
        if not pvar:
            d = pc.read_var(varfile=varfile, datadir=data_directory+'data', trimall=True, quiet=(not verbose))
        else:
            d = pc.read_pvar(varfile=varfile, datadir=data_directory+'data', verbose=verbose)
    except:
        print(f'Error encounter while collecting data using PENCIL python scripts.')
        return 1
    else:
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

        return 0

varname_list = ['dx', 'dy', 'dz', 'np', 'rho', 'rhop', 't', 'ux', 'uy', 'uz', 'x', 'y', 'z']
pvarname_list = ['ipars', 'ivpx', 'ivpy', 'ivpz', 'ixp', 'iyp', 'izp', 'vpx', 'vpy', 'vpz', 'xp', 'yp', 'zp']
default_auto_wait = 10

# Delete original snapshot
def delete_original_snapshot(varfile=None, data_directory=None, verbose=False):

    if varfile is not None and data_directory is not None:
        proc_dirs = glob.glob(os.path.join(data_directory, 'proc*'))
        for proc_dir in proc_dirs:
            varfile_proc = os.path.join(proc_dir, varfile)
            if os.path.exists(varfile_proc):
                os.remove(varfile_proc)
        if verbose:
            print(f'Orginal snapshot {varfile} in {data_directory} deleted!')
    else:
        if verbose:
            print(f'Varfile and data_directory needed in order to delete original snapshot')

# Main CLI
def main():

    # Parse command line argument
    import argparse
    parser = argparse.ArgumentParser(description = 'Export variables from pencil snapshot.')
    parser.add_argument('--varnames', nargs = "*", help = 'Variable name(s) to export, leave empty to get list of available names if varfile provided (default: '+' '.join(sorted(varname_list))+')', default = varname_list)
    parser.add_argument('--varfiles', nargs = "*", help = 'Name(s) of snapshot files, automatically find snapshots if none provided (default: None)', default = [])
    parser.add_argument('--pvarnames', nargs = "*", help = 'Particle variable name(s) to export, leave empty to get list of available names if pvarfile provided (default: '+' '.join(sorted(pvarname_list))+')', default = pvarname_list)
    parser.add_argument('--pvarfiles', nargs = "*", help = 'Name(s) of particle snapshot files, automatically find snapshots if none provided (default: None)', default = [])
    parser.add_argument('--directory', help = 'Directory of input data (default: .)', default = '.')
    parser.add_argument('--verbose', action = 'store_true', help = 'Verbose output (default: False)', default = False)
    parser.add_argument('--daemon_mode', action = 'store_true', help = 'Daemon mode, automatically restart code after set wait time (default: False)', default = False)
    parser.add_argument('--wait_time', help = 'Wait time when running in daemon mode (default: '+str(default_auto_wait)+')', default = default_auto_wait, type=int)
    parser.add_argument('--analysis', help='Analysis command to be run after export (default: None)', default = None)
    parser.add_argument('--analysis_dir', help='Analysis directory (default: .)', default = '.')
    parser.add_argument('--delete_originals', action = 'store_true', help = 'Delete original snapshot(s) after successful data collation (default: False)', default = False)

    args = parser.parse_args()

    export(args)

def export(args):

    # Check arguments are compatible
    if args.daemon_mode:
        if len(args.varfiles) > 0 or len(args.pvarfiles) > 0:
            print('pySnapCollate: Explicit snapshot names given, turning off daemon mode')
            args.daemon_mode = False
    skip_varfiles = (len(args.varfiles) == 0 and len(args.varnames) == 0)
    skip_pvarfiles = (len(args.pvarfiles) == 0 and len(args.pvarnames) == 0)

    # Run export routines at least once        
    while True:

        # Check if snapshot names are provided
        if len(args.varfiles) > 0:
            varfiles = args.varfiles
        elif not skip_varfiles:
            # Automatically discover snapshots
            if args.verbose:
                print(f"Looking for VAR files ...", flush=True)
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
            varfiles = natsorted(needed_varfiles)
            if args.verbose:
                if len(varfiles) > 0:
                    print(f"New or incompletely exported varfile(s) found: {', '.join(varfiles)}", flush=True)

        # Check if snapshot names are provided
        if len(args.pvarfiles) > 0:
            pvarfiles = args.pvarfiles
        elif not skip_pvarfiles: # Automatically discover snapshots
            if args.verbose:
                print(f"Looking for PVAR files ...", flush=True)
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
            pvarfiles = natsorted(needed_pvarfiles)
            if args.verbose:
                if len(pvarfiles) > 0:
                    print(f"New or incompletely exported pvarfile(s) found: {', '.join(pvarfiles)}", flush=True)

        # Export variables
        if not skip_varfiles:
            for varfile in varfiles:
                error_code = export_pencil(varnames=args.varnames, varfile=varfile, data_directory=args.directory+'/', pvar=False, verbose=args.verbose)
                if error_code == 0 and args.delete_originals:
                    delete_original_snapshot(varfile=varfile, data_directory=args.directory+'/', verbose=args.verbose)

        # Export particle variables
        if not skip_pvarfiles:
            for pvarfile in pvarfiles:
                error_code = export_pencil(varnames=args.pvarnames, varfile=pvarfile, data_directory=args.directory+'/', pvar=True, verbose=args.verbose)
                if error_code == 0 and args.delete_originals:
                    delete_original_snapshot(varfile=varfile, data_directory=args.directory+'/', verbose=args.verbose)

        # Run analysis code if provided
        if args.analysis is not None:
            try:
                from pySnapCollate.utilities import remove_enclosing_quotes
                subprocess.run(remove_enclosing_quotes(args.analysis).split(), 
                               cwd=args.analysis_dir, # Change to analysis directory
                               stdout=open(os.path.join(args.analysis_dir, remove_enclosing_quotes(args.analysis).split()[0]+'.output'),'a'), # Redirect standard output to file
                               stderr=subprocess.STDOUT, # Redirect standard error to the same file
                               check=True)  # Raises CalledProcessError if command fails
            except subprocess.CalledProcessError as e:
                print(f"Analysis command failed with error: {e}", flush=True)
            except FileNotFoundError as e:
                print(f"Analysis executable/script not found: {e}", flush=True)
    
        # Keep on looping if in daemon mode
        if args.daemon_mode:  
            if args.verbose:
                print("Waiting for next attempt at finding new snapshots ...", flush=True)
            time.sleep(args.wait_time * 60)
        else: # otherwise break out
            break

if __name__ == "__main__":
    main()

##################################################################
# End of file: export_from_pencil_snapshot.py                    #
##################################################################
