"""Core module for pySnapCollate: contains core functions."""

# ==== Imports ============================

import os
import subprocess
import json
import re
import glob
import errno
import shutil
import time
import numpy as np
import glob
from natsort import natsorted
from .utils import remove_enclosing_quotes, resolve_path
from concurrent.futures import ProcessPoolExecutor

# ==== Defaults ===========================

config_dir_name = "~/.pySnapCollate"
default_default_queue = 'normal'
default_default_lifetime = 8
default_default_resources = 'select=1:ncpus=24:mpiprocs=1:ompthreads=48:model=has'
default_default_environment = 'conda activate pencil'
varname_list = ['dx', 'dy', 'dz', 'np', 'rho', 'rhop', 't', 'ux', 'uy', 'uz', 'x', 'y', 'z']
pvarname_list = ['vpx', 'vpy', 'vpz', 'xp', 'yp', 'zp']
default_auto_wait = 1

# =========================================

def export_pencil(varnames, varfile, data_directory, pvar=False, verbose=False):
    """
    Export specific variable from PENCIL snaphshot to numpy.
    """
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
            d = pc.read_pvar(varfile=varfile, datadir=data_directory+'data', verbose=False)
    except:
        print(f'Error encountered while collecting data using PENCIL python scripts.')
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

# =========================================

def delete_original_snapshot(varfile=None, data_directory=None, verbose=False):
    """
    Delete original snapshot
    """

    if varfile is not None and data_directory is not None:
        proc_dirs = glob.glob(os.path.join(data_directory, 'data/proc*'))
        n_deleted_files = 0
        for proc_dir in proc_dirs:
            varfile_proc = os.path.join(proc_dir, varfile)
            try:
                os.remove(varfile_proc)
            except Exception:
                pass
            if not os.path.exists(varfile_proc):
                n_deleted_files += 1
        if verbose:
            if len(proc_dirs) == n_deleted_files:
                print(f'Original snapshot {varfile} in {data_directory} successfully deleted!')
            elif n_deleted_files == 0:
                print(f'Original snapshot {varfile} in {data_directory} NOT deleted!')
            else:
                print(f'Original snapshot {varfile} in {data_directory} PARTIALLY deleted!')
    else:
        if verbose:
            print(f'Varfile and data_directory needed in order to delete original snapshot')


# =========================================

def export(args):
    """
    Export multiple variables
    """

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
            if args.one_batch_at_a_time:
                varfiles = varfiles[:args.batch_size] # Use only batch size number of newly discovered file(s)
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
            if args.one_batch_at_a_time:
                pvarfiles = pvarfiles[:args.batch_size] # Use only batch size number of newly discovered file(s)
            if args.verbose:
                if len(pvarfiles) > 0:
                    print(f"New or incompletely exported pvarfile(s) found: {', '.join(pvarfiles)}", flush=True)

        # Export variables
        if not skip_varfiles:
            with ProcessPoolExecutor(max_workers=args.batch_size) as pool:
                error_codes = pool.map(export_pencil, [args.varnames]*len(varfiles), varfiles, [args.directory+'/']*len(varfiles), [False]*len(varfiles), [False]*len(varfiles))
            if args.delete_originals:
                for error_code, varfile in zip(error_codes, varfiles):
                    if error_code == 0:
                        delete_original_snapshot(varfile=varfile, data_directory=args.directory+'/', verbose=args.verbose)

        # Export particle variables
        if not skip_pvarfiles:
            with ProcessPoolExecutor(max_workers=args.batch_size) as pool:
                error_codes = pool.map(export_pencil, [args.pvarnames]*len(pvarfiles), pvarfiles, [args.directory+'/']*len(pvarfiles), [True]*len(pvarfiles), [False]*len(pvarfiles))
            if args.delete_originals:
                for error_code, pvarfile in zip(error_codes, pvarfiles):
                    if error_code == 0:
                        delete_original_snapshot(varfile=pvarfile, data_directory=args.directory+'/', verbose=args.verbose)

        # Run analysis code if provided
        if args.analysis is not None:
            try:
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

# =========================================

def setup_daemon(args):
    """
    Set up daemon configuration
    """

    # Define the path to the daemon configuration directory
    config_dir = os.path.expanduser(config_dir_name)
    os.makedirs(config_dir, exist_ok=True)  # Create the directory if it doesn't exist

    # Define the path for the specific daemon
    daemon_dir = os.path.join(config_dir, args.name)
    os.makedirs(daemon_dir, exist_ok=True)  # Create the daemon directory if it doesn't exist

    # Define the path for the configuration file
    config_path = os.path.join(daemon_dir, "config.json")

    # Check if the configuration file exists
    if os.path.exists(config_path) and not args.force:
            print(f"Existing configuration file found for daemon '{args.name}'. Rerun with flag --force to override existing configuration.")
            return
        
    # Prepare the configuration data
    config_data = {
        "name": args.name,
        "source": resolve_path(args.source),
        "target": resolve_path(args.target),
        "lifetime": args.lifetime,
        "group": args.group,
        "resources": args.resources,
        "environment": args.environment,
        "queue": args.queue,
        "varnames":  ' '.join(args.varnames),
        "pvarnames":  ' '.join(args.pvarnames),
        "verbose": args.verbose,
        "analysis": args.analysis if args.analysis is not None else "",
        "analysis_dir": resolve_path(args.analysis_dir) if args.analysis_dir is not None else resolve_path(args.target),
        "delete_originals": args.delete_originals,
        "wait_time": args.wait_time,
        "one_batch_at_a_time": args.one_batch_at_a_time,
        "batch_size": args.batch_size
    }

    # Write the configuration data to the JSON file
    try:
        with open(config_path, 'w') as config_file:
            json.dump(config_data, config_file, indent=4)
        print(f"Daemon '{args.name}' has been set up successfully.")
    except Exception as e:
        print(f"An error occurred while setting up the daemon: {e}")

# =========================================

def copy_daemon(args):
    """
    Cpoy daemon configuration
    """

    config_dir = os.path.expanduser(config_dir_name) # Daemon configuration directory
    src_daemon_dir = os.path.join(config_dir, args.src_name) # Directory for the source daemon
    src_config_path = os.path.join(src_daemon_dir, "config.json") # Source deamon configuration file
    des_daemon_dir = os.path.join(config_dir, args.dest_name) # Directory for the destination daemon
    des_config_path = os.path.join(des_daemon_dir, "config.json") # Destination deamon configuration file

    # Return early if destination daemon already exists
    if os.path.exists(des_config_path):
        print(f"Destination daemon {args.dest_name} already exists. Abort!")
        return

    # Read source configuration data from the JSON file
    try:
        with open(src_config_path, 'r') as config_file:
            config_data = json.load(config_file)
    except Exception as e:
        print(f"An error occurred while loading source daemon configuration: {e}")
        return

    # Create destination daemon configuration directory
    os.makedirs(des_daemon_dir, exist_ok=True) 

    # Modify configuration name
    config_data["name"] = args.dest_name

    # Write destination configuration data to the JSON file
    try:
        with open(des_config_path, 'w') as config_file:
            json.dump(config_data, config_file, indent=4)
        print(f"Daemon '{args.dest_name}' has been created successfully.")
    except Exception as e:
        print(f"An error occurred while writing daemon configuration: {e}")

# =========================================

def modify_daemon(args):
    """
    Modify daemon configuration.
    """

    config_dir = os.path.expanduser(config_dir_name) # Daemon configuration directory
    daemon_dir = os.path.join(config_dir, args.name) # Directory for the specific daemon
    config_path = os.path.join(daemon_dir, "config.json") # Deamon configuration file
    script_path = os.path.join(daemon_dir, "run.csh") # Run script for the specific daemon
    active_pattern = os.path.join(daemon_dir, "active_job.*") # Acive daemo tag

    # Check if the configuration file exists
    if not os.path.exists(config_path):
        print(f"No configuration file found for daemon '{args.name}'.")
        return

    # Read the configuration data from the JSON file
    try:
        with open(config_path, 'r') as config_file:
            config_data = json.load(config_file)
    except Exception as e:
        print(f"An error occurred while loading the daemon configuration: {e}")
        return

    # Check if queued/running
    for active_file in glob.glob(active_pattern):
        # Active daemon file found, let's check if daemon is actually queued or running
        command = ["qstat", active_file.rsplit('.', 1)[-1]]
        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            is_in_queue = True
            print(f"Daemon '{args.name}' is queued/running. Cannot modify configuration.")
        except subprocess.CalledProcessError as e:
            # Delete active daemon file because daemon is no longer queued/running
            os.remove(active_file)
        else:
            return

    # Remove run script if existing, it will get recreated when daemon is started again
    try:
        if os.path.exists(script_path):
            os.remove(script_path)
    except Exception as e:
        print(f"An error occurred while removing old daemon run script: {e}")

    # Now modify configuration
    if args.name is not None: config_data["name"] = args.name
    if args.source is not None: config_data["source"] = resolve_path(args.source)
    if args.target is not None: config_data["target"] = resolve_path(args.target)
    if args.lifetime is not None: config_data["lifetime"] = args.lifetime
    if args.group is not None: config_data["group"] = args.group
    if args.resources is not None: config_data["resources"] = args.resources
    if args.environment is not None: config_data["environment"] = args.environment
    if args.queue is not None: config_data["queue"] = args.queue
    if args.varnames is not None: config_data["varnames"] = ' '.join(args.varnames)
    if args.pvarnames is not None: config_data["pvarnames"] = ' '.join(args.pvarnames)
    if args.verbose is not None: config_data["verbose"] = args.verbose
    if args.analysis is not None: config_data["analysis"] = args.analysis if args.analysis is not None else ""
    if args.analysis_dir is not None: config_data["analysis_dir"] = resolve_path(args.analysis_dir) if args.analysis_dir is not None else resolve_path(args.target)
    if args.delete_originals is not None: config_data["delete_originals"] = args.delete_originals
    if args.wait_time is not None: config_data["wait_time"] = args.wait_time
    if args.one_batch_at_a_time is not None: config_data["one_batch_at_a_time"] = args.one_batch_at_a_time
    if args.batch_size is not None: config_data["batch_size"] = args.batch_size

    # Write the configuration data to the JSON file
    try:
        with open(config_path, 'w') as config_file:
            json.dump(config_data, config_file, indent=4)
        print(f"Daemon '{args.name}' has been modified successfully.")
    except Exception as e:
        print(f"An error occurred while writing daemon configuration: {e}")

# =========================================

def inspect_daemon(args):
    """
    Inspect daemon configuration and status.
    """

    config_dir = os.path.expanduser(config_dir_name) # Daemon configuration directory
    daemon_dir = os.path.join(config_dir, args.name) # Directory for the specific daemon
    config_path = os.path.join(daemon_dir, "config.json") # Deamon configuration file
    active_pattern = os.path.join(daemon_dir, "active_job.*") # Acive daemo tag

    # Check if the configuration file exists
    if not os.path.exists(config_path):
        print(f"No configuration file found for daemon '{args.name}'.")
        return

    # Read the configuration data from the JSON file
    try:
        with open(config_path, 'r') as config_file:
            config_data = json.load(config_file)
        
        # Output the parameters
        print("Daemon Configuration:")
        for key, value in config_data.items():
            print(f"{key}: {value}")
    except Exception as e:
        print(f"An error occurred while inspecting the daemon: {e}")

    # Check if queued/running
    for active_file in glob.glob(active_pattern):
        # Active daemon file found, let's check if daemon is actually queued or running
        command = ["qstat", active_file.rsplit('.', 1)[-1]]
        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            is_in_queue = True
            print(f"Daemon '{args.name}' is queued/running.")
        except subprocess.CalledProcessError as e:
            # And let user know it wasn't even queued/running
            print(f"Daemon '{args.name}' is no longer queued/running.")
            # Delete active daemon file
            os.remove(active_file)

# =========================================

def start_daemon(args):
    """
    Start daemon by submitting it to the run queue.
    """

    config_dir = os.path.expanduser(config_dir_name) # Daemon configuration directory
    daemon_dir = os.path.join(config_dir, args.name) # Directory for the specific daemon
    config_path = os.path.join(daemon_dir, "config.json") # Configuration file for the specific daemon
    script_path = os.path.join(daemon_dir, "run.csh") # Run script for the specific daemon
    active_pattern = os.path.join(daemon_dir, "active_job.*") # Acive daemo tag

    # Check if the configuration file exists
    if not os.path.exists(config_path):
        print(f"No configuration file found for daemon '{args.name}'.")
        return

    # Read the configuration data from the JSON file
    try:
        with open(config_path, 'r') as config_file:
            config_data = json.load(config_file)
    except Exception as e:
        print(f"An error occurred while inspecting the daemon: {e}")

    # Check if daemon is active
    already_in_queue = False
    for active_file in glob.glob(active_pattern):
        # Active daemon file found, let's check if daemon is actually queued or running
        command = ["qstat", active_file.rsplit('.', 1)[-1]]
        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            print(f"Daemon '{args.name}' is aready queued/running.")
            already_in_queue = True
        except subprocess.CalledProcessError as e:
            # Old active daemon file exists but is no longeer queued or running, let's delete it
            os.remove(active_file)
    # Leave here if there is already an active daemon is queued/running
    if already_in_queue:
        print(f"No need to start another one.")
        return

    # Define the queue name
    if args.queue is not None:
        queue_name = args.queue # Use the queue name from the arguments
    else:
        queue_name = config_data["queue"] # Use the queue name from the configuration file

    # Define daemon lifetime
    if args.lifetime is not None:
        lifetime = args.lifetime # Use the lifetime from the arguments
    else:
        lifetime = config_data["lifetime"] # Use the lifetime from the configuration file

    # Define the rest of the configuration
    group = config_data["group"]
    resources = config_data["resources"]
    environment = config_data["environment"]
    target = config_data["target"]
    source_string = " --directory "+config_data["source"]
    varnames_string = " --varnames "+config_data["varnames"]
    pvarnames_string = " --pvarnames "+config_data["pvarnames"]
    analysis_string = " --analysis "+config_data["analysis"] if config_data["analysis"] != "" else ""
    analysis_dir_string = " --analysis_dir "+config_data["analysis_dir"] if config_data["analysis_dir"] != "" else ""
    verbose_string = " --verbose" if config_data["verbose"] else ""
    daemon_mode_string = " --daemon_mode" if not args.once_only else ""
    delete_originals_string = " --delete_originals" if config_data["delete_originals"] else ""
    wait_time_string = " --wait_time "+str(config_data["wait_time"])
    one_batch_at_a_time_string = " --one_batch_at_a_time" if config_data["one_batch_at_a_time"] else ""
    batch_size_string = " --batch_size "+str(config_data["batch_size"])

    # Generate run script
    lines = [
        f"#!/bin/csh\n",
        f"#PBS -S /bin/csh\n", # specify job shell
        f"#PBS -W group_list={group}\n", # specify group ID to charge for this job
        f"#PBS -l {resources}\n", # specify requested resources
        f"#PBS -l walltime={lifetime}:00:00\n", # specify job walltime
        f"#PBS -N {args.name}\n",  # set job name
        f"#PBS -e {daemon_dir}\n", # direct standard error output to deamon config directory
        f"#PBS -o {daemon_dir}\n", # direct standard output to deamon config directory
        f"{environment}\n", # shell command to setup environment
        f"cd {target}\n", # change into working directory
        f"pySnapCollate direct"+source_string+varnames_string+pvarnames_string+verbose_string+analysis_string+analysis_dir_string+daemon_mode_string+delete_originals_string+wait_time_string+one_batch_at_a_time_string+batch_size_string+" >> pySnapCollate.output \n" # run command
    ]

    # Write run script to file
    with open(script_path, 'w') as script_file:
        script_file.writelines(lines)

    # Construct the qsub command
    command = ["qsub", "-q", queue_name, script_path]

    # Execute the command
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(f"Daemon '{args.name}' has been sbumitted successfully.")
        print("Scheduler Output:", result.stdout)

        # Store job ID from the output
        job_id_match = re.search(r'(\d+)', result.stdout)
        if job_id_match:
            job_id = job_id_match.group(1)
            active_file_path = os.path.join(daemon_dir, f"active_job.{job_id}")
            with open(active_file_path, 'w') as active_file:
                pass  # Create an empty file
            print(f"Job ID {job_id} submitted. Created file: {active_file_path}")
        else:
            print("Job ID not found in the scheduler output.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while starting the daemon: {e.stderr}")

# =========================================

def stop_daemon(args):
    """
    Stop daemon by deleting it from the run queue.
    """

    config_dir = os.path.expanduser(config_dir_name) # Daemon configuration directory
    daemon_dir = os.path.join(config_dir, args.name) # Directory for the specific daemon
    config_path = os.path.join(daemon_dir, "config.json") # Configuration file for the specific daemon
    script_path = os.path.join(daemon_dir, "run.csh") # Run script for the specific daemon
    active_pattern = os.path.join(daemon_dir, "active_job.*") # Acive daemo tag

    # Check if the configuration file exists
    if not os.path.exists(config_path):
        print(f"No configuration file found for daemon '{args.name}'.")
        return

    # Read the configuration data from the JSON file
    try:
        with open(config_path, 'r') as config_file:
            config_data = json.load(config_file)
    except Exception as e:
        print(f"An error occurred while inspecting the daemon: {e}")

    # Check if daemon is active
    is_in_queue = False
    for active_file in glob.glob(active_pattern):
        # Active daemon file found, let's check if daemon is actually queued or running
        command = ["qstat", active_file.rsplit('.', 1)[-1]]
        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            is_in_queue = True
        except subprocess.CalledProcessError as e:
            # And let user know it wasn't even queued/running
            print(f"Daemon '{args.name}' was already no longer queued/running.")
            # Delete active daemon file
            os.remove(active_file)
        if is_in_queue:
            # Delete queued or running job
            command = ["qdel", active_file.rsplit('.', 1)[-1]]
            try:
                result = subprocess.run(command, check=True, capture_output=True, text=True)
                print(f"Daemon '{args.name}' is being stopped.")
                # Delete active daemon file
                os.remove(active_file)
            except subprocess.CalledProcessError as e:
                # And let user know it wasn't even queued/running
                print(f"Unable to stop daemon '{args.name}'. Try again")
        else:
            print(f"Daemon '{args.name}' is not submitted/running.")
    if len(glob.glob(active_pattern)) == 0:
        print(f"Daemon '{args.name}' is not submitted/running.")


# =========================================

def list_daemons():
    """
    List existing daemon configurations.
    """

    # Define the path to the daemon configuration directory
    config_dir = os.path.expanduser("~/.pySnapCollate")
    
    # Check if the directory exists
    if not os.path.exists(config_dir):
        print("No daemons found. The configuration directory does not exist.")
        return

    # List all files in the directory
    try:
        daemon_names = os.listdir(config_dir)
        # Print the names
        if daemon_names:
            print("Existing daemons:")
            for name in daemon_names:
                if os.path.isdir(os.path.join(config_dir, name)):
                    print(name)
        else:
            print("No daemons found.")
    except Exception as e:
        print(f"An error occurred while listing daemons: {e}")

# =========================================

def remove_daemon(args):
    """
    Remove daemon by deleting its configuration files.
    """

    config_dir = os.path.expanduser(config_dir_name) # Daemon configuration directory
    daemon_dir = os.path.join(config_dir, args.name) # Directory for the specific daemon
    config_path = os.path.join(daemon_dir, "config.json") # Configuration file for the specific daemon
    script_path = os.path.join(daemon_dir, "run.csh") # Run script for the specific daemon
    active_pattern = os.path.join(daemon_dir, "active_job.*") # Acive daemo tag

    # Check if the daemion directory
    if not os.path.exists(daemon_dir):
        print(f"No daemon named '{args.name}' found.")
        return

    # Check if daemon is active
    already_in_queue = False
    for active_file in glob.glob(active_pattern):
        # Active daemon file found, let's check if daemon is actually queued or running
        command = ["qstat", active_file.rsplit('.', 1)[-1]]
        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            print(f"Daemon '{args.name}' is aready queued/running.")
            already_in_queue = True
        except subprocess.CalledProcessError as e:
            # Old active daemon file exists but is no longeer queued or running, let's delete it
            os.remove(active_file)

    # Leave here if there is an active daemon queued/running
    if already_in_queue:
        print(f"Daemon is queue/running. Stop daemon first before removing.")
        return
    else: # otherwise delete directory for this daemon
        for filepath in [config_path, script_path]:
            if os.path.exists(filepath):
                os.remove(filepath)
        try:
            os.rmdir(daemon_dir)
        except OSError as e:
            # Specific check for directory not being empty
            if e.errno == errno.ENOTEMPTY:
                if not args.force:
                    print(f"Error: Cannot remove daemon directory because of unknown file(s). Use flag --force to delete anyways - {e}")
                else:
                    shutil.rmtree(daemon_dir)
            else:
                # Handle other potential OS-related errors
                print(f"Unexpected OS error: {e}")

# =========================================

def read_defaults():
    """
    Read default settings for daemon configuration.
    """

    config_dir = os.path.expanduser(config_dir_name) # Daemon configuration directory
    defaults_path = os.path.join(config_dir, "defaults.json") # Defaults JSON file
    
    # Read the configuration data from the JSON file
    try:
        with open(defaults_path, 'r') as defaults_file:
            defaults = json.load(defaults_file)
    except Exception as e:
        defaults = {"group": None,
                    "resources": default_default_resources,
                    "environment": default_default_environment,
                    "lifetime": default_default_lifetime,
                    "queue": default_default_queue,
                    }

    return defaults

# =========================================

def set_defaults(args):
    """
    Set default daemon configuration.
    """

    config_dir = os.path.expanduser(config_dir_name) # Daemon configuration directory
    os.makedirs(config_dir, exist_ok=True)  # Create the directory if it doesn't exist
    defaults_path = os.path.join(config_dir, "defaults.json") # Defaults JSON file

    # Prepare the defaults data
    defaults = {"group": args.group,
                "resources": args.resources,
                "environment": args.environment,
                "lifetime": args.lifetime,
                "queue": args.queue,
               }

    # Write defaults to the JSON file
    try:
        with open(defaults_path, 'w') as defaults_file:
            json.dump(defaults, defaults_file, indent=4)
    except Exception as e:
        print(f"An error occurred while setting defaults: {e}")
