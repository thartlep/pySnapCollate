#!/usr/bin/env python
####################################################
# FILE 'daemon.py'
####################################################
# Written by
# Thomas Hartlep
# Bay Area Environmental Research Institute
# August 2025
####################################################

import os
import argparse
import subprocess
import json
import re
import glob
import errno
import shutil

default_config_dir_name = "~/.pySnapCollate"
default_queue = 'normal'
default_lifetime = 8
default_resources = 'select=1:ncpus=24:mpiprocs=1:ompthreads=48:model=has'
default_environment = 'conda activate pencil'
varname_list = ['dx', 'dy', 'dz', 'np', 'rho', 'rhop', 't', 'ux', 'uy', 'uz', 'x', 'y', 'z']
pvarname_list = ['ipars', 'ivpx', 'ivpy', 'ivpz', 'ixp', 'iyp', 'izp', 'vpx', 'vpy', 'vpz', 'xp', 'yp', 'zp']

def setup_daemon(args):
    # Define the path to the daemon configuration directory
    config_dir = os.path.expanduser(default_config_dir_name)
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
        "source": args.source,
        "target": args.target,
        "lifetime": args.lifetime,
        "group": args.group,
        "resources": args.resources,
        "environment": args.environment,
        "queue": args.queue,
        "varnames":  ' '.join(args.varnames),
        "pvarnames":  ' '.join(args.pvarnames),
        "verbose": args.verbose,
    }

    # Write the configuration data to the JSON file
    try:
        with open(config_path, 'w') as config_file:
            json.dump(config_data, config_file, indent=4)
        print(f"Daemon '{args.name}' has been set up successfully.")
    except Exception as e:
        print(f"An error occurred while setting up the daemon: {e}")

####################################################
def inspect_daemon(args):
    # Define the path to the daemon configuration directory
    config_dir = os.path.expanduser(default_config_dir_name)
    daemon_dir = os.path.join(config_dir, args.name)  # Directory for the specific daemon
    config_path = os.path.join(daemon_dir, "config.json")
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

####################################################
def start_daemon(args):
    config_dir = os.path.expanduser(default_config_dir_name) # Daemon configuration directory
    daemon_dir = os.path.join(config_dir, args.name)  # Directory for the specific daemon
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
    source = config_data["source"]
    varnames = config_data["varnames"]
    pvarnames = config_data["pvarnames"]
    verbose = config_data["verbose"]

    # Generate run script
    lines = [
        f"#!/bin/csh\n",
        f"#PBS -S /bin/csh\n",
        f"#PBS -W group_list={group}\n",
        f"#PBS -l {resources}\n",
        f"#PBS -l walltime={lifetime}:00:00\n",
        f"#PBS -r n\n",
        f"{environment}\n",
        f"cd {target}\n",
        f"pySnapCollate --directory {source} --varnames {varnames} --pvarnames {pvarnames} "+"--verbose "*verbose+" --daemon_mode >> pySnapCollate.output \n"
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

####################################################
def stop_daemon(args):
    config_dir = os.path.expanduser(default_config_dir_name) # Daemon configuration directory
    daemon_dir = os.path.join(config_dir, args.name)  # Directory for the specific daemon
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


####################################################
def list_daemons():
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
                print(name)
        else:
            print("No daemons found.")
    except Exception as e:
        print(f"An error occurred while listing daemons: {e}")

####################################################
def remove_daemon(args):
    config_dir = os.path.expanduser(default_config_dir_name) # Daemon configuration directory
    daemon_dir = os.path.join(config_dir, args.name)  # Directory for the specific daemon
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

####################################################
def main():
    parser = argparse.ArgumentParser(description="Manage your pySnapCollate daemons.")
    subparsers = parser.add_subparsers(dest='command', required=True)

    # Setup command
    setup_parser = subparsers.add_parser('setup', help='Set up a new daemon')
    setup_parser.add_argument('--name', help='Name of the daemon', required=True)
    setup_parser.add_argument('--source', help='Source directory', required=True)
    setup_parser.add_argument('--target', help='Target directory', required=True)
    setup_parser.add_argument('--group', help='Group ID', required=True)
    setup_parser.add_argument('--resources', help='Resource string (default '+default_resources+')', default = default_resources)
    setup_parser.add_argument('--environment', help='Command line for setting up environment (default: '+default_environment+')', default = default_environment)
    setup_parser.add_argument('--lifetime', help='Lifetime in hours (default: '+str(default_lifetime)+')', default = default_lifetime, type = int)
    setup_parser.add_argument('--queue', help='Name of scheduler queue (default: '+default_queue+')', default = default_queue)
    setup_parser.add_argument('--varnames', nargs = "*", help='Name(s) of variable(s) to export (default: '+' '.join(sorted(varname_list))+')', default = varname_list)
    setup_parser.add_argument('--pvarnames', nargs = "*", help='Name(s) of particle variable(s) name(s) to export (default: '+' '.join(sorted(pvarname_list))+')', default = pvarname_list)
    setup_parser.add_argument('--force', action = 'store_true', help = 'Forced override of existing daemon configuration (default: False)', default = False)
    setup_parser.add_argument('--verbose', action = 'store_true', help = 'Verbose output (default: False)', default = False)

    # Start command
    start_parser = subparsers.add_parser('start', help='Start a daemon')
    start_parser.add_argument('--name', help='Name of the daemon', required=True)
    start_parser.add_argument('--lifetime', help='Lifetime in hours (overrides setup value)', default = None, type = int)
    start_parser.add_argument('--queue', help='Name of scheduler queue (overrides setup value)', default = None)

    # Stop command
    stop_parser = subparsers.add_parser('stop', help='Stop a daemon')
    stop_parser.add_argument('--name', help='Name of the daemon', required=True)

    # Inspect command
    inspect_parser = subparsers.add_parser('inspect', help='Inspect daemon configuration')
    inspect_parser.add_argument('--name', help='Name of the daemon', required=True)

    # Remove command
    remove_parser = subparsers.add_parser('remove', help='Remove a daemon, that is, delete the daemon configuration')
    remove_parser.add_argument('--name', help='Name of the daemon', required=True)
    remove_parser.add_argument('--force', action = 'store_true', help='Force deleting even with unknown files are in configuration directory', default = False)

    # List command
    list_parser = subparsers.add_parser('list', help='List all daemons')

    args = parser.parse_args()

    if args.command == 'setup':
        setup_daemon(args)
    elif args.command == 'start':
        start_daemon(args)
    elif args.command == 'stop':
        stop_daemon(args)
    elif args.command == 'inspect':
        inspect_daemon(args)
    elif args.command == 'remove':
        remove_daemon(args)
    elif args.command == 'list':
        list_daemons()  # Call without arguments

if __name__ == "__main__":
    main()

##################################################################
# End of file: daemon.py
##################################################################
