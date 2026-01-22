"""CLI module for pySnapCollate: parses arguments and calls core functions."""

# ==== Imports ============================

import argparse
from . import __full_version_info__
from .core import export as run_direct
from .core import read_defaults, set_defaults
from .core import setup_daemon, copy_daemon, modify_daemon, start_daemon, stop_daemon, inspect_daemon, remove_daemon, list_daemons
from .core import default_auto_wait, default_default_environment, default_default_lifetime, default_default_queue, default_default_resources
from .core import varname_list, pvarname_list

# =========================================

def main():

    defaults = read_defaults()
    default_group = 'None' if defaults["group"] is None else defaults["group"]
    default_resources = defaults["resources"]
    default_environment = defaults["environment"]
    default_lifetime = defaults["lifetime"]
    default_queue = defaults["queue"]

    parser = argparse.ArgumentParser(description="Manage pySnapCollate daemons on PBS or run export directly on local machine")
    parser.add_argument('-v',  '--version', action='version', version='pySnapCollate {}'.format(__full_version_info__))
    subparsers = parser.add_subparsers(dest='command') #, required=True)

    # Default command
    default_parser = subparsers.add_parser('defaults', help='Define default configuration settings')
    default_parser.add_argument('--group', help='Group ID', default = None, required = True)
    default_parser.add_argument('--resources', help='Resource string (default: '+default_default_resources+')', default = default_default_resources)
    default_parser.add_argument('--environment', help='Command line for setting up environment (default: '+default_default_environment+')', default = default_default_environment)
    default_parser.add_argument('--lifetime', help='Lifetime in hours (default: '+str(default_default_lifetime)+')', default = default_default_lifetime, type = int)
    default_parser.add_argument('--queue', help='Name of scheduler queue (default: '+default_default_queue+')', default = default_default_queue)

    # Setup command
    setup_parser = subparsers.add_parser('setup', help='Set up a new daemon for PBS execution')
    setup_parser.add_argument('name', help='Name of the daemon')
    setup_parser.add_argument('--source', help='Source directory', required=True)
    setup_parser.add_argument('--target', help='Target directory', required=True)
    setup_parser.add_argument('--group', help='Group ID (default: '+default_group+')', default = default_group)
    setup_parser.add_argument('--resources', help='Resource string (default: '+default_resources+')', default = default_resources)
    setup_parser.add_argument('--environment', help='Command line for setting up environment (default: '+default_environment+')', default = default_environment)
    setup_parser.add_argument('--lifetime', help='Lifetime in hours (default: '+str(default_lifetime)+')', default = default_lifetime, type = int)
    setup_parser.add_argument('--queue', help='Name of scheduler queue (default: '+default_queue+')', default = default_queue)
    setup_parser.add_argument('--varnames', nargs = "*", help='Name(s) of variable(s) to export (default: '+' '.join(sorted(varname_list))+')', default = varname_list)
    setup_parser.add_argument('--pvarnames', nargs = "*", help='Name(s) of particle variable(s) name(s) to export (default: '+' '.join(sorted(pvarname_list))+')', default = pvarname_list)
    setup_parser.add_argument('--force', action = 'store_true', help = 'Forced override of existing daemon configuration (default: False)', default = False)
    setup_parser.add_argument('--verbose', action = 'store_true', help = 'Verbose output (default: False)', default = False)
    setup_parser.add_argument('--analysis', help='Analysis command to be run in target directory after export (default: None)', default = None)
    setup_parser.add_argument('--analysis_dir', help='Analysis directory (default: same as target directory)', default = None)
    setup_parser.add_argument('--delete_originals', action = 'store_true', help = 'Delete original snapshot(s) after successful data collation (default: False)', default = False)
    setup_parser.add_argument('--wait_time', help = 'Wait time for next snapshot discovery (default: '+str(default_auto_wait)+')', default = default_auto_wait, type=int)
    setup_parser.add_argument('--one_batch_at_a_time', action = 'store_true', help = 'Process one batch of snapshots at a time (export + analysis), otherwise export all available data first (default: False)', default = False)
    setup_parser.add_argument('--batch_size', help='Batch size, number of snapshots exported in parallel (default: 1)', default=1, type = int)
    
     # Modify command
    modify_parser = subparsers.add_parser('modify', help='Modify a daemon configuration')
    modify_parser.add_argument('name', help='Name of the daemon')
    modify_parser.add_argument('--source', help='Source directory (default: no change)', default=None)
    modify_parser.add_argument('--target', help='Target directory (default: no change)', default=None)
    modify_parser.add_argument('--group', help='Group ID (default: no change)', default = None)
    modify_parser.add_argument('--resources', help='Resource string (default: no change)', default = None)
    modify_parser.add_argument('--environment', help='Command line for setting up environment (default: no change)', default=None)
    modify_parser.add_argument('--lifetime', help='Lifetime in hours (default: no change)', default=None, type = int)
    modify_parser.add_argument('--queue', help='Name of scheduler queue (default: no change)', default=None)
    modify_parser.add_argument('--varnames', nargs = "*", help='Name(s) of variable(s) to export (default: no change)', default=None)
    modify_parser.add_argument('--pvarnames', nargs = "*", help='Name(s) of particle variable(s) name(s) to export (default: no change)', default=None)
    modify_parser.add_argument('--force', action = 'store_true', help = 'Forced override of existing daemon configuration (default: no change)', default=None)
    modify_parser.add_argument('--analysis', help='Analysis command to be run in target directory after export (default: no change)', default=None)
    modify_parser.add_argument('--analysis_dir', help='Analysis directory (default: same as target directory)', default = None)
    group = modify_parser.add_mutually_exclusive_group()
    group.add_argument('--delete_originals', dest='delete_originals', action='store_true', help = 'Delete original snapshot(s) after successful data collation (default: no change)')
    group.add_argument('--no_delete_originals', dest='delete_originals', action='store_false', help = 'Do not delete original snapshot(s) after successful data collation (default: no change)')
    modify_parser.set_defaults(delete_originals=None)
    modify_parser.add_argument('--wait_time', help = 'Wait time for next snapshot discovery (default: no change)', default=None, type=int)
    group2 = modify_parser.add_mutually_exclusive_group()
    group2.add_argument('--one_batch_at_a_time', dest='one_batch_at_a_time', action='store_true', help = 'Process one batch of snapshots at a time (export + analysis), otherwise export all available data first (default: no change)')
    group2.add_argument('--no_one_batch_at_a_time', dest='one_batch_at_a_time', action='store_false', help = 'Export all available data first before calling analysis, if applicable (default: no change)')
    modify_parser.set_defaults(one_batch_at_a_time=None)
    modify_parser.add_argument('--batch_size', help='Batch size (default: no change)', default=None, type = int)
    group3 = modify_parser.add_mutually_exclusive_group()
    group3.add_argument('--verbose', dest='verbose', action='store_true', help = 'Verbose output (default: no change)')
    group3.add_argument('--no_verbose', dest='verbose', action='store_false', help = 'Turn off verbose output (default: no change)')
    modify_parser.set_defaults(verbose=None)
 
     # Copy command
    copy_parser = subparsers.add_parser('copy', help='Copy a daemon configuration')
    copy_parser.add_argument('src_name', help='Name of the source daemon')
    copy_parser.add_argument('dest_name', help='Name of the destination daemon')
 
    # Start command
    start_parser = subparsers.add_parser('start', help='Start a daemon via PBS')
    start_parser.add_argument('name', help='Name of the daemon')
    start_parser.add_argument('--lifetime', help='Lifetime in hours (overrides setup value)', default = None, type = int)
    start_parser.add_argument('--queue', help='Name of scheduler queue (overrides setup value)', default = None)
    start_parser.add_argument('--once_only', action = 'store_true', help = 'Run once only, then automatically stop (default: False)', default = False)

    # Stop command
    stop_parser = subparsers.add_parser('stop', help='Stop a daemon via PBS')
    stop_parser.add_argument('name', help='Name of the daemon')

    # Inspect command
    inspect_parser = subparsers.add_parser('inspect', help='Inspect a daemon configuration')
    inspect_parser.add_argument('name', help='Name of the daemon')

    # Remove command
    remove_parser = subparsers.add_parser('remove', help='Remove a daemon configuration')
    remove_parser.add_argument('name', help='Name of the daemon')
    remove_parser.add_argument('--force', action = 'store_true', help='Force deleting even with unknown files are in configuration directory', default = False)

    # List command
    list_parser = subparsers.add_parser('list', help='List configured daemon(s)')

    # Direct command
    direct_parser = subparsers.add_parser('direct', help='Run collate operation directly bypassing daemon configuration setup')
    direct_parser.add_argument('--varnames', nargs = "*", help = 'Variable name(s) to export, leave empty to get list of available names if varfile provided (default: '+' '.join(sorted(varname_list))+')', default = varname_list)
    direct_parser.add_argument('--varfiles', nargs = "*", help = 'Name(s) of snapshot files, automatically find snapshots if none provided (default: None)', default = [])
    direct_parser.add_argument('--pvarnames', nargs = "*", help = 'Particle variable name(s) to export, leave empty to get list of available names if pvarfile provided (default: '+' '.join(sorted(pvarname_list))+')', default = pvarname_list)
    direct_parser.add_argument('--pvarfiles', nargs = "*", help = 'Name(s) of particle snapshot files, automatically find snapshots if none provided (default: None)', default = [])
    direct_parser.add_argument('--directory', help = 'Directory of input data (default: .)', default = '.')
    direct_parser.add_argument('--verbose', action = 'store_true', help = 'Verbose output (default: False)', default = False)
    direct_parser.add_argument('--daemon_mode', action = 'store_true', help = 'Daemon mode, automatically restart code after set wait time (default: False)', default = False)
    direct_parser.add_argument('--wait_time', help = 'Wait time when running in daemon mode (default: '+str(default_auto_wait)+')', default = default_auto_wait, type=int)
    direct_parser.add_argument('--analysis', help='Analysis command to be run after export (default: None)', default = None)
    direct_parser.add_argument('--analysis_dir', help='Analysis directory (default: .)', default = '.')
    direct_parser.add_argument('--delete_originals', action = 'store_true', help = 'Delete original snapshot(s) after successful data collation (default: False)', default = False)
    direct_parser.add_argument('--one_batch_at_a_time', action = 'store_true', help = 'Process one batch of snapshots at a time (export + analysis), otherwise export all available data first (default: False)', default = False)
    direct_parser.add_argument('--batch_size', help='Batch size, number of snapshots exported in parallel (default: 1)', default=1, type = int)

    args = parser.parse_args()

    if args.command == 'defaults':
        set_defaults(args)
    elif args.command == 'setup':
        setup_daemon(args)
    elif args.command == 'copy':
        copy_daemon(args)
    elif args.command == 'modify':
        modify_daemon(args)
    elif args.command == 'start':
        start_daemon(args)
    elif args.command == 'stop':
        stop_daemon(args)
    elif args.command == 'inspect':
        inspect_daemon(args)
    elif args.command == 'remove':
        remove_daemon(args)
    elif args.command == 'list':
        list_daemons()
    elif args.command == 'direct':
        run_direct(args)
    else:
        parser.print_help()

# =========================================

if __name__ == "__main__":
    main()
