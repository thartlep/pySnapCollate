####################################################
# FILE 'utilities.py'
####################################################
# Written by
# Thomas Hartlep
# Bay Area Environmental Research Institute
# January, October 2024, August 2025
####################################################

import sys, os, errno, glob, gc, time
from datetime import datetime
import numpy as np

#-----------------------------------------
class parameter_class():

    def initialize(self, parameter_dict):

        pass

#-----------------------------------------
def add_argparse_argument(parser, what, parameters):
    import pySnapCollate
    if what == 'version':
        parser.add_argument('-v',  '--version', action='version', version='pySnapCollate {}'.format(pySnapCollate.__full_version_info__))
    elif what == 'dry_run':
        parser.add_argument('-dr', '--dry_run', action = 'store_true', help = 'dry run; nothing is actually done (default: False)', default = False)
#-----------------------------------------
def mkdir(dir):
  try:
      os.makedirs(dir)
  except OSError as exc: # Python >2.5
      if exc.errno == errno.EEXIST and os.path.isdir(dir):
          pass
      else: raise
#-----------------------------------------
def metadata(filename, with_metadata=True):
    if with_metadata:
        from pySensitivityKernels import __full_version_info__
        from getpass import getuser
        import __main__ as main
        import os
        try:
            script_name = os.path.basename(main.__file__)
        except:
            script_name = 'Console '
        creator = script_name + ' using pySnapCollate {}'.format(__full_version_info__)
        if filename[-4:] == '.pdf':
            metadata = {'Author':getuser(), 'Creator':creator}
        elif filename[-4:] == '.png':
            metadata = {'Author':getuser(), 'Software':creator}
    else:
        metadata = {}
    return metadata
#-----------------------------------------
# Generate version info (just the version number if it is NOT a development version, otherwise include last git commit date, author and hash)
def generate_full_version_info(version, path):
    if version.find('dev') > 0:
        import git
        import time
        repo = git.Repo(path)
        hash = repo.head.object.hexsha
        commit_date = time.strftime("%Y/%m/%d %H:%M:%S %Z", time.localtime(repo.head.object.committed_date))
        author = repo.head.object.author
        return  'v.{} last committed {} by {} with hash {}'.format(version, commit_date, author, hash)
    else:
        return 'v.{}'.format(version)
#-----------------------------------------
def dump_parameters(parameter_dict):
    import json
    with open('parameters', 'w') as file:
        json.dump(parameter_dict, file)
#-----------------------------------------
def load_parameters():
    import json
    try:
        with open('parameters', 'r') as file:
            parameter_dict = json.load(file)
        parameters = parameter_class()
        parameters.initialize(parameter_dict)
    except FileNotFoundError:
        print('No parameter file found! Must initialize analysis pipeline with pyTrajStat__step0__setup')
        quit()
    return parameters
#-----------------------------------------
def gc_callback(phase, info):
    if phase == 'stop':
        # Garbage collection has completed, so we can proceed
        time.sleep(0.1)  # Wait a bit to ensure memory is released
    
##################################################################
# End of file: utilities.py                                      #
##################################################################
