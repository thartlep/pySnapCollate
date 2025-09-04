####################################################
# FILE 'utilities.py'
####################################################
# Written by
# Thomas Hartlep
# Bay Area Environmental Research Institute
# January, October 2024, August, September 2025
####################################################

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
def resolve_path(path):
    """
    Safely expand and resolve a path with both user and relative path support
    """
    try:
        import os
        return os.path.abspath(os.path.expanduser(path))
    except Exception as e:
        return path  # Fallback to original path if resolution fails   

##################################################################
# End of file: utilities.py                                      #
##################################################################
