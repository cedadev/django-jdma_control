"""Generic class for a JASMIN Data Migration App backend.
   All other backends should inherit from this class.
   See FakeTapeBackend for a fully documented example of a derived class."""

class Backend(object):
    """Super class for all JASMIN Data Migration App backends.abs
       All of these functions should be overloaded, i.e. the class is pure virtual."""
    def monitor(self): pass     # monitor the external storage, return which requests have completed
    def get(self, batch_id, target_dir): pass   # get the batch from the external storage and download to a target_dir
    def put(self, filelist): pass   # put a list of files onto the external storage - return the external storage batch id

    # permissions / quota
    def user_has_permission(self, user, workspace):
        return False   # does the user have permission to write to the workspace
    def user_has_remaining_quota(self, filelist, user, workspace):
        return False  # get the remaining quota for the user in the workspace

    def get_name(self): pass     # get the name for error messages
