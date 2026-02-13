import os
import tempfile

# Some system have a ramdisk mounted by default, we can use it instead of /tmp
# as the default folder to dump big arrays to share with subprocesses.
SYSTEM_SHARED_MEM_FS = "/dev/shm"

# Minimal number of bytes available on SYSTEM_SHARED_MEM_FS to consider using
# it as the default folder to dump big arrays to share with subprocesses.
SYSTEM_SHARED_MEM_FS_MIN_SIZE = int(2e9)


def _get_temp_dir(pool_folder_name, temp_folder=None):
    """Get the full path to a subfolder inside the temporary folder.

    Parameters
    ----------
    pool_folder_name : str
        Sub-folder name used for the serialization of a pool instance.

    temp_folder: str, optional
        Folder to be used by the pool for memmapping large arrays
        for sharing memory with worker processes. If None, this will try in
        order:

        - a folder pointed by the JOBLIB_TEMP_FOLDER environment
          variable,
        - /dev/shm if the folder exists and is writable: this is a
          RAMdisk filesystem available by default on modern Linux
          distributions,
        - the default system temporary folder that can be
          overridden with TMP, TMPDIR or TEMP environment
          variables, typically /tmp under Unix operating systems.

    Returns
    -------
    pool_folder : str
       full path to the temporary folder
    use_shared_mem : bool
       whether the temporary folder is written to the system shared memory
       folder or some other temporary folder.
    """
    use_shared_mem = False
    if temp_folder is None:
        temp_folder = os.environ.get("JOBLIB_TEMP_FOLDER", None)
    if temp_folder is None:
        if os.path.exists(SYSTEM_SHARED_MEM_FS) and hasattr(os, "statvfs"):
            try:
                shm_stats = os.statvfs(SYSTEM_SHARED_MEM_FS)
                available_nbytes = shm_stats.f_bsize * shm_stats.f_bavail
                if available_nbytes > SYSTEM_SHARED_MEM_FS_MIN_SIZE:
                    # Try to see if we have write access to the shared mem
                    # folder only if it is reasonably large (that is 2GB or
                    # more).
                    temp_folder = SYSTEM_SHARED_MEM_FS
                    pool_folder = os.path.join(temp_folder, pool_folder_name)
                    if not os.path.exists(pool_folder):
                        os.makedirs(pool_folder)
                    use_shared_mem = True
            except (IOError, OSError):
                # Missing rights in the /dev/shm partition, fallback to regular
                # temp folder.
                temp_folder = None
    if temp_folder is None:
        # Fallback to the default tmp folder, typically /tmp
        temp_folder = tempfile.gettempdir()
    temp_folder = os.path.abspath(os.path.expanduser(temp_folder))
    pool_folder = os.path.join(temp_folder, pool_folder_name)
    return pool_folder, use_shared_mem
