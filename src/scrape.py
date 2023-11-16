# Scrape a public Mega.nz folder from the exportlink (URL) and update a specified local folder with
# the missing/updated contents from the remote location.

"""Process Flow
Check that MEGAcmd server is running.
    If the server is not running, start it.
    Check if the server has started on a reasonable interval (once a second?) until the server
      starts or gives an error.
If we are already logged in, log out.
Log in to the remote folder. (command: mega-login folder_url)
Get/compute the directory structure starting at the given folder url
  (command: mega-ls -l remote_path --time-format=ISO6081_WITH_TIME).
    Recursively check each folder all the way to the end, depth first.
      (flag_0={d=folder, -=file, r=root, i=inbox, b=rubbish, x=unsupported})
Create any directories that are present in the remote and not present in the local.
Compare all files in remote to corresponding local files.
    If the remote file is not present at the local location, add to download queue.
    If the remote file size is different, add to download queue (overwrite).
    If the remote file modify date is newer than the local file, add to the download queue
      (overwrite).
"""

import os
import sys
import logging
import argparse
import datetime
import subprocess
from typing import TextIO, Self, List, Dict
from platform import system


class DualLogger():
    """Send logging messages to both the specified file and the stderr of the CLI application."""
    def __init__(
        self: Self,
        filename: str,
        fileLevel=logging.DEBUG,
        stream: TextIO = sys.stderr,
        streamLevel=logging.INFO,
    ):
        # Hard-coded formatting info.
        format = "%(asctime)s.%(msecs)03d [%(threadName)-12.12s] [%(levelname)-8.8s]  %(message)s"
        datefmt = '%Y-%m-%dT%H:%M:%S'
        encoding = 'utf-8'

        # Initialize general logging objects.
        msgFormatter = logging.Formatter(fmt=format, datefmt=datefmt)
        rootLogger = logging.getLogger()
        rootLogger.setLevel(min(fileLevel, streamLevel))

        # Make the log directory if it doesn't exist yet.
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))

        # Start the to-file logging.
        fileHandler = logging.FileHandler(filename=filename, encoding=encoding)
        fileHandler.setFormatter(msgFormatter)
        fileHandler.setLevel(fileLevel)
        rootLogger.addHandler(fileHandler)

        # Start the to-terminal logging.
        consoleHandler = logging.StreamHandler(stream=stream)
        consoleHandler.setFormatter(msgFormatter)
        consoleHandler.setLevel(streamLevel)
        rootLogger.addHandler(consoleHandler)

    def testLogger(self: Self) -> None:
        logging.critical("DualLogger test message (critical)")
        logging.error("DualLogger test message (error)")
        logging.warning("DualLogger test message (warning)")
        logging.info("DualLogger test message (info)")
        logging.debug("DualLogger test message (debug)")


class MEGAsync():

    def __init__(
        self: Self,
        folder_url: str,
        dest_path: str,
    ):
        self.remoteRoot = folder_url
        self.localRoot = dest_path

        # OS-specific shell call.
        if system() == "Windows":
            self.OSShell = "Powershell"
        else:
            msg = f"Not suppoted for OS: {system()}"
            logging.critical(msg)
            raise NotImplementedError(msg)

        self.remoteTree = []
        self.downloadNodes = []
        self.replaceNodes = []

    def login(
        self: Self,
    ) -> int:
        """
        login Using MEGAcmd API log in to the remote folder.

        Returns
        -------
        int
            Return code of the MEGA-LOGIN cmdlet (0 is success).
        """
        # Log out (in case we are already logged in to MEGA).
        if self.logout():
            raise Exception("MEGA-LOGOUT failed")

        # Log in to the remotepath.
        cmd = [self.OSShell, "mega-login", self.remoteRoot]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        while not p.poll():
            logging.error(p.stdout.readline())

        subprocess.run(["mega-cd" "/"])

        return p.returncode

    def ls(
        self: Self,
        path: str,
    ) -> List[Dict[str, str]]:
        """
        ls Run the mega-ls command for the given node.

        Parameters
        ----------
        path : str
            Path to the desired directory relative to the remote URL provided.

        Returns
        -------
        List[Dict[str, str]]
            All nodes present within the path.

        Raises
        ------
        OSError
            If the mega-ls call returns a non-zero return code, that code is sent up the stack as
            an OSError Exception. The path that caused the mega-ls issue is included as the
            filename.
        """
        nodes = []
        # command: mega-ls -l $remote_path --time-format=ISO6081_WITH_TIME
        cmd = [self.OSShell, "mega-ls", "-l", path, "--time-format=ISO6081_WITH_TIME"]
        p = subprocess.run(cmd, stdout=subprocess.STDOUT)

        if p.returncode != 0:
            raise OSError(errno=p.returncode, filename=path)

        for line in str(p.stdout()).split('\n'):
            type = line[0]
            export = line[1]
            exportDuration = line[2]
            shared = line[3]
            version = int(line[5:9].lstrip())
            size = int(line[10:20].lstrip())
            date = datetime.datetime.strptime(line[21:40].strip(), "%Y-%m-%dT%H:%M:%S")
            name = line[41:].strip()
            nodePath = os.path.join(path, name).lstrip("/\\")

            node = {
                "type": type,
                "export": export,
                "export_duration": exportDuration,
                "shared": shared,
                "version": version,
                "size": size,
                "date": date,
                "name": name,
                "path": nodePath,
            }
            nodes.append(node)

        return nodes

    def getRemoteTree(
        self: Self,
    ) -> int:
        """
        getRemoteTree Compute the directory structure of the MEGA.nz public folder.

        Returns
        -------
        int
            Number of bad nodes found.
        """
        badNodes = 0
        tree = []
        nodesToCheck = [{
            "path": "/", "name": "/", "type": "d", "export": "-", "export_duration": "-",
            "shared": "-", "version": "-", "size": "-", "date": "-"
        }]  # Start with the root directory.

        while nodesToCheck:
            for n, node in enumerate(nodesToCheck):
                try:
                    newNodes = self.ls(node["path"])
                except OSError:
                    logging.error(f"Invalid path at: {node['path']}")
                    nodesToCheck.pop(n)
                    badNodes += 1
                    continue

                for node in newNodes:
                    if node["type"] == "d":  # Folder
                        nodesToCheck.extend(node)
                        tree.extend(node)
                    elif node["type"] == "-":  # File
                        tree.extend(node)
                    else:  # root, inbox, rubbish, or unsupported
                        msg = str(f"The node {node['path']} (type {node['type']})"
                                  + " is not a file or folder, ignoring.")
                        logging.info(msg)
                nodesToCheck.pop(n)

        # Make the tree accessible to the rest of the object.
        self.remoteTree = sorted(tree, key=lambda n: n["path"])

        return badNodes

    def getNewFolders(
        self: Self
    ) -> int:
        """
        getNewFolders Create all folders that are present on the remote and not on the local.

        Returns
        -------
        int
            Number of folders created.
        """
        newFolders = 0

        # Check all the nodes in the tree to see if there are any missing from the local.
        for node in self.remoteTree:
            if node['type'] == 'd':  # Only check folders.
                localNodePath = os.path.join(self.localRoot, node['path'])
                if not os.path.exists(localNodePath):
                    # Make sure a containing folder hasn't already been added.
                    isNeeded = True
                    for needNode in self.downloadNodes:
                        if node['path'].startswith(needNode['path']):
                            # A folder that contains the current node is already in the sync list.
                            isNeeded = False
                            break

                    if isNeeded:
                        self.downloadNodes.append(node)
                        newFolders += 1

        return newFolders

    def filesToSync(
        self: Self,
    ) -> int:
        """
        filesToSync Compute which files from the remote need to be downloaded.

        Returns
        -------
        int
            Number of files that need to be synced.
        """

        """Logic:
        If the remote file is not present at the local location, add to download queue.
        Else
            If the remote file size is different, add to the replace queue.
            If the remote file modify date is newer than the local file, add to the replace queue.
        """

        nSyncFiles = 0

        for node in self.remoteTree:
            if node['type'] == '-':  # File
                localPath = os.path.join(self.localRoot, node['path'])
                localDir = os.path.dirname(localPath)

                # Only check for single files that need to be downloaded; full folders are handled
                # in the getNewFolders method.
                if os.path.exists(localDir):
                    if not os.path.exists(localPath):
                        logging.debug(f"New download {node['path']}")
                        self.downloadNodes.append(node)
                        nSyncFiles += 1

                    else:  # Do we need to replace the file.
                        isSameSize = (os.path.getsize(localPath) == node['size'])
                        isRemoteNewer = (os.stat(localPath).st_mtime < node['date'])

                        if (not isSameSize) or isRemoteNewer:
                            logging.debug(f"Replace {node['path']}")
                            self.replaceNodes.append(node)
                            nSyncFiles += 1

        return nSyncFiles

    def queueDownloads(
        self: Self,
    ) -> int:
        """
        queueDownloads Assign all needed downloads to the MEGA-GET cmdlet.

        Returns
        -------
        int
            Number of downloads queued to MEGA-GET.
        """

        """Logic
        Handle files to be replaced.
            Move the old files to a temporary directory (retain relative directory structure),
              pending removal.
            DO NOT ADD THE REPLACEMENT FILES TO THE QUEUE UNTIL DEBUGGING IS COMPLETE
        Add new files to the MEGA-GET download queue.
          (mega-get -q $remotepath $localpath)
        Add new folders to the MEGA-GET download queue.
          (mega-get -q $remotepath $localpath)
        """
        newDownloads = 0

        # Prepare files to be replaced.
        tmpDir = os.path.join(self.localRoot, "tmp")
        with open(os.path.join(tmpDir, "_replace.log"), 'x') as f:
            for node in self.replaceNodes:
                logging.debug(f"Replace {node['path']}")
                f.write(node)

                # # Move the file to the tmp directory pending removal.
                # oldLocalPath = os.path.join(self.localRoot, node['path'])
                # newLocalPath = os.path.join(tmpDir, node['path'])
                # os.makedirs(os.path.dirname(newLocalPath), exist_ok=True)
                # os.rename(oldLocalPath, newLocalPath)

                # # Add to the download queue
                # self.downloadNodes.append(node)

        # Add new nodes to the download queue.
        for node in self.downloadNodes:
            cmd = [
                self.OSShell, "mega-get", "-q",
                os.path.join(self.remoteRoot, node['path']),
                os.path.join(self.localRoot, node['path'])
            ]
            p = subprocess.run(cmd, stdout=subprocess.STDOUT)
            if p.stdout:
                logging.error(p.stdout)

            newDownloads += 1

        return newDownloads

    def logout(
        self: Self,
    ) -> int:
        """
        logout Log out of the current MEGAcmd session.

        Returns
        -------
        int
            Return code of the MEGA-LOGOUT cmdlet process (0 is success).
        """
        cmd = [self.OSShell, "mega-logout"]
        p = subprocess.run(cmd, stdout=subprocess.STDOUT)
        if p.returncode:
            logging.critical(p.stdout)

        return p.returncode

    def sync(
        self: Self,
    ) -> bool:
        """Sync the specified local directory with the remote URL as truth.

        This method is the main access mode for this class. All checking happens in internal
        methods.

        Returns
        -------
        bool
            Sucess (or failure) of the sync.
        """
        if self.login():
            logging.critical("Failed to log in to MEGA.nz remote path. Please verify the link.")
            return False

        # Compute the remote file tree.
        nBadNodes = self.getRemoteTree()
        logging.debug(f"Encountered {nBadNodes} bad remote nodes.")

        # Get the list of all the new folders to be downloaded.
        nNewFolders = self.getNewFolders()
        logging.debug(f"Queued {nNewFolders} new folders to download.")

        # Compare all remote files to their local counterparts.
        nSyncFiles = self.filesToSync()
        logging.debug("Queued {} new file downloads ({} new, {} updated).".format(
            nSyncFiles, len(self.downloadNodes), len(self.replaceNodes)
        ))

        # Download all missing/old files.
        nNewDownloads = self.queueDownloads()
        logging.debug(f"Started {nNewDownloads} new downloads.")

        # Log out of the MEGA-CMD session.
        # self.logout()

        return True


if __name__ == "__main__":
    # Read command line.
    parser = argparse.ArgumentParser(
        description="""Scrape a public Mega.nz folder from the exportlink (URL) and update a
        specified local folder with the missing/updated contents from the remote location.""",
    )
    parser.add_argument(
        '-r', '--remote', type=str, help="Remote Mega.nz location (URL)", required=True,
        default=r"https://mega.nz/folder/soFQjR7B#18lj20ndYSjFzmBAIVdCaA"
        )
    parser.add_argument(
        '-l', '--local', help="File path to sync the remote files to", required=True,
        default=r"D:\3D Printing\Games\Dungeons and Dragons\Minis\MZ4250 3D Miniatures Models"
        )
    parser.add_argument(
        '-v', '--verbose', action='store_true', help="Expanded console logging",
    )
    args = parser.parse_args()
    folder_url = args.remote
    dest_path = args.local
    verbose = args.verbose

    # Start logging.
    streamLevel = logging.CRITICAL
    if verbose:
        streamLevel = logging.DEBUG
    logger = DualLogger(
        filename=f"log/{datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S')}.log",
        fileLevel=logging.DEBUG,
        stream=sys.stderr,
        streamLevel=streamLevel,
    )

    logging.debug(f"{folder_url=}")
    logging.debug(f"{dest_path=}")
    logging.debug(f"{verbose=}")

    # Run the scraper.
    sync = MEGAsync(folder_url, dest_path)
    sync.sync()

"""My Default Arguments
python3 .\src\scrape.py -r "https://mega.nz/folder/soFQjR7B#18lj20ndYSjFzmBAIVdCaA" -l "D:\3D Printing\Games\Dungeons and Dragons\Minis\MZ4250 3D Miniatures Models" -v # noqa
"""
