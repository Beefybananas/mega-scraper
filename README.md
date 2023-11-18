# mega-sync #
Sync a Mega.nz public folder (read only) to a local directory without downloading redundant data.


## Usage ##
The sync object is initialized as follows:

>>> sync = MEGAsync(remoteURL, localPath)

Where `remoteURL` is the `exportedLink#key` or `remotePath` from MEGA, and `localPath` is the directory on your machine you want to download the files to.

To run the sync, simply call the `sync` method as follows:

>>> sync.sync()
