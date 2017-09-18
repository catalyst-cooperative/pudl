"""Download the original public data sources used by PUDL.

This module provides programmatic, platform-independent access to the original
data sources which are used to populate the PUDL database. Those sources
currently include: FERC Form 1, EIA Form 860, and EIA Form 923. The module
can be used to download the data, and populate a local data store which is
organized such that the rest of the PUDL package knows where to find all the
raw data it needs.

Support for selectively downloading portions of the EPA's large Continuous
Emissions Monitoring System dataset will be added in the future.
"""

import os
import sys
import urllib
import pudl.constants as pc
from pudl import settings


def source_url(source, year):
    """
    Construct a download URL for the specified federal data source and year.

    Args:
        source (str): A string indicating which data source we are going to be
            downloading. Currently it must be one of the following:
            - 'ferc1'
            - 'eia923'
            - 'eia860'
        year (int): the year for which data should be downloaded. Must be
            within the range of valid data years, which is specified for
            each data source in the pudl.constants module.
    Returns:
        download_url (string): a full URL from which the requested data may
            be obtained.
    """
    assert source in pc.data_sources, \
        "Source '{}' not found in valid data sources.".format(source)
    assert source in pc.data_years, \
        "Source '{}' not found in valid data years.".format(source)
    assert source in pc.base_data_urls, \
        "Source '{}' not found in valid base download URLs.".format(source)
    assert year in pc.data_years[source], \
        "Year {} is not valid for source {}.".format(year, source)

    base_url = pc.base_data_urls[source]

    if (source == 'ferc1'):
        download_url = '{}/f1_{}.zip'.format(base_url, year)
    elif (source == 'eia923'):
        if(year < 2008):
            prefix = 'f906920_'
        else:
            prefix = 'f923_'
        download_url = '{}/{}{}.zip'.format(base_url, prefix, year)
    elif (source == 'eia860'):
        download_url = '{}/eia860{}.zip'.format(base_url, year)
    # elif (source == 'epacems'):
    # We have not yet implemented downloading of EPA CEMS data.
    else:
        # we should never ever get here because of the assert statement.
        assert False, \
            "Bad data source '{}' requested.".format(source)

    return(download_url)


def path(source, year=0, file=True, datadir=settings.DATA_DIR):
    """
    Construct a variety of local datastore paths for a given data source.

    PUDL expects the original data it ingests to be organized in a particular
    way. This function allows you to easily construct useful paths that refer
    to various parts of the data store, by specifying the data source you are
    interested in, and optionally the year of data you're seeking, as well as
    whether you want the originally downloaded files for that year, or the
    directory in which a given year's worth of data for a particular data
    source can be found.

    Args:
        source (str): A string indicating which data source we are going to be
            downloading. Currently it must be one of the following:
            - 'ferc1'
            - 'eia923'
            - 'eia860'
        year (int): the year of data that the returned path should pertain to.
            Must be within the range of valid data years, which is specified
            for each data source in pudl.constants.data_years, unless year is
            set to zero, in which case only the top level directory for the
            data source specified in source is returned.
        file (bool): If True, return the full path to the originally
            downloaded file specified by the data source and year.
            If file is true, year must not be set to zero, as a year is
            required to specify a particular downloaded file.
        datadir (os.path): path to the top level directory that contains the
            PUDL data store.

    Returns:
        dstore_path (os.pat): the path to the requested resource within the
            local PUDL datastore.
    """
    assert source in pc.data_sources
    if year != 0:
        assert year in pc.data_years[source], \
            "Invalid year {} specified for datastore path.".format(year)

    if(file):
        assert year != 0, \
            "Non-zero year required to generate full datastore file path."

    if (source == 'ferc1'):
        dstore_path = os.path.join(datadir, 'ferc', 'form1')
        if(year != 0):
            dstore_path = os.path.join(dstore_path, 'f1_{}'.format(year))
    elif (source == 'eia860'):
        dstore_path = os.path.join(datadir, 'eia', 'form860')
        if(year != 0):
            dstore_path = os.path.join(dstore_path, 'eia860{}'.format(year))
    elif (source == 'eia923'):
        dstore_path = os.path.join(datadir, 'eia', 'form923')
        if(year != 0):
            if(year < 2008):
                prefix = 'f906920_'
            else:
                prefix = 'f923_'
            dstore_path = os.path.join(dstore_path,
                                       '{}{}'.format(prefix, year))
    # elif (source == 'epacems'):
    # We have not yet implemented downloading of EPA CEMS data.
    else:
        # we should never ever get here because of the assert statement.
        assert False, \
            "Bad data source '{}' requested.".format(source)

    # Current naming convention requires the name of the directory to which
    # an original data source is downloaded to be the same as the basename
    # of the file itself...
    if(file):
        dstore_path = \
            os.path.join(dstore_path,
                         '{}.zip'.format(os.path.basename(dstore_path)))

    return(dstore_path)


def download(source, year, datadir=settings.DATA_DIR, verbose=True):
    """
    Download the original data for the specified data source and year.

    Given a data source and the desired year of data, download the original
    data files from the appropriate federal website, and place them in a
    temporary directory within the data store. This function does not do any
    checking to see whether the file already exists, or needs to be updated,
    and does not do any of the organization of the datastore after download,
    it simply gets the requested file.
    """
    assert source in pc.data_sources, \
        "Source '{}' not found in valid data sources.".format(source)
    assert source in pc.data_years, \
        "Source '{}' not found in valid data years.".format(source)
    assert source in pc.base_data_urls, \
        "Source '{}' not found in valid base download URLs.".format(source)
    assert year in pc.data_years[source], \
        "Year {} is not valid for source {}.".format(year, source)

    src_url = source_url(source, year)
    tmp_dir = os.path.join(datadir, 'tmp')

    # Ensure that the temporary download directory exists:
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    tmp_file = os.path.join(tmp_dir, os.path.basename(path(source, year)))

    if(verbose):
        print("""Downloading {} data for {}...
    {}""".format(source, year, src_url))

    outfile, msg = urllib.request.urlretrieve(src_url, filename=tmp_file)

    return(outfile)


def organize(source, year, unzip=True, datadir=settings.DATA_DIR):
    """
    Put a downloaded original data file where it belongs in the datastore.

    Once we've downloaded an original file from the public website it lives on
    we need to put it where it belongs in the datastore. Optionally, we also
    unzip it and clean up the directory hierarchy that results from unzipping.
    """
    import zipfile
    import shutil
    import stat
    assert source in pc.data_sources, \
        "Source '{}' not found in valid data sources.".format(source)
    assert source in pc.data_years, \
        "Source '{}' not found in valid data years.".format(source)
    assert source in pc.base_data_urls, \
        "Source '{}' not found in valid base download URLs.".format(source)
    assert year in pc.data_years[source], \
        "Year {} is not valid for source {}.".format(year, source)

    tmpdir = os.path.join(datadir, 'tmp')
    newfile = os.path.join(tmpdir, os.path.basename(path(source, year)))
    destfile = path(source, year, file=True, datadir=datadir)
    # paranoid safety check to make sure these files match...
    assert os.path.basename(newfile) == os.path.basename(destfile)

    # If we've gotten to this point, we're wiping out the previous version of
    # the data for this source and year... so lets wipe it! Scary!
    destdir = path(source, year, file=False, datadir=datadir)
    if(os.path.exists(destdir)):
        shutil.rmtree(destdir)

    # move the new file from wherever it is, to its rightful home.
    if not os.path.exists(destdir):
        os.makedirs(destdir)
    os.rename(newfile, destfile)

    # If we're unzipping the downloaded file, then we may have some
    # reorganization to do. Currently all data sources will get unzipped.
    # However, when we integrate the CEMS data, we may need to store it
    # in its zipped form -- since unzipped it is ~100GB, and it's extremely
    # compressible.
    if(unzip):
        # Unzip the downloaded file in its new home:
        zip_ref = zipfile.ZipFile(destfile, 'r')
        zip_ref.extractall(destdir)
        zip_ref.close()
        # Most of the data sources can just be unzipped in place and be done
        # with it, but FERC Form 1 requires some special attention:
        # data source we're working with:
        if(source == 'ferc1'):
            topdirs = [os.path.join(destdir, td)
                       for td in ['UPLOADERS', 'FORMSADMIN']]
            for td in topdirs:
                if os.path.exists(td):
                    bottomdir = os.path.join(td, 'FORM1', 'working')
                    tomove = os.listdir(bottomdir)
                    for fn in tomove:
                        shutil.move(os.path.join(bottomdir, fn), destdir)
                    shutil.rmtree(td)

    # Change the permissions on the files we just moved into the datastore
    # so they can't be overwritten accidentally by the user in the filesystem.
    # For some reason... Python doesn't seem to be respecting the filesystem
    # permissions settings. Not absolutely vital. Not going to debug now.
    # for paths, subdirs, filenames in os.walk(destdir):
    #     for filename in filenames:
    #         os.chmod(os.path.join(paths, filename), stat.S_IREAD)


def update(source, year, clobber=False, unzip=True, verbose=True,
           datadir=settings.DATA_DIR):
    """
    Update the local datastore for the given source and year.

    If necessary, pull down a new copy of the data for the specified data
    source and year. If we already have the requested data, do nothing,
    unless clobber is True -- in which case remove the existing data and
    replace it with a freshly downloaded copy.
    """
    # Do we really need to download the requested data? Only case in which
    # we don't have to do anything is when the downloaded file already exists
    # and clobber is False.
    if os.path.exists(path(source, year, datadir=datadir)):
        if clobber:
            if verbose:
                print('{} data for {} already present, CLOBBERING.'.
                      format(source, year))
        else:
            if verbose:
                print('{} data for {} already present, skipping.'.
                      format(source, year))
            return

    # Otherwise we're downloading:
    download(source, year, datadir=datadir, verbose=verbose)
    organize(source, year, unzip=unzip, datadir=datadir)
