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
import urllib
import ftplib
import zipfile
import shutil
import warnings
import pudl.constants as pc
from pudl.settings import SETTINGS


def assert_valid_param(source, year, month=None, state=None, check_month=None):
    assert source in pc.data_sources, \
        f"Source '{source}' not found in valid data sources."
    assert source in pc.data_years, \
        f"Source '{source}' not found in valid data years."
    assert source in pc.base_data_urls, \
        f"Source '{source}' not found in valid base download URLs."
    assert year in pc.data_years[source], \
        f"Year {year} is not valid for source {source}."
    if check_month is None:
        check_month = source == 'epacems'

    if source == 'epacems':
        valid_states = pc.cems_states.keys()
    else:
        valid_states = pc.us_states.keys()

    if check_month:
        assert month in range(1, 13), \
            f"Month {month} is not valid (must be 1-12)"
        assert state.upper() in valid_states, \
            f"State '{state}' is not valid. It must be a US state abbreviation."


def source_url(source, year, month=None, state=None):
    """
    Construct a download URL for the specified federal data source and year.

    Args:
        source (str): A string indicating which data source we are going to be
            downloading. Currently it must be one of the following:
            - 'eia860'
            - 'eia861'
            - 'eia923'
            - 'ferc1'
            - 'mshamines'
            - 'mshaops'
            - 'mshaprod'
            - 'epacems'
        year (int): the year for which data should be downloaded. Must be
            within the range of valid data years, which is specified for
            each data source in the pudl.constants module.
        month (int): the month for which data should be downloaded.
            Only used for EPA CEMS.
        state (str): the state for which data should be downloaded.
            Only used for EPA CEMS.
    Returns:
        download_url (string): a full URL from which the requested
            data may be obtained
    """
    assert_valid_param(source=source, year=year, month=month, state=state)

    base_url = pc.base_data_urls[source]

    if source == 'eia860':
        if year < 2017:
            download_url = f'{base_url}/archive/xls/eia860{year}.zip'
        else:
            download_url = f'{base_url}/xls/eia860{year}.zip'
    elif source == 'eia861':
        if year < 2012:
            # Before 2012 they used 2 digit years. Y2K12 FTW!
            download_url = '{}/f861{}.zip'.format(base_url, str(year)[2:])
        else:
            download_url = '{}/f861{}.zip'.format(base_url, year)
    elif source == 'eia923':
        if year < 2008:
            prefix = 'f906920_'
        else:
            prefix = 'f923_'
        if year < 2017:
            arch_path = 'archive/xls'
        else:
            arch_path = 'xls'
        download_url = f'{base_url}/{arch_path}/{prefix}{year}.zip'
    elif source == 'ferc1':
        download_url = '{}/f1_{}.zip'.format(base_url, year)
    elif source == 'mshamines':
        download_url = '{}/Mines.zip'.format(base_url)
    elif source == 'mshaops':
        download_url = '{}/ControllerOperatorHistory.zip'.format(base_url)
    elif source == 'mshaprod':
        download_url = '{}/MinesProdQuarterly.zip'.format(base_url)
    elif (source == 'epacems'):
        # lowercase the state and zero-pad the month
        download_url = '{base_url}/{year}/{year}{state}{month}.zip'.format(
            base_url=base_url, year=year,
            state=state.lower(), month=str(month).zfill(2)
        )
    else:
        # we should never ever get here because of the assert statement.
        assert False, \
            "Bad data source '{}' requested.".format(source)

    return download_url


def path(source, year=0, month=None, state=None, file=True, datadir=SETTINGS['data_dir']):
    """
    Construct a variety of local datastore paths for a given data source.

    PUDL expects the original data it ingests to be organized in a particular
    way. This function allows you to easily construct useful paths that refer
    to various parts of the data store, by specifying the data source you are
    interested in, and optionally the year of data you're seeking, as well as
    whether you want the originally downloaded files for that year, or the
    directory in which a given year's worth of data for a particular data
    source can be found.
    Note: if you change the default arguments here, you should also change them
    for paths_for_year()
    Args:
        source (str): A string indicating which data source we are going to be
            downloading. Currently it must be one of the following:
            - 'ferc1'
            - 'eia923'
            - 'eia860'
            - 'epacems'
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
    assert_valid_param(source=source, year=year, month=month, state=state,
                       check_month=False)

    if file:
        assert year != 0, \
            "Non-zero year required to generate full datastore file path."

    if source == 'eia860':
        dstore_path = os.path.join(datadir, 'eia', 'form860')
        if year != 0:
            dstore_path = os.path.join(dstore_path, 'eia860{}'.format(year))
    elif source == 'eia861':
        dstore_path = os.path.join(datadir, 'eia', 'form861')
        if year != 0:
            dstore_path = os.path.join(dstore_path, 'eia861{}'.format(year))
    elif source == 'eia923':
        dstore_path = os.path.join(datadir, 'eia', 'form923')
        if year != 0:
            if year < 2008:
                prefix = 'f906920_'
            else:
                prefix = 'f923_'
            dstore_path = os.path.join(dstore_path,
                                       '{}{}'.format(prefix, year))
    elif source == 'ferc1':
        dstore_path = os.path.join(datadir, 'ferc', 'form1')
        if year != 0:
            dstore_path = os.path.join(dstore_path, 'f1_{}'.format(year))
    elif source == 'mshamines' and file:
        dstore_path = os.path.join(datadir, 'msha')
        if year != 0:
            dstore_path = os.path.join(dstore_path, 'Mines.zip')
    elif source == 'mshaops':
        dstore_path = os.path.join(datadir, 'msha')
        if year != 0 and file:
            dstore_path = os.path.join(dstore_path,
                                       'ControllerOperatorHistory.zip')
    elif source == 'mshaprod' and file:
        dstore_path = os.path.join(datadir, 'msha')
        if year != 0:
            dstore_path = os.path.join(dstore_path, 'MinesProdQuarterly.zip')
    elif (source == 'epacems'):
        dstore_path = os.path.join(datadir, 'epa', 'cems')
        if(year != 0):
            dstore_path = os.path.join(dstore_path, 'epacems{}'.format(year))
    else:
        # we should never ever get here because of the assert statement.
        assert False, \
            "Bad data source '{}' requested.".format(source)

    # Handle month and state, if they're provided
    if month is None:
        month_str = ''
    else:
        month_str = str(month).zfill(2)
    if state is None:
        state_str = ''
    else:
        state_str = state.lower()
    # Current naming convention requires the name of the directory to which
    # an original data source is downloaded to be the same as the basename
    # of the file itself...
    if (file and source not in ['mshamines', 'mshaops', 'mshaprod']):
        basename = os.path.basename(dstore_path)
        # For all the non-CEMS data, state_str and month_str are '',
        # but this should work for other monthly data too.
        dstore_path = os.path.join(dstore_path,
                                   f"{basename}{state_str}{month_str}.zip")

    return dstore_path


def paths_for_year(source, year=0, states=pc.cems_states.keys(),
                   file=True, datadir=SETTINGS['data_dir']):
    """Get all the paths for a given source and year. See path() for details."""
    # TODO: I'm not sure this is the best construction, since it relies on
    # the order being the same here as in the url list comprehension
    if source == 'epacems':
        paths = [path(source=source, year=year, month=month, state=state,
                      file=file, datadir=datadir)
                 # For consistency, it's important that this is state, then
                 # month
                 for state in states
                 for month in range(1, 13)]
    else:
        paths = [path(source=source, year=year, file=file, datadir=datadir)]
    return paths


def download(source, year, states, datadir=SETTINGS['data_dir'], verbose=True):
    """
    Download the original data for the specified data source and year.

    Given a data source and the desired year of data, download the original
    data files from the appropriate federal website, and place them in a
    temporary directory within the data store. This function does not do any
    checking to see whether the file already exists, or needs to be updated,
    and does not do any of the organization of the datastore after download,
    it simply gets the requested file.

    Args:
        source (str): the data source to retrieve. Must be one of: 'eia860',
            'eia923', 'ferc1', or 'epacems'.
        year (int): the year of data that the returned path should pertain to.
            Must be within the range of valid data years, which is specified
            for each data source in pudl.constants.data_years. Note that for
            data (like EPA CEMS) that have multiple datasets per year, this
            function will download all the files for the specified year.
        datadir (str): path to the top level directory of the datastore.
        verbose (bool): If True, print messages about what's happening.
    Returns:
        outfile (str): path to the local downloaded file.
    """
    assert_valid_param(source=source, year=year, check_month=False)

    tmp_dir = os.path.join(datadir, 'tmp')

    # Ensure that the temporary download directory exists:
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    if source == 'epacems':
        src_urls = [source_url(source, year, month=month, state=state)
                    # For consistency, it's important that this is state, then
                    # month
                    for state in states
                    for month in range(1, 13)]
        tmp_files = [os.path.join(tmp_dir, os.path.basename(f))
                     for f in paths_for_year(source, year, states=states)]
    else:
        src_urls = [source_url(source, year)]
        tmp_files = [os.path.join(
            tmp_dir, os.path.basename(path(source, year)))]
    if(verbose):
        if source != 'epacems':
            print(
                f"Downloading {source} data for {year}...\n    {src_urls[0]}")
        else:
            print(f"Downloading {source} data for {year}...")
    url_schemes = {urllib.parse.urlparse(url).scheme for url in src_urls}
    # Pass all the URLs at once, rather than looping here, because that way
    # we can use the same FTP connection for all of the src_urls
    # (without going all the way to a global FTP cache)
    if url_schemes == {"ftp"}:
        _download_FTP(src_urls, tmp_files)
    else:
        _download_default(src_urls, tmp_files)
    return tmp_files


def _download_FTP(src_urls, tmp_files, allow_retry=True):
    assert len(src_urls) == len(tmp_files) > 0
    parsed_urls = [urllib.parse.urlparse(url) for url in src_urls]
    domains = {url.netloc for url in parsed_urls}
    within_domain_paths = [url.path for url in parsed_urls]
    if len(domains) > 1:
        # This should never be true, but it seems good to check
        raise NotImplementedError(
            "I don't yet know how to download from multiple domains")
    domain = domains.pop()
    ftp = ftplib.FTP(domain, timeout=10)  # timeout after 10 seconds
    login_result = ftp.login()
    assert login_result.startswith("230"), \
        f"Failed to login to {domain}: {login_result}"
    url_to_retry = []
    tmp_to_retry = []
    error_messages = []
    for path, tmp_file, src_url in zip(within_domain_paths, tmp_files, src_urls):
        with open(tmp_file, "wb") as f:
            try:
                ftp.retrbinary(f"RETR {path}", f.write)
            except ftplib.all_errors as e:
                error_messages.append(e)
                url_to_retry.append(src_url)
                tmp_to_retry.append(tmp_file)
    # Now retry failures recursively
    num_failed = len(url_to_retry)
    if num_failed > 0:
        if allow_retry and len(src_urls) == 1:
            # If there was only one URL and it failed, retry once.
            return _download_FTP(url_to_retry, tmp_to_retry, allow_retry=False)
        elif allow_retry and src_urls != url_to_retry:
            # If there were multiple URLs and at least one didn't fail,
            # keep retrying until all fail or all succeed.
            return _download_FTP(url_to_retry, tmp_to_retry, allow_retry=allow_retry)
        if url_to_retry == src_urls:
            err_msg = (
                f"Download failed for all {num_failed} URLs. " +
                "Maybe the server is down?\n" +
                "Here are the failure messages:\n " +
                " \n".join(error_messages)
            )
        if not allow_retry:
            err_msg = (
                f"Download failed for {num_failed} URLs and no more " +
                "retries are allowed.\n" +
                "Here are the failure messages:\n " +
                " \n".join(error_messages)
            )
        warnings.warn(err_msg)


def _download_default(src_urls, tmp_files, allow_retry=True):
    """Download URLs to files. Designed to be called by `download` function.

    Args:
        src_urls (list of str): the source URLs to download.
        tmp_files (list of str): the corresponding files to save.
        allow_retry (bool): Should the function call itself again to
            retry the download? (Default will try twice for a single file, or
            until all files fail)
    Returns:
        None

    If the file cannot be downloaded, the program will issue a warning.
    """
    assert len(src_urls) == len(tmp_files) > 0
    url_to_retry = []
    tmp_to_retry = []
    for src_url, tmp_file in zip(src_urls, tmp_files):
        try:
            outfile, _ = urllib.request.urlretrieve(src_url, filename=tmp_file)
        except urllib.error.URLError:
            url_to_retry.append(src_url)
            tmp_to_retry.append(tmp_to_retry)
    # Now retry failures recursively
    num_failed = len(url_to_retry)
    if num_failed > 0:
        if allow_retry and len(src_urls) == 1:
            # If there was only one URL and it failed, retry once.
            return _download_default(url_to_retry, tmp_to_retry, allow_retry=False)
        elif allow_retry and src_urls != url_to_retry:
            # If there were multiple URLs and at least one didn't fail,
            # keep retrying until all fail or all succeed.
            return _download_default(url_to_retry, tmp_to_retry, allow_retry=allow_retry)
        if url_to_retry == src_urls:
            err_msg = f"ERROR: Download failed for all {num_failed} URLs. Maybe the server is down?"
        if not allow_retry:
            err_msg = f"ERROR: Download failed for {num_failed} URLs and no more retries are allowed"
        warnings.warn(err_msg)


def organize(source, year, states, unzip=True,
             datadir=SETTINGS['data_dir'],
             verbose=False, no_download=False):
    """
    Put a downloaded original data file where it belongs in the datastore.

    Once we've downloaded an original file from the public website it lives on
    we need to put it where it belongs in the datastore. Optionally, we also
    unzip it and clean up the directory hierarchy that results from unzipping.

    Args:
        source (str): the data source to retrieve. Must be one of: 'eia860',
            'eia923', 'ferc1', or 'epacems'.
        year (int): the year of data that the returned path should pertain to.
            Must be within the range of valid data years, which is specified
            for each data source in pudl.constants.data_years.
        unzip (bool): If true, unzip the file once downloaded, and place the
            resulting data files where they ought to be in the datastore.
        datadir (str): path to the top level directory of the datastore.
        verbose (bool): If True, print messages about what's happening.
        no_download (bool): If True, the files were not downloaded in this run

    Returns: nothing
    """
    assert source in pc.data_sources, \
        "Source '{}' not found in valid data sources.".format(source)
    assert source in pc.data_years, \
        "Source '{}' not found in valid data years.".format(source)
    assert source in pc.base_data_urls, \
        "Source '{}' not found in valid base download URLs.".format(source)
    assert year in pc.data_years[source], \
        "Year {} is not valid for source {}.".format(year, source)
    assert_valid_param(source=source, year=year, check_month=False)

    tmpdir = os.path.join(datadir, 'tmp')
    # For non-CEMS, the newfiles and destfiles lists will have length 1.
    newfiles = [os.path.join(tmpdir, os.path.basename(f))
                for f in paths_for_year(source, year, states)]
    destfiles = paths_for_year(
        source, year, states, file=True, datadir=datadir)

    # If we've gotten to this point, we're wiping out the previous version of
    # the data for this source and year... so lets wipe it! Scary!
    destdir = path(source, year, file=False, datadir=datadir)
    if not no_download:
        if os.path.exists(destdir):
            shutil.rmtree(destdir)
        # move the new file from wherever it is, to its rightful home.
        if not os.path.exists(destdir):
            os.makedirs(destdir)
        for newfile, destfile in zip(newfiles, destfiles):
            # paranoid safety check to make sure these files match...
            assert os.path.basename(newfile) == os.path.basename(destfile)
            shutil.move(newfile, destfile)  # works more cases than os.rename
    # If no_download is True, then we already did this rmtree and move
    # The last time this program ran.

    # If we're unzipping the downloaded file, then we may have some
    # reorganization to do. Currently all data sources will get unzipped,
    # except the CEMS, because they're really big and take up 92% less space.
    if(unzip and source != 'epacems'):
        # Unzip the downloaded file in its new home:
        zip_ref = zipfile.ZipFile(destfile, 'r')
        print(f"unzipping {destfile}")
        zip_ref.extractall(destdir)
        zip_ref.close()
        # Most of the data sources can just be unzipped in place and be done
        # with it, but FERC Form 1 requires some special attention:
        # data source we're working with:
        if source == 'ferc1':
            topdirs = [os.path.join(destdir, td)
                       for td in ['UPLOADERS', 'FORMSADMIN']]
            for td in topdirs:
                if os.path.exists(td):
                    bottomdir = os.path.join(td, 'FORM1', 'working')
                    tomove = os.listdir(bottomdir)
                    for fn in tomove:
                        shutil.move(os.path.join(bottomdir, fn), destdir)
                    shutil.rmtree(td)


def check_if_need_update(source, year, states, datadir, clobber, verbose):
    """
    Do we really need to download the requested data? Only case in which
    we don't have to do anything is when the downloaded file already exists
    and clobber is False.
    """
    paths = paths_for_year(source=source, year=year, states=states,
                           datadir=datadir)
    need_update = False
    message = None
    for path in paths:
        if os.path.exists(path):
            if clobber:
                message = f'{source} data for {year} already present, CLOBBERING.'
                need_update = True
            else:
                message = f'{source} data for {year} already present, skipping.'
        else:
            message = ''
            need_update = True
    if verbose and message is not None:
        print(message)
    return need_update


def update(source, year, states, clobber=False, unzip=True, verbose=True,
           datadir=SETTINGS['data_dir'], no_download=False):
    """
    Update the local datastore for the given source and year.

    If necessary, pull down a new copy of the data for the specified data
    source and year. If we already have the requested data, do nothing,
    unless clobber is True -- in which case remove the existing data and
    replace it with a freshly downloaded copy.

    Note that update_datastore.py runs this function in parallel, so files
    multiple sources and years may be in progress simultaneously.
    Args:
        source (str): the data source to retrieve. Must be one of: 'eia860',
            'eia923', 'ferc1', or 'epacems'.
        year (int): the year of data that the returned path should pertain to.
            Must be within the range of valid data years, which is specified
            for each data source in pudl.constants.data_years.
        unzip (bool): If true, unzip the file once downloaded, and place the
            resulting data files where they ought to be in the datastore.
            EPA CEMS files will never be unzipped.
        clobber (bool): If true, replace existing copy of the requested data
            if we have it, with freshly downloaded data.
        datadir (str): path to the top level directory of the datastore.
        verbose (bool): If True, print messages about what's happening.
        no_download (bool): If True, don't download the files, only unzip ones
            that are already present. If False, do download the files. Either
            way, still obey the unzip and clobber settings. (unzip=False and
            no_download=True will do nothing.)

    Returns: nothing
    """
    need_update = check_if_need_update(source=source, year=year, states=states,
                                       datadir=datadir, clobber=clobber, verbose=verbose)
    if need_update:
        # Otherwise we're downloading:
        if not no_download:
            download(source, year, states, datadir=datadir, verbose=verbose)
        organize(source, year, states, unzip=unzip, datadir=datadir,
                 verbose=verbose, no_download=no_download)
