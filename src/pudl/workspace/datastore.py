"""
Download the original public data sources used by PUDL.

This module provides programmatic, platform-independent access to the original
data sources which are used to populate the PUDL database. Those sources
currently include: FERC Form 1, EIA Form 860, and EIA Form 923. The module
can be used to download the data, and populate a local data store which is
organized such that the rest of the PUDL package knows where to find all the
raw data it needs.

Support for selectively downloading portions of the EPA's large Continuous
Emissions Monitoring System dataset will be added in the future.
"""

import concurrent.futures
import ftplib  # nosec: B402 We sadly need ftplib for ferc1 & epacems. :-(
import logging
import os
import shutil
import urllib
import warnings
import zipfile

import pudl.constants as pc

logger = logging.getLogger(__name__)


def assert_valid_param(source, year,  # noqa: 901
                       month=None, state=None, check_month=None):
    """
    Check whether parameters used in various datastore functions are valid.

    Args:
        source (str): A string indicating which data source we are going to be
            downloading. Currently it must be one of the following: eia860,
            eia861, eia923, ferc1, epacems.
        year (int or None): the year for which data should be downloaded. Must
            be within the range of valid data years, which is specified for
            each data source in the pudl.constants module. Use None for data
            sources that do not have multiple years.
        month (int): the month for which data should be downloaded. Only used
            for EPA CEMS.
        state (str): the state for which data should be downloaded. Only used
            for EPA CEMS.
        check_month (bool): Check whether the input month is valid? This is
            automaticlaly set to True for EPA CEMS.

    Raises:
        AssertionError: If the source is not among the list of valid sources.
        AssertionError: If the source is not found in the valid data years.
        AssertionError: If the year is not valid for the specified source.
        AssertionError: If the source is not found in valid base download URLs.
        AssertionError: If the month is not valid (1-12).
        AssertionError: If the state is not a valid US state abbreviation.

    """
    if source not in pc.data_sources:
        raise AssertionError(
            f"Source '{source}' not found in valid data sources.")
    if year is not None:
        if source not in pc.data_years:
            raise AssertionError(
                f"Source '{source}' not found in valid data years.")
        if year not in pc.data_years[source]:
            raise AssertionError(
                f"Year {year} is not valid for source {source}.")
    if source not in pc.base_data_urls:
        raise AssertionError(
            f"Source '{source}' not found in valid base download URLs.")

    if check_month is None:
        check_month = source == 'epacems'

    if source == 'epacems':
        valid_states = pc.cems_states.keys()
    else:
        valid_states = pc.us_states.keys()

    if check_month:
        if month not in range(1, 13):
            raise AssertionError(f"Month {month} is not valid (must be 1-12)")
        if state.upper() not in valid_states:
            raise AssertionError(
                f"Invalid state '{state}'. Must use US state abbreviations.")


def source_url(source, year, month=None, state=None, table=None):  # noqa: C901
    """Construct a download URL for the specified federal data source and year.

    Args:
       source (str): A string indicating which data source we are going to be
           downloading. Currently it must be one of the following:
           - 'eia860'
           - 'eia861'
           - 'eia923'
           - 'ferc1'
           - 'epacems'
       year (int or None): the year for which data should be downloaded. Must
           be within the range of valid data years, which is specified for
           each data source in the pudl.constants module. Use None for data
           sources that do not have multiple years.
       month (int): the month for which data should be downloaded. Only used
           for EPA CEMS.
       state (str): the state for which data should be downloaded. Only used
           for EPA CEMS.
       table (str): the table for which data should be downloaded. Only used
           for EPA IPM.

    Returns:
       download_url (str): a full URL from which the requested data may be
       obtained

    """
    assert_valid_param(source=source, year=year, month=month, state=state)

    base_url = pc.base_data_urls[source]

    if source == 'eia860':
        if year < max(pc.data_years['eia860']):
            download_url = f'{base_url}/archive/xls/eia860{year}.zip'
        else:
            download_url = f'{base_url}/xls/eia860{year}.zip'
    elif source == 'eia861':
        if year < 2012:
            # Before 2012 they used 2 digit years. Y2K12 FTW!
            download_url = f"{base_url}/f861{str(year)[2:]}.zip"
        else:
            download_url = f"{base_url}/f861{year}.zip"
    elif source == 'eia923':
        if year < 2008:
            prefix = 'f906920_'
        else:
            prefix = 'f923_'
        if year < max(pc.data_years['eia923']):
            arch_path = 'archive/xls'
        else:
            arch_path = 'xls'
        download_url = f"{base_url}/{arch_path}/{prefix}{year}.zip"
    elif source == 'ferc1':
        download_url = f"{base_url}/f1_{year}.zip"
    elif (source == 'epacems'):
        # lowercase the state and zero-pad the month
        download_url = (
            f"{base_url}/{year}/"
            f"{year}{state.lower()}{str(month).zfill(2)}.zip"
        )
    elif source == 'epaipm':
        table_url_ext = pc.epaipm_url_ext[table]
        download_url = f"{base_url}/{table_url_ext}"
    else:
        # we should never ever get here because of the assert statement.
        raise AssertionError(f"Bad data source '{source}' requested.")

    return download_url


def path(source, data_dir,  # noqa: C901
         year=None, month=None, state=None, file=True):
    """Construct a variety of local datastore paths for a given data source.

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
            downloading. Currently it must be one of the following: ferc1,
            eia923, eia860, epacems.
        data_dir (path-like): Path to the top level datastore directory.
        year (int or None): the year of data that the returned path should
            pertain to. Must be within the range of valid data years, which is
            specified for each data source in pudl.constants.data_years, unless
            year is set to zero, in which case only the top level directory for
            the data source specified in source is returned. If None, no
            subdirectory is used for the data source.
        month (int): Month of year (1-12). Only applies to epacems.
        state (str): Two letter US state abbreviation. Only applies to epacems.
        file (bool): If True, return the full path to the originally downloaded
            file specified by the data source and year. If file is true, year
            must not be set to zero, as a year is required to specify a
            particular downloaded file.

    Returns:
        str: the path to requested resource within the local PUDL datastore.

    """
    assert_valid_param(source=source, year=year, month=month, state=state,
                       check_month=False)

    if file is True and year is None and source != 'epaipm':
        raise AssertionError(
            "A year is required to generate full datastore file path.")

    if source == 'eia860':
        dstore_path = os.path.join(data_dir, 'eia', 'form860')
        if year is not None:
            dstore_path = os.path.join(dstore_path, f"eia860{year}")
    elif source == 'eia861':
        dstore_path = os.path.join(data_dir, 'eia', 'form861')
        if year is not None:
            dstore_path = os.path.join(dstore_path, f"eia861{year}")
            if year > 2011:
                folder = f'f861{year}'
            elif year in list(range(2001, 2006)) + list(range(2007, 2010)):
                folder = str(year)
            elif year in list(range(1990, 2001)) + [2006, 2010, 2011]:
                folder = f'f861{str(year)[-2:]}'
            dstore_path = os.path.join(dstore_path, folder)
    elif source == 'eia923':
        dstore_path = os.path.join(data_dir, 'eia', 'form923')
        if year is not None:
            if year < 2008:
                prefix = 'f906920_'
            else:
                prefix = 'f923_'
            dstore_path = os.path.join(dstore_path, f"{prefix}{year}")
    elif source == 'ferc1':
        dstore_path = os.path.join(data_dir, 'ferc', 'form1')
        if year is not None:
            dstore_path = os.path.join(dstore_path, f"f1_{year}")
    elif (source == 'epacems'):
        dstore_path = os.path.join(data_dir, 'epa', 'cems')
        if year is not None:
            dstore_path = os.path.join(dstore_path, f"epacems{year}")
    elif source == 'epaipm':
        dstore_path = os.path.join(data_dir, 'epa', 'ipm', 'epaipm')
    else:
        # we should never ever get here because of the assert statement.
        raise AssertionError(f"Bad data source '{source}' requested.")

    # Handle month and state, if they're provided
    if month is None:
        month_str = ''
    else:
        month_str = str(month).zfill(2)
    if state is None:
        state_str = ''
    else:
        state_str = state.lower()

    if file is True:
        # Current naming convention requires the name of the directory to which
        # an original data source is downloaded to be the same as the basename
        # of the file itself...
        basename = os.path.basename(dstore_path)
        # For all the non-CEMS data, state_str and month_str are '',
        # but this should work for other monthly data too.
        dstore_path = os.path.join(
            dstore_path, f"{basename}{state_str}{month_str}.zip")

    return dstore_path


def paths_for_year(source, data_dir, year=None, states=None, file=True):
    """Derive all paths for a given source and year. See path() for details.

    Args:
        source (str): A string indicating which data source we are going to be
            downloading. Currently it must be one of the following: ferc1,
            eia923, eia860, epacems.
        data_dir (path-like): Path to the top level datastore directory.
        year (int or None): the year of data that the returned path should
            pertain to. Must be within the range of valid data years, which is
            specified for each data source in pudl.constants.data_years, unless
            year is set to zero, in which case only the top level directory for
            the data source specified in source is returned. If None, no
            subdirectory is used for the data source.
        month (int): Month of year (1-12). Only applies to epacems.
        state (str): Two letter US state abbreviation. Only applies to epacems.
        file (bool): If True, return the full path to the originally downloaded
            file specified by the data source and year. If file is true, year
            must not be set to zero, as a year is required to specify a
            particular downloaded file.

    Returns:
        str: the path to requested resource within the local PUDL datastore.

    """
    # TODO: I'm not sure this is the best construction, since it relies on
    # the order being the same here as in the url list comprehension
    if states is None:
        states = pc.cems_states.keys()

    if source == 'epacems':
        paths = [path(source, data_dir, year=year, month=month,
                      state=state, file=file)
                 # For consistency, it's important that this is state, then
                 # month
                 for state in states
                 for month in range(1, 13)]
    else:
        paths = [path(source, data_dir, year=year, file=file)]

    return paths


def download(source, year, states, data_dir):
    """Download the original data for the specified data source and year.

    Given a data source and the desired year of data, download the original
    data files from the appropriate federal website, and place them in a
    temporary directory within the data store. This function does not do any
    checking to see whether the file already exists, or needs to be updated,
    and does not do any of the organization of the datastore after download,
    it simply gets the requested file.

    Args:
        source (str): the data source to retrieve. Must be one of: 'eia860',
            'eia923', 'ferc1', or 'epacems'.
        year (int or None): the year of data that the returned path should
            pertain to. Must be within the range of valid data years, which is
            specified for each data source in pudl.constants.data_years. Note
            that for data (like EPA CEMS) that have multiple datasets per year,
            this function will download all the files for the specified year.
            Use None for data sources that do not have multiple years.
        states (iterable): List of two letter US state abbreviations indicating
            which states data should be downloaded for.
        data_dir (path-like): Path to the top level datastore directory.

    Returns:
        path-like: The path to the local downloaded file.

    """
    assert_valid_param(source=source, year=year, check_month=False)

    tmp_dir = os.path.join(data_dir, 'tmp')

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
                     for f in paths_for_year(
                         source, data_dir, year=year, states=states)]
    elif source == 'epaipm':
        # This is going to download all of the IPM tables listed in
        # pudl.constants.epaipm_pudl_tables and pc.epaipm_url_ext.
        # I'm finding it easier to
        # code the url and temp files than use provided functions.
        fns = pc.epaipm_url_ext.values()
        base_url = pc.base_data_urls['epaipm']

        src_urls = [f'{base_url}/{f}' for f in fns]
        tmp_files = [os.path.join(tmp_dir, f) for f in fns]
    else:
        src_urls = [source_url(source, year)]
        tmp_files = [os.path.join(
            tmp_dir, os.path.basename(path(source, data_dir, year)))]
    if source == 'epacems':
        logger.info(f"Downloading {source} data for {year}.")
    elif year is None:
        logger.info(f"Downloading {source} data.")
    else:
        logger.info(f"Downloading {source} data for {year} from {src_urls[0]}")
    url_schemes = {urllib.parse.urlparse(url).scheme for url in src_urls}
    # Pass all the URLs at once, rather than looping here, because that way
    # we can use the same FTP connection for all of the src_urls
    # (without going all the way to a global FTP cache)
    if url_schemes == {"ftp"}:
        _download_ftp(src_urls, tmp_files)
    else:
        _download_default(src_urls, tmp_files)
    return tmp_files


def _download_ftp(src_urls, tmp_files, allow_retry=True):  # noqa: C901
    """
    Download a source data using FTP, retrying as necessary.

    Args:
        src_urls (list): A list of complete source URLs for the files to be
            downloaded.
        tmp_files (list): A corresponding list of local temporary files to
            which the downloaded files should be saved.
        allow_retry (bool): If True, retry on errors. Otherwise do not retry.

    Returns:
        None

    """
    if len(src_urls) != len(tmp_files):
        raise ValueError(
            "The number of source URLs and temporary files are not equal.")
    if len(src_urls) == 0:
        raise ValueError("Got zero source URLs!")
    parsed_urls = [urllib.parse.urlparse(url) for url in src_urls]
    domains = {url.netloc for url in parsed_urls}
    within_domain_paths = [url.path for url in parsed_urls]
    if len(domains) > 1:
        # This should never be true, but it seems good to check
        raise NotImplementedError(
            "I don't yet know how to download from multiple domains")
    domain = domains.pop()
    ftp = ftplib.FTP(domain)  # nosec: B321 Sadly required for ferc1 & epacems
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
            return _download_ftp(url_to_retry, tmp_to_retry, allow_retry=False)
        elif allow_retry and src_urls != url_to_retry:
            # If there were multiple URLs and at least one didn't fail,
            # keep retrying until all fail or all succeed.
            return _download_ftp(url_to_retry,
                                 tmp_to_retry,
                                 allow_retry=allow_retry)
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

    If the file cannot be downloaded, the program will issue a warning.

    Args:
        src_urls (list of str): the source URLs to download.
        tmp_files (list of str): the corresponding files to save.
        allow_retry (bool): Should the function call itself again to
            retry the download? (Default will try twice for a single file, or
            until all files fail)
    Returns:
        None

    Todo:
        Replace assert statement

    """
    assert len(src_urls) == len(tmp_files) > 0
    url_to_retry = []
    tmp_to_retry = []
    for src_url, tmp_file in zip(src_urls, tmp_files):
        try:
            # While we aren't auditing the url to ensure it's http/https, the
            # URLs are hard-coded in pudl.constants, so we ought to know what
            # we are connecting to. Thus the # nosec comment to avoid the
            # security linter (bandit) from complaining.
            _ = urllib.request.urlretrieve(  # nosec
                src_url, filename=tmp_file)
        except urllib.error.URLError:
            url_to_retry.append(src_url)
            tmp_to_retry.append(tmp_to_retry)
    # Now retry failures recursively
    num_failed = len(url_to_retry)
    if num_failed > 0:
        if allow_retry and len(src_urls) == 1:
            # If there was only one URL and it failed, retry once.
            return _download_default(url_to_retry,
                                     tmp_to_retry,
                                     allow_retry=False)
        elif allow_retry and src_urls != url_to_retry:
            # If there were multiple URLs and at least one didn't fail,
            # keep retrying until all fail or all succeed.
            return _download_default(url_to_retry,
                                     tmp_to_retry,
                                     allow_retry=allow_retry)
        if url_to_retry == src_urls:
            err_msg = f"""ERROR: Download failed for all {num_failed} URLs.
Maybe the server is down?"""
        if not allow_retry:
            err_msg = f"""ERROR: Download failed for {num_failed}
URLs and no more retries are allowed."""
        warnings.warn(err_msg)


def organize(source, year, states, data_dir,  # noqa: C901
             unzip=True, dl=True):
    """Put downloaded original data file where it belongs in the datastore.

    Once we've downloaded an original file from the public website it lives on
    we need to put it where it belongs in the datastore. Optionally, we also
    unzip it and clean up the directory hierarchy that results from unzipping.

    Args:
        source (str): the data source to retrieve. Must be one of: 'eia860',
            'eia923', 'ferc1', or 'epacems'.
        year (int or None): the year of data that the returned path should
            pertain to. Must be within the range of valid data years, which is
            specified for each data source in pudl.constants.data_years. Use
            None for data sources that do not have multiple years.
        data_dir (path-like): Path to the top level datastore directory.
        unzip (bool): If True, unzip the file once downloaded, and place the
            resulting data files where they ought to be in the datastore.
        dl (bool): If False, the files were not downloaded in this run.

    Returns:
        None

    Todo:
        Replace 4 assert statements

    """
    assert source in pc.data_sources, \
        f"Source '{source}' not found in valid data sources."
    if year is not None:
        assert source in pc.data_years, \
            f"Source '{source}' not found in valid data years."
        assert year in pc.data_years[source], \
            f"Year {year} is not valid for source {source}."
    assert source in pc.base_data_urls, \
        f"Source '{source}' not found in valid base download URLs."

    assert_valid_param(source=source, year=year, check_month=False)

    tmpdir = os.path.join(data_dir, 'tmp')
    # For non-CEMS, the newfiles and destfiles lists will have length 1.
    if source == 'epaipm':
        fns = list(pc.epaipm_url_ext.values())
        epaipm_files = [os.path.join(tmpdir, f) for f in fns]
        # Create a ZIP file for epaipm so that it can behave more like the
        # other sources, including having a well defined "path" to check
        # whether it exists in the datastore.
        zip_path = os.path.join(tmpdir, 'epaipm.zip')
        with zipfile.ZipFile(zip_path, mode='w') as epaipm_zip:
            for f in epaipm_files:
                epaipm_zip.write(f, arcname=os.path.basename(f))
        for file in epaipm_files:
            os.remove(file)
    newfiles = [os.path.join(tmpdir, os.path.basename(f))
                for f in paths_for_year(source=source,
                                        year=year,
                                        states=states,
                                        data_dir=data_dir)]
    destfiles = paths_for_year(source=source,
                               year=year,
                               states=states,
                               file=True, data_dir=data_dir)

    # If we've gotten to this point, we're wiping out the previous version of
    # the data for this source and year... so lets wipe it! Scary!
    destdir = path(source=source, year=year, file=False, data_dir=data_dir)
    if dl:
        if os.path.exists(destdir) and source != 'epacems':
            shutil.rmtree(destdir)
        # move the new file from wherever it is, to its rightful home.
        if not os.path.exists(destdir):
            os.makedirs(destdir)
        for newfile, destfile in zip(newfiles, destfiles):
            # paranoid safety check to make sure these files match...
            assert os.path.basename(newfile) == os.path.basename(destfile)
            shutil.move(newfile, destfile)  # works more cases than os.rename
    # If download is False, then we already did this rmtree and move
    # The last time this program ran.

    # If we're unzipping the downloaded file, then we may have some
    # reorganization to do. Currently all data sources will get unzipped,
    # except the CEMS, because they're really big and take up 92% less space.
    if(unzip and source != 'epacems'):
        # Unzip the downloaded file in its new home:
        zip_ref = zipfile.ZipFile(destfile, 'r')
        logger.info(f"unzipping {destfile}")
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


def check_if_need_update(source, year, states, data_dir, clobber=False):
    """Check to see if the file is already downloaded and clobber is False.

    Do we really need to download the requested data? Only case in which
    we don't have to do anything is when the downloaded file already exists
    and clobber is False.

    Args:
        source (str): the data source to retrieve. Must be one of: eia860,
            eia923, ferc1, or epacems.
        year (int or None): the year of data that the returned path should
            pertain to. Must be within the range of valid data years, which is
            specified for each data source in pudl.constants.data_years. Note
            that for data (like EPA CEMS) that have multiple datasets per year,
            this function will download all the files for the specified year.
            Use None for data sources that do not have multiple years.
        states (iterable): List of two letter US state abbreviations indicating
            which states data should be downloaded for.
        data_dir (path-like): Path to the top level datastore directory.
        clobber (bool): If True, clobber the existing file and note that the
            file will need to be replaced with an updated file.

    Returns:
        bool: Whether an update is needed (True) or not (False)

    """
    paths = paths_for_year(source=source, year=year, states=states,
                           data_dir=data_dir)
    need_update = False
    msg = None
    for path in paths:
        if os.path.exists(path):
            if clobber:
                msg = f"{source} data for {year} already present, CLOBBERING."
                need_update = True
            else:
                msg = f"{source} data for {year} already present, skipping."
        else:
            need_update = True
    if msg is not None:
        logger.info(msg)
    return need_update


def update(source, year, states, data_dir, clobber=False, unzip=True,
           dl=True):
    """Update the local datastore for the given source and year.

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
        states (iterable): List of two letter US state abbreviations indicating
            which states data should be downloaded for. Currently only affects
            the epacems dataset.
        clobber (bool): If true, replace existing copy of the requested data
            if we have it, with freshly downloaded data.
        unzip (bool): If true, unzip the file once downloaded, and place the
            resulting data files where they ought to be in the datastore.
            EPA CEMS files will never be unzipped.
        data_dir (str): The ``data`` directory which holds the PUDL datastore.
        dl (bool): If False, don't download the files, only unzip ones
            that are already present. If True, do download the files. Either
            way, still obey the unzip and clobber settings. (unzip=False and
            dl=False will do nothing.)

    Returns:
        None

    """
    need_update = check_if_need_update(
        source=source, year=year, states=states,
        data_dir=data_dir, clobber=clobber)

    if need_update:
        # Otherwise we're downloading:
        if dl:
            download(source=source, year=year,
                     states=states, data_dir=data_dir)
        organize(source=source, year=year, states=states, unzip=unzip,
                 data_dir=data_dir, dl=dl)


def parallel_update(sources,
                    years_by_source,
                    states,
                    data_dir,
                    clobber=False,
                    unzip=True,
                    dl=True):
    """Download many original source data files in parallel using threads."""
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for source in sources:
            for year in years_by_source[source]:
                executor.submit(update, source, year, states,
                                clobber=clobber,
                                unzip=unzip,
                                data_dir=data_dir,
                                dl=download)
