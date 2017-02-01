The Public Utility Data Liberation project aims to provide a useful interface
to publicly available electric utility data in the US.  It uses information
from the Federal Energy Regulatory Commission (FERC), the Energy Information
Administration (EIA), and the Environmental Protection Agency (EPA), among
others.

Most of the code is written in Python. Development is managed through a shared 
Git repository, which can be found at:

https://gitlab.com/catalyst-cooperative/pudl

Until January 1st, 2018, the project is private. After that date it will be
available publicly.

For more information, get in touch with:
Catalyst Cooperative
http://catalyst.coop
hello@catalyst.coop

===========================================================================
Project Layout
===========================================================================
A brief layout and explanation of the files and directories you should find
here.  This is generally based on the "Good Enough Practices in Scientific
Computing" white paper from the folks at Software Carpentry, which you can
and should read in its entirety here: https://arxiv.org/pdf/1609.00037v2.pdf

--------------------------------------------------
LICENSE.txt
--------------------------------------------------
Copyright and licensing information.

--------------------------------------------------
README.txt
--------------------------------------------------
The file you're reading right now...

--------------------------------------------------
REQUIREMENTS.txt
--------------------------------------------------
An explanation of the other software and data you'll need to have installed in
order to make use of PUDL.

--------------------------------------------------
data/
--------------------------------------------------
A data store containing the original data from FERC, EIA, EPA and other
agencies. It's organized first by agency, form, and year. For example, the FERC
Form 1 data from 2014 would be found in ./data/ferc/form1/2014 and the EIA data
from 2010 can be found in ./data/eia/form923/2010.  Beneath the directory that
identifies the year, the filesystem structure varies, and is generally whatever
results from unzipping the archived files available from the reporting
agencies.

Altogether the data comprises tens of gigabytes of information, so it's too
large to store using Git, and we've had trouble getting it to work on Google
Drive.  For now it can be downloaded in whole or in part from:

https://spideroak.com/browse/share/CatalystCoop/pudl

--------------------------------------------------
docs/
--------------------------------------------------
Documentation related to the data sources or our results, preferably in text
format if we're creating them, and under revision control. Other files that
help explain the data sources are also stored under here, in a hierarchy
similar to the data store.  E.g. a blank copy of the FERC Form 1 is available
in docs/ferc/form1/ as a PDF.

--------------------------------------------------
notebooks/
--------------------------------------------------
A shared collection of Jupyter notebooks presenting various data processing or
analysis tasks.

--------------------------------------------------
pudl/
--------------------------------------------------
The PUDL python package, where all of our actual code ends up.  For now there's
one module related to each data set (e.g. EIA Form 923, or FERC Form 1). In
addition there's a pudl.py module which pulls data from those other sources and
integrates it into a single database.

--------------------------------------------------
results/
--------------------------------------------------
The results directory contains derived data products. These are outputs from
our manipulation and combination of the original data, that are necessary for
the integration of those data sets into the central database. It will also
be the place where outputs we're going to hand off to others get put.

--------------------------------------------------
setup.py
--------------------------------------------------
The setup.py script is the main packaging script, which gets run when the
package is installed on a user's machine.  This is a placeholder for now.

--------------------------------------------------
test/
--------------------------------------------------
The test directory holds test cases which are run with pytest.  There's one
test file for each of the python modules in the pudl directory, each prefixed
with test_  More information on pytest can be found here:

http://docs.pytest.org/en/latest/
