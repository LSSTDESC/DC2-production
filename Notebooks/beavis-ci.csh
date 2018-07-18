#! /usr/bin/env tcsh
#=======================================================================
#+
# NAME:
#   beavis-ci.csh
#
# PURPOSE:
#   Enable occasional integration and testing. Like travis-ci but dumber.
#
# COMMENTS:
#   Makes "rendered" notebooks and deploys them to a "rendered" orphan
#   branch, pushed to GitHub for web display.
#
# INPUTS:
#
# OPTIONAL INPUTS:
#   -h --help     Print this header
#   -u --username GITHUB_USERNAME, defaults to the environment variable
#   -k --key      GITHUB_API_KEY, defaults to the environment variable
#   -n --no-push  Only run the notebooks, don't deploy the outputs
#   -f --force    Go ahead with deployment, no waiting
#   --html        Make html outputs instead
#
# OUTPUTS:
#
# EXAMPLES:
#
#   ./beavis-ci.csh -u $GITHUB_USERNAME -k $GITHUB_API_KEY
#
#-
# ======================================================================

set help = 0
set just_testing = 0
set html = 0
set force = 0

while ( $#argv > 0 )
    switch ($argv[1])
    case -h:
        shift argv
        set help = 1
        breaksw
    case --{help}:
        shift argv
        set help = 1
        breaksw
    case -f:
        shift argv
        set force = 1
        breaksw
    case --{force}:
        shift argv
        set force = 1
        breaksw
    case -n:
        shift argv
        set just_testing = 1
        breaksw
    case --{no-push}:
        shift argv
        set just_testing = 1
        breaksw
    case -u:
        shift argv
        set GITHUB_USERNAME = $argv[1]
        shift argv
        breaksw
    case --{username}:
        shift argv
        set GITHUB_USERNAME = $argv[1]
        shift argv
        breaksw
    case -k:
        shift argv
        set GITHUB_API_KEY = $argv[1]
        shift argv
        breaksw
    case --{key}:
        shift argv
        set GITHUB_API_KEY = $argv[1]
        shift argv
        breaksw
    case --{html}:
        shift argv
        set html = 1
        breaksw
    endsw
end

if ($help) then
  more $0
  goto FINISH
endif

echo "Welcome to beavis-ci: manual occasional integration"

if ( $just_testing == 0 ) then
  if ( $?GITHUB_USERNAME && $?GITHUB_API_KEY ) then
    echo "with deployment via GitHub token $GITHUB_API_KEY and username $GITHUB_USERNAME"
  else
    echo "No GITHUB_API_KEY and/or GITHUB_USERNAME set, giving up."
    goto FINISH
  endif
endif

echo "Cloning the DC2_Repo into the .beavis workspace:"

# Check out a fresh clone in a temporary hidden folder, over-writing
# any previous edition:
\rm -rf .beavis ; mkdir .beavis ; cd .beavis
git clone git@github.com:LSSTDESC/DC2_Repo.git
cd DC2_Repo/Notebooks

if ($html) then
    echo "Making static HTML pages from the available master branch notebooks:"
    set outputformat = "HTML"
    set ext = "html"
    set branch = "html"
else
    echo "Rendering the available master branch notebooks:"
    set outputformat = "notebook"
    set ext = "nbconvert.ipynb"
    set branch = "rendered"
endif
mkdir -p log
set webdir = "https://github.com/LSSTDESC/DC2_Repo/tree/${branch}/Notebooks"

ls -l *.ipynb

set outputs = ()
foreach notebook ( *.ipynb )
    # Rename files to make them easier to work with:
    set ipynbfile = `echo "$notebook" | sed s/' '/'_'/g`
    if ($ipynbfile != "$notebook") mv -f "$notebook" $ipynbfile
    set logfile = $cwd/log/$ipynbfile:r.log
    set svgfile = $cwd/log/$ipynbfile:r.svg
    echo "Running nbconvert on $notebook ..."
    jupyter nbconvert --ExecutePreprocessor.kernel_name=desc-stack \
                      --ExecutePreprocessor.timeout=600 --to $outputformat \
                      --execute $ipynbfile >& $logfile
    set output = $ipynbfile:r.$ext
    if ( -e $output ) then
        set outputs = ( $outputs $output )
        echo "SUCCESS: $output produced."
        cp ../../../.badges/passing.svg $svgfile
    else
        echo "WARNING: $output was not created, read the log in $logfile for details."
        cp ../../../.badges/failing.svg $svgfile
    endif
end

if $just_testing goto CLEANUP
# Otherwise:

echo "Attempting to push the rendered outputs to GitHub in an orphan branch..."

    if ($force == 0) then
        echo -n "If you are ready, hit any key to continue..."
        set goforit = $<
    endif

    cd ../
    git branch -D $branch >& /dev/null
    git checkout --orphan $branch
    git rm -rf .
    cd Notebooks
    git add -f $outputs
    git add -f log
    git commit -m "pushed rendered notebooks and log files"
    git push -q -f \
        https://${GITHUB_USERNAME}:${GITHUB_API_KEY}@github.com/LSSTDESC/DC2_Repo  $branch
    echo "Done!"
    git checkout $branch

    echo ""
    echo "Please read the above output very carefully to see that things are OK. To check we've come back to the dev branch correctly, here's a git status:"
    echo ""

    git status

echo "beavis-ci finished: view the results at "
echo "   $webdir   "

CLEANUP:
cd ../../
# \rm -rf .beavis
# Uncomment the above when script works!

# ======================================================================
FINISH:
# ======================================================================
