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
#
# INPUTS:
#
# OPTIONAL INPUTS:
#   -h --help     Print this header
#   -u --username GITHUB_USERNAME, defaults to the environment variable
#   -k --key      GITHUB_API_KEY, defaults to the environment variable
#   -n --no-push  Only run the notebooks, don't deploy the html
#
# OUTPUTS:
#
# EXAMPLES:
#
#   ./beavis-ci.csh
#
#-
# ======================================================================

set help = 0
set just_testing = 0

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
    endsw
end

if ($help) then
  more $0
  goto FINISH
endif

# Check out a fresh clone in a temporary hidden folder, over-writing 
# any previous edition:
\rm -rf .beavis ; mkdir .beavis ; cd .beavis
git clone git@github.com:LSSTDESC/DC2_Repo.git
cd DC2_Repo/Notebooks

echo "Making static HTML pages from the available notebooks:"
set htmlfiles = ()
foreach notebook ( *.ipynb )
    # Rename files to make them easier to work with:
    set ipynbfile = `echo "$notebook" | sed s/' '/'_'/g`
    mv "$notebook" $ipynbfile
    set logfile = $cwd/$ipynbfile:r.log
    jupyter nbconvert --ExecutePreprocessor.kernel_name=desc-stack \
                      --ExecutePreprocessor.timeout=600 --to HTML \
                      --execute $ipynbfile >& $logfile
    set htmlfile = $ipynbfile:r.html
    if ( -e $htmlfile ) then
        set htmlfiles = ( $htmlfiles $htmlfile )
        wc -l $htmlfile
    else
        echo "WARNING: $htmlfile was not created, read the log in $logfile for details."
    endif
end

if $just_testing goto CLEANUP
# Otherwise:

echo "Attempting to push the HTML to GitHub in an orphan html branch..."
# Check for GitHub credentials and then push the pages up:
if ( $?GITHUB_USERNAME && $?GITHUB_API_KEY ) then

    set branch = html
    echo "...with key $GITHUB_API_KEY and username $GITHUB_USERNAME"

    echo -n "If this looks OK, hit any key to continue..."
    set goforit = $<

    cd ../
    git branch -D $branch >& /dev/null
    git checkout --orphan $branch
    git rm -rf .
    cd Notebooks
    git add -f $htmlfiles
    git commit -m "pushed HTML"
    git push -q -f \
        https://${GITHUB_USERNAME}:${GITHUB_API_KEY}@github.com/LSSTDESC/DC2_Repo  $branch
    echo "Done!"
    git checkout $branch

    echo ""
    echo "Please read the above output very carefully to see that things are OK. To check we've come back to the dev branch correctly, here's a git status:"
    echo ""

    git status

else
    echo "...No GITHUB_API_KEY and/or GITHUB_USERNAME set, giving up."
endif


CLEANUP:
cd ../../
# \rm -rf .beavis
# Uncomment the above when script works!

# ======================================================================
FINISH:
# ======================================================================