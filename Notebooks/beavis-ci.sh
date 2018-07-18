#! /usr/bin/env bash
#=======================================================================
#+
# NAME:
#   beavis-ci.sh
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
#   --html        Make html outputs instead
#
# OUTPUTS:
#
# EXAMPLES:
#
#   ./beavis-ci.sh -u $GITHUB_USERNAME -k $GITHUB_API_KEY
#
#-
# ======================================================================

HELP=0
just_testing=0
html=0
src="$0"

while [ $# -gt 0 ]; do
    key="$1"
    case $key in
        -h|--help)
            HELP=1
            ;;
        -n|--no-push)
            just_testing=1
            ;;
        -u|--username)
            shift
            GITHUB_USERNAME="$1"
            ;;
        -k|--key)
            shift
            GITHUB_API_KEY="$1"
            ;;
        --html)
            html=1
            ;;
    esac
    shift
done

if [ $HELP -gt 0 ]; then
  more $src
  exit 1
fi

echo "Welcome to beavis-ci: manual occasional integration"

if [ $just_testing -eq 0 ]; then
  if [ -z $GITHUB_USERNAME ] || [ -z $GITHUB_API_KEY ]; then
      echo "No GITHUB_API_KEY and/or GITHUB_USERNAME set, giving up."
      exit 1
  else
    echo "with deployment via GitHub token $GITHUB_API_KEY and username $GITHUB_USERNAME"
  fi
fi

echo "Cloning the DC2_Repo into the .beavis workspace:"

# Check out a fresh clone in a temporary hidden folder, over-writing
# any previous edition:
\rm -rf .beavis ; mkdir .beavis ; cd .beavis
git clone git@github.com:LSSTDESC/DC2_Repo.git
cd DC2_Repo/Notebooks

if [ $html -gt 0 ]; then
    echo "Making static HTML pages from the available master branch notebooks:"
    outputformat="HTML"
    ext="html"
    branch="html"
else
    echo "Rendering the available master branch notebooks:"
    outputformat="notebook"
    ext="nbconvert.ipynb"
    branch="rendered"
fi
mkdir -p log
webdir="https://github.com/LSSTDESC/DC2_Repo/tree/${branch}/Notebooks"

ls -l *.ipynb

declare -a outputs
for notebook in *Stamps.ipynb; do
    # Rename files to make them easier to work with:
    ipynbfile="$( echo "$notebook" | sed s/' '/'_'/g )"
    if [ $ipynbfile != "$notebook" ]; then
        mv -f "$notebook" $ipynbfile
    fi
    stem=$PWD/log/$ipynbfile
    logfile=${stem%.*}.log
    svgfile=${stem%.*}.svg
    echo "Running nbconvert on $notebook ..."
    jupyter nbconvert --ExecutePreprocessor.kernel_name=desc-stack \
                      --ExecutePreprocessor.timeout=600 --to $outputformat \
                      --execute $ipynbfile &> $logfile
    output=${ipynbfile%.*}.$ext
    if [ -e $output ]; then
        outputs=( $outputs $output )
        echo "SUCCESS: $output produced."
        cp ../../../.badges/passing.svg $svgfile
    else
        echo "WARNING: $output was not created, read the log in $logfile for details."
        cp ../../../.badges/failing.svg $svgfile
    fi
done

if [ $just_testing -gt 0 ]; then
    sleep 0

else
    echo "Attempting to push the rendered outputs to GitHub in an orphan branch..."

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

fi

echo "beavis-ci finished: view the results at "
echo "   $webdir   "

cd ../../
# \rm -rf .beavis
# Uncomment the above when script works!

# ======================================================================
