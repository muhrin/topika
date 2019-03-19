
if [ "$#" -ne 2 ]; then
    echo "Expected: release.sh <version number> <remote>"
    exit 1
fi

PACKAGE="topika"
VERSION_FILE=${PACKAGE}/version.py

version=$1
upstream=$2

while true; do
    read -p "Release version ${version} using remote $upstream? " yn
    case $yn in
        [Yy]* ) break;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
done


version_info="(`echo $version | sed 's/\./, /g'`)"
sed -i "s/version_info.*=.*/version_info = ${version_info}/" $VERSION_FILE

current_branch=`git rev-parse --abbrev-ref HEAD`

tag="v${version}"
relbranch="release-${version}"

echo Releasing version $version

git checkout -b $relbranch 
git add ${VERSION_FILE}
git commit -m "Release ${version}"

git tag -a $tag -m "Version $version"


# Merge into master

git checkout master
git merge --no-ff $relbranch

git checkout $current_branch
git merge --no-ff $relbranch

git branch -d $relbranch

# Push everything

git push $upstream master
git push $upstream $tag


# Release on pypi

rm -r dist
rm -r build
rm -r *.egg-info
python setup.py sdist
python setup.py bdist_wheel --universal

twine upload dist/*




