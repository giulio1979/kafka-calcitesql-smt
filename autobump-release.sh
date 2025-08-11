#!/bin/bash
set -e
# Reliable Maven patch bump and release automation

# 1. Get current version
CUR_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
echo "Current version: $CUR_VERSION"

# 2. Bump to next patch-SNAPSHOT
echo "Bumping to next patch-SNAPSHOT..."
mvn versions:set -DnewVersion=$(echo $CUR_VERSION | awk -F. '{printf "%d.%d.%d-SNAPSHOT", $1, $2, $3+1}') -DgenerateBackupPoms=false
mvn versions:commit

# 3. Remove -SNAPSHOT for release
echo "Removing -SNAPSHOT for release..."
mvn versions:set -DremoveSnapshot=true -DgenerateBackupPoms=false
mvn versions:commit

# 4. Build package
echo "Building package..."
mvn clean package

# 5. Get new version
VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
echo "New version is $VERSION"

# 6. Commit, tag, and push
echo "Committing and tagging..."
git add pom.xml
git commit -m "Release $VERSION"
git tag v$VERSION
git push
git push --tags

echo "Released version $VERSION"
