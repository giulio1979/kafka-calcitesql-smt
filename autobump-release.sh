#!/bin/bash
set -e
# Auto-bump release version, build, and commit/tag for Maven project

echo "Setting release version..."
mvn versions:set -DremoveSnapshot=true
mvn versions:commit

echo "Building package..."
mvn clean package

echo "Getting new version from pom.xml..."
VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
echo "New version is $VERSION"

echo "Committing and tagging..."
git add pom.xml
git commit -m "Release $VERSION"
git tag v$VERSION
git push
git push --tags

echo "Released version $VERSION"
