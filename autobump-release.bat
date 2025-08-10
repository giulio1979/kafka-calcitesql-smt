@echo off
REM Auto-bump patch version, build, and commit/tag for Maven project

REM Bump patch version using versions-maven-plugin
mvn versions:set -DnextSnapshot=true
mvn versions:commit

REM Build package
mvn clean package

REM Get new version from pom.xml
for /f "delims=" %%v in ('mvn help:evaluate -Dexpression=project.version -q -DforceStdout') do set VERSION=%%v

REM Commit and tag
git add pom.xml
git commit -m "Release %VERSION%"
git tag v%VERSION%
git push
git push --tags

echo Released version %VERSION%
