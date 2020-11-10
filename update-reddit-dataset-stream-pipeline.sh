#!/bin/bash

# Downloads the newest version from the Azure Blob Store (East Norway).
# The newest version gets automatically updated by the Github Actions Release Build, as soon
# as the master branch changes.

rm -r redditdatasetstreampipeline-0.1/
curl --silent https://redditpipeline.blob.core.windows.net/deploy/rdsp-latest.zip | jar xv