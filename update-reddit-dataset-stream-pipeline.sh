#!/bin/bash

# Downloads the newest version from the Azure Blob Store (East Norway).
# The newest version gets automatically updated by the Github Actions Release Build, as soon
# as the master branch changes.

cd ~/bin
rm -r redditdatasetstreampipeline-0.1/
curl --silent https://redditpipeline.blob.core.windows.net/deploy/rdsp-latest.zip | jar xv
chmod +x redditdatasetstreampipeline-0.1/bin/redditdatasetstreampipeline

if [ ! -f "rdsp" ]; then
        # Create softlink to the executable
        ln -s redditdatasetstreampipeline-0.1/bin/redditdatasetstreampipeline rdsp

fi

echo "Successfully installed. Use the command rdsp to call the program"