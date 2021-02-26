#!/bin/bash

# Downloads the newest version from the Azure Blob Store (East Norway).
# The newest version gets automatically updated by the Github Actions Release Build, as soon
# as the master branch changes.

cd ~/bin

rm redditgraph.jar

curl --silent https://redditpipeline.blob.core.windows.net/deploy/redditgraph.jar -o redditgraph.jar

if [ ! -f "rgraph" ]; then
  echo -e "#!/bin/bash\nexec java -jar ~/bin/redditgraph.jar \"\$@\"" > rgraph
  chmod +x rgraph
fi

echo "Successfully installed. Use the command rgraph call the program"