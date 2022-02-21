#!/bin/bash
exec java -Xmx22G -Xms16G -jar build/libs/RedditGraph-1.0-SNAPSHOT-all.jar "$@"