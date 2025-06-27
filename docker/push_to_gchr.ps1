Write-Output $CR_PAT | docker login ghcr.io -u $GH_USER --password-stdin

docker build --no-cache -t ghcr.io/wacl-york/btt_nox_processing:dev .

docker push ghcr.io/wacl-york/btt_nox_processing:dev