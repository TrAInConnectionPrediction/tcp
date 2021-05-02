# Run this script to test your changes on next.trainconnectionprediction.de

sudo DOCKER_BUILDKIT=1 docker build -f webserver/Dockerfile.webserver . -t trainconnectionprediction/next-webserver:latest

sudo docker push trainconnectionprediction/next-webserver:latest

kubectl rollout restart deployment/next-webserver