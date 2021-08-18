# TrAIn_Connection_Prediction: TCP [https://trainconnectionprediction.de/](https://trainconnectionprediction.de/)

```bash
                            ╔═══╗
   ╔════════════════════════╩═══╩════════════════════════╗
   ║            ████████    ██████   ██████              ║
   ║               ██      ██        ██   ██             ║
   ║               ██      ██        ██████              ║
   ║               ██      ██        ██                  ║
   ║               ██       ██████   ██                  ║
   ╚════════════════════════╦═══╦════════════════════════╝
                    \''''───║   ╟───''''/
                     )__,--─║   ╟─--,__(
                            ║   ║
      Your friendly         ║   ║
TrAIn_Connection_Prediction ║   ║
          Service           ║   ║
^^^^^^^^^^^^^^^^^^^^^^^^^^^^╜   ╙^^^^^^^^^^^^^^^^^^^^^^^^^^^
```

Winning project of the German National Artificial Intelligence Competition 2019 called [BWKI](https://bw-ki.de).
Visit this project on our website at [https://trainconnectionprediction.de/](https://trainconnectionprediction.de/).

For the youth competition [Jugend Forscht](https://www.jugend-forscht.de/) (JuFo) in Germany we have written a paper (in german) about our project,  
which can be found in our repository under [docs/langfassung_tcp.pdf](https://github.com/TrAInConnectionPrediction/tcp/blob/master/docs/langfassung_tcp.pdf).

We also have some interesting plots from our data under [docs/analysis.md](https://github.com/TrAInConnectionPrediction/tcp/blob/master/docs/analysis.md).  
You can also generate plots on our [website](https://purl.org/tcp/data/stations).

## Running webserver

If you for whatever reason want to run our website on your computer just do as described below.  
But you are going to need a connection to our database, to do so [contact us...](mailto:theo.doellmann@gmx.de)

To run our webserver we strongly recommend to use Docker.

First, the usergroup 420 has to have the rights to write to the cache volume. In order to add the permission, do the following
```bash
sudo chown -R :420 /path/to/your/cache/
```

Then in the project directory run:

```bash
# In order to build:
DOCKER_BUILDKIT=1 docker build -f webserver/Dockerfile.webserver . -t webserver
# In order to serve:
docker run -p 5000:5000 -v $(pwd)/config.py:/mnt/config/config.py -v $(pwd)/cache:/usr/src/app/cache webserver
```
The webserver should now be running on http://localhost:5000/

## Frontend development

For the development of the frontend/website, build the vue frontend in development mode:

```bash
# Go to the website
cd webserver/website
# Build Website
./node_modules/.bin/vue-cli-service build --mode development
```

## Credits

- Marius De Kuthy Meurers aka [NotSomeBot](https://github.com/mariusdkm)
- Theo Döllman aka [McToel](https://github.com/mctoel)
  
