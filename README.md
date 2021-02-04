# TrAIn_Connection_Prediction: TCP [TrAInConnectionPrediction.github.io](https://trainconnectionprediction.github.io)

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
Visit our website at [TrAInConnectionPrediction.github.io](https://trainconnectionprediction.github.io).

## Setup

If you for whatever reason want to run our website on you computer just do as described below.
Our latest ML-models will always be in the repository under [ml_models](ml_models)

To run our webserver we strongly recommend to use use Docker:
(You are going to our config, with access to our Database, to do so [contact us...](mailto:marius@kepi.de))

First it's important to set this enviroment variable `export DOCKER_BUILDKIT=1`

```bash
docker build -f webserver/Dockerfile.webserver . -t webserver
docker run -p 5000:5000 -v $(pwd)/config.py:/mnt/config/config.py webserver
```

### Credits

- Marius De Kuthy Meurers aka [NotSomeBot](https://github.com/mariusdkm)
- Theo Döllman aka [McToel](https://github.com/mctoel)
  