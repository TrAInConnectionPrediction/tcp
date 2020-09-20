# TrAIn_Connection_Prediction: TCP [kepiserver.de/tcp](http://http://kepiserver.de/tcp/)

```
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

## Setup

If you for whatever reason want to run our website on you computer just do as described below.
Our latest ML-Model will always be in the Repository under [server/ml_models](server/ml_models)

To setup the project on Linux (or MacOS) please run:

```bash
sudo ./setup.sh <ip:port>
```

This will try to setup a `systemd` service named `webserver`, which hosts a gunicorn server serving the actual flask application.

On windows or any other platform if you don't want a systemd service you can just do the following steps:

- Create python >3.5 virtual enviroment
- Activate virtual enviroment
- Install dependencies using `pip install -e .`
- Run gunicorn `gunicorn "server:create_app()"`
- Or to run using flask follow [these steps](https://flask.palletsprojects.com/en/1.1.x/tutorial/factory/#run-the-application) and use `server` as the app name

### Credits

- Marius De Kuthy Meurers aka [kuthy](https://github.com/mariusdkm)
- Theo Döllman aka [McToel](https://github.com/mctoel)
  