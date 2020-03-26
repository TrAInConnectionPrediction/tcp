.ONESHELL:

venv: venv/bin/activate

venv/bin/activate: server/requirements.txt
	python3 -m venv venv
	test -d venv || virtualenv venv
	source venv/bin/activate; pip install --upgrade pip; pip install -Ur server/requirements.txt
	touch venv/bin/activate

clean:
	rm -rf venv
	find . -iname "*.pyc" -delete

.PHONY: run
run: venv/bin/activate
	source venv/bin/activate; \
	python server/website.py

debug: venv/bin/activate
	source venv/bin/activate; \
	python3 server/website.py -ip localhost -p 5000 --debug --verbose

runschule: venv/bin/activate
	source venv/bin/activate; \
	python server/website.py -ip 10.16.1.200 -p 25565