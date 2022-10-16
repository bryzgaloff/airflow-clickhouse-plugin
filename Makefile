# define the name of the virtual environment directory
VENV := venv

# default target, when make executed without arguments
all: venv

$(VENV)/bin/activate: requirements.txt
	python3 -m venv $(VENV)
	./$(VENV)/bin/pip install -r requirements.txt
	./$(VENV)/bin/pip install pandas

# venv is a shortcut target
venv: $(VENV)/bin/activate

clean:
	rm -rf $(VENV)
	find . -type f -name '*.pyc' -delete

run-clickhouse:
	@docker run -p 9000:9000 --ulimit nofile=262144:262144 -it clickhouse/clickhouse-server

unit: venv
	./$(VENV)/bin/python3 -m unittest discover -s tests/unit

integration: venv
	./$(VENV)/bin/python3 -m unittest discover -s tests/integration

tests: venv
	./$(VENV)/bin/python3 -m unittest discover -s tests

.PHONY: all venv unit clean
