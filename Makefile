all: env

env:
	pip3 install -r test_requirements.txt
	pip3 install -r requirements.txt

.PHONY: flake8
flake8:
	@echo "----- Running linter -----"
	flake8 --config ./.flake8 .


.PHONY: isort
isort:
	@echo "----- Optimizing imports -----"
	isort --atomic .

.PHONY: tools
tools: flake8 isort

.PHONY: test
test:
	@echo "----- Running tests -----"
	nosetests malware_scanner/ phishlabs_stats/

