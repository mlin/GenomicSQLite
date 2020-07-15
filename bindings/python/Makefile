install: clean
	python3 setup.py bdist_wheel
	pip3 install --upgrade dist/*.whl

clean:
	-rm -rf build dist
	-rm -rf *.egg-info

.PHONY: install clean
