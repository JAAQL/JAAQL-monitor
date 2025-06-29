python update.py

python setup.py sdist bdist_wheel
python monitor\version.py>version.txt
set /p VERSION=<version.txt
del version.txt
py -m twine upload dist/*

rmdir jaaql_monitor.egg-info /s /q
rmdir build /s /q
rmdir dist /s /q
