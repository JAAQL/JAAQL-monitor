python update.py
pyinstaller -F monitor/main.py
copy dist\main.exe C:\Users\aaron\Dropbox\Programs\FIESTA\jaaql.exe

rmdir build /s /q
rmdir dist /s /q
del main.spec

python setup.py sdist bdist_wheel
python version.py>version.txt
set /p VERSION=<version.txt
del version.txt
py -m twine upload dist/*

rmdir jaaql_monitor.egg-info /s /q
rmdir build /s /q
rmdir dist /s /q
