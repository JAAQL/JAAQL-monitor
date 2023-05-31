python update.py
pyinstaller -F monitor/main.py
copy dist\main.exe C:\Users\aaron\Dropbox\Programs\FIESTA\_jaaql.exe
copy monitor\__init__.py C:\Users\aaron\Dropbox\Programs\FIESTA\jaaql\monitor\__init__.py
copy monitor\main.py C:\Users\aaron\Dropbox\Programs\FIESTA\jaaql\monitor\main.py
copy monitor\version.py C:\Users\aaron\Dropbox\Programs\FIESTA\jaaql\monitor\version.py

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
