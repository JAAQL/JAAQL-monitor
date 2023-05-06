python update.py
pyinstaller -F monitor/main.py
copy dist\main.exe C:\Users\aaron\Dropbox\Programs\FIESTA\jaaql.exe

rmdir build /s /q
rmdir dist /s /q
del main.spec

