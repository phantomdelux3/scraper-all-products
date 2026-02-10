@echo off

:loop
echo Starting product_updater.py...

REM Automatically provide input: apparel + Enter
echo apparel | python product_updater.py

echo Script stopped or crashed. Restarting in 5 seconds...
timeout /t 5 > nul
goto loop
