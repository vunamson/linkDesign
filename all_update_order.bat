@echo off
REM Chuyển code page sang UTF-8
chcp 65001 > nul

cd /d "C:\Lay_link\linkDesign"
py main.py >> "C:\Lay_link\linkDesign\update_order_all_log.txt" 2>&1