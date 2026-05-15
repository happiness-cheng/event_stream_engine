@echo off
wsl bash -c "cd /root/event_stream_engine && mkdir -p build && cd build && cmake .. && make -j12 && ./engine"
pause
