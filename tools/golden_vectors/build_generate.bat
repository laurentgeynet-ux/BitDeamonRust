@echo off
call "C:\Program Files\Microsoft Visual Studio\18\Community\VC\Auxiliary\Build\vcvars64.bat" >nul
cl /std:c++17 /EHsc /MD /O2 /DTORRENT_LINKING_SHARED /DTORRENT_USE_RTC=0 /DTORRENT_ABI_VERSION=2 /DTORRENT_EXPORT_EXTRA /DBOOST_ASIO_NO_DEPRECATED /DBOOST_SYSTEM_USE_UTF8 /DBOOST_ALL_NO_LIB /I "D:\Projet\libtorrent-v2.1.2\include" /I "D:\Projet\boost-1.89\boost_1_89_0" generate.cpp /link /LIBPATH:"D:\Projet\libtorrent-v2.1.2\build\Release" torrent-rasterbar.lib
