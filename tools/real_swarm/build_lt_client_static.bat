@echo off
rem Static build of lt_client.exe: links libtorrent (static, Release)
rem + static OpenSSL -> single self-contained exe, no DLL needed at runtime.
rem Prereqs:
rem   - libtorrent checkout at D:\Projet\libtorrent\libtorrent (deps/try_signal submodule init)
rem   - Boost 1.89 headers aggregated at D:\Projet\boost-1.89\headers
rem   - cmake static build at tools\real_swarm\lt-build\Release\torrent-rasterbar.lib:
rem       cmake -S D:\Projet\libtorrent\libtorrent -B lt-build -DCMAKE_BUILD_TYPE=Release ^
rem         -DBUILD_SHARED_LIBS=OFF -DBoost_INCLUDE_DIR=D:\Projet\boost-1.89\headers ^
rem         -DOPENSSL_ROOT_DIR="C:\Program Files\OpenSSL-Win64" -DOPENSSL_USE_STATIC_LIBS=TRUE ^
rem         -Dwebtorrent=OFF -Dbuild_tests=OFF -Dbuild_examples=OFF -Dbuild_tools=OFF
rem       cmake --build lt-build --config Release
call "C:\Program Files\Microsoft Visual Studio\18\Community\VC\Auxiliary\Build\vcvars64.bat" >nul
cl /std:c++17 /EHsc /MD /O2 /Zc:__cplusplus /bigobj /utf-8 ^
 /DNDEBUG /DWIN32 /D_WINDOWS /DWIN32_LEAN_AND_MEAN /D_WIN32_WINNT=0x0A00 ^
 /DTORRENT_ABI_VERSION=2 /DTORRENT_USE_RTC=0 /DTORRENT_EXPORT_EXTRA ^
 /DTORRENT_USE_OPENSSL /DTORRENT_USE_LIBCRYPTO /DTORRENT_SSL_PEERS ^
 /DBOOST_ASIO_NO_DEPRECATED /DBOOST_SYSTEM_USE_UTF8 /DBOOST_ALL_NO_LIB ^
 /DBOOST_EXCEPTION_DISABLE /DBOOST_ASIO_HAS_STD_CHRONO /DBOOST_ASIO_ENABLE_CANCELIO ^
 /D_SCL_SECURE_NO_DEPRECATE /D_CRT_SECURE_NO_DEPRECATE /D_SILENCE_CXX17_ALLOCATOR_VOID_DEPRECATION_WARNING ^
 /I "D:\Projet\libtorrent\libtorrent\include" /I "D:\Projet\boost-1.89\headers" ^
 /I "C:\Program Files\OpenSSL-Win64\include" ^
 lt_client.cpp /link /LIBPATH:"lt-build\Release" torrent-rasterbar.lib ^
 /LIBPATH:"C:\Program Files\OpenSSL-Win64\lib\VC\x64\MD" libssl_static.lib libcrypto_static.lib ^
 ws2_32.lib crypt32.lib advapi32.lib user32.lib bcrypt.lib iphlpapi.lib mswsock.lib dbghelp.lib
