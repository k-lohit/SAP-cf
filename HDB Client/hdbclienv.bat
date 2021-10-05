@echo off
echo **********************************************************************
echo Copyright (c) SAP SE or an SAP affiliate company. All rights reserved.
echo **********************************************************************
echo.
set HDB_CLIENT_HOME=%~dp0
set PATH=%HDB_CLIENT_HOME%;%PATH%

rem --
rem -- Check for an SAP CommonCryptoLib installation
rem -- See: https://help.sap.com/viewer/8e208b44c0784f028b948958ef1d05e7/latest/en-US/463d3ceeb7404eca8762dfe74e9cff62.html
rem --
if exist "%HDB_CLIENT_HOME%sapcrypto.dll" (
  if not exist "%HDB_CLIENT_HOME%sapcli.pse" (
      echo   WARNING: %HDB_CLIENT_HOME%sapcli.pse does not exist! Please run:
      echo.
      echo    "%HDB_CLIENT_HOME%sapgenpse" gen_pse -p "%HDB_CLIENT_HOME%sapcli.pse"
      echo.
      echo   to create a CommonCryptoLib client encryption keystore. Accept the defaults,
      echo   do not provide a PIN, and use \"CN=Client 001\" as the Distinguished Name.
      echo   After creating the keystore, import the public certificate of the Certificate
      echo   Authority for the HANA server you are connecting to:
      echo.
      echo    "%HDB_CLIENT_HOME%sapgenpse" maintain_pk -p "%HDB_CLIENT_HOME%sapcli.pse" -a public-ca-cert.crt
      echo.
      echo   When connecting to an SAP HANA Cloud or SAP HANA Service cloud instance, import the
      echo   'DigiCert Global Root CA' certificate from:
      echo.
      echo     https://dl.cacerts.digicert.com/DigiCertGlobalRootCA.crt
      echo.
  )
  set SECUDIR=%HDB_CLIENT_HOME%
)
set HDBDOTNETCORE=%HDB_CLIENT_HOME%dotnetcore
