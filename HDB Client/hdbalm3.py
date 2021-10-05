#!/usr/bin/env python

# ---------------------------------------------------------------------------
# HANA Application Lifecycle Management Command Line Client
# ---------------------------------------------------------------------------

# standard python imports
#
from optparse import OptionParser
import os
import os.path
import logging
import sys
import linecache
import traceback
import socket
import random
import platform
import tempfile
import time
import subprocess
import codecs
import string
import base64
from zipfile import ZipFile, BadZipfile, LargeZipFile
from sys import stderr

hidden_commands = frozenset(["fs_transport", "register"])

nl = "\n"

# test for ssl support
try:
    import ssl
except ImportError:
    hasSSL = False
else:
    hasSSL = True

SSL_NOTE= """
This version of the Python interpreter does not include SSL.
"""

# Version of this command line client
#
HDBALM_VERSION = "2.0.1"

# default directory in temporary directory where hdbalm log files
# are created
HDBALM_TEMPDIR = "hdbalm"

MINIMUM_REGI_VERSION="1.0.14"

# Commands provided by HALM
#
halm_commands = {
  "help": {"class": "help", 'pos':0}
}

# ---------------------------------------------------------------------------
# Utility Classes
# ---------------------------------------------------------------------------

import json
import re

class utils:

    class StdWriter:

        def __init__(self, delegate):
            self.delegate = delegate

        def write(self, text):
            self.delegate.write(text)

    stdout = StdWriter(sys.stdout)
    stderr = StdWriter(sys.stderr)

    class UserReportableException(Exception):
        def __init__(self, value):
            self.value = value
        def __str__(self):
            return self.value

    class UserCancelException(Exception):
        def __init__(self, value):
            self.value = value
        def __str__(self):
            return self.value

    @staticmethod
    def readJson(text, headers, exception=True):

        def get_charset(appHeader):

            for ah in appHeader.split(";"):
                ah = ah.strip()
                if ah.startswith("charset"):
                    cs = ah.split("=")
                    charset = cs[1] if len(cs) > 0 else None
                    return charset
            return None

        if not headers['content-type'].startswith('application/json'):
            if exception:
                raise utils.UserReportableException("not a json object. Content type is " + headers['content-type'])
            else:
                return None
        else:
            return json.loads(text)

    @staticmethod
    def dump_json(jsonObject):
        return json.dumps(jsonObject, ensure_ascii=False)

    @staticmethod
    def dump_json_pretty(jsonObject):
        return json.dumps(jsonObject, indent=4, ensure_ascii=False)

    @staticmethod
    def readXml(text, headers):
        if not headers['content-type'].startswith('text/xml'):
            raise utils.UserReportableException("not an xml document. Content type is " + headers['content-type'])
        else:
            return text.decode()

    @staticmethod
    def json_request(conn, path, parameters, method='GET', body=None):
        (code, res, hdrs) = conn.request(path, parameters=parameters, method=method, body=body)
        utils.tryReadWebError(code, res, hdrs)
        return utils.readJson(res, hdrs)

    @staticmethod
    def prettyPrintJson(jmsg):
        utils.stdout.write(utils.dump_json_pretty(jmsg))

    @staticmethod
    def tryReadWebError(code, res, hdrs):
        if code != 200:
            utils.tryReadError(res, hdrs)
            raise utils.UserReportableException('Server returned http code ' + str(code))

    @staticmethod
    def tryReadError(text, headers):
        if headers.get('content-type') != None and headers.get('content-type').startswith('application/json'):
            msgJson = json.loads(text)
            if 'error' in msgJson:
                raise utils.UserReportableException('Error: ' + msgJson.get('error'))
            else:
                raise utils.UserReportableException('Server returned unknown error message: {0}'.format(text))

    @staticmethod
    def tryReadErrorNoException(code, text, headers):
        if headers.get('content-type') != None and headers.get('content-type').startswith('application/json'):
            msgJson = json.loads(text)
            if msgJson['error']:
                sys.stderr.write(msgJson['error'] + "\n")
            return msgJson
        else:
            sys.stderr.write('Server returned http code ' + str(code))
            return None

    @staticmethod
    def tryReadErrorNoException2(code, text, headers):
        if code == 200:
            return None
        if headers.get('content-type') != None and headers.get('content-type').startswith('application/json'):
            msgJson = json.loads(text)
            if 'error' in msgJson:
                return msgJson.get('error')
        return text

    @staticmethod
    def tryReadErrorNoException3(code, text, headers):
        if code == 200:
            return None
        if headers.get('content-type') != None and headers.get('content-type').startswith('application/json'):
            msgJson = json.loads(text)
            return msgJson
        else:
            return text

    @staticmethod
    def transportModeCtsDisabled(pingReply):
        # TODO throw exception if cts transport is enambled
        pass

    @staticmethod
    def hasMinimalVersion(minVersion, testVersion, message=None):

        if message == None:
            message = \
      "This feature is not supported by the SAP HANA release. This feature requires\n"\
      + "version " + minVersion + " of SAP HANA Application Lifecycle Management but only\n"\
      + "version " + testVersion + " is available."

        versionMatch = "([0-9]+)\.([0-9]+)\.([0-9]+)$"
        minMatch = re.match(versionMatch, minVersion)
        testMatch = re.match(versionMatch, testVersion)
        if not (minMatch and testMatch):
            raise utils.UserReportableException(message)

        for i in range(1,4):
            tm = int(testMatch.group(i))
            mm = int(minMatch.group(i))
            if tm > mm:
                break
            elif tm < mm:
                raise utils.UserReportableException(message)

    @staticmethod
    def isInVersionRange(range, testVersion):

        try:
            utils.testForVersionRange(range, testVersion, "id")
            return True
        except utils.UserReportableException as e:
            if e.__str__() == "id":
                return False
            else:
                raise e

    # checks whether a given vesion (testVersion) is in the specified version
    # range
    # only (a.b, c.d) pattern supported
    @staticmethod
    def testForVersionRange(range, testVersion, message=None):

        if message == None:
            message = "This feature is not supported by this version of the SAP HANA database"

        rangePattern = "^([\([])([0-9]+)\.([0-9]+)\.([0-9]+),([0-9]+)\.([0-9]+)\.([0-9]+)([\)\]])$"
        rangeMatch = re.match(rangePattern, range)
        if not rangeMatch:
            raise utils.UserReportableException("Illegal range " + range)

        versionMatch = "([0-9]+)\.([0-9]+)\.([0-9]+)$"
        testMatch = re.match(versionMatch, testVersion)
        if not testMatch:
            raise utils.UserReportableException("Illegal version identifier " + testVersion)

        testVersion = [int(testMatch.group(1)), int(testMatch.group(2)), int(testMatch.group(3))]
        lower = [int(rangeMatch.group(2)), int(rangeMatch.group(3)), int(rangeMatch.group(4))]
        upper = [int(rangeMatch.group(5)), int(rangeMatch.group(6)), int(rangeMatch.group(7))]
        lowerInclude = rangeMatch.group(1) == "["
        upperInclude = rangeMatch.group(8) == "]"


        def compl(x,y):
            if x < y:
                raise utils.UserReportableException(message)
            if x > y:
                return True
            else:
                return False

        def compu(x,y):
            if x > y:
                raise utils.UserReportableException(message)
            if x < y:
                return True
            else:
                return False

        def compLower():
            done = compl(testVersion[0], lower[0])
            if done:
                return
            done = compl(testVersion[1], lower[1])
            if done:
                return
            done = compl(testVersion[2], lower[2])

            if not lowerInclude:
                raise utils.UserReportableException(message)

        def compupper():
            done = compu(testVersion[0], upper[0])
            if done:
                return
            done = compu(testVersion[1], upper[1])
            if done:
                return
            done = compu(testVersion[2], upper[2])

            if not upperInclude:
                raise utils.UserReportableException(message)

        compLower()
        compupper()

    @staticmethod
    def defCommand(command_args, availableCommands):
        if len(command_args) == 0:
            raise utils.UserReportableException("No command given")
        command = command_args[0]
        if not command in availableCommands:
            raise utils.UserReportableException("Command " + command + " not recognized.")
        command_args = command_args[1:]
        return (command, command_args)

    @staticmethod
    def remove_transport_history(conn, dus):
        """removes delta transport state information"""
        if len(dus) == 0:
            return

        params = {
          "operation": "removeTransportHistory",
          "dus": json.dumps(dus)
        }
        (code, res, hdrs) = conn.request(Context.Constants.xslm_service, method='POST', parameters=params)
        utils.tryReadWebError(code, res, hdrs)

    # retrieve vendor information
    #
    @staticmethod
    def getVendor(conn):

        param = { "operation": "getUserDetails"}
        (code, res, hdrs) = conn.request(Context.Constants.xslm_service, parameters=param)
        utils.tryReadWebError(code, res, hdrs)

        jd = utils.readJson(res, hdrs)
        if not "vendor" in jd:
            raise utils.UserReportableException("vendor not in message")
        return jd["vendor"]

    @staticmethod
    def isHostWildcardCertificate(certhost):

        return certhost.find("*.") == 0 and len(certhost) > 6

    @staticmethod
    def validateHostWildcardCertificate(host, certhost):

        # the format is <host>.host.org
        dotPos = host.find(".")
        if dotPos == -1:
            raise utils.UserReportableException("Cannot math host name '%s' against server certificate '%s'." % (host, certhost))

        if len(host) == (dotPos+1):
            # last character of host is a "." and it is the only "."
            raise utils.UserReportableException("Cannot math host name '%s' against server certificate '%s'." % (host, certhost))

        valHost = host[dotPos+1:]

        # make sure it is a wildcard certificate and cut off
        # the first two symbols ("*.")

        if not utils.isHostWildcardCertificate(certhost):
            raise utils.UserReportableException("Cannot match host name '%s' against server certificate '%s'." % (host, certhost))

        certhost = certhost[2:]

        if certhost != valHost:
            raise utils.UserReportableException("Host name '%s' doesn't match certificate host '%s'" % (host, certhost))

    @staticmethod
    def isWindows():
        return "windows" in platform.platform().lower()

    @staticmethod
    def get_sapcar_path():
        if utils.isWindows():
            return utils.get_exe_path(Context.Constants.SAPCAR_WINDOWS)
        else:
            return utils.get_exe_path(Context.Constants.SAPCAR_UNIX)

    @staticmethod
    def get_regi_path():
        if utils.isWindows():
            return utils.get_exe_path(Context.Constants.REGI_WINDOWS)
        else:
            return utils.get_exe_path(Context.Constants.REGI_UNIX)

    @staticmethod
    def get_exe_path(exeName):
        exePathCand = os.path.dirname(os.path.realpath(__file__))
        if os.path.isfile(os.path.join(exePathCand, exeName)):
            return os.path.join(exePathCand, exeName)
        # fallback, assume it is in the path
        return exeName

    @staticmethod
    def get_regi_version(regiExe):
        arguments = None
        outputString = None
        errorString = None
        arguments = [regiExe, "version"]

        (ret, output) = utils.execute_external(arguments)
        if ret != 0:
            raise utils.UserReportableException("Unable to obtain regi version: " + output)
        return output.rstrip()

    @staticmethod
    def check_regi_version(outputString):
        versionRegex = "^[0-9.]+"
        m = re.match(versionRegex, outputString)
        if not m:
            sys.stderr("Unable to validate regi version")
            return
        version = m.group(0)
        utils.hasMinimalVersion(MINIMUM_REGI_VERSION, version, "Found regi version {0} but Regi version {1} or higher is required.".format(version, MINIMUM_REGI_VERSION))

    @staticmethod
    def execute_external(arguments):
        argumentsPlatform = " ".join(arguments)
        logging.debug("Invoking command: " + argumentsPlatform)
        try:
            if utils.isWindows():
                process = subprocess.Popen(argumentsPlatform,stdout=subprocess.PIPE,stderr=subprocess.STDOUT);
            else:
                process = subprocess.Popen(argumentsPlatform,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT);
            (output, unused) = process.communicate()
            ret = process.wait();
            return (ret, output.decode())
        except OSError as e:
            # do not throw an error here in order to give the appliation
            # the opportuntiy to react to it
            return (1, "Unable to run " + arguments[0] + ": " + e.strerror)


    @staticmethod
    def unpack_zip(ctx, directory, name, targetDirectory):
        ctx.fileLogger.log("Unzipping " + os.path.join(directory, name) + " into " + targetDirectory + nl)
        try:
            zipFile = ZipFile(os.path.join(directory, name))
            zipFile.extractall(targetDirectory)
            ctx.fileLogger.log(("Successfully unzipped {0}." +nl).format(name))
        except (BadZipfile, LargeZipFile) as e:
            raise utils.UserReportableException("Unable to unzip file {0}: {1}".format(os.path.join(directory, name), e.__str__()))
        except RuntimeError as e:
            raise utils.UserReportableException("Unable to unzip file {0}: {1}".format(os.path.join(directory, name), e.__str__()))

    @staticmethod
    def unpack_sar(ctx, directory, name, targetDirectory):
        fileName = os.path.join(directory, name)
        if " " in fileName:
            # sapcar requires a "," as the last character if the path contains
            # a space
            fileName += ","
        ctx.fileLogger.log("Unsaring " + fileName + " into " + targetDirectory)
        args = [ctx.sapcarExe,"-xvf",'"'+fileName+'"',"-R",'"'+targetDirectory+'"']
        (ret, output) = utils.execute_external(args)
        if ret != 0:
            raise utils.UserReportableException("Unsar operation failed with return code " + str(ret) + ": " + output)
        else:
            ctx.fileLogger.log(("Successfully unsared {0}." +nl).format(name))

    @staticmethod
    def unpack_scv(ctx, directory, name, unpackdir):
        fileName = os.path.join(directory, name)
        if not os.path.isfile(fileName):
            raise utils.UserReportableException("Software component file " + name + " is missing in product archive." + nl)

        os.mkdir(unpackdir)
        ext = os.path.splitext(name)[-1].lower()
        if ext == ".zip":
            utils.unpack_zip(ctx, directory, name, unpackdir)
        elif ext == ".sar":
            utils.unpack_sar(ctx, directory, name, unpackdir)
        else:
            raise utils.UserReportableException("Software component file " + name + " is of unknown type.")

    @staticmethod
    def isZipFile(name):
        if not os.path.isfile(name):
            return False
        return os.path.splitext(name)[-1].lower()== ".zip"

    @staticmethod
    def isSarFile(name):
        if not os.path.isfile(name):
            return False
        return os.path.splitext(name)[-1].lower()== ".sar"

    @staticmethod
    def isDuTgzFile(name):
        if not os.path.isfile(name):
            return False
        return os.path.splitext(name)[-1].lower()== ".tgz"

    @staticmethod
    def isXmlFile(name):
        if not os.path.isfile(name):
            return False
        return os.path.splitext(name)[-1].lower()== ".xml"

    @staticmethod
    def get_sl_manifest(scvDirectory, scvArchiveName):
        metaFile = os.path.join(os.path.join(scvDirectory, "META-INF"), "SL_MANIFEST.XML")
        try:
            f = codecs.open(metaFile, mode='r', encoding='UTF-8')
            return f.read()
            f.close()
        except IOError as e:
            raise utils.UserReportableException("Archive {0} does not appear to be a valid software component archive: there is no \"SL_MANIFEST.XML\" file.".format(scvArchiveName))

    @staticmethod
    def log_environment(ctx):
        def get_env_str(key,envSysMsg):
            value = None
            if key in env:
                value = env[key]
            ukey = key.upper()
            if ukey in env:
                value = env[ukey]
            if value:
                if (key == 'HDBALM_PASSWD'):
                    value = "****"
                return envSysMsg + "{0}={1}".format(key,value) + nl
            else:
                return envSysMsg

        def get_info(vi):
            return str(vi[0]) + "." + str(vi[1]) + "." + str(vi[2])\
                   + "_" + str(vi[3]) + "_" + str(vi[4])

        envMsgPattern = ""
        envMsgPattern += "Environment" + nl
        envMsgPattern += "-----------" +nl
        envMsgPattern += "hdbalm version:   {0}" + nl

        if hasattr(ctx, 'regiExe'):
            envMsgPattern += "SAPCAR location:  {1}" + nl
            envMsgPattern += "regi location:    {2}" + nl
            envMsgPattern += "regi version:     {3}" + nl
            envMsgPattern += "python version    {4}" + nl
            envMsgPattern += "python executable {5}" + nl
            envMsg = envMsgPattern.format(Context.HDBALM_VERSION, ctx.sapcarExe, ctx.regiExe, ctx.regiVersion,
                                        get_info(sys.version_info), sys.executable)
        else:
            envMsgPattern += "python version    {1}" + nl
            envMsgPattern += "python executable {2}" + nl
            envMsg = envMsgPattern.format(Context.HDBALM_VERSION, get_info(sys.version_info), sys.executable)

        env = os.environ
        envSysMsg = ""
        envSysMsg = get_env_str("HDBALM_USER", envSysMsg)
        envSysMsg = get_env_str("HDBALM_PASSWD", envSysMsg)
        envSysMsg = get_env_str("HDBALM_HOST", envSysMsg)
        envSysMsg = get_env_str("HDBALM_PORT", envSysMsg)
        envSysMsg = get_env_str("HDBALM_HOST", envSysMsg)
        envSysMsg = get_env_str("http_proxy", envSysMsg)
        envSysMsg = get_env_str("https_proxy", envSysMsg)
        envSysMsg = get_env_str("no_proxy", envSysMsg)

        if (envSysMsg != ""):
            envMsg += nl + "Environment variables" + nl
            envMsg += "---------------------" +nl
            envMsg += envSysMsg

        ctx.fileLogger.log(envMsg)

    @staticmethod
    def log_server_info(ctx, conn):
        ping = conn.getPingReply()
        text = ""
        text += "System:" + nl
        text += "-------" +nl
        text += "SID:            " + ping.get("sid","") + nl
        text += "Instance:       " + str(ping.get("instance","")) + nl
        text += "Host:           " + ping.get("host","") + nl
#    text += "XS Engine Port: " + ping.get("port","") + nl
        text += "SAP HANA Application Lifecycle Management Version: " + ping.get("halm_version","") + nl
        text += "HANA Version:   " + ping.get("hana_version", "") + nl
        ctx.fileLogger.log(text)

    @staticmethod
    def get_archive_names(directory):

        duTgz = None
        langTgz = None
        for fileName in os.listdir(directory):
            if os.path.splitext(fileName)[-1].lower() == ".tgz":
                if os.path.splitext(fileName)[0].startswith("LANG"):
                    langTgz = os.path.join(directory, fileName)
                else:
                    duTgz = os.path.join(directory, fileName)
        return (duTgz, langTgz)

    @staticmethod
    def unpack_scv_archives(ctx, archiveDir, archives, scvDir):
        scvs = {}
        for name in archives:
            scv = {}
            scv_name = archives[name]["SCV_NAME"]
            scv_vendor = archives[name]["SCV_VENDOR"]
            value = archives[name]["archive"]
            archiveWithoutExt = os.path.splitext(value)[0]
            unpackdir = os.path.join(scvDir, archiveWithoutExt)
            scv["direcotory"] = unpackdir
            scv["archive"] = value
            utils.unpack_scv(ctx, archiveDir, value, unpackdir)
            scv["manifest"] = utils.get_sl_manifest(unpackdir, value)
            (duTgz, langTgz) = utils.get_archive_names(unpackdir)
            scv["duTgz"] = duTgz
            if langTgz != None:
                scv["langTgz"] = langTgz
            scvs[scv_name + " (" + scv_vendor + ")"] = scv
        return scvs

    @staticmethod
    def log_tgz_import_files(ctx, duTgz):
        text = ""
        text += "The following Delivery Unit archives will be imported:" + nl
        for f in duTgz:
            text += f + nl
        ctx.fileLogger.log(text)

    @staticmethod
    def log_lang_tgz_import_files(ctx, duLangTgz):
        if len(duLangTgz) == 0:
            return
        text = ""
        text += "The following Delivery Unit language archives will be imported:" + nl
        for f in duLangTgz:
            text += f + nl
        ctx.fileLogger.log(text)

    # -------------------------------------------------------------------------
    # DU and language DU import
    # -------------------------------------------------------------------------

    @staticmethod
    def get_regi_env(ctx):
        env = os.environ
        if ctx.tunnel != None:
            env["REGI_HOST"] = ctx.tunnel
        else:
            env["REGI_HOST"] = ctx.host+":"+str(ctx.jdbcPort)
        env["REGI_USER"] = ctx.user
        env["REGI_PASSWD"] = ctx.password

    @staticmethod
    def importArchives(ctx, duTgzs):
        utils.hasMinimalVersion("1.3.6", ctx.conn.getPingReply()["halm_version"], "At least version 1.3.6 of delivery unit HANA_XS_LM required.")
        return utils.import_archives_halm(ctx, duTgzs, type="du")

    @staticmethod
    def import_archives_halm(ctx, duTgzs, type="du"):

        def wait(iteration):
            waitTime = 1
            if iteration > 0 and iteration < 10:
                waitTime = 3
            elif iteration >= 10:
                waitTime = 5
            time.sleep(waitTime)

        def dump(logRecords):
            msg = "Log file generated by import operation" +nl
            msg += "--------------------------------------"+nl
            formatString = "{0:2} {1:3} {2}" + nl
            for r in logRecords:
                msg += formatString.format(r['rc'], r['sid'], r['record'])
            return msg

        def join(cids):
            msg = ""
            first = True
            for cid in cids:
                if first:
                    msg += str(cid)
                    first = False
                else:
                    msg += "," + str(cid)
            return msg

        if len(duTgzs) == 0 and type == "language_du":
            ctx.fileLogger.log("No language delivery units to import.")
            return Context.Constants.EXIT_OK

        if len(duTgzs) == 0 and type == "du":
            msg = "No delivery unit archives to import"
            ctx.fileLogger.log(msg)
            utils.stdout.write(msg + nl)
            return Context.Constants.EXIT_OK

        try:
            if type == "du":
                utils.stdout.write("Importing delivery units. This may take some time." + nl)
            else:
                utils.stdout.write("Importing language delivery units." + nl)

            multipartFiles = []
            for name in duTgzs:
                f = open(name, "rb")
                contents = f.read()
                f.close()
                multipartFiles.append({
                  'filename': name,
                  'mimetype': 'application/octet-stream',
                  'content': contents,
                  'name': name
                })

            (code, res, hdrs) = ctx.conn.file_upload(Context.Constants.xslm_file_upload_service, files=multipartFiles)
            utils.tryReadWebError(code, res, hdrs)
            contentIds = utils.readJson(res, hdrs)
            ctidtext = join(contentIds)
            ctx.fileLogger.log("Successfully uploaded archives files, content ids: {0}.".format(ctidtext))

            params = {
              "contentIds": contentIds
            }
            if type == "du":
                params["operation"] = "importMultipleDUs"
            else:
                params["operation"] = "importMultipleDULanguages"

            (code, res, hdrs) = ctx.conn.request(Context.Constants.xslm_import_du_service,
                                                 method='POST',
                                                 body=json.dumps(params))
            utils.tryReadWebError(code, res, hdrs)
            proc = utils.readJson(res, hdrs)
            processId = proc.get('processId')

            # import is running, need to check periodically for completion
            #

            iteration = 0
            startTime = time.time()
            while True:
                wait(iteration)
                param = { "operation": "getProcessStatus", "processId": processId }
                (code, res, hdrs) = ctx.conn.request(Context.Constants.xslm_service, parameters=param)
                utils.tryReadWebError(code, res, hdrs)
                msg = utils.readJson(res, hdrs)
                status = msg.get("status")
                if status == "Initial" or status == "Running":
                    iteration = iteration + 1
                    continue
                endTime = time.time()
                ctx.fileLogger.log("Import operation took {0} seconds.".format(round(endTime - startTime, 2)))
                break;

            # log output regardless of success
            logRecords = utils.getActionLog(ctx, processId)
            ctx.fileLogger.log(dump(logRecords))
            if status == "Success":
                utils.stdout.write("Successfully imported delivery units" + nl)
                ctx.fileLogger.log("Successfully imported delivery units")
                return Context.Constants.EXIT_OK
            elif status == "ActivationError" or status == "Error":
                utils.stderr.write("Delivery unit import finished with errors. Details have been written to the log file." + nl)
                ctx.fileLogger.log("Delivery unit import finished with errors")
                return Context.Constants.EXIT_ERROR

        except utils.UserReportableException as e:
            # the signature of this method does not allow this exception to be thrown
            #
            ctx.fileLogger.log(e.__str__())
            utils.stderr.write(e.__str__()+nl)
            return Context.Constants.EXIT_ERROR

    @staticmethod
    def store_file(ctx, name, contents):
        try:
            fd = open(name,"wb")
            fd.write(contents)
            fd.close()
            ctx.fileLogger.log("File {0} written.".format(name))
        except IOError as io:
            if hasattr(io, "strerror"):
                raise utils.UserReportableException("Unable to write file {0}: {1}".format(name, io.strerror))
            else:
                raise utils.UserReportableException("Unable to write file {0}.".format(name))

    @staticmethod
    def export_archive_halm(ctx, name, vendor, duTgzName, alias, exportVersion):

        parameters = {
          "du_name": name,
          "du_vendor": vendor
        }
        if alias != None:
            parameters["alias"] = alias
        if exportVersion != None:
            parameters["format"] = exportVersion

        ctx.fileLogger.log("Exporting delivery Unit {0} ({1}) via http".format(name, vendor))
        utils.stdout.write("Exporting delivery Unit {0} ({1})".format(name, vendor) + nl)
        (code, res, hdrs) = ctx.conn.request(Context.Constants.xslm_export_archive_service, parameters=parameters)
        utils.tryReadWebError(code, res, hdrs)
        ret = Context.Constants.EXIT_ERROR
        utils.stdout.write("Successfully exported Delivery Unit." + nl)
        try:
            utils.store_file(ctx, duTgzName, res)
            ret = Context.Constants.EXIT_OK
        except utils.UserReportableException as e:
            # must not throw here according to original signature
            ctx.fileLogger('Unable to store exported delivery unit file ' + duTgzName + ': ' + e.__str__())
        return ret

    @staticmethod
    def export_archive_regi(ctx, name, vendor, duTgzName, alias, exportVersion):

        utils.get_regi_env(ctx)

        alias = "--alias=" + '"' + alias + '"' if alias != None else None
        exportVersion= "--exportVersion=" + str(exportVersion) if exportVersion != None else None
        arguments = [ctx.regiExe, "export","deliveryUnit", name, vendor, '"'+duTgzName+'"', alias, exportVersion]
        #filter empty values
        arguments = [x for x in arguments if x!= None]
        ctx.fileLogger.log("Running command: " + " ".join(arguments))
        utils.stdout.write("Exporting delivery Unit {0} ({1})".format(name, vendor) + nl)
        (ret, output) = utils.execute_external(arguments)
        ctx.fileLogger.log("Delivery unit export command ended with return code "
                            + str(ret) +nl
                            + "Messages:" + nl + output)
        if ret == 0:
            utils.stdout.write("Successfully exported Delivery Unit." + nl)
        else:
            utils.stdout.write("Delivery Unit export finished with errors. Details have been written to the log file." + nl)
        return ret

    @staticmethod
    def exportArchive(ctx, name, vendor, duTgzName, alias=None, exportVersion=None):
        if not ctx.command_options.use_regi:
            utils.hasMinimalVersion("1.3.11", ctx.conn.getPingReply()["halm_version"], "At least version 1.3.11 of delivery unit HANA_XS_LM required. Use command line option --use_regi in order to assemble product- or software component archive.")
            return utils.export_archive_halm(ctx, name, vendor, duTgzName, alias, exportVersion)
        else:
            return utils.export_archive_regi(ctx, name, vendor, duTgzName, alias, exportVersion)

    @staticmethod
    def importLanguageArchives(ctx, languageDus):
        utils.hasMinimalVersion("1.3.6", ctx.conn.getPingReply()["halm_version"], "At least version 1.3.6 of delivery unit HANA_XS_LM required.")
        return utils.import_archives_halm(ctx, languageDus, type="language_du")


    @staticmethod
    def export_language_archive_halm(ctx, name, vendor, languageList, fileName, alias, exportVersion):
        parameters = {
          "du_name": name,
          "du_vendor": vendor,
          "languages": json.dumps(languageList)
        }
        if alias != None:
            parameters["alias"] = alias
        if exportVersion != None:
            parameters["format"] = exportVersion

        (code, res, hdrs) = ctx.conn.request(Context.Constants.xslm_export_archive_service, parameters=parameters)
        utils.tryReadWebError(code, res, hdrs)
        ret = Context.Constants.EXIT_ERROR
        try:
            utils.store_file(ctx, fileName, res)
            ret = Context.Constants.EXIT_OK
        except utils.UserReportableException as e:
            # must not throw here according to original signature
            ctx.fileLogger('Unable to store exported delivery unit file ' + fileName + ': ' + e.__str__())
        return ret

    @staticmethod
    def export_language_archive_regi(ctx, name, vendor, languageList, fileName, alias, exportVersion):
        utils.get_regi_env(ctx)
        languages = " ".join(languageList)
        arguments = [ctx.regiExe,"export","languages",name,vendor,'"'+fileName+'"']
        if alias != None:
            arguments.append("--alias=" + '"' + alias + '"')
        if exportVersion != None:
            arguments.append("--exportVersion=" + str(exportVersion))
        arguments.append(languages)

        ctx.fileLogger.log("Running command for language delivery export: " + " ".join(arguments))
        (ret, output) = utils.execute_external(arguments)
        ctx.fileLogger.log("Delivery Unit language export command ended with return code "
                            + str(ret) + nl
                            + "Messages:" + nl + output + nl)
        if ret == 0:
            utils.stdout.write("Successfully exported Delivery Unit language file." + nl)
        else:
            utils.stdout.write("Delivery Unit language file export finished with errors. Details have been written to the logfile." + nl)
        return ret

    @staticmethod
    def exportLanguageArchive(ctx, name, vendor, languageList, fileName, alias=None, exportVersion=None):
        if not ctx.command_options.use_regi:
            utils.hasMinimalVersion("1.3.11", ctx.conn.getPingReply()["halm_version"], "At least version 1.3.11 of delivery unit HANA_XS_LM required. Use command line option --use_regi in order to assemble product- or software component archive.")
            return utils.export_language_archive_halm(ctx, name, vendor, languageList, fileName, alias, exportVersion)
        else:
            return utils.export_language_archive_regi(ctx, name, vendor, languageList, fileName, alias, exportVersion)


    @staticmethod
    def getJDBCProtfromHTTPPort(httpPort, connection):
        jdbcPort = None
        try:
            instance = connection.getPingReply()['instance']
            jdbcPort = 30000 + instance*100 + 15
        except KeyError as e:
            raise utils.UserReportableException("Unable to ping the server. "+str(e))
        except ValueError as e:
            raise utils.UserReportableException("Unable to ping the server. "+str(e))

        return str(jdbcPort)

    @staticmethod
    def make_dir(path):
        try:
            os.makedirs(path)
        except (OSError) as e:
            if hasattr(e, "strerror"):
                raise utils.UserReportableException("Unable to create directory {0}: {1}".format(path, e.strerror))
            else:
                raise utils.UserReportableException("Unable to create directory {0}.".format(path))

    @staticmethod
    def is_python_27():
        v = sys.version_info
        return v[0] == 2 and v[1] == 7

    @staticmethod
    def getActionLog(ctx, process_id):
        logViewer = Context.Constants.xslm_base + '/core/ActionLogViewer.xsjs'
        param = {'action_id': process_id}
        (code, res, hdrs) = ctx.conn.request(logViewer, parameters=param)
        utils.tryReadWebError(code, res, hdrs)
        return utils.readJson(res, hdrs)

    @staticmethod
    def log_command_options():
        msg = 'This program was invoked with the following options: ' + nl
        for o in sys.argv:
            msg += repr(o) + ' '
        return msg

# ---------------------------------------------------------------------------
# File logger
# ---------------------------------------------------------------------------

currentTime = lambda: int(round(time.time() * 1000))

class NullFileLogger:
    def ensureDirectoryExists(self):
        pass

    def log(self, record):
        pass

    def get_log_filename(self):
        return None

    def close(self):
        pass

class FileLogger(NullFileLogger):

    def __init__(self, context, directory=None, namePrefix=None, fileName=None, dummy=False):
        self.ctx = context

        if dummy:
            self.logfile = None
            return

        try:
            if fileName != None:
                fullFile = fileName
                self.filename = os.path.basename(fileName)
                self.directory = os.path.dirname(fileName)
            else:
                if directory == None:
                    tmpdir = tempfile.gettempdir()
                    directory = os.path.join(tmpdir, HDBALM_TEMPDIR)

                self.ensureDirectoryExists(directory)

                if namePrefix == None:
                    namePrefix = "hdbalm"

                filename = namePrefix + "-" + str(currentTime()) + ".log"
                fullFile = os.path.join(directory, filename)
                self.directory = directory
                self.filename = filename
            self.logfile = open(fullFile, "w", encoding = 'utf-8')
        except IOError as io:
            if hasattr(io, "strerror"):
                raise self.ctx.utils.UserReportableException("Unable to open log file " + fullFile + ": " + io.strerror)
            else:
                raise self.ctx.utils.UserReportableException("Unable to open log file " + fullFile)

    def ensureDirectoryExists(self, name):
        if not os.path.exists(name):
            os.makedirs(name)

    def log(self, record):
        if self.logfile:
            tstring = ""
            tstring += nl + "--- " + time.asctime() + " ---" + nl
            tstring += record + nl
            self.logfile.write(tstring)

    def get_log_filename(self):
        return os.path.join(self.directory, self.filename)

    def close(self):
        if self.logfile:
            self.logfile.close()


# ---------------------------------------------------------------------------
# Http stuff with sessions
# ---------------------------------------------------------------------------

import http.cookiejar
import urllib.request, urllib.parse, urllib.error
import urllib.request, urllib.error, urllib.parse
import getpass

xslm_base = '/sap/hana/xs/lm'
xslm_xsrf = xslm_base + '/csrf.xsjs'
xslm_ping = xslm_base + '/xsts/ping.xsjs'

timeout_marker_header = "x-sap-login-page"

class RetryStrategy():
    handled_http_status_codes = {
        104: "Connection reset by peer",
        502: "Bad Gateway",
        503: "Service Unavailable",
        504: "Gateway Timeout"
    }

    # Maximum number of allowed retries if the server responds with a HTTP code
    maxRetries = 5
    # Retry interval between subsequent requests, in seconds
    retryInterval = 1

class HttpConnection():

    handled_http_status_codes = {
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found"
    }

    messages = {
      "no_user": "No user name specified",
      "no_port": "No port specified",
      "no_host": "No host specified"
    }

    @staticmethod
    def get_proxies():
        proxy_pattern = "(?:http://)?([^:]+):(\d+)"

        def get_env_ignore_case(key):
            if key in env:
                return env[key]
            ukey = key.upper()
            if ukey in env:
                return env[ukey]
            return None

        def get_proxy(proxyString):
            if proxyString is None:
                return None
            if proxyString.startswith('https'):
                raise utils.UserReportableException("https proxies are not supported")
            m = re.search(proxy_pattern, proxyString)
            if m:
                return (m.group(1), int(m.group(2)))
            else:
                return None

        def get_no_proxy(noproxy):
            extensions = noproxy.split(',')
            for i in range(0, len(extensions)):
                elem = extensions[i]
                if elem.startswith('*'):
                    extensions[i] = elem[1:]
            return extensions

        env = os.environ
        http_proxy = None
        https_proxy = None
        no_proxy = None
        if get_env_ignore_case("http_proxy"):
            proxy = get_env_ignore_case("http_proxy")
            http_proxy = get_proxy(proxy)

        if get_env_ignore_case("https_proxy"):
            proxy = get_env_ignore_case("https_proxy")
            https_proxy = get_proxy(proxy)

        if get_env_ignore_case("no_proxy"):
            no_proxy = get_no_proxy(get_env_ignore_case("no_proxy"))

        return (http_proxy, https_proxy, no_proxy)

    @staticmethod
    def use_proxy_for_host(no_proxy,host):
        if no_proxy == None:
            return True
        for ex in no_proxy:
            if host.endswith(ex):
                return False
        return True


    @staticmethod
    def initialize_connection(args, minVersion=None):

        env = os.environ
        if args.host:
            host = args.host
        else:
            if "HDBALM_HOST" in env:
                host = env["HDBALM_HOST"]
            else:
                raise utils.UserReportableException(HttpConnection.messages["no_host"])

        if args.port:
            port = args.port
        else:
            if "HDBALM_PORT" in env:
                port = int(env["HDBALM_PORT"])
            else:
                raise utils.UserReportableException(HttpConnection.messages["no_port"])

        if args.user:
            user = args.user
        else:
            if "HDBALM_USER" in env:
                user = env["HDBALM_USER"]
            else:
                raise utils.UserReportableException(HttpConnection.messages["no_user"])

        if "HDBALM_PASSWD" in env:
            password = env["HDBALM_PASSWD"]
        else:
            password = getpass.getpass()

        if args.localhost_only:
            if host.lower() != 'localhost' and host != '127.0.0.1' and host != '::1':
                raise utils.UserReportableException("Option --localhost-only, but the host is {0}".format(host))

        if args.https and not args.localhost_only:
            certs = args.certs
            if not certs:
                raise utils.UserReportableException("No certificate file specified. Server identity cannot be verified.")
            if not (os.path.exists(args.certs) and os.path.isfile(args.certs)):
                raise utils.UserReportableException("Specified certificate file {0} does not exist".format(args.certs))
            try:
                HttpConnection.validateServerIdentity(host, port, certs)
            except ssl.SSLError as e:
                raise utils.UserReportableException(e.__str__())

        conn = HttpConnection(host, port, user, password, args.https, verbose=args.verbose, minVersion=minVersion, tenantHost=args.tenant_host)
        return conn


    def __init__(self, host, port, user, password, useSSL, verbose=False, minVersion=None, tenantHost=None):

        if useSSL and not hasSSL:
            raise utils.UserReportableException("SSL selected but not available.")

        self.host = host
        self.port = port
        self.useSSL = useSSL
        self.user = user
        self.password = password
        self.cj = http.cookiejar.CookieJar()
        self.first = True
        self.protocol = ""
        self.minVersion = minVersion
        self.proxies = HttpConnection.get_proxies()
        self.tenantHost = tenantHost

        if self.useSSL:
            self.protocol = "https://"
        else:
            self.protocol = "http://"
        self.base_url = self.protocol + self.host + ':' + str(self.port)

        self.retry_strategy = RetryStrategy()
        self.open_connection(minVersion=minVersion)

    def open_connection(self, minVersion=None):

        handlers = []
        handlers.append(urllib.request.HTTPCookieProcessor(self.cj))

        (http_proxy, https_proxy, no_proxy) = self.proxies

        if self.useSSL:
            handlers.append(urllib.request.HTTPSHandler())
            if https_proxy != None and HttpConnection.use_proxy_for_host(no_proxy, self.host):
                logging.debug("Using proxy https://{0}:{1}".format(https_proxy[0],https_proxy[1]))
                handlers.append(urllib.request.ProxyHandler({'https': 'http://'+ https_proxy[0]+ ':' + str(https_proxy[1])+ '/'}))
            else:
                handlers.append(urllib.request.ProxyHandler({}))

        else:
            if http_proxy != None and HttpConnection.use_proxy_for_host(no_proxy, self.host):
                logging.debug("Using proxy http://{0}:{1}".format(http_proxy[0],http_proxy[1]))
                handlers.append(urllib.request.ProxyHandler({'http': 'http://'+ http_proxy[0]+ ':' + str(http_proxy[1])+ '/'}))
            else:
                handlers.append(urllib.request.ProxyHandler({}))

        opener = urllib.request.build_opener(*handlers)
        urllib.request.install_opener(opener)

        (code, res, info) = self.ping_system(True)
        if code in HttpConnection.handled_http_status_codes:
            raise utils.UserReportableException("Http error: " + HttpConnection.handled_http_status_codes[code])

        if code != 200:
            raise utils.UserReportableException("Unable to connect to system: " + res +nl)

        self.setPingReply(res, info)

        if minVersion:
            utils.hasMinimalVersion(minVersion, self.getPingReply()["halm_version"])

    def get_xsrf_token(self, initial=False):
        request = urllib.request.Request(self.base_url + xslm_xsrf)
        if initial:
            request.add_header('Authorization', self._get_basic_auth_header())
        request.add_header('X-CSRF-Token','Fetch')
        if self.tenantHost:
            request.add_header('Host', self.tenantHost)
        response = self.send_request_resilient(request, self.base_url + xslm_xsrf)
        xsrf_token = response.headers.get('X-CSRF-Token')
        logging.debug("got xsrf token " + xsrf_token)
        return xsrf_token

    def encode_multipart(self, files, boundary=None):

        _BOUNDARY_CHARS = string.digits + string.ascii_letters

        def escape_quote(s):
            return s.replace('"', '\\"')

        if boundary is None:
            boundary = ''.join(random.choice(_BOUNDARY_CHARS) for i in range(30))
        lines = []

        for msg in files:
            name = msg['name']
            filename = msg['filename']
            mimetype = msg['mimetype']
            content = msg['content']
            lines.extend((
                '--{0}'.format(boundary).encode('utf-8'),
                'Content-Disposition: form-data; name="{0}"; filename="{1}"'.format(
                        escape_quote(name), escape_quote(filename)).encode('utf-8'),
                'Content-Type: {0}'.format(mimetype).encode('utf-8'),
                ''.encode('utf-8'),
                content,
            ))

        lines.extend((
            '--{0}--'.format(boundary).encode('utf-8'),
            ''.encode('utf-8'),
        ))
        body = b'\r\n'.join(lines)

        headers = {
            'Content-Type': 'multipart/form-data; boundary={0}'.format(boundary),
            'Content-Length': str(len(body)),
        }

        return (body, headers)

    def file_upload(self, path, headers={}, files=None):

        headers['X-CSRF-Token'] = self.get_xsrf_token()
        if self.tenantHost:
            headers['Host'] = self.tenantHost
        url = self.base_url + path

        (data, newHeaders) = self.encode_multipart(files)
        headers.update(newHeaders)
        request = urllib.request.Request(url=url, data=data, headers=headers)
        try:
            res = self.send_request_resilient(request)
            return (res.code, res.read(), res.headers)
        except urllib.error.HTTPError as e:
            readMethod = getattr(e, "read", e)
            if readMethod and callable(readMethod):
                msg = e.read()
                if msg:
                    return (e.code, msg, e.headers)
            if hasattr(e, 'reason'):
                if type(e.reason) == str:
                    return (e.code, e.reason, e.headers)
                else:
                    return (-1, "Error ({0}): {1}".format(e.reason.errno, e.reason.strerror), "")
            else:
                return (-1, e.__str__(), e.headers)
        except urllib.error.URLError as e:
            if hasattr(e, 'reason'):
                if type(e.reason) == str:
                    return (-1, e.reason, "")
                else:
                    return (-1, "I/O error({0}): {1}".format(e.reason.errno, e.reason.strerror), "")
            else:
                return (-1, e.__str__(), e.headers)

    def _get_basic_auth_header(self):
        userAndPass = base64.b64encode(bytes(self.user + ':' + self.password, "utf-8")).decode("ascii")
        return 'Basic ' + userAndPass

    def encodeDict(self, inpDict):
        encDict = {}
        for k, v in inpDict.items():
            if isinstance(v, str):
                v = v.encode('utf8')
            elif isinstance(v, str):
                # Must be encoded in UTF-8
                v.decode('utf8')
            encDict[k] = v
        return encDict

    def request(self, path, method='GET', parameters=None, body=None, headers={}, post_parameters=None, post_body=None, initial=False):
        logging.debug("Request: " + method + " " + path)

        req_headers = {}
        url = self.base_url + path
        xsrf_token = None
        if method == 'POST':
            # get xsrf token
            xsrf_token = self.get_xsrf_token(initial)
            req_headers['X-CSRF-Token'] = xsrf_token
        else:
            if initial:
                req_headers["Authorization"] = self._get_basic_auth_header()

        if self.tenantHost:
            req_headers['Host'] = self.tenantHost

        paramStr = None
        post_paramStr = None
        if (parameters != None):
            paramStr = urllib.parse.urlencode(self.encodeDict(parameters))
        if (post_parameters != None):
            post_paramStr = urllib.parse.urlencode(self.encodeDict(post_parameters))

        #if paramStr != None:
        #  logging.debug(method + " " + paramStr)

        if (method == 'GET' and paramStr != None):
            url += '?' + paramStr

        if (method == 'POST' and paramStr != None):
            body = bytes(paramStr, "utf-8")

        # ignore all of the above if post_body is set
        if method == 'POST' and post_body != None:
            body = bytes(post_body, "utf-8")

        if(method=='POST' and post_paramStr != None):
            url += '?' + post_paramStr

        req_headers.update(headers)

        if isinstance(body, str):
            body_b = body.encode('utf-8')
        else:
            body_b = body

        request = urllib.request.Request(url=url, data=body_b, headers=req_headers)
        try:
            res = self.send_request_resilient(request, url)
            return (res.code, res.read(), res.headers)
        except urllib.error.HTTPError as e:
            # check whether someting is in the body
            # There seem to be big differences between various python
            # implementations even with the same version number (e.g.
            # read() does sometimes not exist)

            readMethod = getattr(e, "read", e)
            if readMethod and callable(readMethod):
                msg = e.read()
                if msg:
                    return (e.code, msg, e.headers)
            if hasattr(e, 'reason'):
                if type(e.reason) == str:
                    return (e.code, e.reason, e.headers)
                else:
                    return (-1, "Error ({0}): {1}".format(e.reason.errno, e.reason.strerror), "")
            else:
                return (-1, e.__str__(), HttpConnection.get_headers_from_http_error(e))
        except urllib.error.URLError as e:
            if hasattr(e, 'reason'):
                if type(e.reason) == str:
                    return (-1, e.reason, "")
                else:
                    return (-1, "I/O error({0}): {1}".format(e.reason.errno, e.reason.strerror), "")
            else:
                return (-1, e.__str__(), e.headers)


    # workaround for bug in at least one python version (2.6.8, linux)
    @staticmethod
    def get_headers_from_http_error(ex):

        try:
            return ex.headers
        except AttributeError:
            return ex.hdrs

    # Send request, re-open connection if it has been closed due to a timeout
    #
    def send_request(self, request, url=""):

        logging.debug("Opening " + url)
        res = urllib.request.urlopen(request)
        session_timeout_marker = res.headers.get(timeout_marker_header)
        if session_timeout_marker != None:
            # got a timeout, re-open connection and try again
            logging.debug("Session has expired. Re-opening connection.")
            self.open_connection(minVersion=self.minVersion)
            logging.debug("Opening " + url)
            res = urllib.request.urlopen(request)

        return res

    def send_request_resilient(self, request, url=""):
        counter = 0
        while True:
            counter += 1
            try:
                res = self.send_request(request, url)
                return res
            except urllib.error.HTTPError as e:
                code = e.code
                if code in self.retry_strategy.handled_http_status_codes and counter < self.retry_strategy.maxRetries:
                    logging.debug("Got recoverable error {0}, retry in {1} sec.".format(code, self.retry_strategy.retryInterval))
                    time.sleep(self.retry_strategy.retryInterval)
                else:
                    raise e

    def ping_system(self, initial=False):
        parameters = {
            "HDBALM_VERSION": HDBALM_VERSION
        }
        return self.request(xslm_ping, initial=initial, parameters=parameters)


    def getPingReply(self):
        return self.pingReply

    def setPingReply(self, r, i):
        self.pingReply = utils.readJson(r, i)

    @staticmethod
    def validateServerIdentity(host, port, certs):

        def parse_proxy_response(data):
            pattern = "^HTTP/[\d.]* (\d+) (\w+)"
            m = re.search(pattern, data)
            if not m:
                raise utils.UserReportableException("Unable to parse proxy reply: \"{0}\"".format(data))
            return (m.group(1), m.group(2))

        def validate_subject_CN(cert):
            if not cert:
#       match host name check needs a SSL socket or SSL context
#       with either CERT_OPTIONAL or CERT_REQUIRED
                raise ssl.SSLError("Empty or no certificate")

            certhost = None
            for field in cert['subject']:
                if field[0][0] == 'commonName':
                    certhost = field[0][1]

            if certhost == None:
                raise ssl.SSLError("Empty subject commonName")

            if utils.isHostWildcardCertificate(certhost):
                utils.validateHostWildcardCertificate(host, certhost)
            else:
                if certhost != host:
                    raise ssl.SSLError("Host name '%s' doesn't match certificate host '%s'" % (host, certhost))

        (http_proxy, https_proxy, no_proxy) = HttpConnection.get_proxies()

        if (https_proxy is not None) and HttpConnection.use_proxy_for_host(no_proxy, host):

            try:
                proxy_host = https_proxy[0]
                proxy_port = https_proxy[1]

                # connect to http proxy
                ##
                addr = socket.getaddrinfo(proxy_host, proxy_port)
                socket_args = addr[0][0:3]
                connect_args = addr[0][4]

                sock = socket.socket(*socket_args)
                sock.connect(connect_args)


                # send connect string to connect to real host
                #
                proxyConnectString = "CONNECT " + host + ":" + str(port) + " HTTP/1.0\r\n\r\n"
                sock.sendall(proxyConnectString)

                # check that connection has been established by the proxy
                #
                data = sock.recv(1024)
                (code, text) = parse_proxy_response(data)
                if code != "200":
                    raise utils.UserReportableException("Unable to connect to {0}:{1} via proxy {2}:{3}: {4} {5}".format(host, port, proxy_host, proxy_port, code, text))

            except socket.error as e:
                raise utils.UserReportableException("Unable to connect to {0}:{1}: {2}".format(proxy_host, proxy_port, e.args[1]))

        else:
            try:
                # create socket and connect to server
                #
                addr = socket.getaddrinfo(host, port)
                socket_args = addr[0][0:3]
                connect_args = addr[0][4]
                sock = socket.socket(*socket_args)
                sock.connect(connect_args)

            except socket.error as e:
                raise utils.UserReportableException("Unable to connect to {0}:{1}: {2}".format(host, port, e.args[1]))


        # wrap socket to add SSL support
        sslsock = ssl.wrap_socket(sock,
          # flag that certificate from the other side of connection is required
          # and should be validated when wrapping
          cert_reqs=ssl.CERT_REQUIRED,
          # file with root certificates
          ca_certs=certs
        )
        logging.debug("Validating server identity for host " + host)
        cert = sslsock.getpeercert()
        logging.debug("Server certificate: " + utils.dump_json_pretty(cert))

#   ssl.match_hostname() is available since Python 2.7.9
        req_version = (2,7,9)
        cur_version = sys.version_info
        if cur_version >= req_version:
            try:
                ssl.match_hostname(cert, host)
            except ssl.CertificateError as e:
                raise utils.UserReportableException(e.__str__())
        else:
            logging.debug("Running with an old python version")
            validate_subject_CN(cert)

        sock.close()
        logging.debug("Successfully validated server identity")

# ------------------------------------------------------------------------
# File: install.py
# ------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Implementation of the hdbalm install and import command.
#
# This plug-in requires at least version 1.3.0 of HALM
# ---------------------------------------------------------------------------

import os
import tempfile
import shutil
import json
import sys
import traceback
import codecs
import tarfile
from zipfile import ZipFile
import xml.etree.ElementTree as ET
import time
import calendar
from datetime import datetime, timedelta

nl = "\n"

# ---------------------------------------------------------------------------
# Valid options that can be passed in order to control deployment
# behavior for installation and update
# ---------------------------------------------------------------------------

ALLOW_DU_NEW_IMPORT = "ALLOW_DU_NEW_IMPORT"
ALLOW_DU_VERSION_UPDATE = "ALLOW_DU_VERSION_UPDATE"
ALLOW_DU_SAME_VERSION = "ALLOW_DU_SAME_VERSION"
ALLOW_DU_DOWNGRADE = "ALLOW_DU_DOWNGRADE"
ALLOW_KEEP_DU_NEWER_VERSION = "ALLOW_KEEP_DU_NEWER_VERSION"
CONSIDER_DEPENDENCYTYPE_ABAP = "CONSIDER_DEPENDENCYTYPE_ABAP"
CONSIDER_DEPENDENCYTYPE_HDB = "CONSIDER_DEPENDENCYTYPE_HDB"
USE_TWO_COMMIT_ACTIVATION = "USE_TWO_COMMIT_ACTIVATION"
USE_ALWAYS_COMMIT_ACTIVATION = "USE_ALWAYS_COMMIT_ACTIVATION"
IGNORE_DEPENDENCY_ERRORS = "IGNORE_DEPENDENCY_ERRORS"

PRODUCT_ARCHIVE_OPTIONS = {
    ALLOW_DU_VERSION_UPDATE: True,
    ALLOW_DU_SAME_VERSION: True,
    ALLOW_DU_DOWNGRADE: True,
    ALLOW_KEEP_DU_NEWER_VERSION: True,
    USE_TWO_COMMIT_ACTIVATION: True,
    USE_ALWAYS_COMMIT_ACTIVATION: True,
    IGNORE_DEPENDENCY_ERRORS: True
}

SCV_ARCHIVE_OPTIONS = {
    ALLOW_DU_VERSION_UPDATE: True,
    ALLOW_DU_SAME_VERSION: True,
    ALLOW_DU_DOWNGRADE: True,
    USE_TWO_COMMIT_ACTIVATION: True,
    USE_ALWAYS_COMMIT_ACTIVATION: True,
    IGNORE_DEPENDENCY_ERRORS: True,
    CONSIDER_DEPENDENCYTYPE_ABAP: True,
    CONSIDER_DEPENDENCYTYPE_HDB: True
}

PM_OPTIONS = {
    IGNORE_DEPENDENCY_ERRORS: True,
    ALLOW_KEEP_DU_NEWER_VERSION: True
}

DU_IMPORT_OPTIONS = {}

STACK_XML_NAME = "stack.xml"
# Support registration of <...>stack.xml files created by Landscape Planner
LP_STACK_XML_NAME = "*stack.xml"
PRODUCT_DESCRIPTOR_NAME = "PD.xml"
SL_MANIFEST_NAME = "SL_MANIFEST.XML"

# When searching directory structures for valid product descriptors stop
# after this number of files in case somebody has specified the root
# directory
MAX_SEARCH_DEPTH = 1000

duCaptionSeparator = "\$|"

class Process(dict):
    pass

class install:

    # ---------------------------------------------------------------------------
    # Deployment types
    # ---------------------------------------------------------------------------

    DEPLOY_TYPE_PRODUCT_ARCHIVE = "DEPLOY_TYPE_PRODUCT_ARCHIVE"
    DEPLOY_TYPE_SCV = "DEPLOY_TYPE_SCV"
    DEPLOY_TYPE_DU = "DEPLOY_TYPE_DU"

    INSTALL_TYPE_SCVS = "SCVS"
    INSTALL_TYPE_SCV = "SCV"
    INSTALL_TYPE_PV = "PV"
    INSTALL_TYPE_PM = "PM"

    class MyCtx:
        pass

    def __init__(self, context):
        self.ctx = context
        self.fileLogger = self.ctx.NullFileLogger()
        self.stdout = self.ctx.utils.stdout
        self.stderr = self.ctx.utils.stderr

    # initialize after http connection information is available
    #
    def init2(self, options, command_options, nolog=False):
        self.options = options
        self.command_options = command_options
        self.conn = self.ctx.HttpConnection.initialize_connection(options)
        minimal_halm_version = self.ctx.Constants.halm_sp10_version
        self.ctx.utils.hasMinimalVersion(
                minimal_halm_version,
                self.conn.getPingReply()["halm_version"],
                "At least Version {0} of delivery unit HANA_XS_LM is required to run this command.".format(minimal_halm_version))

        self.jdbcPort = self.ctx.utils.getJDBCProtfromHTTPPort(self.conn.port, self.conn)

        try:
            self.tmpDir = tempfile.mkdtemp()
        except (OSError) as e:
            raise utils.UserReportableException("Unable to create temporary directory.")

        if not nolog:
            if command_options.log:
                self.fileLogger = self.ctx.FileLogger(self.ctx, fileName=command_options.log)
            else:
                self.fileLogger = self.ctx.FileLogger(self.ctx, namePrefix="hdbalm-install")

        self.myctx = install.MyCtx()
        self.myctx.fileLogger = self.fileLogger
        self.myctx.command_options = command_options
        self.myctx.conn = self.conn

        if options.tunnel:
            self.tunnel = options.tunnel
        else:
            self.tunnel = None

    def turn_on_logging(self):

        if isinstance(self.fileLogger, self.ctx.FileLogger):
            # already on
            return
        if self.command_options.log:
            self.fileLogger = self.ctx.FileLogger(self.ctx, fileName=self.command_options.log)
        else:
            self.fileLogger = self.ctx.FileLogger(self.ctx, namePrefix="hdbalm-install")
        self.myctx.fileLogger = self.fileLogger
        self.ctx.utils.log_environment(self.myctx)
        self.ctx.utils.log_server_info(self.myctx, self.conn)

    # -------------------------------------------------------------------------
    # User interaction section
    # -------------------------------------------------------------------------

    @staticmethod
    def description():
        return "Installs and updates product archives and software components"

    @staticmethod
    def help():
        return """
    Install and Update SAP HANA Add-On Products and Software Components

    This command is used to install and update SAP HANA add-on products and
    software components.

    usage: hdbalm install [<command option>]* [<archive>|<directory>]*

    The install command automatically detects whether the archive is an add-on
    product archive or a software component archive. It also detects whether the
    add-on product or software component is already installed and either executes
    an installation or update operation.

    The following command options are supported for all archive types:

      -d, --display
        Displays the archive contents. No changes are applied to the system.

      -l <file name>, --log=<file name>
        Sets an alternate location for the log file.

      -o <option>, --option=<option>
        Options which can be used to override the default behavior of the install
        command. Multiple options can be specified by repeating the -o option.
        The following options are available:

          ALLOW_DU_SAME_VERSION
            Re-installs the same version of the component

          ALLOW_DU_DOWNGRADE
            Allows downgrades of components

          ALLOW_DU_VERSION_UPDATE
            Allows version updates of components

          ALLOW_KEEP_DU_NEWER_VERSION
            Allows to keep the component if it is already installed in a newer
            version

          USE_TWO_COMMIT_ACTIVATION
            If an object that is not part of the installation archives cannot
            be activated because it references an object that is to be installed
            then this object remains broken in the system after the installation.
            By default, the installation is canceled if any errors occur and
            the complete installation is rolled back. You can use this option
            after an installation fails due to the situation described above.
            In this case, the object remains broken in the system after the
            installation, but the installation itself finishes successfully.
            You must correct the errors manually after the installation.



    SAP HANA Add-On Product Installation and Update
    -----------------------------------------------

    You can specify an add-on product archive ZIP file or a directory location.

    The SAP HANA add-on products installation and update command supports the
    following options:

      --instances
        By default all instances will be installed. A comma-separated list of
        instances can be specified to limit the number of instances.

      -o ALLOW_KEEP_DU_NEWER_VERSION, --option=ALLOW_KEEP_DU_NEWER_VERSION
        Skip installation of a software component if a newer version is already
        installed in the system.

    Examples:

    This command installs or updates the product contained in the file
    SAP_APO_ANALYTICS_1.0.zip:

      hdbalm install SAP_APO_ANALYTICS_1.0.zip

    This command installs or updates the product located in the directory
    c:\\products\\SAP_APO_ANALYTICS and writes the log file to the file
    install.log:

      hdbalm install -l install.log c:\\products\\SAP_APO_ANALYTICS

    This command installs instances 1 and 2 of the SAP HANA add-on product
    contained in the my_product.zip file. Any additional instances that might
    be part of this product are not installed.

      hdbalm install --instances 1,2 my_product.zip


    SAP HANA Software Component Installation and Update
    ---------------------------------------------------

    You can specify multiple software component archive files or multiple
    directories containing software component archive files.

    Examples:

    This command installs or updates the software components contained in the
    scv1.zip and scv2.zip files:

      hdbalm install scv1.zip scv2.zip

    The scv.zip file contains software component SCV version 2. Version 1 of SCV
    is currently installed. The following command udpates SCV to version 2:

      hdbalm install --option=ALLOW_DU_VERSION_UPDATE scv.zip

    This command looks for software component files in the c:\\patches directory
    and installs or updates the components in the SAP HANA system:

      hdbalm install c:\\patches
    """

    @staticmethod
    def get_readable_du_representation(scv):
        text = ""
        text += ("Name:          {0}" +nl).format(scv.get("DELIVERY_UNIT"))
        text += ("Vendor:        {0}" +nl).format(scv.get("VENDOR"))
        text += ("Version:       {0}" +nl).format(scv.get("VERSION", ""))
        text += ("SP Version:    {0}" +nl).format(scv.get("VERSION_SP", ""))
        text += ("Patch Version: {0}" +nl).format(scv.get("VERSION_PATCH", ""))
        text += ("Description:   {0}" + nl).format(scv.get("CAPTION", ""))
        return text

    @staticmethod
    def get_readable_product_representation(productMeta, selInstances=[]):
        def check_print_instance(instances, id):
            if instances == []:
                return True
            for i in instances:
                if i == int(id):
                    return True
            return False

        product = productMeta["product"]

        text = ""
        text += nl
        text += ("Product:     {0}" + nl).format(product["NAME"])
        text += ("Vendor:      {0}" + nl).format(product["VENDOR"])
        text += ("Version:     {0}" + nl).format(product["VERSION"])
        text += ("SP Stack:    {0}" + nl).format(product["VERSION_SP"])
        text += ("Description: {0}" + nl).format(product.get("CAPTION", ""))
        text += nl

        text += "Instances" + nl +nl
        for pi in product.get("PRODUCT_INSTANCES", []):
            instanceId = pi.get("INSTANCE_ID")
            if check_print_instance(selInstances, instanceId):
                text += ("Instance number: {0}" + nl).format(pi.get("INSTANCE_ID"))
                text += ("Description:     {0}" + nl).format(pi.get("CAPTION", ""))

                text += nl
                text += "Software Components" +nl+nl

                for scv in pi.get("DELIVERY_UNITS", []):
                    text += install.get_readable_du_representation(scv)
                    text += nl
        return text

    @staticmethod
    def get_readable_product_update_description(report, product):

        line = "---------------------------------------------------------------------------" +nl

        text = ""
        text += line

        text += "Product:         {0}".format(report.get("NAME")) +nl
        text += "Vendor:          {0}".format(report.get("VENDOR")) +nl
        text += "Description:     {0}".format(report.get("CAPTION","")) +nl
        text += "Version:         {0}".format(report.get("VERSION", "")) +nl
        text += "SP Stack:        {0}".format(report.get("SP_STACK_CAPTION", ""))+nl+nl
        text += line +nl
        # Product actions: update, install, skipped
        action = report.get("ACTION")
        if action == "update":
            text += "The installed product will be updated." +nl+nl
            text += "Currently installed Product:"+nl
            text += "Version:          {0}".format(report.get("CURRENT_VERSION", "")) +nl
            text += "Support Package:  {0}".format(report.get("CURRENT_SP_STACK_CAPTION", ""))+nl+nl
        elif action == "install":
            text += "The product will be installed." +nl+nl
        elif action == "skipped":
            text += "The version of this product is already installed." + nl+nl
        text += nl

        text += "SAP HANA Product Instances" +nl+nl

        for pi in report.get("PRODUCT_INSTANCES", []):
            text += line
            text += ("Instance number: {0}" + nl).format(pi.get("INSTANCE_ID"))
            if pi.get("CAPTION", "") != "":
                text += ("Description:     {0}" + nl).format(pi.get("CAPTION"))
                if pi.get("SP_CAPTION", "") != "":
                    text += ("Description:     {0}" + nl).format(pi.get("SP_CAPTION"))
            text += nl
            # Product instance actions: update, install, skipped, error
            action = pi.get("ACTION")
            if action == "update":
                text += "The product instance will be updated" +nl
                if pi.get("SP_STACK_CAPTION", "") != "":
                    text += "New support package: " + pi.get("SP_STACK_CAPTION") +nl
            elif action == "install":
                text += "The product instance will be installed" +nl
                if pi.get("SP_STACK_CAPTION", "") != "":
                    text += "New support package: " + pi.get("SP_STACK_CAPTION") +nl
            elif action == "skipped":
                text += "Product instance is already installed." + nl
            elif action == "error":
                info = pi.get("INFO", None)
                if info != None:
                    text += info[0].format(*info[1:]) + nl
            text += nl

            text += "Software Component:" +nl+nl

            for scv in pi.get("DELIVERY_UNITS"):
                text += ("  Name:          {0}" +nl).format(scv.get("DELIVERY_UNIT"))
                text += ("  Vendor:        {0}" +nl).format(scv.get("VENDOR"))
                text += ("  Version:       {0}" +nl).format(scv.get("VERSION", ""))
                text += ("  SP Version:    {0}" +nl).format(scv.get("VERSION_SP", ""))
                text += ("  Patch Version: {0}" +nl).format(scv.get("VERSION_PATCH", ""))
                text += ("  Description:   {0}" + nl).format(scv.get("CAPTION", ""))

                text += nl
                # DU actions: import, update, overwrite (same version), downgrade, skipped, error
                action = scv.get("ACTION")
                if action == "import":
                    text += "  Software component will be installed." + nl
                elif action == "update":
                    text += "  Software component unit will be updated. Installed version:" + nl+nl
                    text += ("  Version:       {0}" +nl).format(scv.get("CURRENT_VERSION", ""))
                    text += ("  SP Version:    {0}" +nl).format(scv.get("CURRENT_VERSION_SP", ""))
                    text += ("  Patch Version: {0}" +nl).format(scv.get("CURRENT_VERSION_PATCH", ""))
                elif action == "overwrite":
                    text += "  Same version of software component will be imported." + nl
                elif action == "downgrade":
                    text += "  Installed software component will be downgraded. Installed version:" + nl
                    text += ("  Version:       {0}" +nl).format(scv.get("CURRENT_VERSION", ""))
                    text += ("  SP Version:    {0}" +nl).format(scv.get("CURRENT_VERSION_SP", ""))
                    text += ("  Patch Version: {0}" +nl).format(scv.get("CURRENT_VERSION_PATCH", ""))
                elif action == "skipped":
                    # if option ALLOW_KEEP_DU_NEWER_VERSION is used it could be possible
                    # that installed DU has higher version
                    text += "  Software component is already installed. Installed version:" + nl
                    text += ("  Version:       {0}" +nl).format(scv.get("CURRENT_VERSION", ""))
                    text += ("  SP Version:    {0}" +nl).format(scv.get("CURRENT_VERSION_SP", ""))
                    text += ("  Patch Version: {0}" +nl).format(scv.get("CURRENT_VERSION_PATCH", ""))
                elif action == "error":
                    info = scv.get("INFO")
                    text += "  " + info[0].format(*info[1:]) + nl
                text += nl

        productMeta = product['productMeta']
        if productMeta.get("sap-note", None) != None:
            text += ("For information about this product refer to SAP note {0}."+nl).format(productMeta.get("sap-note"))

        return text

    # File log section
    #

    def log_product_model(self, model):

        msgPattern = """Product Model
    -------------

    This is the technical product model that is contained in the specified
    product archive.

    -----------------------------------------------------------------------------
    {0}
    -----------------------------------------------------------------------------
    """

        self.fileLogger.log(msgPattern.format(self.ctx.utils.dump_json_pretty(model)))


    def log_validation_result(self, validationResult):

        msgPattern = """Validation Result
    -----------------

    This is the validation result with instructions on how to run the operation.
    -----------------------------------------------------------------------------
    {0}
    -----------------------------------------------------------------------------
    """
        self.fileLogger.log(msgPattern.format(self.ctx.utils.dump_json_pretty(validationResult)))

#   @staticmethod
#   def _display_help_on_error(product, archive):
#     productMeta = product.get("productMeta")
#     if productMeta.get("sap-note", None) != None:
#       helps = "For further information refer to SAP note " + productMeta.get("sap-note") +" and provide" + nl
#       helps += "the log file written by this process." +nl
#     else:
#       helps = "For help contact your vendor for support. Provide the log file written by" + nl
#       helps += "this process." + nl
#
#     line = "-----------------------------------------------------------------------------" + nl
#     msg = ""
#     msg += nl
#     msg += line
#     msg += nl
#     msg += "The installation of product archive " +nl +nl
#     msg += "     " + archive + nl+nl
#     msg += "was not successful." +nl +nl
#     msg += helps + nl
#     msg += line + nl
#     return msg


    # clean up temporary files
    #
    def _cleanUp(self):
        if hasattr(self, 'tmpDir') and os.path.exists(self.tmpDir):
            try:
                shutil.rmtree(self.tmpDir)
            except Exception as e:
                self.stderr.write(("Unable to remove temporary directory {0}: {1}"+nl).format(self.tmpDir, str(e)))


    def log(self, msg):
        self.stdout.write(msg+nl)
        self.fileLogger.log(msg)


    def findFile(self, rootDir, searchNames):

        def findFile2(rootDir, searchNames, visited, originalRootDir):
            foundDirs = []
            try:
                for name in os.listdir(rootDir):
                    visited[0] += 1
                    if visited[0] > MAX_SEARCH_DEPTH:
                        raise self.ctx.utils.UserReportableException("Directory structure of {0} to deep".format(originalRootDir))
                    fullPath = os.path.join(rootDir, name)
                    if os.path.isfile(fullPath):
                        if self.ctx.utils.isSarFile(fullPath):
                            raise self.ctx.utils.UserReportableException("Error: SAR archives are not supported")
                        found = False
                        for searchName in searchNames:
                            if searchName[0] == "*":
                                searchName = searchName[1:]
                                if name.lower().endswith(searchName):
                                    foundDirs.append(os.path.join(rootDir, name))
                                    found = True
                                    break
                            elif  name.lower() == searchName.lower():
                                foundDirs.append(os.path.join(rootDir, name))
                                found=True
                                break
                        if found == True:
                            continue

                    elif os.path.isdir(fullPath):
                        deepFoundDirs = findFile2(fullPath, searchNames, visited, originalRootDir)
                        foundDirs = foundDirs + deepFoundDirs
                return foundDirs
            except OSError as e:
                raise self.ctx.utils.UserReportableException("Unable to examine directory {0}: {1}".format(rootDir, e.strerror))

        return findFile2(rootDir, searchNames, [0], rootDir)

    def _check_evaluation_result_for_errors(self, evaluationResult):
        if "errors" in evaluationResult:
            msg = ''
            errors = evaluationResult["errors"]
            if len(errors) > 0:
                msg += "The operation cannot be completed due to the following errors:" + nl
                for e in errors:
                    if len(e) == 1:
                        msg += e[0] + nl
                    else:
                        msg += e[0].format(*e[1:]) + nl
                return (True, msg)
            else:
                return (False, None)
        else:
            return (False, None)

    def get_du_manifest(self, duTgzFile):

        duTgz = None
        try:
            duTgz = tarfile.open(duTgzFile, "r")
            for tarinfo in duTgz:
                if tarinfo.isreg() and tarinfo.name == 'manifest.txt':
                    f = duTgz.extractfile(tarinfo)
                    j = f.read()
                    return j
            return None
        except tarfile.TarError as t:
            self.ctx.utils.UserReportableException("Unable to read tar file {0}: {1}".format(duTgzFile, str(t)))
        except (IOError, OSError) as io:
            if hasattr(io, "strerror"):
                raise self.ctx.utils.UserReportableException("Unable to read tar file {0}: {1}".format(duTgzFile, io.strerror))
            else:
                raise self.ctx.utils.UserReportableException("Unable read tar file {0}.".format(duTgzFile))
        finally:
            if duTgz != None:
                duTgz.close()

    def get_du_meta(self, duTgzFile):
        mf = self.get_du_manifest(duTgzFile)
        if mf == None:
            return None
        lines = mf.split(b'\n')
        meta = {}
        for l in lines:
            if l == "":
                continue
            kv = l.split(b'=')
            if len(kv) == 1:
                n = kv[0].strip()
                meta[n] = ""
            elif len(kv) == 2:
                n = kv[0].strip()
                v = kv[1].strip()
                meta[n] = v
        return meta

    @staticmethod
    def has_meta_update(metaUpdate):
        for key in metaUpdate:
            val = metaUpdate.get(key)
            if len(val) != 0:
                return True
        return False

    def _get_files(self, archives, pm):

        def file_fits(file, types):
            fileType = ""
            baseName = os.path.basename(file).lower()

            if baseName == STACK_XML_NAME.lower():
                fileType = STACK_XML_NAME
            # Support registration of <...>stack.xml files created by Landscape Planner
            elif baseName.endswith(STACK_XML_NAME.lower()):
                fileType = LP_STACK_XML_NAME
            elif baseName == PRODUCT_DESCRIPTOR_NAME.lower():
                fileType = PRODUCT_DESCRIPTOR_NAME
            elif self.ctx.utils.isZipFile(file):
                fileType = "*.zip"

            for type in types:
                if type == fileType:
                    return True
            return False

        files = []
        if len(archives) > 1:
            # scvs
            candidateTypes = ["*.zip"]
        else:
            #could be a product
            candidateTypes = ["*.zip", STACK_XML_NAME, PRODUCT_DESCRIPTOR_NAME]

        if pm == True:
            candidateTypes = [STACK_XML_NAME, LP_STACK_XML_NAME, PRODUCT_DESCRIPTOR_NAME]

        for archive in archives:
            if os.path.isdir(archive):
                candidates = self.findFile(archive, candidateTypes)
                for candidate in candidates:
                    files.append(candidate)
            elif file_fits(archive, candidateTypes):
                files.append(archive)
            elif self.ctx.utils.isSarFile(archive):
                raise self.ctx.utils.UserReportableException("Error: SAR archives are not supported")
        return files

    def _upload_files(self, files):

        def join(cids):
            msg = ""
            first = True
            for cid in cids:
                if first:
                    msg += str(cid)
                    first = False
                else:
                    msg += "," + str(cid)
            return msg

        if len(files) == 0:
            msg = "No files to import"
            self.fileLogger.log(msg)
            self.ctx.utils.stdout.write(msg + nl)
            return self.ctx.Constants.EXIT_OK

        #self.stdout.write("Uploading files" + nl)
        fileList = join(files)
        self.log("Uploading files: {0}.".format(fileList) + nl)

#    try:
        multipartFiles = []
        for name in files:
            if self.ctx.utils.isXmlFile(name):
                mimeType = 'text/xml'
            else:
                mimeType = 'application/octet-stream'
            try:
                f = open(name, 'rb')
                contents = f.read()
                f.close()
            except:
                raise self.ctx.utils.UserReportableException("Unable to open file {0}".format(name))

            baseName = os.path.basename(name)
            type = baseName
            # Support registration of <...>stack.xml files created by Landscape Planner
            if baseName.lower().endswith(STACK_XML_NAME.lower()):
                type = STACK_XML_NAME

            multipartFiles.append({
              'filename': type,
              'mimetype': mimeType,
              'content': contents,
              'name': baseName
            })

        (code, res, hdrs) = self.conn.file_upload(self.ctx.Constants.xslm_file_upload_service, files=multipartFiles)
        self.ctx.utils.tryReadWebError(code, res, hdrs)
        contentIds = self.ctx.utils.readJson(res, hdrs)
        ctidtext = join(contentIds)
        self.fileLogger.log("Successfully uploaded files, content ids: {0}.".format(ctidtext))

        # run preInstall
        params = {
            "contentIds": contentIds,
            "operation":  "preInstall"
          }

        (code, res, hdrs) = self.conn.request(self.ctx.Constants.xslm_import_du_service,
                                               method='POST',
                                               body=json.dumps(params))
        self.ctx.utils.tryReadWebError(code, res, hdrs)
        jResponse = self.ctx.utils.readJson(res, hdrs)
        header = jResponse.get('header')
        contDescr = header.get('description')
        containerId = header.get('containerId')
        containerType = header.get('containerType')

        if containerType==self.INSTALL_TYPE_PV:
            ctDescr = "product {0}".format(contDescr)
        elif containerType==self.INSTALL_TYPE_PM:
            ctDescr = "meta data of the product {0}".format(contDescr)
        elif containerType==self.INSTALL_TYPE_SCV:
            ctDescr = "software component {0}".format(contDescr)
        elif containerType==self.INSTALL_TYPE_SCVS:
            ctDescr = contDescr

        msg = "Successfully uploaded {0}".format(ctDescr)
        self.stdout.write(msg + nl)
        self.fileLogger.log(msg + " into container {0}.".format(containerId))

        return header

#     except utils.UserReportableException as e:
#       # the signature of this method does not allow this exception to be thrown
#       #
#       ctx.fileLogger.log(e.__str__())
#       utils.stderr.write(e.__str__()+nl)
#       return Context.Constants.EXIT_ERROR


    def _run_container_validation(self, containerId):
        installOptions = self._get_and_validate_options_product_archive(self.command_options)
        instances = self._parse_selected_instances(self.command_options.instances)

        self.fileLogger.log("Invoking container evaluation.")
        params = {
            "operation": "doValidation",
            "containerId" : containerId,
            "instances": self.ctx.utils.dump_json(instances),
            "options": self.ctx.utils.dump_json(installOptions)
        }
        #errorhere
        self.fileLogger.log(self.ctx.utils.dump_json_pretty(params))

        (code, res, hdrs) = self.conn.request(self.ctx.Constants.xslm_pe_installation_service, parameters=params, method="GET")
        self.ctx.utils.tryReadWebError(code, res, hdrs)
        evaluationResult = self.ctx.utils.readJson(res, hdrs)
        return evaluationResult


    def _get_container_meta(self, containerId):
        params = {
            "operation": "getContainerMetaData",
            "containerId" : containerId,
        }
        if (self.options.verbose):
            self.ctx.utils.stdout.write("Invoking getContainerMetaData.")
            self.ctx.utils.stdout.write(self.ctx.utils.dump_json_pretty(params))

        (code, res, hdrs) = self.conn.request(self.ctx.Constants.xslm_pe_installation_service, parameters=params, method="GET")
        self.ctx.utils.tryReadWebError(code, res, hdrs)
        metaData = self.ctx.utils.readJson(res, hdrs)
        return metaData

    def _log_dep_errors(self, evaluationResult):
        def print_scv_dep(dep):
            msg = "Software component dependency information:" + nl
            msg+=self.ctx.utils.dump_json_pretty(dep)
            self.fileLogger.log(msg)

        def print_inst_dep(dep):
            msg = "Product instances dependency information:" + nl
            msg+=self.ctx.utils.dump_json_pretty(dep)
            self.fileLogger.log(msg)

        printDepInfo = False

        if len(evaluationResult.get("scv_errors"))  > 0:
            print_scv_dep(evaluationResult.get("scv_errors"))
            printDepInfo = True

        if len(evaluationResult.get("scv_warnings"))  > 0:
            print_scv_dep(evaluationResult.get("scv_warnings"))
            printDepInfo = True

        if "instances_errors" in evaluationResult and len(evaluationResult.get("instances_errors"))  > 0:
            print_inst_dep(evaluationResult.get("instances_errors"))
            printDepInfo = True

        if "instances_warnings" in evaluationResult and len(evaluationResult.get("instances_warnings"))  > 0:
            print_inst_dep(evaluationResult.get("instances_warnings"))
            printDepInfo = True

        if printDepInfo == True:
            self.ctx.utils.stdout.write("See dependencies error information in the log" + nl + nl)

    def _run_install_pv(self, contMetaData, evaluationResult, containerId, pvOptions):

        # log (content) validation result
        self.log_product_model(contMetaData)
        product = {
            "productMeta": contMetaData
        }
        validationText = self.get_readable_product_update_description(evaluationResult.get("report"), product)
        self.log(validationText)

        # log validation result object
        self.log_validation_result(evaluationResult)

        (hasErrors, error_messages) = self._check_evaluation_result_for_errors(evaluationResult)
        if hasErrors:
            self.log(error_messages)
            self._log_dep_errors(evaluationResult)
            raise self.ctx.utils.UserReportableException("Process terminated due to errors.")

        # Check whether there will be any changes
        if not self.has_meta_update(evaluationResult.get("metaUpdate"))\
           and len(evaluationResult.get("scvImport")) == 0:
            msg = "Product is already installed. No changes have been applied to the system."
            self.log(msg)

        if not self.options.yes:
            pi_input = input("Continue with the specified operation? [yes/NO] -->")
            if not (pi_input.lower() == 'yes' or pi_input.lower() == 'y'):
                return False
            self.stdout.write(nl)

        return self._run_install(containerId, "product", pvOptions)

    def _run_register_pm(self, contMetaData, containerId, pmOptions):
        # log content validation result
        self.log_product_model(contMetaData)
        instances = self._parse_selected_instances(self.command_options.instances)
        actionText = self.get_readable_product_representation(contMetaData, instances)
        self.log(actionText)

        if not self.options.yes:
            pi_input = input("Continue with the specified operation? [yes/NO] -->")
            if not (pi_input.lower() == 'yes' or pi_input.lower() == 'y'):
                return False
            self.stdout.write(nl)

        return self._run_install(containerId, "productMeta", pmOptions)


    def _run_install_scvs(self, contMetaData, evaluationResult, containerId, scvOptions):
        # log (content) validation result
        self.log(self.get_readable_update_decription(evaluationResult.get("report")))

        # log and analyze validation result object
        self.log_validation_result(evaluationResult)
        (hasErrors, error_messages) = self._check_evaluation_result_for_errors(evaluationResult)
        if hasErrors:
            self.log(error_messages)
            #msg = "At least one software component cannot be installed."
            #self.stderr.write(msg + nl)
            #self.fileLogger.log(msg)
            self.log("At least one software component cannot be installed." + nl)
            self._log_dep_errors(evaluationResult)
            return self.ctx.Constants.EXIT_ERROR

        if not self.options.yes:
            pi_input = input("Continue with the specified operation? [yes/NO] -->")
            if not (pi_input.lower() == 'yes' or pi_input.lower() == 'y'):
                return self.ctx.Constants.EXIT_OK

        return self._run_install(containerId, "scv", scvOptions)


    def utc_to_local(self, utc_dt):
        timestamp = calendar.timegm(utc_dt.timetuple())
        local_dt = datetime.fromtimestamp(timestamp)
        assert utc_dt.resolution >= timedelta(microseconds=1)
        return local_dt.replace(microsecond=utc_dt.microsecond)

    def get_process_info(self, processId):
        params = {'operation': 'processInfo', 'processId': processId}
        (code, res, hdrs) = self.conn.request(self.ctx.Constants.xslm_pe_installation_service, parameters=params, method="GET")
        self.ctx.utils.tryReadWebError(code, res, hdrs)
        return self.ctx.utils.readJson(res, hdrs)

    def monitor_process(self, processId):
        running = True
        process = self.get_process_info(processId)
        self.log('Process with ID: ' + processId + ' started.')
        lastDisplayedLogTime = self.utc_to_local(datetime.utcnow())
        lastDisplayedDot = False
        while running:
            logPrinted = False
            processSteps = process["STEPS"];
            for step in processSteps:
                logs = step["LOGS"]
                for log in logs:
                    logTime = self.utc_to_local(datetime.strptime(log["TIME"], '%Y-%m-%dT%H:%M:%S.%fZ'))
                    if(logTime > lastDisplayedLogTime):
                        if not logPrinted and not self.options.verbose:
                            self.ctx.utils.stdout.write(".")
                            lastDisplayedDot = True
                        lastDisplayedLogTime = logTime

                        rcTxt = "";
                        if log["RC"] == 4:
                            rcTxt = "Warning:"
                        elif log["RC"] == 8:
                            rcTxt = "Content error:"
                        elif log["RC"] == 12:
                            rcTxt = "Error:"

                        message = log["MESSAGE"].encode('utf-8')

                        if rcTxt == "":
                            logLine = message
                        else:
                            logLine = "{0} {1}".format(rcTxt, message)

                        if (log["RC"] > 4 or self.options.verbose):
                            if (lastDisplayedDot == True):
                                self.ctx.utils.stdout.write(nl)
                            self.ctx.utils.stdout.write(logLine + nl)
                            lastDisplayedDot = False

                        if (isinstance(logLine, (bytes, bytearray))):
                            logLine = logLine.decode()

                        self.fileLogger.log(logLine)
                        logPrinted = True

            if not logPrinted and not self.options.verbose:
                self.ctx.utils.stdout.write(".")
                lastDisplayedDot = True

            if process["STATUS"] >= 10:
                if process["STATUS"] == 10:
                    #process.RC == 0
                    self.log(nl+'Process {0} finished successfully.'.format(processId))
                elif process["STATUS"] == 11:
                    #process.RC == 4
                    self.log(nl+'Process {0} finished with warnings.'.format(processId))
                elif process["STATUS"] == 12:
                    #process.RC == 12
                    self.log(nl+'Process {0} failed.'.format(processId))
                elif process["STATUS"] == 13:
                    #process.RC == 12
                    self.log(nl+'Process {0} aborted.'.format(processId))
                running = False

            time.sleep(2)
            process = self.get_process_info(processId)
        return process

    def _run_install(self, containerId, type, installOptions):
        #self.log("Installation starts.")
        #installOptions = self._get_and_validate_options_product_archive(self.command_options)
        #self.log("Options: " + json.dumps(installOptions))
        params = {
            'operation': 'installPV' if (type == "product") else 'registerProduct' if (type == "productMeta") else 'installSCV',
            'containerId': containerId,
            "options": installOptions
        }
        if type == "product" or type == "productMeta":
            instances = self._parse_selected_instances(self.command_options.instances)
            #self.log("Instances: " + json.dumps(instances))
            params["instances"] = instances

        (code, res, hdrs) = self.conn.request(self.ctx.Constants.xslm_pe_installation_service, body=json.dumps(params), method="POST")
        self.ctx.utils.tryReadWebError(code, res, hdrs)
        result = self.ctx.utils.readJson(res, hdrs)
        #self.log("Result: " + json.dumps(result))
        process = self.monitor_process(result["processId"])
        rc = process["RC"]
        self.fileLogger.log("RC: " + str(rc))

        if rc==0 or rc==4:
            return self.ctx.Constants.EXIT_OK
        else:
            return self.ctx.Constants.EXIT_IMPORT_ERROR


    def _delete_container(self, containerId):
        if containerId == "":
            return
        self.fileLogger.log("Invoking operation 'deleteContainer'.")
        params = {
            'operation': 'deleteContainer',
            'containerId': containerId
        }
        (code, res, hdrs) = self.conn.request(self.ctx.Constants.xslm_pe_installation_service, body=json.dumps(params), method="POST")
        self.ctx.utils.tryReadWebError(code, res, hdrs)
        self.fileLogger.log("Successfully deleted container {0}.".format(containerId))

    # -------------------------------------------------------------------------
    # Command line parsing code
    # -------------------------------------------------------------------------

    def _parse_selected_instances(self, instances):
        if instances == None or instances == "":
            return []
        nums = instances.split(",")
        try:
            return list(map(int,nums))
        except ValueError:
            raise self.ctx.utils.UserReportableException("Unable to parse selected instance list. Invalid number {0}.".format(instances))


    def _get_and_validate_options_product_archive(self, options):
        def checkIsSet(option):
            for o in parsedOptions:
                if o == option:
                    return True
            return False

        parsedOptions = {}
        if options.options == None:
            return parsedOptions
        allOptions = options.options.split(",")
        for o in allOptions:
            if o in PRODUCT_ARCHIVE_OPTIONS:
                parsedOptions[o] = True
            else:
                raise self.ctx.utils.UserReportableException("Unknown installation option {0}.".format(o))

        if checkIsSet(ALLOW_DU_DOWNGRADE) and checkIsSet(ALLOW_KEEP_DU_NEWER_VERSION):
            raise self.ctx.utils.UserReportableException("The installation options {0} and {1} cannot be used together.".format(ALLOW_DU_DOWNGRADE, ALLOW_KEEP_DU_NEWER_VERSION))

        return parsedOptions

    def is_duTgz(self, name):
        mf = self.get_du_meta(name)
        if mf == None:
            return False
        if b'delivery_unit' in mf:
            return True
        else:
            return False


    def get_du_files(self, archives):
        allDus = []
        for archive in archives:
            if self.ctx.utils.isDuTgzFile(archive):
                if self.is_duTgz(archive):
                    allDus.append(archive)
                else:
                    raise self.ctx.utils.UserReportableException("File {0} does not appear to be a delivery unit file.".format(archive))
            elif os.path.isdir(archive):
                candidates = self.findFile(archive, ["*.tgz"])
                for candidate in candidates:
                    if self.is_duTgz(candidate):
                        allDus.append(candidate)
                    else:
                        raise self.ctx.utils.UserReportableException("File {0} does not appear to be a delivery unit file.".format(candidate))

        return allDus


    # -------------------------------------------------------------------------
    # SCV deployment specific files
    # -------------------------------------------------------------------------

    @staticmethod
    def get_readable_update_decription(report):
        # for SCVs
        text = ""
        for scv in report:
            text += ("Name:          {0}" +nl).format(scv.get("DELIVERY_UNIT"))
            text += ("Vendor:        {0}" +nl).format(scv.get("VENDOR"))
            text += ("Version:       {0}" +nl).format(scv.get("VERSION", ""))
            text += ("SP Version:    {0}" +nl).format(scv.get("VERSION_SP", ""))
            text += ("Patch Version: {0}" +nl).format(scv.get("VERSION_PATCH", ""))
            text += ("Description:   {0}" + nl).format(scv.get("CAPTION",  ""))

            action = scv.get("ACTION")
            if action == "import":
                text += "Delivery unit will be imported." + nl
            elif action == "update":
                text += "Delivery unit will be updated. Installed version:" + nl
                text += ("Version:       {0}" +nl).format(scv.get("CURRENT_VERSION", ""))
                text += ("SP version:    {0}" +nl).format(scv.get("CURRENT_VERSION_SP", ""))
                text += ("Patch Version: {0}" +nl).format(scv.get("CURRENT_VERSION_PATCH", ""))
            elif action == "error":
                info = scv.get("INFO")
                text += "Error: " + info[0].format(*info[1:]) + nl
                if scv.get("CURRENT_VERSION", None) != None:
                    text += "Installed version:" +nl
                    text += ("Version:       {0}" +nl).format(scv.get("CURRENT_VERSION", ""))
                    text += ("SP version:    {0}" +nl).format(scv.get("CURRENT_VERSION_SP", ""))
                    text += ("Patch Version: {0}" +nl).format(scv.get("CURRENT_VERSION_PATCH", ""))
            text += nl
        return text

    @staticmethod
    def get_readable_scv_decription(report):
        text = "" +nl
        text += "The following software components have been found:" +nl+nl
        for key in report:
            scv = report.get(key).get("DU")
            text += ("Name:          {0}" +nl).format(scv.get("DELIVERY_UNIT"))
            text += ("Vendor:        {0}" +nl).format(scv.get("VENDOR"))
            text += ("Version:       {0}" +nl).format(scv.get("VERSION",""))
            text += ("SP Version:    {0}" +nl).format(scv.get("VERSION_SP",""))
            text += ("Patch Version: {0}" +nl).format(scv.get("VERSION_PATCH",""))
            text += ("Description:   {0}" + nl).format(scv.get("CAPTION", ""))
            text += nl
        return text

    @staticmethod
    def get_readable_du_decription(duManifests):
        text = ""
        for slManifest in duManifests:
            text += ("Name:          {0}" +nl).format(slManifest[b'delivery_unit'].decode())
            text += ("Vendor:        {0}" +nl).format(slManifest[b'vendor'].decode())
            text += ("Version:       {0}" +nl).format(slManifest.get(b'delivery_unit_version', '').decode())
            text += ("SP Version:    {0}" +nl).format(slManifest.get(b'version_sp', '').decode())
            text += ("Patch Version: {0}" +nl).format(slManifest.get(b'version_patch', '').decode())
            text += ("Type:          {0}" +nl).format(slManifest.get(b'export_type_as_string', '').decode())
            captionCombined = slManifest.get(b'caption', '').decode()
            captionSplitted = captionCombined.split(duCaptionSeparator)
            text += ("Description:   {0}" + nl).format(captionSplitted[0])
            if len(captionSplitted) > 1:
                text += ("SAP Note       {0}" +nl).format(captionSplitted[1])
            text += nl
        return text


    def unpack_and_validate_du_archives(self, archives):
        """Reads delivery units """

        meta_inf = []
        allDus = self.get_du_files(archives)

        if len(allDus) == 0:
            raise self.ctx.utils.UserReportableException("No files to import")

        for du in allDus:
            duManifest = self.get_du_meta(du)
            (dirname, filename) = os.path.split(du)
            scv = {
                "dirname": dirname,
                "duTgz": os.path.join(dirname, filename),
                "duManifest": duManifest
            }
            meta_inf.append(scv)
        return meta_inf

    def isLangDu(self, du):
        slManifest = du["duManifest"];
        if slManifest[b'export_type'] == "4":
            return True

    def get_archives_for_installation(self, dus):
        tgzArchives = []
        languageArchives = []
        for du in dus:
            if self.isLangDu(du):
                languageArchives.append(du["duTgz"])
            else:
                tgzArchives.append(du["duTgz"])

        return (tgzArchives, languageArchives)

    def check_duplicate_dus(self, dus):
        def getDuName(du):
            slManifest = du["duManifest"];
            if self.isLangDu(du):
                return "{0} ({1}) (language)".format(slManifest[b'delivery_unit'].decode(), slManifest[b'vendor'].decode())
            else:
                return "{0} ({1})".format(slManifest[b'delivery_unit'].decode(), slManifest[b'vendor'].decode())

        for du1 in dus:
            duCount = 0
            duName1 = getDuName(du1)
            for du2 in dus:
                duName2 = getDuName(du2)
                if duName1 == duName2:
                    duCount = duCount + 1
                    if duCount == 2:
                        msg = "Error: At least two archives for the du {0} provided: {1}, {2}.".format(duName1, du1["duTgz"], du2["duTgz"])
                        raise self.ctx.utils.UserReportableException(msg)

    def run_import_du(self, archives):
        dus = self.unpack_and_validate_du_archives(archives)
        duManifests = [x.get("duManifest") for x in dus]
        self.log(self.get_readable_du_decription(duManifests))
        #check duplicates
        self.check_duplicate_dus(dus)

        if not self.options.yes:
            pi_input = input("Continue with the specified operation? [yes/NO] -->")
            if not (pi_input.lower() == 'yes' or pi_input.lower() == 'y'):
                return self.ctx.Constants.EXIT_OK

        (duTgz, langTgz) = self.get_archives_for_installation(dus)
        self.ctx.utils.log_tgz_import_files(self.myctx, duTgz)
        self.ctx.utils.log_lang_tgz_import_files(self.myctx, langTgz)

        duImportReturn = self.ctx.utils.importArchives(self.myctx, duTgz)
        self.ctx.utils.importLanguageArchives(self.myctx, langTgz)

        if duImportReturn != 0:
            return self.ctx.Constants.EXIT_IMPORT_ERROR
        else:
            return self.ctx.Constants.EXIT_OK


    def run_display_du(self, archives):
        dus = self.unpack_and_validate_du_archives(archives)
        duManifests = [x.get("duManifest") for x in dus]
        self.stdout.write(self.get_readable_du_decription(duManifests))
        return self.ctx.Constants.EXIT_OK


    def _get_and_validate_options_scv(self, options):
        parsedOptions = {}
        if options.options == None:
            return parsedOptions
        allOptions = options.options.split(",")
        for o in allOptions:
            if o in SCV_ARCHIVE_OPTIONS:
                parsedOptions[o] = True
            else:
                raise self.ctx.utils.UserReportableException("Unknown installation option {0}.".format(o))
        return parsedOptions


    def _get_and_validate_options_pm(self, options):
        parsedOptions = {}
        if options.options == None:
            return parsedOptions
        allOptions = options.options.split(",")
        for o in allOptions:
            if o in PM_OPTIONS:
                parsedOptions[o] = True
            else:
                raise self.ctx.utils.UserReportableException("Not supported product registration option {0}.".format(o))
        return parsedOptions


    def _get_and_validate_options_du(self, options):
        parsedOptions = {}
        if options.options == None:
            return parsedOptions
        allOptions = options.options.split(",")
        for o in allOptions:
            if o in DU_IMPORT_OPTIONS:
                parsedOptions[o] = True
            else:
                raise self.ctx.utils.UserReportableException("Unknown installation option {0}.".format(o))
        return parsedOptions

    def log_options(self):
        msg = self.ctx.utils.log_command_options()
        self.fileLogger.log(msg)

        if '\\x' in msg:
            self.ctx.utils.stdout.write(nl)
            self.log("Warning: unicode characters in input. Some options could be ignored" + nl)
            self.ctx.utils.stdout.write(msg + nl)
            self.ctx.utils.stdout.write(nl)


    # -------------------------------------------------------------------------
    # plug-in entry point
    # -------------------------------------------------------------------------

    def run_command(self, options, raw_command_options):

        return self.run_command2(options, raw_command_options, [])


    def run_command2(self, options, raw_command_options, deploymentTypes):

        exitCode = self.ctx.Constants.EXIT_OK
        containerId = ""

        if len(raw_command_options) == 0:
            raise self.ctx.utils.UserReportableException("No archives or directory paths provided.")

        parser = self.ctx.HALMOptionParser(add_help_option=False)
        parser.disable_interspersed_args()
        parser.add_option("-d", "--display", action="store_true", dest="display")
        parser.add_option("-o", "--option", action="store", type="string", dest="options")
        parser.add_option("-l", "--log", action="store", type="string", dest="log")
        parser.add_option("--instances", action="store", type="string", dest="instances")
        (command_options, archives) = parser.parse_args(args=raw_command_options)

        if len(archives) == 0:
            raise self.ctx.utils.UserReportableException("No archived provided.")

        try:
            self.init2(options, command_options, nolog=True)

            # check import du case
            if len(deploymentTypes) > 0 and deploymentTypes[0] == install.DEPLOY_TYPE_DU:
                if command_options.display:
                    exitCode = self.run_display_du(archives)
                else:
                    self._get_and_validate_options_du(command_options)
                    self.turn_on_logging()
                    self.log_options()
                    exitCode = self.run_import_du(archives)
                # stop here
                raise self.ctx.utils.UserCancelException("")

            if len(deploymentTypes) > 0 and deploymentTypes[0] == install.INSTALL_TYPE_PM:
                deplTypePM = True
            else:
                deplTypePM = False

            files = self._get_files(archives, deplTypePM)
            if len(files) == 0:
                if deplTypePM == False:
                    raise self.ctx.utils.UserReportableException("Unable to find a suitable archive or multiple types of archives.")
                else:
                    raise self.ctx.utils.UserReportableException("Unable to find suitable product meta data descriptors.")

            self.turn_on_logging()

            # upload and pre-install
            uploadInfo = self._upload_files(files)
            containerId = uploadInfo.get('containerId')
            deploymentType = uploadInfo.get('containerType')

            # get container meta data
            contMetaData = self._get_container_meta(containerId)

            if deploymentType == install.INSTALL_TYPE_PV:
                if command_options.display:
                    actionText = self.get_readable_product_representation(contMetaData)
                    self.log(actionText)
                else:
                    self.log_options()
                    pvOptions = self._get_and_validate_options_product_archive(command_options)
                    evaluationResult = self._run_container_validation(containerId)
                    exitCode = self._run_install_pv(contMetaData, evaluationResult, containerId, pvOptions)

            elif deploymentType == install.INSTALL_TYPE_SCVS or deploymentType == install.INSTALL_TYPE_SCV:
                if command_options.display:
                    actionText = self.get_readable_scv_decription(contMetaData)
                    self.log(actionText)
                else:
                    self.log_options()
                    scvOptions = self._get_and_validate_options_scv(command_options)
                    evaluationResult = self._run_container_validation(containerId)
                    exitCode = self._run_install_scvs(contMetaData, evaluationResult, containerId, scvOptions)

            elif deploymentType == install.INSTALL_TYPE_PM:
                if command_options.display:
                    instances = self._parse_selected_instances(self.command_options.instances)
                    actionText = self.get_readable_product_representation(contMetaData)
                    self.log(actionText)
                else:
                    self.log_options()
                    pmOptions = self._get_and_validate_options_pm(command_options)
                    exitCode = self._run_register_pm(contMetaData, containerId, pmOptions)

        # make sure exception errors appears in the log
        except self.ctx.utils.UserReportableException as e:
            if (e.__str__ != None):
                self.stderr.write(e.__str__() + nl)
                self.fileLogger.log(e.__str__())
            exitCode = self.ctx.Constants.EXIT_ERROR
        except self.ctx.utils.UserCancelException:
            # used by the application to exit gracefully and clean up below
            pass
        except KeyboardInterrupt:
            self.stderr.write("Process interrupted by user"+nl)
            self.fileLogger.log("Process interrupted by user")
            exitCode = self.ctx.Constants.EXIT_ERROR
        except:
            self.stderr.write("Unknown error:" + nl + traceback.format_exc() + nl)
            self.fileLogger.log("Unknown error:" + nl + traceback.format_exc())
            exitCode = self.ctx.Constants.EXIT_ERROR
        finally:
            try:
                self._delete_container(containerId);
            except:
                self.stderr.write("Could not delete container")
                exitCode = self.ctx.Constants.EXIT_ERROR

            try:
                if isinstance(self.fileLogger, self.ctx.FileLogger):
                    self.fileLogger.close()
                    self.stdout.write('Log file written to ' + self.fileLogger.get_log_filename() + nl)
            except:
                self.stderr.write("Could not close file logger")
                exitCode = self.ctx.Constants.EXIT_ERROR

            try:
                self._cleanUp()
            except:
                self.stderr.write("Cleanup failed")
                exitCode = self.ctx.Constants.EXIT_ERROR

        logging.debug("Finished with return code " + str(exitCode))
        sys.exit(exitCode)

op = getattr(install, 'command_name', None)
if callable(op):
  command_name = install.command_name()
else:
  command_name = "install"
halm_commands[command_name] = {'class': "install", 'pos': 1}

# ------------------------------------------------------------------------
# File: assemble.py
# ------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Implementation of the hdbalm assemble command.
#
# ---------------------------------------------------------------------------

import sys
import os
import shutil
import xml.etree.ElementTree as ET
import json
import copy
import re
import zipfile
import tempfile
import traceback
import time
import io

nl = "\n"

class assemble:

    PD_XML = "PD.XML"
    STACK_XML = "STACK.XML"

    PRODUCT_BUILD_DIR = "product"
    SCV_BUILD_DIR = "scvs"
    PPMS_DU_NAME = "HANA_XS_LM_PPMS"
    PPMS_DU_VENDOR = "sap.com"

    class MyCtx:
        pass

    xslm_assemble = '/sap/hana/xs/lm/assembly/assemble.xsjs'


    def __init__(self, context):
        self.ctx = context
        self.fileLogger = self.ctx.NullFileLogger()
        self.stdout = self.ctx.utils.stdout
        self.stderr = self.ctx.utils.stderr

    # initialize after http connection information is available
    #
    def init2(self, options, command_options):

        self.command_options = command_options
        self.sapcarExe = self.ctx.utils.get_sapcar_path()
        self.regiExe = self.ctx.utils.get_regi_path()
        self.regiVersion = self.ctx.utils.get_regi_version(self.regiExe)
        self.ctx.utils.check_regi_version(self.regiVersion)
        self.options = options

        self.conn = self.ctx.HttpConnection.initialize_connection(options)
        self.ctx.utils.hasMinimalVersion("1.3.0",
                                         self.conn.getPingReply()["halm_version"],
                                         "At least HALM Version 1.3.0 is required to run this command")

        if command_options.ppms:
            # need at least 1.3.9 of HALM
            self.ctx.utils.hasMinimalVersion(
                      "1.3.9",
                      self.conn.getPingReply()["halm_version"],
                      "Assembly with PPMS requires at least version 1.3.9 of the HANA_XS_LM delivery unit")

        # find out whether to use the ppms or ppms2 location
        try:
            self.ctx.utils.hasMinimalVersion(
                      "1.3.14",
                      self.conn.getPingReply()["halm_version"])
            self.PPMS_SERVICE_URL = self.ctx.Constants.PPMS_SERVICE_URL_FROM_TO_1_3_14
        except:
            self.PPMS_SERVICE_URL = self.ctx.Constants.PPMS_SERVICE_URL_UP_TO_1_3_13

        self.can_do_ppms_assembly()
        self.jdbcPort= self.ctx.utils.getJDBCProtfromHTTPPort(self.conn.port, self.conn)
        self.tmpDir = tempfile.mkdtemp()

        if command_options.log:
            self.fileLogger = self.ctx.FileLogger(self.ctx, fileName=command_options.log)
        else:
            self.fileLogger = self.ctx.FileLogger(self.ctx, namePrefix="hdbalm-assemble")

        self.myctx = assemble.MyCtx()
        self.myctx.sapcarExe = self.sapcarExe
        self.myctx.regiExe = self.regiExe
        self.myctx.regiVersion = self.regiVersion
        self.myctx.fileLogger = self.fileLogger
        self.myctx.jdbcPort = self.jdbcPort
        self.myctx.host = self.conn.host
        self.myctx.user = self.conn.user
        self.myctx.password = self.conn.password
        self.myctx.conn = self.conn
        self.myctx.command_options = self.command_options

        if options.tunnel:
            self.myctx.tunnel = options.tunnel
        else:
            self.myctx.tunnel = None

        self.ctx.utils.log_environment(self.myctx)
        self.ctx.utils.log_server_info(self.myctx, self.conn)

    def can_do_ppms_assembly(self):
        if self.command_options.ppms:
            parameters = {
              "operation": "existsDeliveryUnit",
              "name": self.PPMS_DU_NAME,
              "vendor": self.PPMS_DU_VENDOR
            }
            result = self.ctx.utils.json_request(self.conn, self.ctx.Constants.xslm_service, parameters=parameters)
            if result.get('result', False) == False:
                raise self.ctx.utils.UserReportableException("product assembly with PPMS is not available for this system".format(self.PPMS_DU_NAME))

    # -------------------------------------------------------------------------
    # determine version and sp numbers for nexus friendly names
    # -------------------------------------------------------------------------


    def _get_version_and_version_sp_number(self, productMeta):
        """ Return nexus friendly version for product"""

        # product meta can be in the format a.b.c. We take a if it is a
        # number and ignore the rest
        #
        version = productMeta.get("VERSION", "")
        if version == "":
            version = "0"
        else:
            m = re.search(r"^(\d+)", version)
            if not m:
                raise self.ctx.utils.UserReportableException("Version number {0} is not a valid version number. It must start with one or multiple digits.".format(productMeta.get("VERSION")))
            version = str(int(m.group(1)))

        version_sp = productMeta.get("VERSION_SP")
        if version_sp == "":
            version_sp = "0"
        else:
            version_sp = str(int(version_sp))

        if self.command_options.product_patch_level != None:
            version_patch = self.command_options.product_patch_level
            m = re.search(r"^(\d+)", version_patch)
            if not m:
                raise self.ctx.utils.UserReportableException("Provided product patch level {0} is not a number".format(version_patch))
            version_path = str(int(version_patch))
        else:
            version_patch = "0"

        return version + "." + version_sp + "." + version_patch


    @staticmethod
    def description():
        return "Assembles SAP HANA products and software components"

    @staticmethod
    def help():
        return """
    SAP HANA Add-On Product and Software Component Assembly

    This command assembles SAP HANA add-on products and software components.

    usage : hdbalm assemble [<command options>] [<name>,<vendor>]+

    The vendor can be omitted if the name is unique in the system.

    The assemble command supports the following options:

      -d <directory>, --directory=<directory>
        Specifies an alternate location for the assembled product files and
        software component files.

      -l <file name>, --log=<file name>
        Sets an alternate location for the log file

      --languages <languages>
        Comma-separated list of language codes exported for the software
        components.

      --ignore_language_errors
        Ignores errors if languages are inconsistently configured for a
        delivery unit. No language delivery unit is exported.

      --overwrite
        Overwrites archives if they exist in the file system.

      --timestamp
        Adds a timestamp to the archive file name to distinguish different
        assembly builds.

      --products_only
        Assembles only product archives. This might be required if a product and
        a delivery unit have the same name in the system.

      --scvs_only
        Assembles only software components. This might be required if a product
        and a software component have the same name in the system.

      --alias <alias>
        Set alias to anonymize the export.

      --export_version <export version>
        An export format can be specified here to make the file format compatible
        with older SAP HANA versions.

    Examples:

    This command assembles the product "SAP APO ANALYTICS" and writes the product
    archive to the local directory.

      hdbalm assemble "SAP APO ANALYTICS"

    This command assembles the product "SAP APO ANALYTICS" of the vendor "sap.com"
    and writes the product archive to the directory c:\products.

      hdbalm assemble -d c:\products "SAP APO ANALYTICS",sap.com
     """

    def log(self, msg):
        self.stdout.write(msg+nl)
        self.fileLogger.log(msg)

    # -------------------------------------------------------------------------
    # User interaction section
    # -------------------------------------------------------------------------

    @staticmethod
    def get_readable_product_output(data):
        text = ""
        text += "------------------------------------------------------------" +nl
        text += "Running Product Assembly Process" +nl +nl
        text += "Product  :\t"+data.get("NAME") +nl
        text += "Vendor   :\t"+data.get("VENDOR") +nl
        text += "Version  :\t"+data.get("VERSION","") +nl
        text += "Caption  :\t"+data.get("CAPTION","") +nl
        text += "Stack    :\t"+data.get("SP_STACK_CAPTION","") +nl
        text += "------------------------------------------------------------" +nl
        return text

    def get_readable_scv_output(self, du):
        text = ""
        text += "------------------------------------------------------------" +nl
        text += "Running Software Component Assembly Process" +nl +nl
        text += self.ctx.install.get_readable_du_representation(du)
        text += "------------------------------------------------------------" +nl
        return text

    @staticmethod
    def _get_readable_list_of_components(components):
        text = ''
        text += nl
        text += "The following products and software components will be assembled" + nl
        text += "----------------------------------------------------------------" + nl
        for comp in components:
            if "NAME" in comp:
                text += "Product:            {0} ({1})".format(comp["NAME"], comp["VENDOR"]) + nl
            else:
                text += "Software component: {0} ({1})".format(comp["DELIVERY_UNIT"], comp["VENDOR"]) + nl
        text += nl +nl
        return text

    # -------------------------------------------------------------------------
    # remote HALM communication
    # -------------------------------------------------------------------------

    def _find_product_or_delivery_unit(self, name, vendor):
        params = {
            'operation': 'findProductOrDeliveryUnit',
            'name': name,
        }
        if vendor != None:
            params["vendor"] = vendor
        return self.ctx.utils.json_request(self.conn, self.ctx.Constants.xslm_service, parameters=params)

    def get_product(self, name, vendor):
        params = {
            'operation': 'getProduct2Tree',
            'name': name,
            'vendor': vendor
        }
        return self.ctx.utils.json_request(self.conn, self.ctx.Constants.xslm_service, parameters=params)

    def _get_delivery_unit(self, name, vendor):
        params = {
            'operation': 'getDeliveryUnit',
            'name': name,
            'vendor': vendor
        }
        return self.ctx.utils.json_request(self.conn, self.ctx.Constants.xslm_service, parameters=params)

    def calculateAdaptionsForPPMSSynchChange(self, name, vendor, spStackIdForProduct):
        params = {
          'operation' : 'calculateAdaptionsForPPMSSynchChange',
          'name': name,
          'vendor': vendor,
          'instanceId': 1,
          'SP_STACK_ID': spStackIdForProduct,
          'client' :'py',
          'create' : 'no'
        }
        return self.ctx.utils.json_request(self.conn, self.PPMS_SERVICE_URL, method='POST', parameters=params, body = "data")

    def VALIDATE_STACK_PD(self, name, vendor, ppms):
        ppmsFlag = 'yes' if ppms else 'no'
        params = {
          'cmd' : 'VALIDATE_STACK_PD',
          'name' : name,
          'vendor' : vendor,
          'usePPMS' : ppmsFlag,
        }
        (code, res, hdrs) = self.conn.request(self.xslm_assemble, method='GET', parameters=params)
        self.ctx.utils.tryReadWebError(code, res, hdrs)

    def VALIDATE_PD_MANIFEST(self, productName, duName, vendor, ppms):
        ppmsFlag = 'yes' if ppms else 'no'
        params = {
          'cmd' : 'VALIDATE_PD_MANIFEST',
          'pvName': productName,
          'duName': duName,
          'vendor': vendor,
          'usePPMS' : True,
        }
        (code, res, hdrs) = self.conn.request(self.xslm_assemble, method='GET', parameters=params)
        self.ctx.utils.tryReadWebError(code, res, hdrs)

    def MAKE_PD(self, name, vendor, ppms):
        ppmsFlag = 'yes' if ppms else 'no'
        params = {
          'cmd': 'MAKE_PD',
          'name': name,
          'vendor': vendor,
          'usePPMS' : ppmsFlag,
        }
        (code, res, hdrs) = self.conn.request(self.xslm_assemble, parameters=params)
        self.ctx.utils.tryReadWebError(code, res, hdrs)
        return self.ctx.utils.readXml(res, hdrs)

    def MAKE_STACK(self, name, vendor, ppms):
        ppmsFlag = 'yes' if ppms else 'no'
        params = {
          'cmd': 'MAKE_STACK',
          'name': name,
          'vendor': vendor,
          'usePPMS' : ppmsFlag,
        }
        (code, res, hdrs) = self.conn.request(self.xslm_assemble, parameters=params)
        self.ctx.utils.tryReadWebError(code, res, hdrs)
        return self.ctx.utils.readXml(res, hdrs)

    def MAKE_SL_MANIFEST(self, duName, vendor, ppms):
        ppmsFlag = 'yes' if ppms else 'no'
        headers = {
                'Content-type': 'application/x-compressed',
                'Accept': '*/*',
                  }
        params = {
          'cmd': 'MAKE_SL_MANIFEST',
          'name': duName,
          'vendor': vendor,
          'usePPMS' : ppmsFlag,
        }
        (code, res, hdrs) = self.conn.request(self.xslm_assemble, parameters=params,headers=headers)
        self.ctx.utils.tryReadWebError(code, res, hdrs)
        return self.ctx.utils.readXml(res, hdrs)

    def validate_product_in_sync(self, name, vendor):

        # This was only needed for checking whehter the generated stack.xml
        # contains an archives element. This is not needed anymore as we ignore
        # this element.
        #
        #if not self.command_options.weak_checks:
        #  strict_mode = "true"
        #else:
        #  strict_mode = "false"
        strict_mode = "false"

        parameters = {
          "operation": "calculateAdaptionsForPPMSSynchChange",
          "name": name,
          "vendor": vendor,
          "strict_mode": strict_mode
        }
        result = self.ctx.utils.json_request(self.conn, self.PPMS_SERVICE_URL, method='POST', parameters=parameters)

        for i in list(result.keys()):
            instructions = result.get(i)
            if len(instructions) > 0:
                text = result.get("text", None)
                if text != None:
                    msg = "Product {0} ({1}) does not match modeled product in PPMS.".format(name, vendor) + nl +\
                         "The following changes would have to be applied to the product and/or delivery units:" + nl + nl
                    for t in text:
                        msg += t + nl
                    raise self.ctx.utils.UserReportableException(msg)
                else:
                    raise self.ctx.utils.UserReportableException("Product {0} ({1}) does not match modeled product in PPMS".format(name, vendor))

    def validate_product_extra_checks(self, name, vendor):
        if self.command_options.weak_checks:
            self.log("Extra consistency checks have been turned off")
            return
        server_version = self.conn.getPingReply()["halm_version"]
        try:
            self.ctx.utils.hasMinimalVersion("1.3.11", server_version)
        except:
            # server version does not offer checks
            self.log("Version " + server_version + " of delivery unit HANA_XS_LM does not provide extra checks.")
            return

        parameters = {
          "operation": "validateProductExtraChecks",
          "name": name,
          "vendor": vendor
        }
        (code, res, hdrs) = self.conn.request(
                             self.PPMS_SERVICE_URL,
                             method='POST',
                             parameters=parameters)
        errObj = self.ctx.utils.tryReadErrorNoException3(code, res, hdrs)
        if errObj != None:
            text = ""
            if "messages" in errObj and len(errObj["messages"]) > 0:
                text += "Validation of product failed with the following errors:" + nl
                for m in errObj["messages"]:
                    if len(m) == 1:
                        text += m[0] + nl
                    else:
                        text += m[0].format(*m[1:]) + nl
                raise self.ctx.utils.UserReportableException(text)
            else:
                raise self.ctx.utils.UserReportableException(errObj.get("error", ""))

    def validate_and_get_sl_manifest(self, name, vendor):
        parameters = {
          "operation": "validateAndGetSLManifest",
          "name": name,
          "vendor": vendor
        }
        (code, res, hdrs) = self.conn.request(self.PPMS_SERVICE_URL, parameters=parameters, method='POST')
        self.ctx.utils.tryReadWebError(code, res, hdrs)
        return self.ctx.utils.readXml(res, hdrs)

    def get_product_model_and_descriptors(self, name, vendor):
        parameters = {
          "operation": "getProductModelAndDescriptors",
          "name": name,
          "vendor": vendor
        }
        return self.ctx.utils.json_request(self.conn, self.PPMS_SERVICE_URL, method='POST', parameters=parameters)

    # clean up temporary files
    #
    def cleanUp(self):
        if hasattr(self, 'tmpDir') and os.path.exists(self.tmpDir):
            try:
                shutil.rmtree(self.tmpDir)
            except Exception as e:
                self.stderr.write(("Unable to remove temporary directory {0}: {1}"+nl).format(self.tmpDir, str(e)))


    def _validate_directory_option(self, dirName):
        if dirName == None:
            return
        if os.path.isdir(dirName):
            return
        if os.path.exists(dirName):
            # not a directory but something else
            raise self.ctx.utils.UserReportableException("Directory parameter \"{0}\" points to an existing file".format(dirName))
        self.ctx.utils.make_dir(dirName)

    def _validate_export_version_option(self):
        if self.command_options.export_version != None:
            pingReply = self.conn.getPingReply()
            ef = pingReply.get("exportFormats", None)
            if ef != None:
                minVersion = int(ef.get("min"))
                maxVersion = int(ef.get("max"))
                if not minVersion <= self.command_options.export_version <=maxVersion:
                    raise self.ctx.utils.UserReportableException("Speficifed export format {0} not in range of supported export formats {1} and {2}".format(self.command_options.export_version, minVersion, maxVersion))
            else:
                raise self.ctx.utils.UserReportableException("Server does not no support different export format versions")


    def _get_product_archive_name(self, product, directory):

        archive_name = product.get('NAME')

        # replace spaces and dots
        archive_name = re.sub(r'\s', r'_', archive_name)
        archive_name = re.sub(r'\.', r'', archive_name)

        if self.command_options.nexus_friendly:
            version = self._get_version_and_version_sp_number(product)
            archive_name += "-" + version
        else:
            if product.get('VERSION', '') != '':
                archive_name += '_' + product.get('VERSION')
                archive_name = re.sub(r'\.', r'', archive_name)

        if self.command_options.timestamp:
            archive_name += "-" + str(int(round(time.time() * 1000)))

        archive_name += ".ZIP"

        if directory == None:
            return archive_name
        else:
            # existence of directory has been validated before
            return os.path.join(directory, archive_name)


    def _get_scv_archive_name(self, du, directory):

        archive_name = du.get("DELIVERY_UNIT")
        if self.command_options.nexus_friendly:
            try:
                ver = str(int(du.get("VERSION","0")))
            except ValueError as e:
                raise self.ctx.utils.UserReportableException("Unable to get DU version")

            try:
                sp_ver = str(int(du.get("VERSION_SP","0")))
            except ValueError as e:
                raise self.ctx.utils.UserReportableException("Unable to get DU SP version")

            try:
                patch_ver = str(int(du.get("VERSION_PATCH","0")))
            except ValueError as e:
                raise self.ctx.utils.UserReportableException("Unable to get DU patch version")

            version = ver + "." + sp_ver + "." + patch_ver
            archive_name += "-" + version
        else:
            if du.get("VERSION", None) != None:
                archive_name += "_" + du.get("VERSION")

            if self.command_options.timestamp:
                archive_name += "-" + str(int(round(time.time() * 1000)))

        archive_name += ".ZIP"
        if directory == None:
            return archive_name
        else:
            return os.path.join(directory, archive_name)


    def validate_archive_names(self, allDus, duArchiveNames):

        def raise_error():
            self.fileLogger.log("Stack file is inconsistent with installed product." + nl
                            + "Archive information from stack file: " + nl
                            + json.dumps(duArchiveNames, indent=4) +nl+nl
                            + "Product information from system: "+nl
                            + json.dumps(allDus, indent=4) +nl+nl)
            raise self.ctx.utils.UserReportableException("Stack file is inconsistent with installed product. Details have been writen to the log file.")

        if len(allDus) != len(duArchiveNames):
            raise_error()

        cloned = copy.deepcopy(duArchiveNames)
        cloned = [a for a in cloned if a["name"] + " (" + a["vendor"] + ")" not in allDus]
        if len(cloned) != 0:
            raise_error()

    @staticmethod
    def validate_and_parse_language_option(language):
        #todo
        return language


    def _can_assemble_product(self, product):
        '''Validates whether the product can be assembled'''

        def is_number(num):
            numpat = "^\d+$"
            if re.match(numpat, num) is None:
                return False
            else:
                return True

        errors = ""
        # check that all instances have delivery units assigned to them
        #
        instances = product.get("PRODUCT_INSTANCES",[])
        if len(instances) == 0:
            raise self.ctx.utils.UserReportableException("Product {0} ({1}) has no instances assigned to it".format(product["NAME"], product["VENDOR"]))
        for pi in instances:
            if len(pi.get("DELIVERY_UNITS",[])) == 0:
                errors += ('Product instance {0} of product {1} ({2}) has no delivery units assigned to it.' +nl).format(
                              pi.get("INSTANCE_ID",''),product["NAME"], product["VENDOR"])
            for du in pi["DELIVERY_UNITS"]:
                if not is_number(du["VERSION"]):
                    errors += ('Delivery unit {0} ({1}) version must be a number' + nl).format(du["DELIVERY_UNIT"], du["VENDOR"])
                if not is_number(du["VERSION_SP"]):
                    errors += ('Delivery unit {0} ({1}) version sp must be a number' + nl).format(du["DELIVERY_UNIT"], du["VENDOR"])
                if not is_number(du["VERSION_PATCH"]):
                    errors += ('Delivery unit {0} ({1}) version patch must be a number' + nl).format(du["DELIVERY_UNIT"], du["VENDOR"])

        # check that product has a valid version number
        #
        validVersion = "^[.\d]+$"
        v = product.get("VERSION", "")
        if v != "" and re.search(validVersion, v) == None:
            errors += "Not a valid version number for product {0}: {1}. Version numbers must only contain digits and dots.".format(product.get("NAME"), v) + nl

        if len(errors) > 0:
            raise self.ctx.utils.UserReportableException(errors)


    @staticmethod
    def _get_du_key(du):
        return du["DELIVERY_UNIT"] + " (" + du["VENDOR"] + ")"


    def get_assigned_dus(self, product):
        '''get all assigned dus for a product'''
        dus = {}
        instances = product.get("PRODUCT_INSTANCES",[])
        for pi in instances:
            for du in pi.get("DELIVERY_UNITS", []):
                name = self._get_du_key(du)
                dus[name] = du
        return dus


    def store_file(self, name, contents):
        try:
            fd = open(name,"w",encoding="UTF-8")
            fd.write(contents)
            self.fileLogger.log("File {0} written.".format(name))
        except IOError as io:
            if hasattr(io, "strerror"):
                raise self.ctx.utils.UserReportableException("Unable to write file {0}: {1}".format(name, io.strerror))
            else:
                raise self.ctx.utils.UserReportableException("Unable to write file {0}.".format(name))
        except:
            self.ctx.utils.UserReportableException("Unable to write file {0}.".format(name) + nl + traceback.format_exc())
        finally:
            fd.close()


    def create_build_directories(self, actx):
        product = actx["product"]
        actx["productRootDirname"] = (str(product["NAME"])+"_"+str(product["VERSION"])).replace(" ","_").rstrip('_')
        actx["productBuildDir"] = os.path.join(self.tmpDir, assemble.PRODUCT_BUILD_DIR)
        actx["productRootDir"] = os.path.join(actx["productBuildDir"], actx["productRootDirname"])
        actx["scvBuildDir"] = os.path.join(self.tmpDir, assemble.SCV_BUILD_DIR)

        self.ctx.utils.make_dir(actx["productBuildDir"])
        self.ctx.utils.make_dir(actx["productRootDir"])
        self.ctx.utils.make_dir(actx["scvBuildDir"])


    def build_scv(self, duArchive, actx):
        """Build single SCV"""
        archiveName = duArchive.get("archive")
        archiveDirName = os.path.splitext(archiveName)[0]
        path = os.path.join(actx["scvBuildDir"], archiveDirName)
        duArchive["builddir"] = path
        self.ctx.utils.make_dir(duArchive["builddir"])
        self._download_scv(duArchive, actx)
        if duArchive["archive"].lower().endswith('.zip'):
            self.make_zip(duArchive["builddir"], os.path.join(actx["productRootDir"], duArchive["archive"]))
        else:
            self.make_sar(duArchive["builddir"], os.path.join(actx["productRootDir"], duArchive["archive"]))

    def build_scvs(self, duArchives, actx):
        for duArchive in duArchives:
            self.build_scv(duArchive, actx)


    def _run_assembly(self, components):

        self.log(self._get_readable_list_of_components(components))
        for component in components:
            try:
                if "DELIVERY_UNIT" in component:
                    self._run_scv_assembly(component["DELIVERY_UNIT"], component["VENDOR"])
                else:
                    self._run_product_assembly(component["NAME"], component["VENDOR"])
            finally:
                if os.path.exists(self.tmpDir):
                    shutil.rmtree(self.tmpDir)


    def _run_scv_assembly(self, name, vendor):

        du = self._get_delivery_unit(name, vendor)
        self.log(self.get_readable_scv_output(du))
        (target_dir, archive_name) = os.path.split(self._get_scv_archive_name(du, self.command_options.directory))
        fullPath = os.path.join(target_dir, archive_name)
        self.log("Archive name will be {0}".format(fullPath))
        if os.path.exists(fullPath) and not self.command_options.overwrite:
            raise self.ctx.utils.UserReportableException("Software component archive file {0} already exists".format(fullPath))

        duArchive = {
          "name": name,
          "vendor": vendor,
          "archive": archive_name
        }

        actx = {}
        actx["scvBuildDir"] = os.path.join(self.tmpDir, "scv")
        key = self._get_du_key(du)
        actx["allDus"] = {
          key: du
        }
        actx["productRootDir"] = target_dir
        self.build_scv(duArchive, actx)
        self.log(nl+"Software component archive \"{0}\" successfully built".format(fullPath))
        self.stdout.write(nl+nl)

    def _fix_archive_names_and_sp_level(self, actx):

        def is_python_27():
            v = sys.version
            if v.startswith("2.7"):
                return True
            else:
                return False

        def get_du_info(product, du_name, du_vendor):
            for inst in product["PRODUCT_INSTANCES"]:
                for du in inst["DELIVERY_UNITS"]:
                    if du["DELIVERY_UNIT"] == du_name and du["VENDOR"] == du_vendor:
                        return du
            return None

        def get_archive_tag(sc_name, version, version_sp, version_patch):
            archive_name = sc_name + "-" + version + "." + version_sp + "." + version_patch + ".ZIP"
            arch = ET.Element('archives')
            os = ET.SubElement(arch, "os-set", attrib={"os": "OSINDEP"})
            db = ET.SubElement(os, "db-set", attrib={"db": "HDB"})
            an = ET.SubElement(db, "archive-name", attrib={"db":"HDB", "os":"OSINDEP", "patch_level": version_patch})
            an.text = archive_name
            return arch

        def get_element_text(element, tag):
            val = element.find(tag)
            if val == None:
                return ""
            return val.text

        def pretty_print(elem, level=0):
            indent = "\n" + level * "  "
            if len(elem):
                if not elem.text or not elem.text.strip():
                    elem.text = indent + "  "
                if not elem.tail or not elem.tail.strip():
                    elem.tail = indent
                for elem in elem:
                    pretty_print(elem, level+1)
                if not elem.tail or not elem.tail.strip():
                    elem.tail = indent
            else:
                if level and (not elem.tail or not elem.tail.strip()):
                    elem.tail = indent

        def fix_stack_xml(actx):
            inputxml = io.StringIO(actx["stackXML"])
            tree = ET.parse(inputxml)
            root = tree.getroot()
            softwareComponent = root.findall('sp-stack/sp-feature-stack/software-component')
            if len(softwareComponent) > 0 :
                for component in softwareComponent :
                    rt = component.find('runtime-type')
                    if rt is None or rt.text != 'HDB':
                        continue
                    name = get_element_text(component, 'name')
                    vendor = get_element_text(component, 'vendor')
                    du = get_du_info(actx["product"], name, vendor)

                    sp = component.find("support-package")
                    if sp is None:
                        error = "No support-package maintained for software component {0} in the stack xml file".format(name)
                        self.fileLogger.log(error)
                        self.fileLogger.log("The corresponding stack xml file is")
                        self.fileLogger.log(actx["stackXML"])
                        raise self.ctx.utils.UserReportableException(error)
                    pl = sp.find('patch-level')
                    if pl is None:
                        pl = ET.Element('patch-level')
                        pl.text = du["VERSION_PATCH"]
                        sp.append(pl)
                    else:
                        pl.text = du["VERSION_PATCH"]

                    archives = sp.find("archives")
                    if archives != None:
                        sp.remove(archives)
                    sp.append(get_archive_tag(name, du["VERSION"], du["VERSION_SP"], du["VERSION_PATCH"]))

            pretty_print(root)
            actx["stackXML"] = ET.tostring(root, encoding='utf8', method='xml').decode()

        def fix_scv_in_pd(actx, software_components):
            if len(software_components) > 0:
                for software_component in software_components:
                    name = software_component.find('{http://www.sap.com/lm_sl/pd}software-component-version-key/{http://www.sap.com/lm_sl/pd}name').text
                    vendor = software_component.find('{http://www.sap.com/lm_sl/pd}software-component-version-key/{http://www.sap.com/lm_sl/pd}vendor').text
                    du = get_du_info(actx["product"], name, vendor)
                    sp = software_component.find('{http://www.sap.com/lm_sl/pd}sp')
                    patch_level = sp.find('{http://www.sap.com/lm_sl/pd}patch-level')
                    if patch_level is not None:
                        patch_level.text = du["VERSION_PATCH"]
                    else:
                        pl = ET.Element('{http://www.sap.com/lm_sl/pd}patch-level')
                        pl.text = du["VERSION_PATCH"]
                        sp.append(pl)

        def fix_pd_xml(actx):
            inputxml = io.StringIO(actx["pdxml"])

            tree = ET.parse(inputxml)
            root = tree.getroot()
            software_components = root.findall('{http://www.sap.com/lm_sl/pd}product-instance/{http://www.sap.com/lm_sl/pd}software-component-version')
            fix_scv_in_pd(actx, software_components)

            # need to parse related product versions as well
            software_components = root.findall('{http://www.sap.com/lm_sl/pd}related-product-versions/{http://www.sap.com/lm_sl/pd}product-version/{http://www.sap.com/lm_sl/pd}product-instance/{http://www.sap.com/lm_sl/pd}software-component-version')
            fix_scv_in_pd(actx, software_components)

            pretty_print(root)
            actx["pdxml"] = ET.tostring(root, encoding='utf8', method='xml').decode()

        try:
            fix_stack_xml(actx)
            fix_pd_xml(actx)
        except ValueError as e:
            raise self.ctx.utils.UserReportableException('Error replacing archive and patch level information in stack- and product descriptor files: {0}'.format(str(e)))
        except TypeError as e:
            raise self.ctx.utils.UserReportableException('Error replacing archive and patch level information in stack- and product descriptor files: {0}'.format(str(e)))

    def _run_product_assembly(self, name, vendor):

        actx = {}
        actx["product"] = self.get_product(name, vendor)

        self.log(self.get_readable_product_output(actx["product"]))
        self._can_assemble_product(actx["product"])
        actx["productArchiveName"] = self._get_product_archive_name(actx["product"], self.command_options.directory)

        if os.path.exists(actx["productArchiveName"]) and not self.command_options.overwrite:
            raise self.ctx.utils.UserReportableException("Product archive file {0} already exists".format(actx["productArchiveName"]))

        self.log("The name of the product archive will be \"{0}\"".format(actx["productArchiveName"]))
        self.stdout.write(nl)

        self.create_build_directories(actx)

        if self.command_options.ppms:
            # validate that installed product is in sync with PPMS
            #
            self.fileLogger.log("Making PPMS validation and downloading descriptors.")
            self.validate_product_in_sync(name, vendor)
            self.fileLogger.log("Product is in sync with PPMS.")
            descriptors = self.get_product_model_and_descriptors(name, vendor)
            actx["pdxml"] = descriptors["pdxml"]
            actx["stackXML"] = descriptors["stackxml"]
            actx["sl_manifests"] = descriptors["sl_manifests"]

            self.stdout.write("downloaded stack xml" + nl)
            self.stdout.write(actx["stackXML"])
            # do some extra checks
            #
            self.validate_product_extra_checks(name, vendor)

        else:
            self.fileLogger.log("Downloading product descriptor.")
            actx["pdxml"] = self.MAKE_PD(name, vendor, self.command_options.ppms)

            self.fileLogger.log("Downloading stack xml.")
            actx["stackXML"] = self.MAKE_STACK(name, vendor, self.command_options.ppms)

        self._fix_archive_names_and_sp_level(actx)

        self.store_file(os.path.join(actx["productRootDir"], self.STACK_XML), actx["stackXML"])
        self.store_file(os.path.join(actx["productRootDir"], self.PD_XML), actx["pdxml"])

        # get the List of archive names
        actx["duArchiveNames"] = self._parseStackXmlGetDetails(actx["stackXML"])
        # get all scvs assigned to the product (no duplicates)
        actx["allDus"] = self.get_assigned_dus(actx["product"])
        self.validate_archive_names(actx["allDus"], actx["duArchiveNames"])

        # build scv archives
        self.build_scvs(actx["duArchiveNames"], actx)

        #pack the complete Product Version in Zip
        self.make_zip(actx["productRootDir"], actx["productArchiveName"])
        self.log(nl+"Product archive \"{0}\" successfully built".format(actx["productArchiveName"]))

        self.log("Assembly process completed." +nl)
        return self.ctx.Constants.EXIT_OK


    def _getExportLanguages(self, selectedLanguages, duName, vendor):

        params = {
              'operation' : 'getDeliveryUnitLanguages',
              'name': duName,
              'vendor': vendor,
            }
        (code, res, hdrs) = self.conn.request(self.ctx.Constants.xslm_service, parameters=params)
        error = self.ctx.utils.tryReadErrorNoException2(code, res, hdrs)
        if error != None:
            if self.command_options.ignore_language_errors:
                self.log("No languages delivery unit will be exported. Language configuration")
                self.log("appears to be inconsistent: {0}".format(error))
                return []
            else:
                raise self.ctx.utils.UserReportableException("unable to retrieve list of languages: {0}".format(error))
        lan = self.ctx.utils.readJson(res,hdrs)

        if selectedLanguages == None:
            return lan["translations"]
        else:
            duLangsNoCountry = [x[:2] for x in lan["translations"]]
            duLangs = set(duLangsNoCountry)
            langs = selectedLanguages.split(",")
            for l in langs:
                if len(l) != 2:
                    raise self.ctx.utils.UserReportableException("Invalid language code \"{0}\"".format(l))
                if not l in duLangs:
                    raise self.ctx.utils.UserReportableException("Language \"{0}\" is not maintained for du {1} ({2})".format(l, duName, vendor))
            return langs


    @staticmethod
    def remove_duplicate_archives(archives):

        archiveDict = {}
        def isDuplicate(archive):
            key = archive["name"] + " " + archive["vendor"] + " " + archive["archive"]
            if  key in archiveDict:
                return True
            archiveDict[key] = True
        return [x for x in archives if not isDuplicate(x)]


    def _parseStackXmlGetDetails(self, stackxml):
        stack = []
        try:
            root = ET.fromstring(stackxml)
            softwareComponent = root.findall('sp-stack/sp-feature-stack/software-component')

            if len(softwareComponent) > 0 :
                for component in softwareComponent :
                    rt = component.find('runtime-type')
                    if rt is None or rt.text != 'HDB':
                        continue
                    supportPackage = component.findall('support-package')
                    archiveName = None
                    if len(supportPackage)>0 :
                        archives = supportPackage[0].findall('archives/archive-name')
                        if len(archives) > 0:
                            archiveName = archives[0].text
                        else:
                            archives = supportPackage[0].findall('archives/os-set/db-set/archive-name')
                            if(len(archives)>0):
                                archiveName = archives[0].text
                            else:
                                archiveName = component.find('name').text+supportPackage[0].find('sp-level').text+"_"+supportPackage[0].find('patch-level').text+".ZIP"
                    else:
                        archiveName = component.find('name').text+".ZIP"

                    stack.append({
                       "archive": archiveName,
                       "name": component.find('name').text,
                       "vendor": component.find('vendor').text,
                       "ppmsid": component.find('ppms-number').text,
                       "version": component.find('version').text,
                    })
            else:
                raise self.ctx.utils.UserReportableException('No Software components found in Stack XML Entries')

        except AttributeError as e:
            raise self.ctx.utils.UserReportableException('RESULT: Assembly process failed.\nERROR in Stack XML: Attribute missing. '+str(e))
        except TypeError as e:
            raise self.ctx.utils.UserReportableException('RESULT: Assembly process failed.\nERROR in Stack XML: Type mismatch. '+str(e))
        except ET.ParseError as e:
            raise self.ctx.utils.UserReportableException('RESULT: Assembly process failed.\nERROR in Stack XML: {0}. '.format(str(e)))

        return assemble.remove_duplicate_archives(stack)


    def _download_scv(self, duArchiveName, actx):

        scvName = duArchiveName.get('name')
        scvVendor = duArchiveName.get('vendor')
        path = duArchiveName["builddir"]

        # a du is mentioned in the product descriptor but does not
        # exist
        key = self._get_du_key({"DELIVERY_UNIT": scvName, "VENDOR": scvVendor})
        if actx["allDus"].get(key) == None:
            raise self.ctx.utils.UserReportableException("Delivery unit {0} is specified in product descriptor but does not exist on the system or is not part of the product.".format(key))

        self.log("Processing the Delivery Unit {0} ({1}).".format(scvName, scvVendor))
        exportLanguages = self._getExportLanguages(self.command_options.languages, scvName, scvVendor)

        if self.command_options.ppms:
            # if this is a product assembly we have already downloaded the
            # sl manifest. If this is a scv assembly we need to download it
            # from PPMS now
            #
            if 'sl_manifests' in actx:
                key = scvName + " (" + scvVendor + ")"
                slManifest = actx['sl_manifests'][key]['SL_MANIFEST']
            else:
                slManifest = self.validate_and_get_sl_manifest(scvName, scvVendor)

        else:
            self.fileLogger.log("Downloading the sl manifest delivery unit {0} ({1})".format(scvName, scvVendor))
            slManifest = self.MAKE_SL_MANIFEST(scvName, scvVendor, self.command_options.ppms)

        metaInf = os.path.join(path,"META-INF")
        self.ctx.utils.make_dir(metaInf)
        sl_file = os.path.join(metaInf, "SL_MANIFEST.XML")
        self.store_file(sl_file, slManifest)

        scvFilename = os.path.join(path, scvName + ".tgz")
        ret = self.ctx.utils.exportArchive(self.myctx, scvName, scvVendor, scvFilename, alias=self.command_options.alias, exportVersion=self.command_options.export_version)
        if ret != 0:
            raise self.ctx.utils.UserReportableException("Failed to export delivery unit.")

        # language export
        if len(exportLanguages) > 0:
            self.log("Downloading language file for {0} ({1})".format(scvName, scvVendor))
            self.log("The following translations will be downloaded: {0}".format(", ".join(exportLanguages)))
            fileName = os.path.join(path, "LANG_"+scvName+".tgz")
            ret = self.ctx.utils.exportLanguageArchive(self.myctx, scvName, scvVendor, exportLanguages, fileName, alias=self.command_options.alias, exportVersion=self.command_options.export_version)
            if ret != 0:
                raise self.ctx.utils.UserReportableException("Failed to download the Delivery Unit langauge file.")


    # create a zip file including all files located in the
    # inputDirectory
    def make_zip(self, inputDirectory, archiveName):

        # create a Zip file for a given directory recursive.
        def makeZipFile(inputDir, outZipFile):

            for (dirname, _, files) in os.walk(inputDir):
                for f in files:
                    absfn = os.path.join(dirname, f)
                    zfn = absfn[len(inputDir)+len(os.sep):]
                    outZipFile.write(absfn,zfn)

        try:
            outputZip = None
            outputZip = zipfile.ZipFile(archiveName, mode="w")
            makeZipFile(inputDirectory,outputZip)
        except IOError as e:
            raise self.ctx.utils.UserReportableException("Cannot build product archive {0}: {1}".format(archiveName, e.strerror))
        except RuntimeError as e:
            raise self.ctx.utils.UserReportableException("Cannot build product archive {0}: {1}.".format(archiveName, str(e)))
        finally:
            if outputZip != None:
                outputZip.close()

    # create a sar file including all files located in the
    # inputDirectory
    def make_sar(self, inputDir, outSarFile):

        cmd = [self.sapcarExe, "-cf", '"' + outSarFile + '"', "-C", inputDir, "."]
        self.fileLogger.log(" ".join(cmd))
        (code, output) = self.ctx.utils.execute_external(cmd)
        if code != 0:
            raise self.ctx.utils.UserReportableException(output)

    def _get_components(self, candidates, options):
        """Get list of components (SCV/Product) as specified on the command line"""
        def filter_products_and_scvs(clist):
            if not self.command_options.products_only\
                  and not self.command_options.scvs_only:
                # no filtering
                return clist
            candidates = []
            for cand in clist:
                if self.command_options.products_only:
                    if cand.get("NAME", None) != None:
                        candidates.append(cand)
                if self.command_options.scvs_only:
                    if cand.get("DELIVERY_UNIT", None) != None:
                        candidates.append(cand)
            return candidates

        def get_with(clist, name, vendor):
            candidates = []
            for component in clist:
                if name == component.get("NAME", None)\
                    or name == component.get("DELIVERY_UNIT", None):
                    if vendor == None:
                        candidates.append(component)
                    else:
                        if vendor == component.get("VENDOR"):
                            candidates.append(component)
            return candidates

        components = []
        for candidate in candidates:
            parts = candidate.split(",")
            name = parts[0]
            if len(parts) == 2:
                vendor = parts[1]
            else:
                vendor = None
            cp = self._find_product_or_delivery_unit(name, vendor)
            filtered = filter_products_and_scvs(cp)
            filtered = get_with(filtered, name, vendor)
            if len(filtered) == 1:
                components.append(filtered[0])
                continue
            elif len(cp) > 1:
                raise self.ctx.utils.UserReportableException("Ambiguous product or software component name {0}".format(name))
            else:
                if options.scvs_only == True:
                    raise self.ctx.utils.UserReportableException("No software component {0} found".format(name))
                else:
                    raise self.ctx.utils.UserReportableException("No product or software component {0} found".format(name))
        return components

    def log_options(self):
        msg = self.ctx.utils.log_command_options()
        self.fileLogger.log(msg)

        if '\\x' in msg:
            self.ctx.utils.stdout.write(nl)
            self.log("Warning: unicode characters in input. Some options could be ignored" + nl)
            self.ctx.utils.stdout.write(msg + nl)
            self.ctx.utils.stdout.write(nl)

    def run_command(self, options, command_options):

        exitCode = self.ctx.Constants.EXIT_ERROR

        parser = self.ctx.HALMOptionParser(add_help_option=False)
        parser.disable_interspersed_args()
        parser.add_option("-d", "--directory", action="store", type="string", dest="directory")
        parser.add_option("--use_ppms", action="store_true", dest="ppms")
        parser.add_option("--languages", action="store", type="string", dest="languages")
        parser.add_option("--ignore_language_errors", action="store_true", dest="ignore_language_errors")
        parser.add_option("-l", "--log", action="store", type="string", dest="log")
        parser.add_option("--overwrite", action="store_true", dest="overwrite")
        parser.add_option("--timestamp", action="store_true", dest="timestamp")
        parser.add_option("--scvs_only", action="store_true", dest="scvs_only")
        parser.add_option("--products_only", action="store_true", dest="products_only")
        parser.add_option("--export_version", action="store", type="int", dest="export_version")
        parser.add_option("--generate_artifact_and_version", action="store_true", dest="artifact_version")
        parser.add_option("--nexus_friendly", action="store_true", dest="nexus_friendly")
        parser.add_option("--product_patch_level", action="store", type="string", dest="product_patch_level")
        parser.add_option("--use_regi", action="store_true", dest="use_regi")
        parser.add_option("--weak_checks", action="store_true", dest="weak_checks")
        parser.add_option("--alias", action="store", type="string", dest="alias")
        (command_options, assemble_names) = parser.parse_args(args=command_options)

        # DM 15.06.2015: this is too strict
        # if command_options.alias == "SAP" and not command_options.ppms:
        #   raise self.ctx.utils.UserReportableException('Alias "SAP" is reserved for SAP')

        if len(assemble_names) == 0:
            raise self.ctx.utils.UserReportableException("No products or components specified")
        self._validate_directory_option(command_options.directory)

        try:
            self.init2(options, command_options)
            self.log_options()
            self._validate_export_version_option()
            components_to_build = self._get_components(assemble_names, command_options)
            self._run_assembly(components_to_build)
            exitCode = self.ctx.Constants.EXIT_OK

        except self.ctx.utils.UserReportableException as e:
            self.log("Error: " + e.__str__())
            exitCode = self.ctx.Constants.EXIT_ERROR
        except self.ctx.utils.UserCancelException:
            # used by the application to exit gracefully and clean up below
            pass
        except KeyboardInterrupt:
            self.log("Process interrupted by user")
            exitCode = self.ctx.Constants.EXIT_ERROR
        except:
            self.log("Unknown error:" + nl + traceback.format_exc())
            exitCode = self.ctx.Constants.EXIT_ERROR
        finally:
            if isinstance(self.fileLogger, self.ctx.FileLogger):
                self.fileLogger.close()
                sys.stdout.write('Log file written to ' + self.fileLogger.get_log_filename() + nl)
            self.cleanUp()

        sys.exit(exitCode)

op = getattr(assemble, 'command_name', None)
if callable(op):
  command_name = assemble.command_name()
else:
  command_name = "assemble"
halm_commands[command_name] = {'class': "assemble", 'pos': 2}

# ------------------------------------------------------------------------
# File: importdu.py
# ------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Implementation of the hdbalm import command.
#
# This plug-in requires at least version 1.3.0 of HALM
# ---------------------------------------------------------------------------

import sys
import json
import re


class importdu:

    def __init__(self, context):
        self.ctx = context
        self.stdout = self.ctx.utils.stdout
        self.stderr = self.ctx.utils.stderr

    def description(self):
        return "Imports delivery units"

    def help(self):
        return """
    Import SAP HANA Delivery Units

    This command is used to import delivery units.

    usage: hdbalm import [<command option>]* [<du tgz>|<directory>]*

    The following command options are supported:

      -d, --display
        Displays the archive contents. No changes are applied to the system.

      -l <file name>, --log=<file name>
        Sets an alternate location for the log file

    Examples:

    This command imports delivery units mydu1 and mydu2.

      hdbalm import mydu1.tgz mydu2.tgz

    This command looks for delivery units in the c:\delivery_units directory
    and imports them.

      hdbalm import c:\delivery_units
    """

    @staticmethod
    def command_name():
        return "import"

    def run_command(self, options, raw_command_options):

        install = self.ctx.install(self.ctx)
        return install.run_command2(options, raw_command_options, [install.DEPLOY_TYPE_DU])

op = getattr(importdu, 'command_name', None)
if callable(op):
  command_name = importdu.command_name()
else:
  command_name = "importdu"
halm_commands[command_name] = {'class': "importdu", 'pos': 3}

# ------------------------------------------------------------------------
# File: register.py
# ------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Implementation of the hdbalm register command.
#
# This plug-in requires at least version 1.3.0 of HALM
# ---------------------------------------------------------------------------

import sys
import json
import re


class register:

    def __init__(self, context):
        self.ctx = context
        self.stdout = self.ctx.utils.stdout
        self.stderr = self.ctx.utils.stderr

    def description(self):
        return "Register product"

    def help(self):
        return """
    Register SAP HANA product

    This command is used to register products.

    usage: hdbalm register [<command option>]* <stack.xml file> [<pd.xml file>]

    The following command options are supported:

      -d, --display
        Displays the meta data files contents. No changes are applied to the system.

      -l <file name>, --log=<file name>
        Sets an alternate location for the log file

      -o <registration option>, --option <registration option>
        Registration option which can be used to override the default behavior of the
        register command. Multiple options can be specified by repeating the
        -o option. The following registration options are available:

          ALLOW_KEEP_DU_NEWER_VERSION - Allows to keep the component if it is
                                        already installed in a newer version

      --instances
        By default all instances will be registered. A comma-separated list of
        instances can be specified to limit the number of instances.


    Examples:

    This command registers a product:

      hdbalm register STACK.xml

    """

    @staticmethod
    def command_name():
        return "register"

    def run_command(self, options, raw_command_options):

        install = self.ctx.install(self.ctx)
        return install.run_command2(options, raw_command_options, [install.INSTALL_TYPE_PM])

op = getattr(register, 'command_name', None)
if callable(op):
  command_name = register.command_name()
else:
  command_name = "register"
halm_commands[command_name] = {'class': "register", 'pos': 4}

# ------------------------------------------------------------------------
# File: transport.py
# ------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# HANA Application Lifecycle Management Transport
# ---------------------------------------------------------------------------

import sys
import string

class transport:

    commands = frozenset(["list", "start"])

    def __init__(self, context):
        self.ctx = context

    def description(self):
        return "Manages transports"

    def help(self):
        return """
    Manage SAP HANA Transports

    usage: hdbalm transport <transport command>

    transport commands:
    -------------------

    list

      Lists available transport routes.

      Meaning of transport modes:
       "DUs"    : Complete Delivery Units
       "All CLs": All Changelists
       "Sel CLs": Selected Changelists

      usage:
      hdbalm transport list

    start

      Starts a transport operation on the given transport route.

      usage:
      hdbalm transport start <route id>
    """

    def print_action_log(self, conn, actionId):

        log = self.ctx.log(self.ctx)
        log.print_action_log(conn, actionId)

    def cmd_start(self, args, options, command_args):

        if len(command_args) != 1:
            raise self.ctx.utils.UserReportableException("Transport route id expected.")

        transportService = self.ctx.Constants.xslm_base + '/core/TransportService.xsjs'
        route_id = command_args[0]
        param = {"route_id": route_id}
        if options.type:
            if options.type == "full":
                param["delta"] = "false"
            elif options.type == "delta":
                param["delta"] = "true"
            else:
                raise self.ctx.utils.UserReportableException("Transport type can only be full or delta")

        conn = self.ctx.HttpConnection.initialize_connection(args)
        self.ctx.utils.hasMinimalVersion("1.1.0", conn.getPingReply()["halm_version"])
        self.ctx.utils.transportModeCtsDisabled(conn.getPingReply())

        (code, res, hdrs) = conn.request(transportService, parameters=param, method='POST')

        if code != 200:
            jsonObj = self.ctx.utils.tryReadErrorNoException(code, res, hdrs)
            if jsonObj and 'actionId' in jsonObj:
                self.print_action_log(conn, jsonObj['actionId'])
            sys.exit(1)

    def cmd_list(self, args, options, command_args):
        def get_name_width(routes):
            width = 11
            for r in routes:
                if len(r['name']) > width:
                    width = len(r['name'])
            return width

        def get_mode_description(mode):
            modeDescr = ""
            if mode == 'F' : modeDescr = "DUs"       # TRANSPORT_TYPE_FULL=Complete Delivery Units
            if mode == 'W' : modeDescr = "All CLs"   # TRANSPORT_TYPE_FULL_RELEASED=All Changelists
            if mode == 'C' : modeDescr = "Sel CLs"   # TRANSPORT_TYPE_CHANGE=Selected Changelists
            return modeDescr;

        conn = self.ctx.HttpConnection.initialize_connection(args)
        self.ctx.utils.hasMinimalVersion("1.1.0", conn.getPingReply()["halm_version"])
        self.ctx.utils.transportModeCtsDisabled(conn.getPingReply())

        param = {"enrich": "true"}
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_base + '/core/transport.xsjs', parameters=param)
        self.ctx.utils.tryReadWebError(code, res, hdrs)

        routes = self.ctx.utils.readJson(res, hdrs)

        name_width = get_name_width(routes)

        if args.json:
            self.ctx.utils.prettyPrintJson(routes)
        else:
            formatString = "{0:7} {1:<10} {2:"+str(name_width)+"} {3:7} {4:18}  --> {5:18} : {6}\n"
            sys.stdout.write(str.format(formatString,
                             "type", "route id", "route name", "mode", "source system", "target system", "Product/Delivey Unit(s)"))
            line = '-'
            for i in range(1, name_width):
                line += '-'
            sys.stdout.write(line+"---------------------------------------------------------------------------------------------\n")
            for r in routes:

                if r['transport_unit'] == 'D':
                    ttype = "DU"
                    ct = ''
                    first = True
                    for du in r['dus']:
                        if first:
                            first = False
                        else:
                            ct += ', '
                        ct += du['name'] + ' (' + du['vendor'] + ')'
                else:
                    ttype = "Product"
                    ct = r['product_name'] + '(' + r['product_vendor'] + ')'
                source_system = r['src_system_full']['sid'] + " (" + r['src_system_full']['host'] + ")"
                target_system = r['target_system_full']['sid'] + " (" + r['target_system_full']['host'] + ")"
                sys.stdout.write(str.format(formatString,
                  ttype,
                  r['id'],
                  r['name'][:name_width],
                  get_mode_description(r['mode']),
                  source_system,
                  target_system,
                  ct))

    def run_command(self, args, command_args):

        (command, command_args) = self.ctx.utils.defCommand(command_args, transport.commands)
        parser = self.ctx.HALMOptionParser(add_help_option=False)
        parser.disable_interspersed_args()
        parser.add_option("-t", "--type", action="store", type="string", dest="type")
        (options, cmd_args) = parser.parse_args(args=command_args);
        getattr(self, "cmd_" + command)(args, options, cmd_args)

op = getattr(transport, 'command_name', None)
if callable(op):
  command_name = transport.command_name()
else:
  command_name = "transport"
halm_commands[command_name] = {'class': "transport", 'pos': 5}

# ------------------------------------------------------------------------
# File: log.py
# ------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# HANA Application Lifecycle Management Log Viewer
# ---------------------------------------------------------------------------

import sys
import string
import re
from datetime import datetime

class log:

    commands = frozenset(["list", "get"])

    def __init__(self, context):
        self.ctx = context

    def description(self):
        return "Displays log files"

    def help(self):
        return """
    Log viewer for SAP HANA Application Lifecycle Management

    usage: hdbalm log <log command> [<parameter>]*

    log commands:
    -------------

    list

      Lists available log entries.

      usage:
      hdbalm log list

    get

      Gets log for a particular process ID.

      usage:
      hdbalm log get <ID>
    """

    def cmd_list(self, args, options, command_args):

        conn = self.ctx.HttpConnection.initialize_connection(args, minVersion=self.ctx.Constants.halm_sp6_version)
        aclist = self.ctx.Constants.xslm_base + '/core/Process.xsjs'
        param = {'top': '500', 'full': 'true'}
        (code, res, hdrs) = conn.request(aclist, parameters=param)
        self.ctx.utils.tryReadWebError(code, res, hdrs)

        records = self.ctx.utils.readJson(res, hdrs)
        formatString = "{0:2} {1:18} {2:9} {3:15} {4:15}\n"
        sys.stderr.write(str.format(formatString, "RC", "ID", "Type", "Start time", "End Time"))
        sys.stderr.write('------------------------------------------------------------------------------\n')

        for r in records:
            if (r['start_time']):
                st = datetime(*list(map(int, re.split('[^\d]', r['start_time'])[:-1])))
                stf = str.format("{0:02d}.{1:02d} {2:02d}.{3:02d}.{4:02d}", st.day, st.month, st.hour, st.minute, st.second)
            else:
                stf='-'
            if (r['end_time']):
                en = datetime(*list(map(int, re.split('[^\d]', r['end_time'])[:-1])))
                enf = str.format("{0:02d}.{1:02d} {2:02d}.{3:02d}.{4:02d}", en.day, en.month, en.hour, en.minute, en.second)
            else:
                enf = '-'
            sys.stdout.write(str.format(formatString, r['rc'], r['process_id'], r['type'], stf, enf))

    def cmd_get(self, args, options, command_args):

        if not len(command_args) == 1:
            raise self.ctx.utils.UserReportableException("No id specified.")

        conn = self.ctx.HttpConnection.initialize_connection(args, minVersion=self.ctx.Constants.halm_sp6_version)
        self.print_action_log(conn, command_args[0])

    def print_action_log(self, conn, actionId):

        logViewer = self.ctx.Constants.xslm_base + '/core/ActionLogViewer.xsjs'
        param = {'action_id': actionId}
        (code, res, hdrs) = conn.request(logViewer, parameters=param)
        if code != 200:
            sys.stderr.write("Unable to retrieve action log for transport.")
            self.utils.tryReadError(res, hdrs)
            sys.exit(1)

        records = self.ctx.utils.readJson(res, hdrs)
        formatString = "{0:2} {1:3} {2}\n"
        sys.stderr.write(str.format(formatString, "RC", "SID", "Message"))
        sys.stderr.write('------------------------------------------------------------------------------\n')

        for r in records:
            sys.stdout.write(str.format(formatString, r['rc'], r['sid'], r['record']))


    def run_command(self, args, command_args):

        (command, command_args) = self.ctx.utils.defCommand(command_args, log.commands)

        # invoke the method
        getattr(self, "cmd_" + command)(args, None, command_args)


#    parser = self.ctx.HALMOptionParser(add_help_option=False)
#    parser.disable_interspersed_args()
#    parser.add_option("-l", "--list", action="store_true", dest="list")
#    parser.add_option("-d", "--display", action="store", type="string", dest="display")
#    (options, cmd_args) = parser.parse_args(args=command_args);

#    if not options.list and not options.display:
#      raise self.ctx.utils.UserReportableException("At least one option required")

#    if options.list and options.display:
#      raise self.ctx.utils.UserReportableException("Only one option allowed")

#    conn = self.ctx.HttpConnection.initialize_connection(args)
#    if options.list:
#      self.print_list(conn)
#    elif options.display:
#      self.print_action_log(conn, options.display)
#    else:
#      raise self.utils.UserReportableException("No option provided")

op = getattr(log, 'command_name', None)
if callable(op):
  command_name = log.command_name()
else:
  command_name = "log"
halm_commands[command_name] = {'class': "log", 'pos': 6}

# ------------------------------------------------------------------------
# File: product.py
# ------------------------------------------------------------------------

import sys
import json

# ---------------------------------------------------------------------------
# product management
# ---------------------------------------------------------------------------

class product:

    commands = frozenset(["create", "delete", "uninstall", "list", "get",
                          "createInstance", "deleteInstance", "assign",
                          "unassign", "update"])

    def __init__(self, context):
        self.ctx = context
        self.stdout = self.ctx.utils.stdout
        self.stderr = self.ctx.utils.stderr

    def description(self):
        return "Manages SAP HANA products"

    def help(self):
        return """
    Manage SAP HANA Products

    usage: hdbalm product <product command> [<command option>]* [<parameter>]*

    product commands:
    -----------------

    list

      Lists all products installed in the system.

      usage:
      hdbalm product list

    get

      Retrieves product metadata.

      usage:
      hdbalm product get <product name> <vendor name>

    create

      Creates a product in the system (metadata only). The vendor is set to the
      vendor name configured in the system.

      Supported command options are:
        -v <version>, --version=<version>
        -d <description>, --description=<description>

      usage:
      hdbalm product create [<command option>]* <product name>

    update

      Updates a product in the system (metadata only).

      Supported command options are:
        -v <version>, --version=<version>
        -d <description>, --description=<description>

      usage:
      hdbalm product update [<command option>]* <product name> <vendor name>

    delete

      Deletes the product (metadata only). No delivery units are removed from
      the system.

      usage:
      hdbalm product delete <product name> <vendor name>

    createInstance:

      Creates a product instance for the specified product.

      Supported command options are:
        -d <description>, --description=<description>

      usage:
      hdbalm product createInstance [<command option>]*
                              <product name> <vendor name> <instance id>

    deleteInstance:

      Deletes a product instance for the specified product. All assigned
      delivery units are unassigned.

      usage:
      hdbalm product deleteInstance
                             <product name> <vendor name> <instance id>

    assign:

      Assigns a delivery unit to a product instance.

      usage:
      hdbalm product assign <du name> <du vendor>
                          <product name> <product vendor> <instance id>

    unassign:

      Unassigns a delivery unit from a product instance.

      usage:
      hdbalm product unassign <du name> <du vendor>
                          <product name> <product vendor> <instance id>
    """

    def cmd_get(self, args, cmd_args):

        if len(cmd_args) != 2:
            raise self.ctx.utils.UserReportableException("Product name and vendor required.")

        param = {
            "operation": "getProduct2Tree",
            "name":       cmd_args[0],
            "vendor":     cmd_args[1]
        }
        conn = self.ctx.HttpConnection.initialize_connection(args)
        self.ctx.utils.hasMinimalVersion(self.ctx.Constants.halm_sp8_version, conn.getPingReply()["halm_version"])

        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param)
        self.ctx.utils.tryReadWebError(code, res, hdrs)
        product = self.ctx.utils.readJson(res, hdrs)

        if args.json:
            self.ctx.utils.prettyPrintJson(product)
        else:
            result =  "product: " + product["NAME"] + " (" + product["VENDOR"] + ")\n"
            if product["VERSION"] != "":
                result += "version: " + product["VERSION"] + "\n"
            if product["CAPTION"] != "":
                result += "caption: " + product["CAPTION"] + "\n"

            self.stdout.write(result + '\n')

    def name(self, product):
        return product["NAME"] + " (" + product["VENDOR"] + ")"

    def cmd_list(self, args, cmd_args):

        param = {"operation": "getAllProducts2Tree"}
        conn = self.ctx.HttpConnection.initialize_connection(args, minVersion=self.ctx.Constants.halm_sp8_version)
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param)
        self.ctx.utils.tryReadWebError(code, res, hdrs)
        products = self.ctx.utils.readJson(res, hdrs)
        if args.json:
            self.ctx.utils.prettyPrintJson(products)
        else:
            for p in products:
                formatString = "{0} ({1}) {2}\n"
                self.stdout.write(formatString.format(
                                            p['NAME'], p['VENDOR'],
                                            p['VERSION']))


    def parse_create_update_options(self, command_args):
        parser = self.ctx.HALMOptionParser(add_help_option=False)
        parser.disable_interspersed_args()
        parser.add_option("-v", "--version", action="store", type="string", dest="version")
        parser.add_option("--version_sp", action="store", type="string", dest="version_sp")
        parser.add_option("-d", "--description", action="store", type="string", dest="description")
        parser.add_option("--caption", action="store", type="string", dest="description")
        parser.add_option("-p", "--ppms_id", action="store", type="string", dest="ppmsid")
        parser.add_option("-s", "--sp_stack_id", action="store", type="string", dest="spstackid")
        parser.add_option("-c", "--sp_stack_caption", action="store", type="string", dest="spstackcaption")
        parser.add_option("-a", "--alternate_vendor", action="store", type="string", dest="vendor")
        parser.add_option("--sap_note", action="store", type="string", dest="sap_note")
        (options, cmd_args) = parser.parse_args(args=command_args);
        return (options, cmd_args)


    def cmd_create(self, args, command_args):

        (options, cmd_args) = self.parse_create_update_options(command_args)

        if len(cmd_args) != 1:
            raise self.ctx.utils.UserReportableException("Product name required.")

        conn = self.ctx.HttpConnection.initialize_connection(args, minVersion=self.ctx.Constants.halm_sp8_version)
        vendor = options.vendor
        if not vendor:
            vendor = self.ctx.utils.getVendor(conn)

        product = {
          "NAME": cmd_args[0],
          "VENDOR": vendor
        }

        if options.version:
            product["VERSION"] = options.version
        if options.version_sp:
            product["VERSION_SP"] = options.version_sp
        if options.description:
            product["CAPTION"] = options.description
        if options.ppmsid:
            product["PPMS_ID"] = options.ppmsid
        if options.spstackid:
            product["SP_STACK_ID"] = options.spstackid
        if options.spstackcaption:
            product["SP_STACK_CAPTION"] = options.spstackcaption
        if options.sap_note:
            product["SAP_NOTE"] = options.sap_note

        param = {
             "operation": "createProduct2",
             "product": json.dumps(product).encode('UTF-8')
        }

        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param, method="POST")
        self.ctx.utils.tryReadWebError(code, res, hdrs)

    def cmd_update(self, args, command_args):

        (options, cmd_args) = self.parse_create_update_options(command_args)

        if len(cmd_args) != 2:
            raise self.ctx.utils.UserReportableException("Product name and vendor required.")

        param = {
            "operation": "getProduct2",
            "name":       cmd_args[0],
            "vendor":     cmd_args[1]
        }
        conn = self.ctx.HttpConnection.initialize_connection(args)
        self.ctx.utils.hasMinimalVersion(self.ctx.Constants.halm_sp8_version, conn.getPingReply()["halm_version"])

        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param)
        self.ctx.utils.tryReadWebError(code, res, hdrs)
        product = self.ctx.utils.readJson(res, hdrs)

        if options.version:
            product["VERSION"] = options.version
        if options.version_sp:
            product["VERSION_SP"] = options.version_sp
        if options.description:
            product["CAPTION"] = options.description
        if options.ppmsid:
            product["PPMS_ID"] = options.ppmsid
        if options.spstackid:
            product["SP_STACK_ID"] = options.spstackid
        if options.spstackcaption:
            product["SP_STACK_CAPTION"] = options.spstackcaption
        if options.sap_note:
            product["SAP_NOTE"] = options.sap_note

        param = {
            "operation": "editProduct2",
            "product": json.dumps(product).encode('UTF-8')
        }
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param, method="POST")
        self.ctx.utils.tryReadWebError(code, res, hdrs)


    def cmd_createInstance(self, args, command_args):

        parser = self.ctx.HALMOptionParser(add_help_option=False)
        parser.disable_interspersed_args()
        parser.add_option("-d", "--description", action="store", type="string", dest="description")
        parser.add_option("-p", "--ppms_id", action="store", type="string", dest="ppmsid")
        (options, cmd_args) = parser.parse_args(args=command_args);

        if len(cmd_args) != 3:
            raise self.ctx.utils.UserReportableException("Instance id, product name, and product vendor required.")

        productInstance = {
           "PRODUCT_NAME": cmd_args[0],
           "PRODUCT_VENDOR": cmd_args[1],
           "INSTANCE_ID": cmd_args[2]
        }
        if options.ppmsid:
            productInstance["PPMS_ID"] = options.ppmsid
        if options.description:
            productInstance["CAPTION"] = options.description

        param = {
             "operation": "createProductInstance2",
             "productInstance": json.dumps(productInstance)
        }

        conn = self.ctx.HttpConnection.initialize_connection(args, minVersion=self.ctx.Constants.halm_sp8_version)
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param, method="POST")
        self.ctx.utils.tryReadWebError(code, res, hdrs)

    def cmd_deleteInstance(self, args, command_args):

        if len(command_args) != 3:
            raise self.ctx.utils.UserReportableException("Instance id, product name, and product vendor required.")

        param = {
          "operation": "deleteProductInstance2",
          "name": command_args[0],
          "vendor": command_args[1],
          "instance_id" : command_args[2]
        }

        conn = self.ctx.HttpConnection.initialize_connection(args, minVersion=self.ctx.Constants.halm_sp8_version)
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param, method="POST")
        self.ctx.utils.tryReadWebError(code, res, hdrs)

    def cmd_assign(self, args, cmd_args):

        if len(cmd_args) != 5:
            raise self.ctx.utils.UserReportableException("Delivery unit name, delivery unit vendor, instance id, product name, and product vendor required.")

        param = {
          "operation": "assignDeliveryUnit",
          "duName": cmd_args[0],
          "duVendor": cmd_args[1],
          "productName": cmd_args[2],
          "productVendor": cmd_args[3],
          "productInstance": cmd_args[4]
        }

        conn = self.ctx.HttpConnection.initialize_connection(args, minVersion=self.ctx.Constants.halm_sp8_version)
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param, method="POST")
        self.ctx.utils.tryReadWebError(code, res, hdrs)

    def cmd_unassign(self, args, cmd_args):

        if len(cmd_args) != 5:
            raise self.ctx.utils.UserReportableException("Delivery unit name, delivery unit vendor, instance id, product name, and product vendor required.")

        param = {
          "operation": "removeDeliveryUnit",
          "duName": cmd_args[0],
          "duVendor": cmd_args[1],
          "productName": cmd_args[2],
          "productVendor": cmd_args[3],
          "productInstance": cmd_args[4]
        }

        conn = self.ctx.HttpConnection.initialize_connection(args, minVersion=self.ctx.Constants.halm_sp8_version)
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param, method="POST")
        self.ctx.utils.tryReadWebError(code, res, hdrs)

    def cmd_delete(self, args, cmd_args):
        if len(cmd_args) != 2:
            raise self.ctx.utils.UserReportableException("Product name and vendor required.")

        param = {
             "operation": "deleteProduct",
             "name": cmd_args[0],
             "vendor": cmd_args[1]
        }
        conn = self.ctx.HttpConnection.initialize_connection(args, minVersion=self.ctx.Constants.halm_sp7_version)
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param, method="POST")
        self.ctx.utils.tryReadWebError(code, res, hdrs)

    def run_command(self, args, command_args):

        if len(command_args) == 0:
            raise self.ctx.utils.UserReportableException("No command given")
        command = command_args[0]
        if not command in self.commands:
            raise self.ctx.utils.UserReportableException("Command " + command + " not recognized.")
        command_args = command_args[1:]

        # invoke the method
        getattr(self, "cmd_" + command)(args, command_args)

op = getattr(product, 'command_name', None)
if callable(op):
  command_name = product.command_name()
else:
  command_name = "product"
halm_commands[command_name] = {'class': "product", 'pos': 7}

# ------------------------------------------------------------------------
# File: du.py
# ------------------------------------------------------------------------

import sys
import json
import re

# ---------------------------------------------------------------------------
# du management
# ---------------------------------------------------------------------------

class du:

    commands = frozenset(["create", "update", "delete", "undeploy", "list", "get", "make_local", "languages", "set_original_language"])

    def __init__(self, context):
        self.ctx = context
        self.stdout = self.ctx.utils.stdout
        self.stderr = self.ctx.utils.stderr

    def description(self):
        return "Manages delivery units"

    def help(self):
        return """
    Manage SAP HANA Delivery Units

    usage: hdbalm du <du command> [<command option>]* [<parameter>]*

    du commands:
    ------------

    list

      Lists all delivery units deployed in the system.

      usage:
      hdbalm du list

    get

      Retrieves delivery unit metadata.

      usage:
      hdbalm du get <du name> <du vendor>

    create

      Creates a new delivery unit. The vendor is set to the vendor name
      configured in the system.

      The supported command options are:
        -v <version>, --version=<version>
        -r <responsible>, --responsible=<responsible>
        -d <description>, --description=<descriptions>

      The version string must to be in the format a, a.b, or a.b.c
      where a is the version number, b the version SP, and c the patch
      number.

      usage:
      hdbalm du create [<command option>]* <du name>

    update

      Updates a delivery unit.

      The supported options are:
        -v <version>, --version=<version>
        -r <responsible>, --responsible=<responsible>
        -d <description>, --description=<descriptions>

      The version string must to be in the format a, a.b, or a.b.c
      where a is the version number, b the version SP, and c the patch
      number.

      usage:
      hdbalm du update [<command option>]* <du name> <du_vendor>

    delete

      Deletes a delivery unit (metadata only). No objects are removed from the
      system.

      usage:
      hdbalm du delete <du name> <du vendor>

    undeploy

      Undeploys a delivery unit. The delivery unit metadata and all objects are
      removed from the system.

      WARNING: Use this command with caution.

      usage:
      hdbalm du undeploy <du name> <du vendor>

    make_local

      This is a developer feature that sets the source system of a delivery
      unit to the local system. This is not supported for delivery units
      shipped by SAP.

      usage:
      hdbalm du make_local <du name> <du vendor>

    languages

      Retrieves the original language for a delivery unit and all translations
      available in the system.

      usage:
      hdbalm du languages <du name> <du vendor>

    set_original_language

      Sets the original language attribute for all packages that belong to the
      specified delivery unit.

      The language is either a two-character ISO 639-1 language code or a two-
      character ISO 639-1 language code followed by an underscore followed
      by two-character ISO 3166-1 country code.

      usage:
      hdbalm du set_original_language <du name> <du vendor> <language>
    """

    def getDeliveryUnits(self, conn):

        param = {
            "operation": "getAllDeliveryUnits"
        }
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param)
        self.ctx.utils.tryReadWebError(code, res, hdrs)

        dus = self.ctx.utils.readJson(res, hdrs)
        return dus

    def get_delivery_unit(self, conn, name, vendor):

        param = {
            "operation": "getDeliveryUnit",
            "name":       name,
            "vendor":     vendor
        }
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param)
        self.ctx.utils.tryReadWebError(code, res, hdrs)
        return self.ctx.utils.readJson(res, hdrs)

    def cmd_set_original_language(self, args, cmd_args):
        if len(cmd_args) != 3:
            raise self.ctx.utils.UserReportableException("Delivery unit name, vendor, and language code required.")

        param = {
            "operation": "setDeliveryUnitOriginalLanguage",
            "name":       cmd_args[0],
            "vendor":     cmd_args[1],
            "originalLanguage": cmd_args[2]
        }
        conn = self.ctx.HttpConnection.initialize_connection(args, minVersion=self.ctx.Constants.halm_sp8_version)
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param,  method="POST")
        self.ctx.utils.tryReadWebError(code, res, hdrs)


    def cmd_languages(self, args, cmd_args):

        def getTranslations(ta):
            res = ''
            for i in range(len(ta)):
                if i > 0:
                    res += ", "
                res += ta[i]
            return res

        if len(cmd_args) != 2:
            raise self.ctx.utils.UserReportableException("Delivery unit name and vendor required.")

        param = {
            "operation": "getDeliveryUnitLanguages",
            "name":       cmd_args[0],
            "vendor":     cmd_args[1]
        }
        conn = self.ctx.HttpConnection.initialize_connection(args, minVersion=self.ctx.Constants.halm_sp8_version)
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param)
        self.ctx.utils.tryReadWebError(code, res, hdrs)

        res = self.ctx.utils.readJson(res, hdrs)
        if args.json:
            self.ctx.utils.prettyPrintJson(res)
        else:
            print("Original language:      " + res["originalLanguage"])
            print("Available translations: " + getTranslations(res["translations"]))

    def get_full_version(self, du):

        res = ""
        if "VERSION" in du:
            res += du["VERSION"]
        if "VERSION_SP" in du:
            res += "." + du["VERSION_SP"]
        if "VERSION_PATCH" in du:
            res += "." + du["VERSION_PATCH"]
        return res

    def cmd_get(self, args, cmd_args):

        if len(cmd_args) != 2:
            raise self.ctx.utils.UserReportableException("Delivery unit name and vendor required.")

        conn = self.ctx.HttpConnection.initialize_connection(args)
        du = self.get_delivery_unit(conn, cmd_args[0], cmd_args[1])

        if args.json:
            self.ctx.utils.prettyPrintJson(du)
        else:
            result =  "Delivery unit " + du["DELIVERY_UNIT"] + " (" + du["VENDOR"] + ")\n"
            result += "version:     " + self.get_full_version(du) + "\n"
            result += "responsible: " + du["RESPONSIBLE"] + "\n"
            result += "caption:     " + du["CAPTION"] + "\n"
            result += "ACH:         " + du["ACH"] + "\n"
            self.stdout.write(result + '\n')


    def splitVersion(self, version):
        pt = "^([0-9]*)\.?([0-9]*)\.?([0-9]*)$"
        m = re.match(pt, version)
        if not m:
            raise self.ctx.utils.UserReportableException("Version number not in format VERSION.VERSION_SP.VERSION_PATCH")

        glen = len(m.group())

        if glen>0:
            v = m.group(1)

        if glen > 1 and m.group(2) != "":
            vs = m.group(2)

        if glen > 2 and m.group(3) != "":
            vs = m.group(3)

    def parse_create_update_options(self, command_args):

        parser = self.ctx.HALMOptionParser(add_help_option=False)
        parser.disable_interspersed_args()
        parser.add_option("-v", "--version", action="store", type="string", dest="version")
        parser.add_option("-r", "--responsible", action="store", type="string", dest="responsible")
        parser.add_option("-d", "--description", action="store", type="string", dest="description")
        parser.add_option("-a", "--alternate_vendor", action="store", type="string", dest="vendor")
        parser.add_option("--sap_note", action="store", type="string", dest="sap_note")
        parser.add_option("--dependency_type", action="store", type="string", dest="dependency_type")
        parser.add_option("--ppms_id", action="store", type="string", dest="ppms_id")
        parser.add_option("--sp_ppms_id", action="store", type="string", dest="sp_ppms_id")
        parser.add_option("--ach", action="store", type="string", dest="ach")
        (options, cmd_args) = parser.parse_args(args=command_args);
        return (options, cmd_args)


    def cmd_create(self, args, command_args):

        (options, cmd_args) = self.parse_create_update_options(command_args)

        if len(cmd_args) != 1:
            raise self.ctx.utils.UserReportableException("Delivery unit name required for creating a delivery unit.")

        conn = self.ctx.HttpConnection.initialize_connection(args)

        vendor = options.vendor
        if not vendor:
            vendor = self.ctx.utils.getVendor(conn)

        du = {
            "DELIVERY_UNIT": cmd_args[0],
            "VENDOR": vendor
        }

        if options.responsible:
            du["RESPONSIBLE"] = options.responsible

        if options.description:
            du["CAPTION"] = options.description

        if options.version:
            du["VERSION_FULL"] = options.version

        if options.sap_note:
            du["SAP_NOTE"] = options.sap_note

        if options.dependency_type:
            du["DEPENDENCY_TYPE"] = options.dependency_type

        if options.ppms_id:
            du["PPMS_ID"] = options.ppms_id

        if options.sp_ppms_id:
            du["SP_PPMS_ID"] = options.sp_ppms_id

        if options.ach:
            du["ACH"] = options.ach

        param = {
          "operation": "createDeliveryUnit",
          "du": self.ctx.utils.dump_json(du).encode('UTF-8')
        }

        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param, method="POST")
        self.ctx.utils.tryReadWebError(code, res, hdrs)

    def cmd_update(self, args, command_args):

        def remove(du, key):
            if key in du:
                del du[key]

        (options, cmd_args) = self.parse_create_update_options(command_args)
        if len(cmd_args) != 2:
            raise self.ctx.utils.UserReportableException("Delivery unit name and vendor required for updating a delivery unit")

        conn = self.ctx.HttpConnection.initialize_connection(args)
        du = self.get_delivery_unit(conn, cmd_args[0], cmd_args[1])

        changed = False

        if options.responsible:
            du["RESPONSIBLE"] = options.responsible
            changed = True

        if options.description:
            du["CAPTION"] = options.description
            changed = True

        if options.version:
            du["VERSION_FULL"] = options.version
            remove(du, 'VERSION')
            remove(du, 'VERSION_SP')
            remove(du, 'VERSION_PATCH')
            changed = True

        if options.sap_note:
            du["SAP_NOTE"] = options.sap_note
            changed = True

        if options.dependency_type:
            du["DEPENDENCY_TYPE"] = options.dependency_type
            changed = True

        if options.ppms_id:
            du["PPMS_ID"] = options.ppms_id
            changed = True

        if options.sp_ppms_id:
            du["SP_PPMS_ID"] = options.sp_ppms_id
            changed = True

        if options.ach:
            du["ACH"] = options.ach
            changed = True

        if not changed:
            raise self.utils.UserReportableException("No change to delivery unit")

        param = {
          "operation": "editDeliveryUnit",
          "du": self.ctx.utils.dump_json(du).encode('UTF-8')
        }

        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param, method="POST")
        self.ctx.utils.tryReadWebError(code, res, hdrs)

    def cmd_undeploy(self, args, cmd_args):

        if not len(cmd_args) == 2:
            sys.stderr.write("Must specify du name and vendor")
            sys.exit(1)

        param = {"operation": "deepDeleteDeliveryUnit",
                 "name": cmd_args[0],
                 "vendor": cmd_args[1]}

        if not args.yes:
            ans = input("This command will undeploy the delivery unit " + cmd_args[0] + " (" + cmd_args[1] + "). Continue [yes/No] ?")
            if ans.lower() != 'yes':
                sys.exit(1)

        conn = self.ctx.HttpConnection.initialize_connection(args)
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param, method="POST")
        self.ctx.utils.tryReadWebError(code, res, hdrs)

    def cmd_make_local(self, args, cmd_args):

        if not len(cmd_args) == 2:
            raise self.ctx.utils.UserReportableException("Delivery unit name and vendor must be specified.")

        params = {
          "operation": "makeDUlocal",
          "name": cmd_args[0],
          "vendor": cmd_args[1]
        }
        conn = self.ctx.HttpConnection.initialize_connection(args)
        self.ctx.utils.hasMinimalVersion("1.2.0", conn.getPingReply()["halm_version"])
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=params, method="POST")
        self.ctx.utils.tryReadWebError(code, res, hdrs)

    def cmd_list(self, args, command_args):

        conn = self.ctx.HttpConnection.initialize_connection(args)
        dus = self.getDeliveryUnits(conn)
        mystdout = self.ctx.utils.stdout
        if args.json:
            self.ctx.utils.prettyPrintJson(dus)
        else:
            for du in dus:
                formatString = "{0} ({1}) {2}.{3}.{4}\n"
                ip = formatString.format(du['DELIVERY_UNIT'], du['VENDOR'],
                                         du['VERSION'], du['VERSION_SP'], du['VERSION_PATCH'])
                mystdout.write(formatString.format(
                                            du['DELIVERY_UNIT'], du['VENDOR'],
                                            du['VERSION'], du['VERSION_SP'], du['VERSION_PATCH']))

    def cmd_delete(self, args, command_args):

        if len(command_args) != 2:
            raise self.ctx.utils.UserReportableException("Delivery unit name and vendor must be spedified.")

        params={
          "operation": "deleteDeliveryUnit",
          "name": command_args[0],
          "vendor": command_args[1]
        }
        conn = self.ctx.HttpConnection.initialize_connection(args)
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=params, method="POST")
        self.ctx.utils.tryReadWebError(code, res, hdrs)

    def run_command(self, args, command_args):

        if len(command_args) == 0:
            raise self.ctx.utils.UserReportableException("No command given")
        command = command_args[0]
        if not command in self.commands:
            raise self.ctx.utils.UserReportableException("Command " + command + " not recognized.")
        command_args = command_args[1:]

        # invoke the method
        getattr(self, "cmd_" + command)(args, command_args)

op = getattr(du, 'command_name', None)
if callable(op):
  command_name = du.command_name()
else:
  command_name = "du"
halm_commands[command_name] = {'class': "du", 'pos': 8}

# ------------------------------------------------------------------------
# File: dependencies.py
# ------------------------------------------------------------------------

import os
import sys

# ---------------------------------------------------------------------------
# Dependency printer
# ---------------------------------------------------------------------------

class dependencies:

    def __init__(self, context):
        self.ctx = context

    def description(self):
        return "Displays and analyzes delivery unit dependencies"

    def help(self):
        return """
    Display and Analyze Delivery Unit Dependencies

    usage: hdbalm dependencies [<command option>]*
                        [<source du>] [<source du vendor>]
                        [<target du>] [<target du vendor>]

    command options:
      -f, --full                Shows the full dependency view and analysis
      -r, --references          Shows object references between delivery units
      -n, --nirvana             Shows nirvana references for a delivery unit

    Notes:

    The --references option requires name and vendor of source and target
    delivery units.

    The --nirvana option displays object references for objects that are part of
    a delivery unit to objects which are not part of a delivery unit. If a
    delivery unit contains objects with such references it cannot be imported
    into another system.

    If no options are specified a list of delivery unit dependencies is
    displayed. Each line of the output lists a delivery unit followed by a colon
    and a comma-separated list of referenced delivery units. In the following
    example the delivery unit HANA_XS_LM has references to SAPUI5_1 and
    HANA_XS_BASE:

    HANA_XS_LM(sap.com): SAPUI5_1(sap.com), HANA_XS_BASE(sap.com)
    """

    def run_command(self, args, command_args):

        parser = self.ctx.HALMOptionParser(add_help_option=False)
        parser.disable_interspersed_args()
        parser.add_option("-n", "--nirvana", action="store_true", dest="nirvana")
        parser.add_option("-r", "--references", action="store_true", dest="references")
        parser.add_option("-f", "--full", action="store_true", dest="full")
        (options, cmd_args) = parser.parse_args(args=command_args);

        if options.nirvana:
            rep = self.nirvanaReferences(args, cmd_args)
            sys.stdout.write(rep)

        elif options.references:
            res = self.getObjectReferences(args, cmd_args)
            sys.stdout.write(res)

        elif options.full:

            self.full_dependency_report(args, cmd_args)

        else:

            if len(cmd_args) != 0:
                sys.stderr.write("No arguments allowed")
                sys.exit(1)

            conn = self.ctx.HttpConnection.initialize_connection(args)
            jd = self.get_dependency_graph(conn)
            result = self.print_dependency_graph(jd)
            sys.stdout.write(result)

    def nirvanaReferences(self, args, cmd_args):
        if not len(cmd_args) == 2:
            sys.stderr.write("Must specify du name and vendor")
            sys.exit(1)

        conn = self.ctx.HttpConnection.initialize_connection(args)
        self.ctx.utils.hasMinimalVersion("1.2.0", conn.getPingReply()["halm_version"])

        param = {"operation": "getNirvanaDependencies",
                 "du_from": cmd_args[0],
                 "vendor_from": cmd_args[1]}
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param)
        if code != 200:
            self.ctx.utils.tryReadError(res, hdrs)
            sys.stderr.write( res)
            sys.exit(1)

        refs = self.ctx.utils.readJson(res, hdrs)
        return self.printObjectReferences(refs)

    def getObjectReferences(self, args, cmd_args):
        # print out object references
        if not len(cmd_args) == 4:
            sys.stderr.write("Must specify du name and vendor of source and target du")
            sys.exit(1)

        param = {"operation": "getObjectCausingDuDependencies",
                 "du_from": cmd_args[0],
                 "vendor_from": cmd_args[1],
                 "du_to": cmd_args[2],
                 "vendor_to": cmd_args[3]}
        conn = self.ctx.HttpConnection.initialize_connection(args)
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param)
        if code != 200:
            sys.stderr.write("Unable to connect to server: " + res)
            sys.exit(1)

        refs = self.ctx.utils.readJson(res, hdrs)
        return self.printObjectReferences(refs)

    def full_dependency_report(self, args, cmd_args):
       # Full report
       #
        conn = self.ctx.HttpConnection.initialize_connection(args, minVersion=self.ctx.Constants.halm_sp8_version)

        duobj = self.ctx.du(self.ctx)

        deps = self.get_dependency_graph(conn)
        dus = self.get_dus_with_outgoing_dependencies(deps)

        report = "Dependency Report for system " + conn.getPingReply()['sid'] + '\n'
        report += '--------------------------------\n\n'
        report += "Delivery Unit Dependencies:\n"
        report += self.print_dependency_graph(deps)
        report += '\n\n'

        report += 'Object References per Delivery Unit:\n\n'

        for du in dus:
            if du['DELIVERY_UNIT'] == "":
                continue;
            report += "Delivery Unit " + du['DELIVERY_UNIT'] + " (" + du['VENDOR'] + ")\n"

            refs = self.get_outgoing_dependencies(conn, du['DELIVERY_UNIT'], du['VENDOR'])
            report += self.printObjectReferencesWithDUs(refs)
            report += "\n"

        nonDutoDuRefs = self.getReferencesFromNonDuPackagesToDuPackages(conn)
        if len(nonDutoDuRefs) != 0:
            report += "\n"
            report += "References from objects outside delivery units to objects belonging to delivery units\n"
            report += "-------------------------------------------------------------------------------------\n\n"
            formatString = "{0}::{1}.{2} --> {3} ({4}) {5}::{6}.{7}\n"
            fastConcat = []
            for ref in nonDutoDuRefs:
                fastConcat.append(formatString.format(ref["from_package"], ref["from_object_name"], ref["from_object_suffix"],
                                              ref["to_du"], ref["to_vendor"], ref["to_package"], ref["to_object_name"], ref["to_object_suffix"]))
            report += ''.join(fastConcat)

        print(report)

    def get_outgoing_dependencies(self, conn, name, vendor):

        param = {"operation": "getOutgoingDependencies",
                 "name": name,
                 "vendor": vendor
                }
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param)
        self.ctx.utils.tryReadWebError(code, res, hdrs)
        return self.ctx.utils.readJson(res, hdrs)

    def get_dependency_graph(self, conn):

        param = {"operation": "getDuDependencyGraph"}
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param)
        self.ctx.utils.tryReadWebError(code, res, hdrs)
        return self.ctx.utils.readJson(res, hdrs)

    def getReferencesFromNonDuPackagesToDuPackages(self, conn):
        param = {"operation": "getReferencesFromNonDuPackagesToDuPackages"}
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param)
        self.ctx.utils.tryReadWebError(code, res, hdrs)
        return self.ctx.utils.readJson(res, hdrs)

    def get_dus_with_outgoing_dependencies(self, jd):
        dus = {}
        edges = jd["edges"]
        for e in edges:
            fromDu = {
                      "DELIVERY_UNIT": e['fromDU'],
                      "VENDOR": e["fromVendor"]
                      }
            key = e['fromDU'] + " (" + e["fromVendor"] + ")"
            if not key in list(dus.keys()):
                dus[key] = fromDu

        return list(dus.values())

    def print_dependency_graph(self, jd):

        dus = {}
        edges = jd["edges"]
        for e in edges:
            fromDu = e['fromDU'] + "(" + e["fromVendor"] + ")"
            toDu = e["toDU"] + "(" + e["toVendor"] + ")"
            if fromDu in dus:
                dus[fromDu].append(toDu)
            else:
                dus[fromDu] = [toDu]

        result = ""
        firstLine = True
        for d in dus:
            if firstLine:
                firstLine = False
            else:
                result += "\n"
            result += d + ": "
            first = True
            for rd in dus[d]:
                if first:
                    first = False
                else :
                    result += ", "
                result += rd

        return result



    @staticmethod
    def printObjectReferences(refs):
        res = ""
        formatString = "{0}::{1}.{2} --> {3}::{4}.{5}\n"
        for ref in refs:
            res += str.format(formatString, ref["from_package"], ref["from_object_name"], ref["from_object_suffix"],
                                        ref["to_package"], ref["to_object_name"], ref["to_object_suffix"])
        return res

    @staticmethod
    def printObjectReferencesWithDUs(refs):
        res = ""
        formatString = "{0} {1}::{2}.{3} --> {4} {5}::{6}.{7}\n"
        for ref in refs:
            res += str.format(formatString, ref["from_du"], ref["from_package"], ref["from_object_name"], ref["from_object_suffix"],
                                        ref["to_du"], ref["to_package"], ref["to_object_name"], ref["to_object_suffix"])
        return res

op = getattr(dependencies, 'command_name', None)
if callable(op):
  command_name = dependencies.command_name()
else:
  command_name = "dependencies"
halm_commands[command_name] = {'class': "dependencies", 'pos': 9}

# ------------------------------------------------------------------------
# File: package.py
# ------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# HANA Application Lifecycle Management Log Viewer
# ---------------------------------------------------------------------------

import json

class package:

    commands = frozenset(["createPackage", "create", "deletePackage", "delete", "assignPackage", "assign"])

    def __init__(self, context):
        self.ctx = context

    def description(self):
        return "Manages packages"

    def help(self):
        return """
Manage SAP HANA packages

usage: hdbalm package <package command> [<parameter>]*

package commands:
-----------------

create

  Creates a new package.

  usage:
  hdbalm package create <package name>

delete

  Deletes a package. The package must not contain any sub-packages or
  objects.

  usage:
  hdbalm package delete <package name>

assign

  Assigns a package to a delivery unit.

  usage:
  hdbalm package assign <du name> <du vendor> <package name>
"""

    def cmd_create(self, args, options, cmd_args):
        self.cmd_createPackage(args, options, cmd_args)

    def cmd_createPackage(self, args, options, cmd_args):

        parser = self.ctx.HALMOptionParser(add_help_option=False)
        parser.disable_interspersed_args()
        parser.add_option("--originalLanguage", action="store", type="string", dest="originalLanguage")
        (options, cmd_args) = parser.parse_args(args=cmd_args)

        if not len(cmd_args) == 1:
            raise self.ctx.utils.UserReportableException("A single package name is required")

        originalLanguage = options.originalLanguage if options.originalLanguage != None else ""
        params = {
                  "operation": "createPackage",
                  "package": json.dumps({"packageName":cmd_args[0],"description":"","responsible":"","originalLanguage":originalLanguage}),
                  }
        conn = self.ctx.HttpConnection.initialize_connection(args)
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=params, method="POST")
        self.ctx.utils.tryReadWebError(code, res, hdrs)

    def cmd_delete(self, args, options, cmd_args):
        self.cmd_deletePackage(args, options, cmd_args)

    def cmd_deletePackage(self, args, options, cmd_args):
        if not len(cmd_args) == 1:
            raise self.ctx.utils.UserReportableException("A single package name is required")
        params = {
                  "operation": "deletePackage",
                  "package": cmd_args[0],
        }
        conn = self.ctx.HttpConnection.initialize_connection(args)
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=params, method="POST")
        self.ctx.utils.tryReadWebError(code, res, hdrs)

    def cmd_assign(self, args, options, cmd_args):
        self.cmd_assignPackage(args, options, cmd_args)

    def cmd_assignPackage(self, args, options, cmd_args):
        if not len(cmd_args) == 3:
            raise self.ctx.utils.UserReportableException("three parameters are required (du name, du vendor name, package name)")

        params = {
                  "operation": "assignPackage",
                  "name":cmd_args[0],
                  "vendor":cmd_args[1],
                  "packageName": cmd_args[2],
                  "selectSubpackages":"false"
        }
        conn = self.ctx.HttpConnection.initialize_connection(args)
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=params, method="POST")
        self.ctx.utils.tryReadWebError(code, res, hdrs)

    def run_command(self, args, command_args):

        if len(command_args) == 0:
            raise self.ctx.utils.UserReportableException("No command given")
        command = command_args[0]
        if not command in self.commands:
            raise self.ctx.utils.UserReportableException("Package command \"" + command + "\" not recognized.")
        command_args = command_args[1:]
        # invoke the method
        getattr(self, "cmd_" + command)(args, None, command_args)

op = getattr(package, 'command_name', None)
if callable(op):
  command_name = package.command_name()
else:
  command_name = "package"
halm_commands[command_name] = {'class': "package", 'pos': 10}

# ------------------------------------------------------------------------
# File: admin.py
# ------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Administration / set up
# ---------------------------------------------------------------------------

import sys

class admin:

    commands = frozenset(["setvendor", "getvendor", "enablechangemanagement", "disablechangemanagement"])

    def __init__(self, context):
        self.ctx = context

    def description(self):
        return "Manages administration"

    def help(self):
        return """
    Administrative commands

    usage: hdbalm admin <admin command> [<parameter>]*

    admin commands:
    ---------------

    getvendor

      Returns the vendor name of the system.

      usage:
      hdbalm admin getvendor

    setvendor

      Sets the vendor to the new vendor name.

      usage:
      hdbalm admin setvendor <new  vendor>

    enablechangemanagement

      Enables change recording.

      Warning: Enabling change recording makes all existing active objects part
      of a released changelist. Additionally, all objects in follow-up
      activations are assigned to changelists that are released afterwards. Only
      these changelists are transportable from the system.

      usage:
      hdbalm admin enablechangemanagement

    disablechangemanagement

      Disables change recording.

      Warning: Disabling change recording switches off change tracking in the
      system. This makes all active objects transportable, even those active
      objects that were already part of open changelists.

      usage:
      hdbalm admin disablechangemanagement
    """

    def change_management_enabled(self, conn):
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_conf_service, method="GET")
        self.ctx.utils.tryReadWebError(code, res, hdrs)
        config = self.ctx.utils.readJson(res, hdrs)

        for c in config:
            if c["key"] == "change_tracking":
                v = c["value"]
                return v
        return False

    def cmd_enablechangemanagement(self, args, options, cmd_args):

        param = {}
        conn = self.ctx.HttpConnection.initialize_connection(args, minVersion=self.ctx.Constants.halm_sp7_version)

        enabled = self.change_management_enabled(conn)
        if enabled == True:
            raise self.ctx.utils.UserReportableException("Change management is already enabled.")

        print("Change management is disabled and can be enabled now.")
        if not args.yes:
            ans = input("Do you want to continue [yes/No] ?")
            if ans.lower() != 'yes':
                sys.exit(1)

        param = '{"action": "set_change_tracking", "value": true}'
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_conf_service, method="POST", post_body=param)
        self.ctx.utils.tryReadWebError(code, res, hdrs)

    def cmd_disablechangemanagement(self, args, options, cmd_args):

        conn = self.ctx.HttpConnection.initialize_connection(args, minVersion=self.ctx.Constants.halm_sp7_version)

        enabled = self.change_management_enabled(conn)
        if enabled == False:
            raise self.ctx.utils.UserReportableException("Change management is already disabled.")

        param = {"key": "check"}
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_conf_service, parameters=param, method="GET")
        self.ctx.utils.tryReadWebError(code, res, hdrs)
        messages = self.ctx.utils.readJson(res, hdrs)
        for m in messages["value"]:
            print(m)
        print()

        if not args.yes:
            ans = input("Do you want to continue [yes/No] ?")
            if ans.lower() != 'yes':
                sys.exit(1)

        param = '{"action": "set_change_tracking", "value": false}'
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_conf_service, method="POST", post_body=param)
        self.ctx.utils.tryReadWebError(code, res, hdrs)

    def cmd_getvendor(self, args, options, cmd_args):

        if not len(cmd_args) == 0:
            raise self.ctx.utils.UserReportableException("No arguments expected")

        conn = self.ctx.HttpConnection.initialize_connection(args, minVersion=self.ctx.Constants.halm_sp7_version)
        vendor = self.ctx.utils.getVendor(conn)
        print(vendor)

    def cmd_setvendor(self, args, options, cmd_args):

        if not len(cmd_args) == 1:
            raise self.ctx.utils.UserReportableException("New vendor name expected.")

        param = {
          "operation": "changeVendor",
          "vendor": cmd_args[0]
        }
        conn = self.ctx.HttpConnection.initialize_connection(args, minVersion=self.ctx.Constants.halm_sp7_version)
        (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=param, method="POST")
        self.ctx.utils.tryReadWebError(code, res, hdrs)

    def run_command(self, args, command_args):

        (command, command_args) = self.ctx.utils.defCommand(command_args, admin.commands)

        # invoke the method
        getattr(self, "cmd_" + command)(args, None, command_args)

op = getattr(admin, 'command_name', None)
if callable(op):
  command_name = admin.command_name()
else:
  command_name = "admin"
halm_commands[command_name] = {'class': "admin", 'pos': 11}

# ------------------------------------------------------------------------
# File: fs_transport.py
# ------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# File System Based Transport Management
# ---------------------------------------------------------------------------

import sys
import os

nl = "\n"

class fs_transport:

    DEFAULT_TRANSPORT_TYPE = "full"
    DEFAULT_DIRECTORY = "."

    commands = frozenset(["export", "import"])

    def __init__(self, context):
        self.ctx = context
        self.stdout = self.ctx.utils.stdout
        self.stderr = self.ctx.utils.stderr

    def description(self):
        return "file system based transport management"

    def help(self):
        return """
    File System based Transport Management

    This command can be used to export delivery units to the file system. It
    supports the export of 'full' delivery units and additionally the export of only
    the latest released Changes 'delta' since the last export. As a prerequisite of
    the 'delta' export, the Change Recording must be activated on the HANA system.

    Additionally this command can be used to import 'full' and 'delta' delivery
    units from the file system to the HANA system.

    usage: hdbalm [<args>] fs_transport <transport command>

    transport commands:
    -------------------

    export

      Export delivery units to the file system.

      supported options are:
       -t <type>, --type=<type>                        type of export ("full" or "delta")
       -d <directory>, --directory=<directory>         target directory where to store the
                                                       delivery unit
       --export_version <version>                      for compatibility with older SAP HANA
                                                       versions an export format can be
                                                       specified here

      return codes:
       0: all delivery units have been exported successfully
       1: some delivery unit could be exported

      usage:
      hdbalm dlm_transport export [<options>] [<du name> <du vendor>]+


    import

      Import delivery units from file system.

      return codes:
       0: all delivery units have been imported successfully
       1: some delivery unit could be imported

      usage:
      hdbalm fs_transport import [<file>]+
    """

    def _print_import_result(self, msg, stream):

        text = ""
        if msg.get('message'):
            text +=  msg.get("message") + nl + nl
        if msg.get('rc'):
            text += "RC:        " + str(msg.get('rc')) + nl
        if msg.get('actionId'):
            text += "Action id: " + msg.get("actionId") + nl + nl
        if msg.get("files"):
            for f in msg.get("files"):
                text += f["fileName"] + ": RC " + str(f["rc"]) + nl
        text += nl
        stream.write(text)

    def cmd_import(self, args, command_args):

        fileNames = command_args
        conn = self.ctx.HttpConnection.initialize_connection(args, minVersion=self.ctx.Constants.halm_sp9_version)
        multipartFiles = []
        for name in fileNames:
            f = open(name, 'rb')
            contents = f.read()
            multipartFiles.append({
              'filename': name,
              'mimetype': 'application/octet-stream',
              'content': contents,
              'name': name
            })

        (code, res, hdrs) = conn.file_upload(self.ctx.Constants.xslm_service + "?operation=importFsDus", files=multipartFiles)
        response = self.ctx.utils.readJson(res, hdrs)
        if code == 200:
            self._print_import_result(response, self.stdout)
        else:
            if 'error' in response:
                self._print_import_result(response, self.stderr)
                raise self.ctx.utils.UserReportableException(response["error"])


    @staticmethod
    def _get_filename(hdrs, name, vendor):

        alternateFilename = name + "-" + vendor + ".tgz"
        if not "content-disposition" in hdrs:
            return alternateFilename
        items = hdrs['content-disposition'].split(";")
        for item in items:
            nvp = item.split('=')
            if nvp[0] == "filename":
                return nvp[1]
        return alternateFilename


    def cmd_export(self, args, command_args):
        ret_code = 0

        parser = self.ctx.HALMOptionParser(add_help_option=False)
        parser.disable_interspersed_args()
        parser.add_option("-t", "--type", action="store", type="string", dest="type")
        parser.add_option("-d", "--directory", action="store", type="string", dest="directory")
        parser.add_option("--export_version", action="store", type="int", dest="export_version")
        (options, cmd_args) = parser.parse_args(args=command_args);

        if options.directory:
            directory = options.directory
        else:
            directory = fs_transport.DEFAULT_DIRECTORY

        if options.type:
            transport_type = options.type
        else:
            transport_type = fs_transport.DEFAULT_TRANSPORT_TYPE

        if options.export_version:
            export_version = options.export_version
        else:
            export_version = -1

        conn = self.ctx.HttpConnection.initialize_connection(args, minVersion=self.ctx.Constants.halm_sp9_version)

        dus = cmd_args
        if len(dus) == 0  or len(dus) % 2 != 0:
            raise self.ctx.utils.UserReportableException("List of delivery unit names and vendors expected")

        for i in range(len(dus)/2):
            name = dus[i*2]
            vendor = dus[i*2+1]
            params = {
              "operation": "exportFsDu",
              "name": name,
              "vendor": vendor,
              "type": transport_type,
              "exportversion": export_version
            }
            (code, res, hdrs) = conn.request(self.ctx.Constants.xslm_service, parameters=params, method="POST")
            if hdrs['content-type'].startswith('application/json'):
                response = self.ctx.utils.readJson(res, hdrs)
                if code == 200:
                    self._print_import_result(response, self.stdout)
                else:
                    if 'error' in response:
                        self._print_import_result(response, self.stderr)
                        raise self.ctx.utils.UserReportableException(response["error"])

            elif hdrs['content-type'].startswith('application/octet-stream'):
                filename = self._get_filename(hdrs, name, vendor)
                attachment = hdrs['Content-Disposition']
                if attachment:
                    fields = attachment.split(";")
                    f = lambda s: s.split("=")
                    parsed = list(map(f, fields))
                    for i in parsed:
                        if len(i)==2 and i[0] == "filename":
                            filename = i[1]

                    duFile = open (os.path.join(directory,filename), "wb")
                    duFile.write(res)
                    duFile.close()
                    if "actionId" in hdrs:
                        usedactionid = hdrs['actionId']
                        self.stdout.write("Action id: " + usedactionid + nl + nl)

                    self.stdout.write(os.path.join(directory,filename) + " created." +nl +nl +nl)


    def run_command(self, args, command_args):

        (command, command_args) = self.ctx.utils.defCommand(command_args, fs_transport.commands)
        getattr(self, "cmd_" + command)(args, command_args)

op = getattr(fs_transport, 'command_name', None)
if callable(op):
  command_name = fs_transport.command_name()
else:
  command_name = "fs_transport"
halm_commands[command_name] = {'class': "fs_transport", 'pos': 12}




# ---------------------------------------------------------------------------
# Context class se we don't have to import our stuff that we combine
# into the modules (will cause problems during combine)
# ---------------------------------------------------------------------------
class Context:
    HttpConnection = HttpConnection
    utils = utils
    log=log
    du=du
    hasSSL = hasSSL
    HDBALM_VERSION = HDBALM_VERSION
    FileLogger = FileLogger
    NullFileLogger = NullFileLogger
    install = install

    class Constants:
        xslm_base = '/sap/hana/xs/lm'
        xslm_core = xslm_base + '/core'
        xslm_service = xslm_base + '/xsts/services.xsjs'
        xslm_product_installation_service = xslm_base + '/install/install.xsjs'
        xslm_xsrf = xslm_base + '/csrf.xsjs'
        xslm_ping = xslm_base + '/xsts/ping.xsjs'
        xslm_export_archive_service = xslm_base + '/core/ExportArchiveService.xsjs'
        xslm_conf_service = xslm_base + '/core/ConfService.xsjs'
        xslm_file_upload_service = xslm_base + '/delivery/FilesUploader.xsjs'
        xslm_import_du_service = xslm_base + '/delivery/ImportDUContents.xsjs'
        xslm_pe_installation_service = xslm_base + '/sl/interface/public/services.xsjs'
        halm_sp6_version = "1.0.0"
        halm_sp7_version = "1.1.2"
        halm_sp8_version = "1.2.0"
        halm_sp9_version = "1.3.0"
        halm_sp10_version = "1.4.0"
        HDBALM_VERSION = HDBALM_VERSION
        PPMS_SERVICE_URL_UP_TO_1_3_13 = xslm_base + '/ppms/ppms.xsjs'
        PPMS_SERVICE_URL_FROM_TO_1_3_14 = xslm_base + '/ppms2/ppms.xsjs'

        # ---------------------------------------------------------------------------
        # External programs used by this plug-in
        # ---------------------------------------------------------------------------
        REGI_WINDOWS = "regi.exe"
        REGI_UNIX = "regi"
        SAPCAR_WINDOWS = "SAPCAR.EXE"
        SAPCAR_UNIX = "SAPCAR"

        # ---------------------------------------------------------------------------
        # Exit codes for plug-ins
        # ---------------------------------------------------------------------------

        EXIT_OK = 0
        EXIT_ERROR = 1
        EXIT_IMPORT_ERROR = 3

    # make sure option parser errors comply with HALM error messages
    #
    class HALMOptionParser(OptionParser):
        def __init__(self, **arg):
            OptionParser.__init__(self, **arg)

        def error(self, msg):
            sys.stderr.write(msg)
            exit(1);

# ---------------------------------------------------------------------------
# HALM Commands & Parsing
# ---------------------------------------------------------------------------

# help

class help:

    def __init__(self, context):
        self.ctx = context

    def description(self):
        return "Provides information about available commands"

    def run_command(self, args, command_args):
        if len(command_args) > 0:
            cmd = command_args[0]
            if cmd == "help":
                sys.stderr.write(usage())
                sys.exit(0)

            if not cmd in halm_commands:
                raise utils.UserReportableException("Unknown command: " + cmd)

            obj = globals()[halm_commands[cmd]["class"]](self.ctx)
            help = obj.help()

            if help:
                sys.stdout.write(help)
                sys.exit(0)
            else:
                sys.stderr.write("No command " + cmd)
                sys.exit(1)


        sys.stdout.write(usage())
        sys.exit(0)


def usage():
    return """usage: hdbalm [options] <command> [<command option>]

  The commands are:
  """ + getCommands() +\
  """

  The options specified in front of the command are supported by all
  commands. The options specified after the command are command-specific
  and are documented in the help texts of the commands.

  The following options are supported by all commands:

    -u <user>, --user=<user>  User name
    -h <host>, --host=<host>  XS engine host
    -p <port>, --port=<port>  XS engine port
    -v, --verbose             Writes debug messages to standard error
    -s, --https               Sends request using https
    -c <certificate>,
       --certs=<certificate>  Certificate file when using https (X509 PEM)
    -y, --yes                 Non-interactive mode (does not prompt for
                              confirmation)
    -j, --json                Prints result in json notation if successful

  The options -u, -h, and -p take precedence over environment variables. The
  program prompts for a password if no password is set in the environment.

  For further command-specific help and a description of command-specific
  options, enter the following:

    hdbalm help <command>

  The following environment variables can be set:

    HDBALM_USER       User name
    HDBALM_PASSWD     Password
    HDBALM_HOST       XS engine host
    HDBALM_PORT       XS engine port
    http_proxy        http proxy (http://<host>:<port>)
    https_proxy       https proxy (http://<host>:<port>)
    no_proxy          do not use proxy for the specified domains

  """ + "hdbalm version " + HDBALM_VERSION + "\n"


messages = {
  "no_args": "Expected command string.",
  "unknown_command": "Unknown command {0}",
  "no_user": "No user name specified",
  "no_port": "No port specified",
  "no_host": "No host specified"
}


def readArguments():

    # ignore command name
    all_args = sys.argv[1:]

    # no arguments, so just print usage
    if len(all_args) == 0:
        sys.stderr.write(usage())
        sys.exit(1)

    # if there is one argument named --version print it
    # and exit
    if len(all_args) == 1 and all_args[0] == '--version':
        sys.stdout.write(HDBALM_VERSION + "\n")
        sys.exit(0)

    halm_args = []
    for i in range(len(all_args)):
        if all_args[i].startswith("-"):
            halm_args.append(all_args[i])
        else:
            if all_args[i] not in halm_commands:
                halm_args.append(all_args[i])
            else:
                break

    all_args = all_args[len(halm_args):]

    if len(all_args) == 0:
        sys.stderr.write(messages["no_args"])
        sys.exit(1)

    command_name = all_args[0]
    if command_name not in halm_commands:
        sys.stderr.write(messages["unknown_command"].format(command_name) + "\n")
        sys.exit(1)

    cmd_args = all_args[1:]

    parser = Context.HALMOptionParser(add_help_option=False)
    parser.disable_interspersed_args()
    parser.add_option("-u", "--user", action="store", type="string", dest="user")
    parser.add_option("-p", "--port", action="store", type="int", dest="port")
    parser.add_option("-h", "--host", action="store", type="string", dest="host")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose")
    parser.add_option("-s", "--https", action="store_true", dest="https")
    parser.add_option("-c", "--certs", action="store", type="string", dest="certs")
    parser.add_option("-y", "--yes", action="store_true", dest="yes")
    parser.add_option("-j", "--json", action="store_true", dest="json")
    parser.add_option("--tunnel", action="store", type="string", dest="tunnel")
    parser.add_option("--localhost-only", action="store_true", dest="localhost_only")
    parser.add_option("--tenant-host", action="store", type="string", dest="tenant_host")
    (options, args) = parser.parse_args(args=halm_args);

    cmd_class = halm_commands[command_name]["class"]

    if options.https and not hasSSL:
        sys.stderr.write(SSL_NOTE)
        sys.exit(Context.Constants.EXIT_ERROR)

    # configure logging
    if (options.verbose):
        logging.basicConfig(level=logging.DEBUG)

    obj = globals()[cmd_class](Context)
    obj.run_command(options, cmd_args)

# ---------------------------------------------------------------------------
# Help Options
# ---------------------------------------------------------------------------

def getCommands():

    # sort commands so we show them in the right order
    #
    ordered_halm_commands = []
    for cmd in halm_commands:
        ordered_halm_commands.append({cmd: halm_commands[cmd]})
    ordered_halm_commands.sort(key=lambda x: x[list(x.keys())[0]]["pos"])
    msg = ""
    for cmddict in ordered_halm_commands:
        cmd = list(cmddict.keys())[0]
        if cmd in hidden_commands:
            continue

        desc = globals()[halm_commands[cmd]["class"]](Context).description()
        name = cmd
        if len(name)+2 < 8:
            msg += "  " + name + "\t\t" + desc + "\n"
        else:
            msg += "  " + name + "\t" + desc + "\n"
    return msg

def checkPythonVersion(versions):
    v = sys.version
    valid = False
    for version in versions:
        if v.startswith(version):
            valid = True
            break
    if not valid:
        raise utils.UserReportableException("Python version {0} is not supported for this script.".format(v))

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def processRequest():

    try:
        checkPythonVersion(["3"])
        readArguments()

    except utils.UserReportableException as e:
        sys.stderr.write(e.__str__()+nl)
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(1)
    except Exception:
        sys.stderr.write("Unknown error:" + os.linesep)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    processRequest()
