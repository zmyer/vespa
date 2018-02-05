// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "cf-handler.h"
#include <dirent.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <vespa/defaults.h>
#include <vespa/config/common/configsystem.h>
#include <vespa/config/common/exceptions.h>

#include <vespa/log/log.h>
LOG_SETUP(".cf-handler");

CfHandler::CfHandler() : _subscriber() {}

CfHandler::~CfHandler()
{
}

void
CfHandler::subscribe(const std::string & configId, uint64_t timeoutMS)
{
    _handle = _subscriber.subscribe<LogforwarderConfig>(configId, timeoutMS);
}

namespace {

bool fixDir(const vespalib::string &path) {
    if (path.size() == 0) return true;
    size_t lastSlash = path.rfind('/');
    if (lastSlash != vespalib::string::npos) {
        vespalib::string parent = path.substr(0, lastSlash);
        if (!fixDir(parent)) return false;
    }
    DIR *dp = opendir(path.c_str());
    if (dp == NULL) {
        if (errno != ENOENT || mkdir(path.c_str(), 0755) != 0) {
            perror(path.c_str());
            return false;
        }
    } else {
        closedir(dp);
    }
    return true;
}

vespalib::string
cfFilePath(const vespalib::string &parent) {
    vespalib::string path = parent + "/etc/system/local";
    fixDir(path);
    path += "/deploymentclient.conf";
    return path;
}

} // namespace <unnamed>

void
CfHandler::doConfigure()
{
    std::unique_ptr<LogforwarderConfig> cfg(_handle->getConfig());
    const LogforwarderConfig& config(*cfg);

    vespalib::string path = cfFilePath(config.splunkHome);
    vespalib::string tmpPath = path + ".new";
    FILE *fp = fopen(tmpPath.c_str(), "w");
    if (fp == NULL) return;

    fprintf(fp, "[deployment-client]\n");
    fprintf(fp, "clientName = %s\n", config.clientName.c_str());
    fprintf(fp, "\n");
    fprintf(fp, "[target-broker:deploymentServer]\n");
    fprintf(fp, "targetUri = %s\n", config.deploymentServer.c_str());

    fclose(fp);
    rename(tmpPath.c_str(), path.c_str());

    if (config.clientName.size() == 0 ||
        config.deploymentServer.size() == 0)
    {
        childHandler.stopChild(config.splunkHome);
    } else {
        childHandler.startChild(config.splunkHome);
    }
}

void
CfHandler::check()
{
    if (_subscriber.nextConfig(0)) {
        doConfigure();
    }
}

constexpr uint64_t CONFIG_TIMEOUT_MS = 30 * 1000;

void
CfHandler::start(const char *configId)
{
    LOG(debug, "Reading configuration with id '%s'", configId);
    try {
        subscribe(configId, CONFIG_TIMEOUT_MS);
    } catch (config::ConfigTimeoutException & ex) {
        LOG(warning, "Timout getting config, please check your setup. Will exit and restart: %s", ex.getMessage().c_str());
        exit(EXIT_FAILURE);
    } catch (config::InvalidConfigException& ex) {
        LOG(error, "Fatal: Invalid configuration, please check your setup: %s", ex.getMessage().c_str());
        exit(EXIT_FAILURE);
    } catch (config::ConfigRuntimeException& ex) {
        LOG(error, "Fatal: Could not get config, please check your setup: %s", ex.getMessage().c_str());
        exit(EXIT_FAILURE);
    }
}
