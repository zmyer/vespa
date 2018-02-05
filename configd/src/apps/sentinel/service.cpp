// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "service.h"
#include "output-connection.h"
#include <vespa/vespalib/util/stringfmt.h>
#include <vespa/vespalib/util/signalhandler.h>

#include <csignal>
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>

#include <vespa/log/log.h>
LOG_SETUP(".service");
#include <vespa/log/llparser.h>

static bool stop()
{
    return (vespalib::SignalHandler::INT.check() ||
            vespalib::SignalHandler::TERM.check());
}

using vespalib::make_string;

namespace config::sentinel {

namespace {

vespalib::string getVespaTempDir() {
    vespalib::string tmp = getenv("ROOT");
    tmp += "/var/db/vespa/tmp";
    return tmp;
}

}

Service::Service(const SentinelConfig::Service& service, const SentinelConfig::Application& application,
                 std::list<OutputConnection *> &ocs, StartMetrics &metrics)
    : _pid(-1),
      _rawState(READY),
      _state(_rawState),
      _exitStatus(0),
      _config(new SentinelConfig::Service(service)),
      _isAutomatic(true),
      _restartPenalty(0),
      _last_start(0),
      _application(application),
      _outputConnections(ocs),
      _metrics(metrics)
{
    LOG(debug, "%s: created", name().c_str());
    LOG(debug, "autostart: %s", _config->autostart ? "YES" : "NO");
    LOG(debug, "  restart: %s", _config->autorestart ? "YES" : "NO");
    LOG(debug, "  command: %s", _config->command.c_str());
    LOG(debug, " configid: %s", _config->id.c_str());

    if (_config->autostart) {
        start();
    }
}

void
Service::reconfigure(const SentinelConfig::Service& config)
{
    if (config.command != _config->command) {
        LOG(debug, "%s: reconfigured command '%s' -> '%s' - this will "
            "take effect at next restart", name().c_str(),
            _config->command.c_str(), config.command.c_str());
    }
    if (config.autostart != _config->autostart) {
        LOG(debug, "%s: reconfigured autostart %s", name().c_str(),
            config.autostart ? "OFF -> ON" : "ON -> OFF");
    }
    if (config.autorestart != _config->autorestart) {
        LOG(debug, "%s: reconfigured autorestart %s", name().c_str(),
            config.autorestart ? "OFF -> ON" : "ON -> OFF");
    }
    if (config.id != _config->id) {
        LOG(warning, "%s: reconfigured config id '%s' -> '%s' - signaling service restart",
            name().c_str(), _config->id.c_str(), config.id.c_str());
        terminate();
    }

    delete _config;
    _config = new SentinelConfig::Service(config);

    if (_isAutomatic
        && ((_config->autostart && _state == READY)
            || (_config->autorestart && _state == FINISHED)))
    {
        LOG(debug, "%s: Restarting due to new config", name().c_str());
        start();
    }
}

Service::~Service()
{
    terminate(false, false);
    delete _config;
}

int
Service::terminate(bool catchable, bool dumpState)
{
    if (isRunning()) {
        runPreShutdownCommand();
        LOG(debug, "%s: terminate(%s)", name().c_str(), catchable ? "cleanly" : "NOW");
        resetRestartPenalty();
        if (catchable) {
            setState(TERMINATING);
            int ret = kill(_pid, SIGTERM);
            LOG(debug, "%s: kill -SIGTERM %d: %s", name().c_str(), (int)_pid,
                ret == 0 ? "OK" : strerror(errno));
            return ret;
        } else {
            if (dumpState && _state != KILLING) {
                vespalib::string pstackCmd = make_string("pstack %d > %s/%s.pstack.%d",
                                                         _pid, getVespaTempDir().c_str(), name().c_str(), _pid);
                LOG(info, "%s:%d failed to stop. Stack dumped at %s", name().c_str(), _pid, pstackCmd.c_str());
                int pstackRet = system(pstackCmd.c_str());
                if (pstackRet != 0) {
                    LOG(warning, "'%s' failed with return value %d", pstackCmd.c_str(), pstackRet);
                }
            }
            setState(KILLING);
            kill(_pid, SIGCONT); // if it was stopped for some reason
            int ret = kill(_pid, SIGKILL);
            LOG(debug, "%s: kill -SIGKILL %d: %s", name().c_str(), (int)_pid,
                ret == 0 ? "OK" : strerror(errno));
            return ret;
        }
    }

    return 0; // Not running, so all is ok.
}

void
Service::runPreShutdownCommand()
{
    if (_config->preShutdownCommand.length() > 0) {
        LOG(debug, "%s: runPreShutdownCommand(%s)", name().c_str(), _config->preShutdownCommand.c_str());
        runCommand(_config->preShutdownCommand);
    }
}

void
Service::runCommand(const std::string & command)
{
    int ret = system(command.c_str());
    if (ret != 0) {
        LOG(info, "%s: unable to run showdown command (%s): %d (%s)", name().c_str(), command.c_str(), ret, strerror(ret));
    }
}

int
Service::start()
{
    // make sure the service does not restart in a tight loop:
    time_t now = time(0);
    int diff = now - _last_start;
    if (diff < 10) {
        incrementRestartPenalty();
        now += _restartPenalty; // will delay start this much
    }
    _last_start = now;

// make a pipe, close the good ends of it, mark it close-on-exec
// if exec fails, write a complaint on the fd (which will then be read
// by mother program).
//
// Return 0 on success, -1 on failure
    setState(STARTING);

    int pipes[2];
    int err = pipe(pipes);
    int stdoutpipes[2];
    err |= pipe(stdoutpipes);
    int stderrpipes[2];
    err |= pipe(stderrpipes);

    if (err == -1) {
        LOG(error, "%s: Attempted to start, but pipe() failed: %s", name().c_str(),
            strerror(errno));
        setState(FAILED);
        return -1;
    }

    fflush(nullptr);
    _pid = fork();
    if (_pid == -1) {
        LOG(error, "%s: Attempted to start, but fork() failed: %s", name().c_str(),
            strerror(errno));
        setState(FAILED);
        close(pipes[0]);
        close(pipes[1]);
        close(stdoutpipes[0]);
        close(stdoutpipes[1]);
        close(stderrpipes[0]);
        close(stderrpipes[1]);
        return -1;
    }

    if (_pid == 0) {
        close(pipes[0]); // Close reading end
        close(stdoutpipes[0]);
        close(stderrpipes[0]);

        close(1);
        dup2(stdoutpipes[1], 1);
        close(stdoutpipes[1]);

        close(2);
        dup2(stderrpipes[1], 2);
        close(stderrpipes[1]);

        LOG(debug, "%s: Started as pid %d", name().c_str(),
            static_cast<int>(getpid()));
        signal(SIGTERM, SIG_DFL);
        signal(SIGINT, SIG_DFL);
        if (stop()) {
            kill(getpid(), SIGTERM);
        }
        if (_restartPenalty > 0) {
            LOG(debug, "%s: Applying %u sec restart penalty", name().c_str(),
                _restartPenalty);
            sleep(_restartPenalty);
        }
        EV_STARTING(name().c_str());
        runChild(pipes); // This function should not return.
        _exit(EXIT_FAILURE);
    }

    close(pipes[1]); // close writing end
    close(stdoutpipes[1]);
    close(stderrpipes[1]);

    // do not call ensureChildRuns, as the pipe magic did not work as intended
    // This also ensures that the process does not wait while the service process waits in penalty.
    // ensureChildRuns(pipes[0]); // This will wait until the execl goes through
    setState(RUNNING);
    _metrics.currentlyRunningServices++;
    _metrics.sentinel_running.sample(_metrics.currentlyRunningServices);
    close(pipes[0]); // close reading end

    using ns_log::LLParser;
    LLParser *p = new LLParser();
    p->setService(_config->name.c_str());
    p->setComponent("stdout");
    p->setPid(_pid);
    fcntl(stdoutpipes[0], F_SETFL,
          fcntl(stdoutpipes[0], F_GETFL) | O_NONBLOCK);
    OutputConnection *c = new OutputConnection(stdoutpipes[0], p);
    _outputConnections.push_back(c);

    p = new LLParser();
    p->setService(_config->name.c_str());
    p->setComponent("stderr");
    p->setPid(_pid);
    p->setDefaultLevel(ns_log::Logger::warning);
    fcntl(stderrpipes[0], F_SETFL,
          fcntl(stderrpipes[0], F_GETFL) | O_NONBLOCK);
    c = new OutputConnection(stderrpipes[0], p);
    _outputConnections.push_back(c);

    return (_state == RUNNING) ? 0 : -1;
}


// TODO: Garbage collect this, since it did not work as intended when execl'ing /bin/sh
void
Service::ensureChildRuns(int fd)
{
    char buf[200];
    int len;
    do {
        len = read(fd, buf, sizeof buf);
    } while (len == -1 && errno == EINTR);
    if (len > 0) {
        // Failed to do an execl.. pick up the remains
        _exitStatus = 0;
        waitpid(_pid, &_exitStatus, 0);
        setState(FAILED);
    } else {
        setState(RUNNING);
    }
}


void
Service::youExited(int status)
{
    // Someone did a waitpid() and figured out that we exited.
    _exitStatus = status;
    if (WIFEXITED(status)) {
        LOG(debug, "%s: Exited with exit code %d", name().c_str(),
            WEXITSTATUS(status));
        EV_STOPPED(name().c_str(), _pid, WEXITSTATUS(status));
        setState(FINISHED);
    } else if (WIFSIGNALED(status)) {
        bool expectedDeath = (_state == KILLING || _state == TERMINATING
                              || _state == KILLED  || _state == TERMINATED);
        if (expectedDeath) {
            EV_STOPPED(name().c_str(), _pid, WTERMSIG(status));
            LOG(debug, "%s: Exited expectedly by signal %d", name().c_str(),
                WTERMSIG(status));
        } else {
            EV_CRASH(name().c_str(), _pid, WTERMSIG(status));
            setState(FAILED);
        }
    } else if (WIFSTOPPED(status)) {
        LOG(warning, "%s: STOPPED by signal %d!", name().c_str(), WSTOPSIG(status));
        setState(FAILED);
    } else {
        LOG(error, "%s: Weird exit code %d", name().c_str(), status);
        setState(FAILED);
    }
    _metrics.currentlyRunningServices--;
    _metrics.sentinel_running.sample(_metrics.currentlyRunningServices);

    if (_state == TERMINATING) {
        setState(TERMINATED);
    } else if (_state == KILLING) {
        setState(KILLED);
    }
    if (_isAutomatic && _config->autorestart && !stop()) {
        // ### Implement some rate limiting here maybe?
        LOG(debug, "%s: Has autorestart flag, restarting.", name().c_str());
        setState(READY);
        _metrics.totalRestartsCounter++;
        _metrics.totalRestartsLastPeriod++;
        _metrics.sentinel_restarts.add();
        start();
    }
}

void
Service::runChild(int pipes[2])
{
    // child process - this should exec or signal error
    for (int n = 3; n < 1024; ++n) { // Close all open fds on exec()
        fcntl(n, F_SETFD, FD_CLOEXEC);
    }

    // TODO: Garbage collect the clever pipes magic, as it does not work when the execl target is /bin/sh
    fcntl(pipes[1], F_SETFD, FD_CLOEXEC); // close on exec()

    // Set up environment
    setenv("VESPA_SERVICE_NAME", _config->name.c_str(), 1);
    setenv("VESPA_CONFIG_ID", _config->id.c_str(), 1);
    setenv("VESPA_APPLICATION_TENANT", _application.tenant.c_str(), 1);
    setenv("VESPA_APPLICATION_NAME", _application.name.c_str(), 1);
    setenv("VESPA_APPLICATION_ENVIRONMENT", _application.environment.c_str(), 1);
    setenv("VESPA_APPLICATION_REGION", _application.region.c_str(), 1);
    setenv("VESPA_APPLICATION_INSTANCE", _application.instance.c_str(), 1);
    if (_config->affinity.cpuSocket >= 0) {
        setenv("VESPA_AFFINITY_CPU_SOCKET", std::to_string(_config->affinity.cpuSocket).c_str(), 1);
    }
    // ROOT is already set

    // Set up file descriptor 0 (1 and 2 should be setup already)
    close(0);
    int fd = open("/dev/null", O_RDONLY | O_NOCTTY, 0666);
    if (fd != 0) {
        char buf[200];
        snprintf(buf, sizeof buf, "open /dev/null for fd 0: got %d "
                                  "(%s)", fd, strerror(errno));
        (void) write(pipes[1], buf, strlen(buf));
        _exit(EXIT_FAILURE);
    }
    fcntl(0, F_SETFD, 0); // Don't close on exec

    execl("/bin/sh", "/bin/sh", "-c", _config->command.c_str(), nullptr);

    char buf[200];
    snprintf(buf, sizeof buf, "exec error: %s for /bin/sh -c '%s'",
             strerror(errno), _config->command.c_str());
    (void) write(pipes[1], buf, strlen(buf));
    _exit(EXIT_FAILURE);
}

const vespalib::string &
Service::name() const
{
    return _config->name;
}

bool
Service::isRunning() const
{
    switch (_state) {
    case READY:
    case FINISHED:
    case KILLED:
    case TERMINATED:
    case FAILED:
        return false;

    case STARTING:
    case RUNNING:
    case TERMINATING:
    case KILLING:
        return true;
    }
    return true; // this will not be reached
}

void
Service::setAutomatic(bool autoStatus)
{
    _isAutomatic = autoStatus;
    resetRestartPenalty();
}


void
Service::incrementRestartPenalty()
{
    if (_restartPenalty < MAX_RESTART_PENALTY) {
        _restartPenalty++;
    } else {
        _restartPenalty = MAX_RESTART_PENALTY;
    }
}


void
Service::setState(ServiceState state)
{
    if (state != _state) {
        LOG(debug, "%s: %s->%s", name().c_str(), stateName(_state), stateName(state));
        _rawState = state;
    }

    // penalize failed services
    if (state == FAILED) {
        incrementRestartPenalty();
    }
}

const char *
Service::stateName(ServiceState state) const
{
    switch (state) {
    case READY: return "READY";
    case STARTING: return "STARTING";
    case RUNNING: return "RUNNING";
    case TERMINATING: return "TERMINATING";
    case KILLING: return "KILLING";
    case FINISHED: return "FINISHED";
    case TERMINATED: return "TERMINATED";
    case KILLED: return "KILLED";
    case FAILED: return "FAILED";
    }
    return "--BAD--";
}

}
