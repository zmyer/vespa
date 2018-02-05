// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "document_db_maintenance_config.h"

namespace proton {

constexpr double MAX_DELAY_SEC = 300;

DocumentDBPruneConfig::
DocumentDBPruneConfig()
    : _delay(MAX_DELAY_SEC),
      _interval(21600.0),
      _age(1209600.0)
{
}

DocumentDBPruneConfig::
DocumentDBPruneConfig(double interval,
                      double age)
    : _delay(std::min(MAX_DELAY_SEC, interval)),
      _interval(interval),
      _age(age)
{
}

bool
DocumentDBPruneConfig::
operator==(const DocumentDBPruneConfig &rhs) const
{
    return _delay == rhs._delay &&
           _interval == rhs._interval &&
           _age == rhs._age;
}

DocumentDBHeartBeatConfig::DocumentDBHeartBeatConfig()
    : _interval(60.0)
{
}

DocumentDBHeartBeatConfig::DocumentDBHeartBeatConfig(double interval)
    : _interval(interval)
{
}

bool
DocumentDBHeartBeatConfig::
operator==(const DocumentDBHeartBeatConfig &rhs) const
{
    return _interval == rhs._interval;
}

DocumentDBLidSpaceCompactionConfig::DocumentDBLidSpaceCompactionConfig()
    : _delay(MAX_DELAY_SEC),
      _interval(3600),
      _allowedLidBloat(1000000000),
      _allowedLidBloatFactor(1.0),
      _disabled(false),
      _maxDocsToScan(10000)
{
}

DocumentDBLidSpaceCompactionConfig::DocumentDBLidSpaceCompactionConfig(double interval,
                                                                       uint32_t allowedLidBloat,
                                                                       double allowedLidBloatFactor,
                                                                       bool disabled,
                                                                       uint32_t maxDocsToScan)
    : _delay(std::min(MAX_DELAY_SEC, interval)),
      _interval(interval),
      _allowedLidBloat(allowedLidBloat),
      _allowedLidBloatFactor(allowedLidBloatFactor),
      _disabled(disabled),
      _maxDocsToScan(maxDocsToScan)
{
}

DocumentDBLidSpaceCompactionConfig
DocumentDBLidSpaceCompactionConfig::createDisabled()
{
    DocumentDBLidSpaceCompactionConfig result;
    result._disabled = true;
    return result;
}

bool
DocumentDBLidSpaceCompactionConfig::operator==(const DocumentDBLidSpaceCompactionConfig &rhs) const
{
   return _delay == rhs._delay &&
           _interval == rhs._interval &&
           _allowedLidBloat == rhs._allowedLidBloat &&
           _allowedLidBloatFactor == rhs._allowedLidBloatFactor &&
           _disabled == rhs._disabled;
}


BlockableMaintenanceJobConfig::BlockableMaintenanceJobConfig()
    : _resourceLimitFactor(1.0),
      _maxOutstandingMoveOps(10)
{}

BlockableMaintenanceJobConfig::BlockableMaintenanceJobConfig(double resourceLimitFactor,
                                                             uint32_t maxOutstandingMoveOps)
    : _resourceLimitFactor(resourceLimitFactor),
      _maxOutstandingMoveOps(maxOutstandingMoveOps)
{}

bool
BlockableMaintenanceJobConfig::operator==(const BlockableMaintenanceJobConfig &rhs) const
{
    return _resourceLimitFactor == rhs._resourceLimitFactor &&
           _maxOutstandingMoveOps == rhs._maxOutstandingMoveOps;
}

DocumentDBMaintenanceConfig::DocumentDBMaintenanceConfig()
    : _pruneRemovedDocuments(),
      _heartBeat(),
      _sessionCachePruneInterval(900.0),
      _visibilityDelay(0),
      _lidSpaceCompaction(),
      _attributeUsageFilterConfig(),
      _attributeUsageSampleInterval(60.0),
      _blockableJobConfig(),
      _flushConfig()
{
}

DocumentDBMaintenanceConfig::
DocumentDBMaintenanceConfig(const DocumentDBPruneRemovedDocumentsConfig &
                            pruneRemovedDocuments,
                            const DocumentDBHeartBeatConfig &heartBeat,
                            double groupingSessionPruneInterval,
                            fastos::TimeStamp visibilityDelay,
                            const DocumentDBLidSpaceCompactionConfig &lidSpaceCompaction,
                            const AttributeUsageFilterConfig &attributeUsageFilterConfig,
                            double attributeUsageSampleInterval,
                            const BlockableMaintenanceJobConfig &blockableJobConfig,
                            const DocumentDBFlushConfig &flushConfig)
    : _pruneRemovedDocuments(pruneRemovedDocuments),
      _heartBeat(heartBeat),
      _sessionCachePruneInterval(groupingSessionPruneInterval),
      _visibilityDelay(visibilityDelay),
      _lidSpaceCompaction(lidSpaceCompaction),
      _attributeUsageFilterConfig(attributeUsageFilterConfig),
      _attributeUsageSampleInterval(attributeUsageSampleInterval),
      _blockableJobConfig(blockableJobConfig),
      _flushConfig(flushConfig)
{
}

bool
DocumentDBMaintenanceConfig::
operator==(const DocumentDBMaintenanceConfig &rhs) const
{
    return _pruneRemovedDocuments == rhs._pruneRemovedDocuments &&
        _heartBeat == rhs._heartBeat &&
        _sessionCachePruneInterval == rhs._sessionCachePruneInterval &&
        _visibilityDelay == rhs._visibilityDelay &&
        _lidSpaceCompaction == rhs._lidSpaceCompaction &&
        _attributeUsageFilterConfig == rhs._attributeUsageFilterConfig &&
        _attributeUsageSampleInterval == rhs._attributeUsageSampleInterval &&
        _blockableJobConfig == rhs._blockableJobConfig &&
        _flushConfig == rhs._flushConfig;
}

} // namespace proton
