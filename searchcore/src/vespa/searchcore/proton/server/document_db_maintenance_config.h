// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#pragma once

#include <vespa/vespalib/stllike/string.h>
#include <vespa/searchcore/proton/attribute/attribute_usage_filter_config.h>
#include <vespa/fastos/timestamp.h>
#include "document_db_flush_config.h"

namespace proton {

class DocumentDBPruneConfig
{
private:
    double _delay;
    double _interval;
    double _age;

public:
    DocumentDBPruneConfig();
    DocumentDBPruneConfig(double interval, double age);

    bool operator==(const DocumentDBPruneConfig &rhs) const;
    double getDelay() const { return _delay; }
    double getInterval() const { return _interval; }
    double getAge() const { return _age; }
};

typedef DocumentDBPruneConfig DocumentDBPruneRemovedDocumentsConfig;

class DocumentDBHeartBeatConfig
{
private:
    double _interval;

public:
    DocumentDBHeartBeatConfig();
    DocumentDBHeartBeatConfig(double interval);

    bool operator==(const DocumentDBHeartBeatConfig &rhs) const;
    double getInterval() const { return _interval; }
};

class DocumentDBLidSpaceCompactionConfig
{
private:
    double   _delay;
    double   _interval;
    uint32_t _allowedLidBloat;
    double   _allowedLidBloatFactor;
    bool     _disabled;
    uint32_t _maxDocsToScan;

public:
    DocumentDBLidSpaceCompactionConfig();
    DocumentDBLidSpaceCompactionConfig(double interval,
                                       uint32_t allowedLidBloat,
                                       double allowwedLidBloatFactor,
                                       bool disabled = false,
                                       uint32_t maxDocsToScan = 10000);

    static DocumentDBLidSpaceCompactionConfig createDisabled();
    bool operator==(const DocumentDBLidSpaceCompactionConfig &rhs) const;
    double getDelay() const { return _delay; }
    double getInterval() const { return _interval; }
    uint32_t getAllowedLidBloat() const { return _allowedLidBloat; }
    double getAllowedLidBloatFactor() const { return _allowedLidBloatFactor; }
    bool isDisabled() const { return _disabled; }
    uint32_t getMaxDocsToScan() const { return _maxDocsToScan; }
};

class BlockableMaintenanceJobConfig {
private:
    double _resourceLimitFactor;
    uint32_t _maxOutstandingMoveOps;

public:
    BlockableMaintenanceJobConfig();
    BlockableMaintenanceJobConfig(double resourceLimitFactor,
                                  uint32_t maxOutstandingMoveOps);
    bool operator==(const BlockableMaintenanceJobConfig &rhs) const;
    double getResourceLimitFactor() const { return _resourceLimitFactor; }
    uint32_t getMaxOutstandingMoveOps() const { return _maxOutstandingMoveOps; }
};

class DocumentDBMaintenanceConfig
{
public:
    typedef std::shared_ptr<DocumentDBMaintenanceConfig> SP;

private:
    DocumentDBPruneRemovedDocumentsConfig _pruneRemovedDocuments;
    DocumentDBHeartBeatConfig             _heartBeat;
    double                                _sessionCachePruneInterval;
    fastos::TimeStamp                     _visibilityDelay;
    DocumentDBLidSpaceCompactionConfig    _lidSpaceCompaction;
    AttributeUsageFilterConfig            _attributeUsageFilterConfig;
    double                                _attributeUsageSampleInterval;
    BlockableMaintenanceJobConfig         _blockableJobConfig;
    DocumentDBFlushConfig                 _flushConfig;

public:
    DocumentDBMaintenanceConfig();

    DocumentDBMaintenanceConfig(const DocumentDBPruneRemovedDocumentsConfig &pruneRemovedDocuments,
                                const DocumentDBHeartBeatConfig &heartBeat,
                                double sessionCachePruneInterval,
                                fastos::TimeStamp visibilityDelay,
                                const DocumentDBLidSpaceCompactionConfig &lidSpaceCompaction,
                                const AttributeUsageFilterConfig &attributeUsageFilterConfig,
                                double attributeUsageSampleInterval,
                                const BlockableMaintenanceJobConfig &blockableJobConfig,
                                const DocumentDBFlushConfig &flushConfig);

    bool
    operator==(const DocumentDBMaintenanceConfig &rhs) const;

    const DocumentDBPruneRemovedDocumentsConfig &getPruneRemovedDocumentsConfig() const {
        return _pruneRemovedDocuments;
    }
    const DocumentDBHeartBeatConfig &getHeartBeatConfig() const {
        return _heartBeat;
    }
    double getSessionCachePruneInterval() const {
        return _sessionCachePruneInterval;
    }
    fastos::TimeStamp getVisibilityDelay() const { return _visibilityDelay; }
    const DocumentDBLidSpaceCompactionConfig &getLidSpaceCompactionConfig() const {
        return _lidSpaceCompaction;
    }
    const AttributeUsageFilterConfig &getAttributeUsageFilterConfig() const {
        return _attributeUsageFilterConfig;
    }
    double getAttributeUsageSampleInterval() const {
        return _attributeUsageSampleInterval;
    }
    const BlockableMaintenanceJobConfig &getBlockableJobConfig() const {
        return _blockableJobConfig;
    }
    const DocumentDBFlushConfig &getFlushConfig() const { return _flushConfig; }
};

} // namespace proton

