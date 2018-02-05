// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "domainpart.h"
#include <vespa/vespalib/util/crc.h>
#include <vespa/vespalib/xxhash/xxhash.h>
#include <vespa/vespalib/util/stringfmt.h>
#include <vespa/vespalib/data/fileheader.h>
#include <vespa/searchlib/common/fileheadercontext.h>
#include <vespa/fastlib/io/bufferedfile.h>

#include <vespa/log/log.h>
LOG_SETUP(".transactionlog.domainpart");

using vespalib::make_string;
using vespalib::FileHeader;
using vespalib::string;
using vespalib::getLastErrorString;
using vespalib::IllegalHeaderException;
using vespalib::LockGuard;
using vespalib::nbostream;
using vespalib::nbostream_longlivedbuf;
using vespalib::alloc::Alloc;
using search::common::FileHeaderContext;
using std::runtime_error;

namespace search::transactionlog {

namespace {

void
handleSync(FastOS_FileInterface &file) __attribute__ ((noinline));

string
handleWriteError(const char *text,
                 FastOS_FileInterface &file,
                 int64_t lastKnownGoodPos,
                 const Packet::Entry &entry,
                 int bufLen) __attribute__ ((noinline));

bool
handleReadError(const char *text,
                FastOS_FileInterface &file,
                ssize_t len,
                ssize_t rlen,
                int64_t lastKnownGoodPos,
                bool allowTruncate) __attribute__ ((noinline));

bool
addPacket(Packet &packet,
          const Packet::Entry &e) __attribute__ ((noinline));

bool
tailOfFileIsZero(FastOS_FileInterface &file, int64_t lastKnownGoodPos) __attribute__ ((noinline));

bool
addPacket(Packet &packet, const Packet::Entry &e)
{
    LOG(spam, "Adding serial #%" PRIu64 ", of type %d and size %zd into packet of size %zu and %zu bytes",
              e.serial(), e.type(), e.data().size(), packet.size(), packet.sizeBytes());
    return ! packet.add(e);
}

void
handleSync(FastOS_FileInterface &file)
{
    if ( file.IsOpened() && ! file.Sync() ) {
        int osError = errno;
        throw runtime_error(make_string("Failed to synchronize file '%s' of size %" PRId64 " due to '%s'. "
                                        "Does not know how to handle this so throwing an exception.",
                                        file.GetFileName(), file.GetSize(), FastOS_File::getErrorString(osError).c_str()));
    }
}

string
handleWriteError(const char *text,
                 FastOS_FileInterface &file,
                 int64_t lastKnownGoodPos,
                 const Packet::Entry &entry,
                 int bufLen)
{
    string last(FastOS_File::getLastErrorString());
    string e(make_string("%s. File '%s' at position %" PRId64 " for entry %" PRIu64 " of length %u. "
                         "OS says '%s'. Rewind to last known good position %" PRId64 ".",
                         text, file.GetFileName(), file.GetPosition(), entry.serial(), bufLen,
                         last.c_str(), lastKnownGoodPos));
    LOG(error, "%s",  e.c_str());
    if ( ! file.SetPosition(lastKnownGoodPos) ) {
        last = FastOS_File::getLastErrorString();
        throw runtime_error(make_string("Failed setting position %" PRId64 " of file '%s' of size %" PRId64 ": OS says '%s'",
                                        lastKnownGoodPos, file.GetFileName(), file.GetSize(), last.c_str()));
    }
    handleSync(file);
    return e;
}

string
getError(FastOS_FileInterface & f)
{
    return make_string("File '%s' of size %ld has last error of '%s'.",
                       f.GetFileName(), f.GetSize(), FastOS_File::getLastErrorString().c_str());
}

bool
tailOfFileIsZero(FastOS_FileInterface &file, int64_t lastKnownGoodPos)
{
    ssize_t rest(file.GetSize() - lastKnownGoodPos);
    if (rest < 0 || rest > 0x100000) {
        return false;
    }
    std::vector<char> buf(rest, 0);
    file.ReadBuf(&buf[0], buf.size(), lastKnownGoodPos);
    for (char c : buf) {
        if (c != 0) {
            return false;
        }
    }
    return true;
}

bool
handleReadError(const char *text,
                FastOS_FileInterface &file,
                ssize_t len,
                ssize_t rlen,
                int64_t lastKnownGoodPos,
                bool allowTruncate)
{
    bool retval(true);
    if (rlen != -1) {
        string e;
        if (len == rlen) {
            e = make_string("Error in data read of size %zd bytes at pos %" PRId64 " trying to read %s. ",
                             len, file.GetPosition() - rlen, text);
        } else {
            e = make_string("Short Read. Got only %zd of %zd bytes at pos %" PRId64 " trying to read %s. ",
                             rlen, len, file.GetPosition() - rlen, text);
        }
        e += getError(file);
        if (!allowTruncate) {
            LOG(error, "%s", e.c_str());
            throw runtime_error(e);
        }
        // Short read. Log error, Truncate, continue.
        e += make_string(" Truncate to %" PRId64 " and continue", lastKnownGoodPos);
        LOG(error, "%s", e.c_str());
        FastOS_File truncateFile(file.GetFileName());
        file.Close();
        if ( truncateFile.OpenWriteOnlyExisting()) {
            if (truncateFile.SetSize(lastKnownGoodPos)) {
                if (truncateFile.Close()) {
                    if (file.OpenReadOnly()) {
                        if (file.SetPosition(lastKnownGoodPos)) {
                            retval = false;
                        } else {
                            throw runtime_error(make_string("Failed setting position %" PRId64 ". %s", lastKnownGoodPos, getError(file).c_str()));
                        }
                    } else {
                        throw runtime_error(make_string("Failed reopening file after truncate: %s", getError(file).c_str()));
                    }
                } else {
                    throw runtime_error(make_string("Failed closing truncated file: %s", getError(truncateFile).c_str()));
                }
            } else {
                throw runtime_error(make_string("Failed truncating to %" PRId64 ": %s", lastKnownGoodPos, getError(truncateFile).c_str()));
            }
        } else {
            throw runtime_error(make_string("Failed opening for truncating: %s", getError(file).c_str()));
        }
    } else {
        // Some kind of IO error throw exception.
        string errString = FastOS_File::getLastErrorString();
        throw runtime_error(make_string("IO error when reading %zd bytes at pos %" PRId64 "trying to read %s."
                                        " Last known good position is %" PRId64 ": %s",
                                        len, file.GetPosition(), text, lastKnownGoodPos, getError(file).c_str()));
    }
    return retval;
}

}

int64_t
DomainPart::buildPacketMapping(bool allowTruncate)
{
    Fast_BufferedFile transLog;
    transLog.EnableDirectIO();
    if ( ! transLog.OpenReadOnly(_transLog->GetFileName())) {
        throw runtime_error(make_string("Failed opening '%s' for buffered readinf with direct io.", transLog.GetFileName()));
    }
    int64_t fSize(transLog.GetSize());
    int64_t currPos(0);
    try {
        FileHeader header;
        _headerLen = header.readFile(transLog);
        transLog.SetPosition(_headerLen);
        currPos = _headerLen;
    } catch (const IllegalHeaderException &e) {
        transLog.SetPosition(0);
        try {
            FileHeader::FileReader fr(transLog);
            uint32_t header2Len = FileHeader::readSize(fr);
            if (header2Len <= fSize)
                e.throwSelf(); // header not truncated
        } catch (const IllegalHeaderException &e2) {
        }
        if (fSize > 0) {
            // Truncate file (dropping header) if cannot even read
            // header length, or if header has been truncated.
            handleReadError("file header", transLog, 0, FileHeader::getMinSize(), 0, allowTruncate);
        }
    }
    while ((currPos < fSize)) {
        Packet packet;
        SerialNum firstSerial(0);
        SerialNum lastSerial(0);
        int64_t firstPos(currPos);
        bool full(false);
        Alloc buf;
        for(size_t i(0); !full && (currPos < fSize); i++) {
            Packet::Entry e;
            if (read(transLog, e, buf, allowTruncate)) {
                if (e.valid()) {
                    if (i == 0) {
                        firstSerial = e.serial();
                        if (currPos == _headerLen) {
                            _range.from(firstSerial);
                        }
                    }
                    try {
                        full = addPacket(packet, e);
                        if ( ! full ) {
                            lastSerial = e.serial();
                            currPos = transLog.GetPosition();
                            _sz++;
                        } else {
                            transLog.SetPosition(currPos);
                        }
                    } catch (const std::exception & ex) {
                        throw runtime_error(make_string("%s : Failed creating packet for list %s(%" PRIu64 ") at pos(%" PRIu64 ", %" PRIu64 ")",
                                                    ex.what(), transLog.GetFileName(), fSize, currPos, transLog.GetPosition()));
                    }
                } else {
                    throw runtime_error(make_string("Invalid entry reading file %s(%" PRIu64 ") at pos(%" PRIu64 ", %" PRIu64 ")",
                                                transLog.GetFileName(), fSize, currPos, transLog.GetPosition()));
                }
            } else {
                if (transLog.GetSize() != fSize) {
                    fSize = transLog.GetSize();
                } else {
                    throw runtime_error(make_string("Failed reading file %s(%" PRIu64 ") at pos(%" PRIu64 ", %" PRIu64 ")",
                                                transLog.GetFileName(), fSize, currPos, transLog.GetPosition()));
                }
            }
        }
        packet.close();
        if (!packet.empty()) {
            _packets[firstSerial] = packet;
            _range.to(lastSerial);
            {
                LockGuard guard(_lock);
                _skipList.push_back(SkipInfo(firstSerial, firstPos));
            }
        }
    }
    transLog.Close();
    return currPos;
}

DomainPart::DomainPart(const string & name, const string & baseDir, SerialNum s, Crc defaultCrc,
                       const FileHeaderContext &fileHeaderContext, bool allowTruncate) :
    _defaultCrc(defaultCrc),
    _lock(),
    _fileLock(),
    _range(s),
    _sz(0),
    _byteSize(0),
    _packets(),
    _fileName(make_string("%s/%s-%016" PRIu64, baseDir.c_str(), name.c_str(), s)),
    _transLog(std::make_unique<FastOS_File>(_fileName.c_str())),
    _skipList(),
    _headerLen(0),
    _writeLock(),
    _writtenSerial(0),
    _syncedSerial(0)
{
    if (_transLog->OpenReadOnly()) {
        int64_t currPos = buildPacketMapping(allowTruncate);
        if ( ! _transLog->Close() ) {
            throw runtime_error(make_string("Failed closing file '%s' after reading.", _transLog->GetFileName()));
        }
        if ( ! _transLog->OpenWriteOnlyExisting() ) {
            string e(make_string("Failed opening existing file '%s' for writing: %s", _transLog->GetFileName(), getLastErrorString().c_str()));
            LOG(error, "%s", e.c_str());
            throw runtime_error(e);
        }
        if (currPos == 0) {
            // Previous header was truncated.  Write new one.
            writeHeader(fileHeaderContext);
            currPos = _headerLen;
        }
        _byteSize = currPos;
    } else {
        if ( ! _transLog->OpenWriteOnly()) {
            string e(make_string("Failed opening new file '%s' for writing: '%s'", _transLog->GetFileName(), getLastErrorString().c_str()));

            LOG(error, "%s", e.c_str());
            throw runtime_error(e);
        }
        writeHeader(fileHeaderContext);
        _byteSize = _headerLen;
    }
    if ( ! _transLog->SetPosition(_transLog->GetSize()) ) {
        throw runtime_error(make_string("Failed moving write pointer to the end of the file %s(%" PRIu64 ").",
                                        _transLog->GetFileName(), _transLog->GetSize()));
    }
    handleSync(*_transLog);
    _writtenSerial = _range.to();
    _syncedSerial = _writtenSerial;
}

DomainPart::~DomainPart()
{
    close();
}

void
DomainPart::writeHeader(const FileHeaderContext &fileHeaderContext)
{
    typedef vespalib::GenericHeader::Tag Tag;
    FileHeader header;
    assert(_transLog->IsOpened());
    assert(_transLog->IsWriteMode());
    assert(_transLog->GetPosition() == 0);
    fileHeaderContext.addTags(header, _transLog->GetFileName());
    header.putTag(Tag("desc", "Transaction log domain part file"));
    _headerLen = header.writeFile(*_transLog);
}

bool
DomainPart::close()
{
    bool retval(false);
    {
        LockGuard guard(_fileLock);
        /*
         * Sync old domainpart before starting writing new, to avoid
         * hole.  XXX: Feed latency spike due to lack of delayed open
         * for new domainpart.
         */
        handleSync(*_transLog);
        _transLog->dropFromCache();
        retval = _transLog->Close();
        LockGuard wguard(_writeLock);
        _syncedSerial = _writtenSerial;
    }
    if ( ! retval ) {
        throw runtime_error(make_string("Failed closing file '%s' of size %" PRId64 ".",
                                        _transLog->GetFileName(), _transLog->GetSize()));
    }
    {
        LockGuard guard(_lock);
        _packets.clear();
    }
    return retval;
}

bool
DomainPart::isClosed() const {
    return ! _transLog->IsOpened();
}

bool
DomainPart::openAndFind(FastOS_FileInterface &file, const SerialNum &from)
{
    bool retval(file.OpenReadOnly(_transLog->GetFileName()));
    if (retval) {
        int64_t pos(_headerLen);
        LockGuard guard(_lock);
        for(SkipList::const_iterator it(_skipList.begin()), mt(_skipList.end());
            (it < mt) && (it->id() <= from);
            it++)
        {
            pos = it->filePos();
        }
        retval = file.SetPosition(pos);
    }
    return retval;
}

bool
DomainPart::erase(SerialNum to)
{
    bool retval(true);
    if (to > _range.to()) {
        close();
        _transLog->Delete();
    } else {
        _range.from(std::max(to, _range.from()));
    }
    return retval;
}

void
DomainPart::commit(SerialNum firstSerial, const Packet &packet)
{
    int64_t firstPos(_transLog->GetPosition());
    nbostream_longlivedbuf h(packet.getHandle().c_str(), packet.getHandle().size());
    if (_range.from() == 0) {
        _range.from(firstSerial);
    }
    for (size_t i(0); h.size() > 0; i++) {
        //LOG(spam,
        //"Pos(%d) Len(%d), Lim(%d), Remaining(%d)",
        //h.getPos(), h.getLength(), h.getLimit(), h.getRemaining());
        Packet::Entry entry;
        entry.deserialize(h);
        if (_range.to() < entry.serial()) {
            write(*_transLog, entry);
            _sz++;
            _range.to(entry.serial());
        } else {
            throw runtime_error(make_string("Incomming serial number(%ld) must be bigger than the last one (%ld).",
                                            entry.serial(), _range.to()));
        }
    }

    bool merged(false);
    LockGuard guard(_lock);
    if ( ! _packets.empty() ) {
        Packet & lastPacket = _packets.rbegin()->second;
        if (lastPacket.sizeBytes() < 0xf000) {
            if ( ! (merged = lastPacket.merge(packet)) ) {
                LOG(error, "Failed merging packet [%" PRIu64 ", %" PRIu64 "] with [%" PRIu64 ", %" PRIu64 "]",
                           lastPacket.range().from(), lastPacket.range().to(),
                           packet.range().from(), packet.range().to());
            }
        }
    }
    if (! merged ) {
        _packets[firstSerial] = packet;
        _skipList.push_back(SkipInfo(firstSerial, firstPos));
    }
}

void DomainPart::sync()
{
    SerialNum syncSerial(0);
    {
        LockGuard guard(_writeLock);
        syncSerial = _writtenSerial;
    }
    LockGuard guard(_fileLock);
    handleSync(*_transLog);
    LockGuard wguard(_writeLock);
    if (_syncedSerial < syncSerial) {
        _syncedSerial = syncSerial;
    }
}

bool
DomainPart::visit(SerialNumRange &r, Packet &packet)
{
    bool retval(false);
    LockGuard guard(_lock);
    LOG(debug, "Visit r(%" PRIu64 ", %" PRIu64 "] Checking %" PRIu64 " packets",
               r.from(), r.to(), uint64_t(_packets.size()));
    if ( ! isClosed() ) {
        PacketList::const_iterator start(_packets.lower_bound(r.from() + 1));
        PacketList::const_iterator end(_packets.upper_bound(r.to()));
        if (start != _packets.end()) {
            if ( ! start->second.range().contains(r.from() + 1) &&
                (start != _packets.begin())) {
                PacketList::const_iterator prev(start);
                prev--;
                if (prev->second.range().contains(r.from() + 1)) {
                    start--;
                }
            }
        } else {
            if (!_packets.empty())
                start--;
        }
        if ( start != _packets.end() && start->first <= r.to()) {
            PacketList::const_iterator next(start);
            next++;
            if ((r.from() < start->first) &&
                ((next != end) || ((next != _packets.end()) && ((r.to() + 1) == next->first))))
            {
                packet = start->second;
                LOG(debug, "Visit whole packet[%" PRIu64 ", %" PRIu64 "]", packet.range().from(), packet.range().to());
                if (next != _packets.end()) {
                    r.from(next->first - 1);
                    retval = true;
                } else {
                    /// This is the very last package. Can safely finish.
                }
            } else {
                const nbostream & tmp = start->second.getHandle();
                nbostream_longlivedbuf h(tmp.c_str(), tmp.size());
                LOG(debug, "Visit partial[%" PRIu64 ", %" PRIu64 "] (%zd, %zd, %zd)",
                           start->second.range().from(), start->second.range().to(), h.rp(), h.size(), h.capacity());
                Packet newPacket(h.size());
                for (; (h.size() > 0) && (r.from() < r.to()); ) {
                    Packet::Entry e;
                    e.deserialize(h);
                    if (r.from() < e.serial()) {
                        if (e.serial() <= r.to()) {
                            LOG(spam, "Adding serial #%" PRIu64 ", of type %d and size %zd into packet of size %zu and %zu bytes",
                                      e.serial(), e.type(), e.data().size(), newPacket.size(), newPacket.sizeBytes());
                            if (newPacket.add(e)) {
                                r.from(e.serial());
                            } else {
                                throw runtime_error("Could not add entry to packet. Here is some mumbo jumbo. Fix.");
                            }
                        } else {
                            // Force breakout on visiting empty interval.
                            r.from(r.to());
                        }
                    }
                }
                newPacket.close();
                packet = newPacket;
                retval = next != _packets.end();
            }
        } else {
            packet.close();
        }
    } else {
        /// File has been closed must continue from file.
        retval = true;
    }
    return retval;
}


bool
DomainPart::visit(FastOS_FileInterface &file, SerialNumRange &r, Packet &packet)
{
    bool retval(true);
    if ( ! file.IsOpened() ) {
        retval = openAndFind(file, r.from() + 1);
    }
    if (retval) {
        Packet newPacket;
        Alloc buf;
        for (bool full(false);!full && retval && (r.from() < r.to());) {
            Packet::Entry e;
            int64_t fPos = file.GetPosition();
            retval = read(file, e, buf, false);
            if (retval &&
                e.valid() &&
                (r.from() < e.serial()) &&
                (e.serial() <= r.to())) {
                try {
                    full = addPacket(newPacket, e);
                } catch (const std::exception & ex) {
                    throw runtime_error(make_string("%s : Failed creating packet for visit %s(%" PRIu64 ") at pos(%" PRIu64 ", %" PRIu64 ")",
                                                    ex.what(), file.GetFileName(), file.GetSize(), fPos, file.GetPosition()));
                }
                if ( !full ) {
                    r.from(e.serial());
                } else {
                    if ( ! file.SetPosition(fPos) ) {
                        throw runtime_error(make_string("Failed setting read position for file '%s' of size %" PRId64 " from %" PRId64 " to %" PRId64 ".",
                                                        file.GetFileName(), file.GetSize(), file.GetPosition(), fPos));
                    }
                }
            }
        }
        newPacket.close();
        packet = newPacket;
    }

    return retval;
}

void
DomainPart::write(FastOS_FileInterface &file, const Packet::Entry &entry)
{
    int64_t lastKnownGoodPos(file.GetPosition());
    int32_t crc(0);
    uint32_t len(entry.serializedSize() + sizeof(crc));
    nbostream os;
    os << static_cast<uint8_t>(_defaultCrc);
    os << len;
    size_t start(os.size());
    entry.serialize(os);
    size_t end(os.size());
    crc = calcCrc(_defaultCrc, os.c_str()+start, end - start);
    os << crc;
    size_t osSize = os.size();
    assert(osSize == len + sizeof(len) + sizeof(uint8_t));

    LockGuard guard(_writeLock);
    if ( ! file.CheckedWrite(os.c_str(), osSize) ) {
        throw runtime_error(handleWriteError("Failed writing the entry.", file, lastKnownGoodPos, entry, end - start));
    }
    _writtenSerial = entry.serial();
    _byteSize.store(lastKnownGoodPos + osSize, std::memory_order_release);
}

bool
DomainPart::read(FastOS_FileInterface &file,
                 Packet::Entry &entry,
                 Alloc & buf,
                 bool allowTruncate)
{
    bool retval(true);
    char tmp[5];
    int64_t lastKnownGoodPos(file.GetPosition());
    size_t rlen = file.Read(tmp, sizeof(tmp));
    nbostream his(tmp, sizeof(tmp));
    uint8_t version(-1);
    uint32_t len(0);
    his >> version >> len;
    if ((retval = (rlen == sizeof(tmp)))) {
        if ( ! (retval = (version == ccitt_crc32) || version == xxh64)) {
            string msg(make_string("Version mismatch. Expected 'ccitt_crc32=1' or 'xxh64=2',"
                                             " got %d from '%s' at position %ld",
                                             version, file.GetFileName(), lastKnownGoodPos));
            if ((version == 0) && (len == 0) && tailOfFileIsZero(file, lastKnownGoodPos)) {
                LOG(warning, "%s", msg.c_str());
                return handleReadError("packet version", file, sizeof(tmp), rlen, lastKnownGoodPos, allowTruncate);
            } else {
                throw runtime_error(msg);
            }
        }
        if (len > buf.size()) {
            Alloc::alloc(len).swap(buf);
        }
        rlen = file.Read(buf.get(), len);
        retval = rlen == len;
        if (!retval) {
            retval = handleReadError("packet blob", file, len, rlen, lastKnownGoodPos, allowTruncate);
        } else {
            nbostream_longlivedbuf is(buf.get(), len);
            entry.deserialize(is);
            int32_t crc(0);
            is >> crc;
            int32_t crcVerify(calcCrc(static_cast<Crc>(version), buf.get(), len - sizeof(crc)));
            if (crc != crcVerify) {
                throw runtime_error(make_string("Got bad crc for packet from '%s' (len pos=%" PRId64 ", len=%d) : crcVerify = %d, expected %d",
                                                file.GetFileName(), file.GetPosition() - len - sizeof(len),
                                                static_cast<int>(len), static_cast<int>(crcVerify), static_cast<int>(crc)));
            }
        }
    } else {
        if (rlen == 0) {
           // Eof
        } else {
           retval = handleReadError("packet length", file, sizeof(len), rlen, lastKnownGoodPos, allowTruncate);
        }
    }
    return retval;
}

int32_t DomainPart::calcCrc(Crc version, const void * buf, size_t sz)
{
    if (version == xxh64) {
        return static_cast<int32_t>(XXH64(buf, sz, 0ll));
    } else if (version == ccitt_crc32) {
        vespalib::crc_32_type calculator;
        calculator.process_bytes(buf, sz);
        return calculator.checksum();
    } else {
        abort();
    }
}

}
