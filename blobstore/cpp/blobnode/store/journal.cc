// Copyright 2025 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

#include "journal.h"

namespace blobstore {
namespace blobnode {

bool LogRecord::AddEntry(LogEntryPtr entry) {
    const uint8_t size = entry->data_->Size();
    const auto type = entry->data_->Type();
    if (data_size_ + size + sizeof(type) + sizeof(size) > kLogRecordMaxDataSize) {
        return false;
    }

    char *ptr = data_.get_write();
    ptr += kLogRecordHeaderSize + data_size_;

    std::memcpy(ptr, &type, sizeof(type));
    ptr += sizeof(type);
    std::memcpy(ptr, &size, sizeof(size));
    ptr += sizeof(size);

    entry->data_->MarshalTo(ptr);

    data_size_ += size;
    data_size_ += sizeof(size);
    data_size_ += sizeof(type);
    entries_.push_back(std::move(entry));

    return true;
}

FutureStatus<> LogRecord::Write(Device *device_, const uint64_t offset) {
    char *ptr = data_.get_write();

    // Write Header
    blobstore::BigEndian::PutUint16(ptr, data_size_);

    // padding set 0
    ptr += kLogHeaderSize + data_size_;
    if (data_size_ < kLogRecordMaxDataSize) {
        std::memset(ptr, 0, kLogRecordMaxDataSize - data_size_);
    }

    // write footer
    ptr = data_.get_write() + kLogRecordSize - kLogRecordFooterSize - kCrcSize;
    blobstore::BigEndian::PutUint64(ptr, ver_);
    ptr += kLogRecordFooterSize;

    uint32_t calc_crc = CRC32_IEEE(0, reinterpret_cast<const unsigned char *>(data_.get()),
                                   kLogRecordSize - kCrcSize);
    blobstore::BigEndian::PutUint32(ptr, calc_crc);

    Status<> s = co_await device_->Write(offset, data_.get_write(), kLogRecordSize);
    co_return s;
}

LogArena::LogArena(const LogArenaConfig &config, Device *device)
    : config_(config), device_(device) {
    header_.flag = LogHeaderFlag::CheckpointDone;
    offset_ = config.arena_offset + config.header_size;
}

seastar::future<Status<>> LogArena::LoadHeader() {
    Status<> s;

    auto buffer = device_->Alloc(config_.header_size);

    const auto read_s =
        co_await device_->Read(config_.arena_offset, buffer.get_write(), config_.header_size);
    if (!read_s) {
        s.SetCode(read_s.Code()).SetReason(read_s.Reason());
        co_return s;
    }
    if (read_s.Value() < config_.header_size) {
        s.SetCode(ErrCode::ErrEOF).SetReason("log: read header failed");
        co_return s;
    }

    size_t size = header_.Size();
    size_t cal_len = kLogHeaderCrcBlockSize;
    size_t off = 0;
    while (size > 0) {
        if (size < kLogHeaderCrcBlockSize) {
            cal_len = size;
        }
        uint32_t calc_crc =
            CRC32_IEEE(0, reinterpret_cast<const unsigned char *>(buffer.get() + off), cal_len);
        uint32_t crc =
            blobstore::BigEndian::Uint32(buffer.get() + off + kLogHeaderCrcBlockSize - kCrcSize);
        if (crc != calc_crc) {
            s.SetCode(ErrCode::ErrUnknown).SetReason("log: header crc mismatch");
            co_return s;
        }
        size -= cal_len;
        off += cal_len;
    }

    s = header_.UnmarshalFrom(buffer.get(), config_.header_size);

    co_return s;
}

seastar::future<Status<>> LogArena::WriteEntry(LogRecordPtr record) {
    // if first write
    Status<> s;
    if (header_.flag == LogHeaderFlag::CheckpointDone) {
        offset_ = config_.arena_offset + config_.header_size;
        s = co_await record->Write(device_, offset_);
        if (!s) {
            co_return s;
        }
        LogHeader header{};
        header.version = record->GetVersion();
        header.flag = LogHeaderFlag::UnCheckpoint;
        s = co_await UpdateHeader(header);
        co_return s;
    }
    if (offset_ >= config_.arena_offset + config_.arena_size) {
        s.SetCode(ErrCode::ErrTooLarge).SetReason("log: arena has append full, should checkpoint");
        co_return s;
    }
    s = co_await record->Write(device_, offset_);
    if (!s) {
        co_return s;
    }
    co_return s;
}

FutureStatus<LogHeaderVer> LogArena::Replay(
    const std::function<FutureStatus<>(LogRecordType, const char *, size_t)> &apply_fn) {
    Status<LogHeaderVer> s;

    uint64_t read_offset = config_.arena_offset + config_.header_size;

    auto buf = device_->Alloc(config_.replay_batch_size);
    while (true) {
        auto read_s =
            co_await device_->Read(read_offset, buf.get_write(), config_.replay_batch_size);
        if (!read_s) {
            s.SetCode(read_s.Code()).SetReason(read_s.Reason());
            co_return s;
        }
        const auto read_size = read_s.Value();

        auto res = co_await ReplayBatch(buf.get(), read_size, apply_fn);
        if (!res) {
            s.SetCode(res.Code()).SetReason(res.Reason());
            co_return s;
        }
        if (const auto finished = res.Value(); finished) {
            break;
        }
        read_offset += config_.replay_batch_size;
    }
    s.SetValue(max_record_version_);
    co_return s;
}

FutureStatus<bool> LogArena::ReplayBatch(
    const char *buf, const size_t batch_size,
    const std::function<FutureStatus<>(LogRecordType, const char *, size_t)> &apply_fn) {
    Status<bool> s;
    uint64_t version = header_.version;

    size_t off = 0;
    while (off < batch_size && off + kLogRecordSize <= batch_size) {
        auto ptr = buf + off;
        LogHeaderVer ver =
            blobstore::BigEndian::Uint64(ptr + kLogRecordSize - kLogRecordFooterSize - kCrcSize);
        uint32_t crc = blobstore::BigEndian::Uint32(ptr + kLogRecordSize - kCrcSize);
        if (uint32_t crc32c = CRC32_IEEE(0, reinterpret_cast<const unsigned char *>(ptr),
                                         kLogRecordSize - kCrcSize);
            crc32c != crc) {
            // if entry version less than header version, the entry is outdate
            if (ver < version) {
                s.SetValue(true);
                co_return s;
            }
            s.SetCode(ErrCode::ErrUnknown).SetReason("crc32 mismatched");
            co_return s;
        }
        // if entry version less than header version, the entry is outdate
        if (ver < version) {
            s.SetValue(true);
            co_return s;
        }
        if (ver > max_record_version_) {
            max_record_version_ = ver;
        }
        uint16_t data_size = blobstore::BigEndian::Uint16(ptr);
        ptr += sizeof(data_size);
        while (data_size > 0) {
            LogRecordType type;
            uint8_t size;
            std::memcpy(&type, ptr, sizeof(type));
            ptr += sizeof(type);
            data_size -= sizeof(type);
            std::memcpy(&size, ptr, sizeof(size));
            ptr += sizeof(size);
            data_size -= sizeof(size);
            if (const auto res = co_await apply_fn(type, ptr, size); !res) {
                s.SetCode(res.Code()).SetReason(res.Reason());
                co_return s;
            }
            ptr += size;
            data_size -= size;
        }
        off += kLogRecordSize;
    }
    s.SetValue(false);
    co_return s;
}

seastar::future<Status<>> LogArena::UpdateHeader(const LogHeader header) {
    Status<> s;

    auto buffer = device_->Alloc(config_.header_size);

    s = header.MarshalTo(buffer.get_write(), buffer.size());
    if (!s) {
        s.SetReason("log: write header failed");
        co_return s;
    }
    size_t size = header.Size();
    size_t cal_len = kLogHeaderCrcBlockSize;
    size_t off = 0;
    while (size > 0) {
        if (size < kLogHeaderCrcBlockSize) {
            cal_len = size;
        }
        uint32_t calc_crc =
            CRC32_IEEE(0, reinterpret_cast<const unsigned char *>(buffer.get() + off), cal_len);
        blobstore::BigEndian::PutUint32(buffer.get_write() + kLogHeaderCrcBlockSize - kCrcSize, calc_crc);
        size -= cal_len;
        off += cal_len;
    }
    s = co_await device_->Write(config_.arena_offset, buffer.get(), config_.header_size);
    if (!s) {
        co_return s;
    }
    header_ = header;
    co_return s;
}

FutureStatus<> Journal::Format(Device *device, const JournalConfig &cfg) {
    Status<> s;

    size_t batch_size = 1 * 1024 * 1024;  // 1 MB
    auto buffer = device->Alloc(batch_size);
    std::memset(buffer.get_write(), 0, batch_size);
    auto remain = cfg.log_arena_size * 2;
    size_t off = cfg.start_offset;
    while (remain > 0) {
        if (remain < batch_size) {
            batch_size = remain;
        }
        s = co_await device->Write(off, buffer.get(), batch_size);
        if (!s) {
            co_return s;
        }
        remain -= batch_size;
        off += batch_size;
    }

    LogHeader header{};
    header.flag = LogHeaderFlag::CheckpointDone;
    header.version = kInvalidLogVersion;
    s = header.MarshalTo(buffer.get_write(), buffer.size());
    if (!s) {
        s.SetReason("log: write header failed");
        co_return s;
    }
    size_t size = header.Size();
    size_t cal_len = kLogHeaderCrcBlockSize;
    off = 0;
    while (size > 0) {
        if (size < kLogHeaderCrcBlockSize) {
            cal_len = size;
        }
        uint32_t calc_crc =
            CRC32_IEEE(0, reinterpret_cast<const unsigned char *>(buffer.get() + off), cal_len);
        blobstore::BigEndian::PutUint32(buffer.get_write() + off + kLogHeaderCrcBlockSize - kCrcSize,
                                  calc_crc);
        size -= cal_len;
        off += cal_len;
    }

    off = cfg.start_offset;
    s = co_await device->Write(off, buffer.get(), kLogHeaderSize);
    if (!s) {
        co_return s;
    }

    off = off + cfg.log_arena_size;
    s = co_await device->Write(off, buffer.get(), kLogHeaderSize);
    co_return s;
}

FutureStatus<JournalPtr> Journal::Create(const JournalConfig &cfg, Device *device,
                                         std::map<LogRecordType, CheckpointCallBack> &callbacks) {
    Status<JournalPtr> s;
    JournalPtr journal = std::unique_ptr<Journal>(new Journal());

    journal->device_ = device;

    // Create Arena A configuration
    LogArenaConfig cfg_a{
        .arena_offset = cfg.start_offset,
        .arena_size = cfg.log_arena_size,
        .header_size = kLogHeaderSize,
        .replay_batch_size = 32 * kLogRecordSize,  // 128KB
    };
    LogArenaConfig cfg_b = cfg_a;
    cfg_b.arena_offset = cfg_a.arena_offset + cfg.log_arena_size;

    // Create two LogArenas
    journal->lgs_[0] = std::make_unique<LogArena>(cfg_a, device);
    journal->lgs_[1] = std::make_unique<LogArena>(cfg_b, device);
    journal->entry_queue_ = std::make_unique<seastar::queue<LogEntryPtr>>(128);

    for (auto &[type, cb] : callbacks) {
        journal->supported_types_.insert(type);
        journal->checkpoint_cbs_[static_cast<uint8_t>(type)] = &cb;
    }

    if (const auto res = co_await journal->LoadHeader(); !res) {
        s.SetCode(res.Code()).SetReason(res.Reason());
        co_return s;
    }

    auto buffer = device->Alloc(kLogRecordSize);
    journal->current_write_record_ =
        seastar::make_lw_shared<LogRecord>(std::move(buffer), journal->next_log_version_);
    journal->StartBackgroundTasks();

    s.SetValue(std::move(journal));
    co_return s;
}

seastar::future<Status<>> Journal::LoadHeader() {
    Status<> s = co_await lgs_[0]->LoadHeader();
    if (!s) {
        co_return s;
    }
    s = co_await lgs_[1]->LoadHeader();
    if (!s) {
        co_return s;
    }
    co_return s;
}

seastar::future<Status<>> Journal::Append(BaseLogEntryPtr data) {
    Status<> s;
    seastar::gate::holder holder(gate_);
    // 0. Check if the type is supported
    if (!supported_types_.contains(data->Type())) {
        s.SetCode(ErrCode::ErrInvalid).SetReason("log: unsupported type");
        co_return s;
    }

    auto entry = seastar::make_lw_shared<LogEntry>(std::move(data));
    auto fut = entry->GetFuture();
    try {
        co_await entry_queue_->push_eventually(std::move(entry));
    } catch (const std::runtime_error &e) {
        s.SetCode(ErrCode::ErrUnknown).SetReason(e.what());
        co_return s;
    }

    // 3. Wake up FlushLoop
    flush_cv_.signal();
    s = co_await std::move(fut);
    co_return s;
}

seastar::future<Status<>> Journal::WriteEntry() {
    Status<> s = co_await lgs_[write_idx_]->WriteEntry(current_write_record_);
    if (s) {
        co_return s;
    }
    if (ErrCode::ErrTooLarge != s.Code()) {
        co_return s;
    }
    // Log area is full, switch to another log area
    checkpoint_cv_.signal();
    write_idx_ = 1 - write_idx_;
    co_await checkpoint_done_cv_.wait();
    s = co_await lgs_[write_idx_]->WriteEntry(current_write_record_);

    for (const auto &r : current_write_record_->GetEntries()) {
        dirty_records_[static_cast<uint8_t>(r->data_->Type())][write_idx_][r->data_->ID()] =
            r->data_;
    }

    co_return s;
}

void Journal::HandleIOError(Status<> &s) {
    if (arena_broken) {
        return;
    }
    auto e = std::make_exception_ptr(std::runtime_error(s.Reason()));
    entry_queue_->abort(e);
    // notify all waiters
    while (!entry_queue_->empty()) {
        auto entry = entry_queue_->pop();
        auto st = s;
        entry->pr.set_value(std::move(st));
    }
    current_write_record_->Notify(s);
    arena_broken = true;
}

seastar::future<> Journal::FlushLoop() {
    seastar::gate::holder holder(gate_);
    while (!gate_.is_closed()) {
        if (entry_queue_->empty()) {
            co_await flush_cv_.wait();
        }
        while (!entry_queue_->empty()) {
            const auto entry = entry_queue_->pop();
            if (current_write_record_->AddEntry(entry)) {
                continue;
            }

            // Entry is full at 4K, flush this type's entry
            auto s = co_await WriteEntry();
            if (!s) {
                HandleIOError(s);
                co_return co_await seastar::make_ready_future();
            }
            // Clear entry and reinitialize
            current_write_record_->Notify(s);
            current_write_record_->Reset();
            lgs_[write_idx_]->UpdateOffset();

            // Retry adding (because the entry has been cleared, it should be possible to add now)
            current_write_record_->AddEntry(entry);
        }
        if (current_write_record_->IsEmpty()) {
            continue;
        }
        auto s = co_await WriteEntry();
        if (!s) {
            HandleIOError(s);
            co_return co_await seastar::make_ready_future();
        }
        current_write_record_->Notify(s);
    }
}

seastar::future<> Journal::FlushCheckpointLoop() {
    auto holder = seastar::gate::holder(gate_);

    checkpoint_done_cv_.signal();
    while (!gate_.is_closed()) {
        co_await checkpoint_cv_.wait();

        const auto idx = 1 - write_idx_;
        for (const auto &type : supported_types_) {
            const auto type_ = static_cast<uint8_t>(type);
            if (!checkpoint_cbs_[type_]) {
                continue;
            }
            if (auto res = co_await (*checkpoint_cbs_[type_])(dirty_records_[type_][idx]); !res) {
                HandleIOError(res);
                co_return co_await seastar::make_ready_future();
            }
            dirty_records_[type_][idx] = {};
        }
        // Update header
        auto header = lgs_[idx]->GetHeader();
        header.flag = LogHeaderFlag::CheckpointDone;
        if (auto s = co_await lgs_[idx]->UpdateHeader(header); !s) {
            HandleIOError(s);
            co_return co_await seastar::make_ready_future();
        }
        checkpoint_done_cv_.signal();
    }
}

seastar::future<Status<>> Journal::Replay(
    const std::function<FutureStatus<>(LogRecordType, const char *, size_t)> &apply_fn,
    const std::function<FutureStatus<>()> &flush_fn) {
    Status<> s;

    LogHeaderVer max_entry_ver = 0;
    size_t idx = 0;

    if (lgs_[0]->GetHeader().version > lgs_[1]->GetHeader().version) {
        idx = 1;
        write_idx_ = idx;
    }
    for (int i = 0; i < 2; ++i) {
        if (const auto log_header = lgs_[idx]->GetHeader();
            log_header.flag == LogHeaderFlag::CheckpointDone) {
            idx = 1 - idx;
            continue;
        }
        auto res = co_await lgs_[idx]->Replay(apply_fn);
        if (!res) {
            s.SetCode(res.Code()).SetReason(res.Reason());
            co_return s;
        }
        // notify flush meta data
        if (auto apply_res = co_await flush_fn(); !apply_res) {
            s.SetCode(apply_res.Code()).SetReason(apply_res.Reason());
            co_return s;
        }
        if (max_entry_ver < res.Value()) {
            max_entry_ver = res.Value();
        }

        LogHeader header{};
        header.flag = LogHeaderFlag::CheckpointDone;
        header.version = res.Value();
        if (auto up_res = co_await lgs_[idx]->UpdateHeader(header); !up_res) {
            s.SetCode(res.Code()).SetReason(res.Reason());
            co_return s;
        }
        idx = 1 - idx;
    }
    next_log_version_ = max_entry_ver + 1;
    auto buffer = device_->Alloc(kLogRecordSize);
    current_write_record_ =
        seastar::make_lw_shared<LogRecord>(std::move(buffer), next_log_version_);
    co_return s;
}

void Journal::StartBackgroundTasks() {
    flush_task_ = FlushLoop();
    checkpoint_task_ = FlushCheckpointLoop();
}

seastar::future<Status<>> Journal::Stop() {
    Status<> s;

    // Wake up all waiting coroutines, let FlushLoop exit and flush remaining data
    flush_cv_.signal();
    checkpoint_cv_.signal();

    // Close gate, wait for all ongoing Append operations to be enqueued
    co_await gate_.close();

    // Wait for FlushLoop to exit
    co_await std::move(flush_task_);
    co_await std::move(checkpoint_task_);

    // Finally close the queue
    entry_queue_->abort(std::make_exception_ptr(std::runtime_error("log arena stopped")));

    co_return s;
}

}  // namespace blobnode
}  // namespace blobstore
