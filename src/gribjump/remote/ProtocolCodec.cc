/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Caragh Bradley

#include "gribjump/remote/ProtocolCodec.h"

#include <algorithm>
#include <sstream>

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/Offset.h"
#include "eckit/log/Log.h"
#include "eckit/log/Plural.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------
// Request header

void ProtocolCodec::writeRequestHeader(eckit::Stream& stream, RequestType type, const LogContext& context) {
    stream << remoteProtocolVersion;
    stream << context;
    stream << static_cast<uint16_t>(type);
}

RequestType ProtocolCodec::readRequestHeader(eckit::Stream& stream) {
    uint16_t version;
    stream >> version;
    if (version != remoteProtocolVersion) {
        throw eckit::SeriousBug(
            "Gribjump remote-protocol mismatch: Serverside version: " + std::to_string(remoteProtocolVersion) +
            ", Clientside version: " + std::to_string(version));
    }

    LogContext ctx(stream);
    ContextManager::instance().set(ctx);

    uint16_t i_requestType;
    stream >> i_requestType;
    return static_cast<RequestType>(i_requestType);
}

//----------------------------------------------------------------------------------------------------------------------
// Error block

void ProtocolCodec::encodeErrors(eckit::Stream& stream, const std::vector<std::string>& errors) {
    stream << errors.size();
    for (const auto& e : errors) {
        stream << e;
    }
}

bool ProtocolCodec::decodeErrors(eckit::Stream& stream, bool raise) {
    size_t nErrors;
    stream >> nErrors;
    if (nErrors == 0) {
        return false;
    }

    std::stringstream ss;
    ss << "RemoteGribJump received server-side " << eckit::Plural(nErrors, "error") << std::endl;
    for (size_t i = 0; i < nErrors; i++) {
        std::string error;
        stream >> error;
        ss << error << std::endl;
    }
    if (raise) {
        throw eckit::RemoteException(ss.str(), Here());
    }
    else {
        eckit::Log::error() << ss.str() << std::endl;
    }
    return true;
}

//----------------------------------------------------------------------------------------------------------------------
// EXTRACT

void ProtocolCodec::encodeExtractRequest(eckit::Stream& stream, const std::vector<ExtractionRequest>& requests) {
    stream << requests.size();
    for (const auto& req : requests) {
        stream << req;
    }
}

std::vector<ExtractionRequest> ProtocolCodec::decodeExtractRequest(eckit::Stream& stream) {
    size_t nRequests;
    stream >> nRequests;
    std::vector<ExtractionRequest> requests;
    requests.reserve(nRequests);
    for (size_t i = 0; i < nRequests; i++) {
        requests.emplace_back(stream);
    }
    return requests;
}

void ProtocolCodec::encodeExtractReply(eckit::Stream& stream, const std::vector<const ExtractionResult*>& results) {
    for (const auto* result : results) {
        size_t nfields = 1;  // @todo: remove this (bump protocol version)
        stream << nfields;
        stream << *result;
    }
}

std::vector<std::unique_ptr<ExtractionResult>> ProtocolCodec::decodeExtractReply(eckit::Stream& stream,
                                                                                 size_t nRequests) {
    std::vector<std::unique_ptr<ExtractionResult>> results;
    results.reserve(nRequests);
    for (size_t i = 0; i < nRequests; i++) {
        size_t nfields;
        stream >> nfields;
        ASSERT(nfields == 1);  // temporary; see encodeExtractReply
        results.push_back(std::make_unique<ExtractionResult>(stream));
    }
    return results;
}

//----------------------------------------------------------------------------------------------------------------------
// SCAN

void ProtocolCodec::encodeScanRequest(eckit::Stream& stream, const std::vector<metkit::mars::MarsRequest>& requests,
                                      bool byfiles) {
    stream << byfiles;
    stream << requests.size();
    for (const auto& req : requests) {
        stream << req;
    }
}

std::vector<metkit::mars::MarsRequest> ProtocolCodec::decodeScanRequest(eckit::Stream& stream, bool& byfiles) {
    stream >> byfiles;
    size_t nRequests;
    stream >> nRequests;
    std::vector<metkit::mars::MarsRequest> requests;
    requests.reserve(nRequests);
    for (size_t i = 0; i < nRequests; i++) {
        requests.emplace_back(metkit::mars::MarsRequest(stream));
    }
    return requests;
}

void ProtocolCodec::encodeScanReply(eckit::Stream& stream, size_t nFields) {
    stream << nFields;
}

size_t ProtocolCodec::decodeScanReply(eckit::Stream& stream) {
    size_t nFields;
    stream >> nFields;
    return nFields;
}

//----------------------------------------------------------------------------------------------------------------------
// AXES

void ProtocolCodec::encodeAxesRequest(eckit::Stream& stream, const std::string& request, int level) {
    stream << request;
    stream << level;
}

void ProtocolCodec::decodeAxesRequest(eckit::Stream& stream, std::string& request, int& level) {
    stream >> request;
    stream >> level;
}

void ProtocolCodec::encodeAxesReply(eckit::Stream& stream,
                                    const std::map<std::string, std::unordered_set<std::string>>& axes) {
    stream << axes.size();
    for (const auto& [name, vals] : axes) {
        stream << name;
        stream << vals.size();
        for (const auto& val : vals) {
            stream << val;
        }
    }
}

std::map<std::string, std::unordered_set<std::string>> ProtocolCodec::decodeAxesReply(eckit::Stream& stream) {
    std::map<std::string, std::unordered_set<std::string>> result;
    size_t nAxes;
    stream >> nAxes;
    for (size_t i = 0; i < nAxes; i++) {
        std::string axisName;
        stream >> axisName;
        size_t nVals;
        stream >> nVals;
        std::unordered_set<std::string> vals;
        for (size_t j = 0; j < nVals; j++) {
            std::string val;
            stream >> val;
            vals.insert(val);
        }
        result[axisName] = std::move(vals);
    }
    return result;
}

//----------------------------------------------------------------------------------------------------------------------
// FORWARD_SCAN

void ProtocolCodec::encodeForwardScanRequest(eckit::Stream& stream, const scanmap_t& scanmap) {
    stream << scanmap.size();
    for (const auto& [fname, offsets] : scanmap) {
        stream << fname;
        stream << offsets;
    }
}

scanmap_t ProtocolCodec::decodeForwardScanRequest(eckit::Stream& stream) {
    scanmap_t scanmap;
    size_t nFiles;
    stream >> nFiles;
    for (size_t i = 0; i < nFiles; i++) {
        std::string fname;
        eckit::OffsetList offsets;
        stream >> fname;
        stream >> offsets;
        scanmap[fname] = offsets;
    }
    return scanmap;
}

//----------------------------------------------------------------------------------------------------------------------
// FORWARD_EXTRACT

void ProtocolCodec::encodeForwardExtractRequest(eckit::Stream& stream, filemap_t& filemap) {
    stream << filemap.size();
    for (auto& [fname, extractionItems] : filemap) {
        // Send (and receive) the extraction items in ascending offset order.
        std::sort(extractionItems.begin(), extractionItems.end(),
                  [](const ExtractionItem* a, const ExtractionItem* b) { return a->offset() < b->offset(); });

        stream << fname;
        stream << extractionItems.size();
        for (const auto& item : extractionItems) {
            // We have the URI, so no need to send a request string.
            ExtractionRequest req("", item->intervals(), item->gridHash());
            stream << req;
            stream << item->URI();
        }
    }
}

ProtocolCodec::ForwardExtractRequest ProtocolCodec::decodeForwardExtractRequest(eckit::Stream& stream) {
    ForwardExtractRequest out;
    size_t nFiles;
    stream >> nFiles;
    for (size_t i = 0; i < nFiles; i++) {
        std::string fname;
        size_t nItems;
        stream >> fname;
        stream >> nItems;
        out.filemap[fname] = std::vector<ExtractionItem*>();  // non-owning pointers
        out.filemap[fname].reserve(nItems);

        for (size_t j = 0; j < nItems; j++) {
            auto extractionItem = std::make_unique<ExtractionItem>(std::make_unique<ExtractionRequest>(stream));
            extractionItem->URI(eckit::URI("file", stream));
            out.filemap[fname].push_back(extractionItem.get());  // non-owning pointer
            out.items.push_back(std::move(extractionItem));
        }
    }
    return out;
}

void ProtocolCodec::encodeForwardExtractReply(eckit::Stream& stream, const filemap_t& filemap) {
    for (const auto& [fname, extractionItems] : filemap) {
        stream << fname;  // sanity check
        stream << extractionItems.size();
        for (const auto& item : extractionItems) {
            stream << *(item->result());
        }
    }
}

void ProtocolCodec::decodeForwardExtractReply(eckit::Stream& stream, filemap_t& filemap) {
    const size_t nFiles = filemap.size();
    for (size_t i = 0; i < nFiles; i++) {
        std::string fname;
        stream >> fname;
        size_t nItems;
        stream >> nItems;
        ASSERT(nItems == filemap[fname].size());
        for (size_t j = 0; j < nItems; j++) {
            filemap[fname][j]->result(std::make_unique<ExtractionResult>(stream));
        }
    }
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
