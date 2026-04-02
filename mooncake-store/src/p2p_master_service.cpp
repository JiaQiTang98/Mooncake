#include "p2p_master_service.h"

#include <algorithm>
#include <glog/logging.h>
#include <limits>

#include "p2p_client_meta.h"

namespace mooncake {

P2PMasterService::P2PObjectMetadata& P2PMasterService::AsP2PObjectMetadata(
    ObjectMetadata& metadata) {
    auto* p2p_metadata = dynamic_cast<P2PObjectMetadata*>(&metadata);
    CHECK(p2p_metadata != nullptr) << "unexpected metadata type";
    return *p2p_metadata;
}

const P2PMasterService::P2PObjectMetadata&
P2PMasterService::AsP2PObjectMetadata(const ObjectMetadata& metadata) {
    auto* p2p_metadata = dynamic_cast<const P2PObjectMetadata*>(&metadata);
    CHECK(p2p_metadata != nullptr) << "unexpected metadata type";
    return *p2p_metadata;
}

auto P2PMasterService::QueryP2PClient(const UUID& client_id) const
    -> tl::expected<std::shared_ptr<P2PClientMeta>, ErrorCode> {
    auto client = std::static_pointer_cast<P2PClientMeta>(
        client_manager_->GetClient(client_id));
    if (!client) {
        LOG(ERROR) << "client not found"
                   << ", client_id: " << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    return client;
}

std::optional<size_t> P2PMasterService::FindGroupReplicaIndex(
    const P2PObjectMetadata& metadata, const UUID& client_id,
    const std::string& group_id) const {
    for (size_t i = 0; i < metadata.group_replicas_.size(); ++i) {
        const auto& group_meta = metadata.group_replicas_[i];
        if (group_meta.client_id == client_id &&
            group_meta.group_id == group_id) {
            return i;
        }
    }
    return std::nullopt;
}

auto P2PMasterService::SelectBestResidentSegment(
    const std::shared_ptr<P2PClientMeta>& client,
    const P2PGroupReplicaMeta& group_meta,
    const P2PGetReplicaListConfigExtra& config) const
    -> std::shared_ptr<Segment> {
    // Pick one resident segment inside a logical group as read-route target.
    // Selection order:
    // 1) segment must exist and be a P2P segment;
    // 2) segment must pass read filters (tag exclusion + priority threshold);
    // 3) prefer higher priority; tie-break by smaller segment_id for stability.
    std::shared_ptr<Segment> best_segment;
    int best_priority = std::numeric_limits<int>::min();

    for (const auto& segment_id : group_meta.resident_segments) {
        auto segment_res = client->QuerySegment(segment_id);
        if (!segment_res.has_value()) {
            continue;
        }

        auto segment = segment_res.value();
        if (!segment || !segment->IsP2PSegment()) {
            continue;
        }

        const auto& extra = segment->GetP2PExtra();
        // Read tag filter semantics: any hit means exclusion.
        bool excluded_by_tag = false;
        for (const auto& tag : config.tag_filters) {
            if (std::find(extra.tags.begin(), extra.tags.end(), tag) !=
                extra.tags.end()) {
                excluded_by_tag = true;
                break;
            }
        }
        if (excluded_by_tag || extra.priority < config.priority_limit) {
            continue;
        }

        if (!best_segment || extra.priority > best_priority ||
            (extra.priority == best_priority &&
             segment->id < best_segment->id)) {
            best_priority = extra.priority;
            best_segment = std::move(segment);
        }
    }

    return best_segment;
}

Replica::Descriptor P2PMasterService::MakeP2PDescriptor(
    const std::shared_ptr<P2PClientMeta>& client,
    const std::shared_ptr<Segment>& segment, size_t object_size) {
    Replica::Descriptor desc;
    desc.status = ReplicaStatus::COMPLETE;
    P2PProxyDescriptor proxy_desc;
    proxy_desc.client_id = client->get_client_id();
    proxy_desc.segment_id = segment->id;
    proxy_desc.ip_address = client->get_ip_address();
    proxy_desc.rpc_port = client->get_rpc_port();
    proxy_desc.object_size = object_size;
    if (segment->IsP2PSegment()) {
        proxy_desc.segment_group_id = segment->GetP2PExtra().group_id;
    }
    desc.descriptor_variant = std::move(proxy_desc);
    return desc;
}

P2PMasterService::P2PMasterService(const MasterServiceConfig& config)
    : MasterService(config),
      max_replicas_per_key_(config.max_replicas_per_key) {
    client_manager_ = std::make_shared<P2PClientManager>(
        config.client_live_ttl_sec, config.client_crashed_ttl_sec,
        config.view_version);
    InitializeClientManager();
    client_manager_->Start();
}

std::vector<Replica::Descriptor> P2PMasterService::FilterReplicas(
    const GetReplicaListRequestConfig& config, const ObjectMetadata& metadata) {
    const auto& p2p_metadata = AsP2PObjectMetadata(metadata);
    const auto& p2p_config = config.p2p_config ? config.p2p_config.value()
                                               : P2PGetReplicaListConfigExtra();

    // Step 1: for each logical replica group, select one best resident segment
    // that satisfies read filters (tag/priority).
    std::vector<std::pair<uint32_t, Replica::Descriptor>> candidates;
    for (const auto& group_meta : p2p_metadata.group_replicas_) {
        auto client_res = QueryP2PClient(group_meta.client_id);
        if (!client_res.has_value()) {
            continue;
        }

        auto client = client_res.value();
        if (!client->is_health()) {
            continue;
        }

        auto segment =
            SelectBestResidentSegment(client, group_meta, p2p_config);
        if (!segment) {
            continue;
        }

        candidates.push_back(
            {segment->GetP2PExtra().priority,
             MakeP2PDescriptor(client, segment, metadata.size_)});
    }

    // Step 2: apply max_candidates limit on group-collapsed results.
    if (config.max_candidates ==
            GetReplicaListRequestConfig::RETURN_ALL_CANDIDATES ||
        config.max_candidates >= candidates.size() || candidates.empty()) {
        // return all candidates
        std::vector<Replica::Descriptor> result;
        result.reserve(candidates.size());
        for (const auto& p : candidates) {
            result.push_back(p.second);
        }
        return result;
    }

    // 2. the number of qualified replicas is larger than limit,
    // choose the best ones.
    std::sort(candidates.begin(), candidates.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });

    std::vector<Replica::Descriptor> result;
    result.reserve(config.max_candidates);
    for (size_t i = 0; i < config.max_candidates; ++i) {
        result.push_back(candidates[i].second);
    }
    return result;
}

auto P2PMasterService::GetWriteRoute(const WriteRouteRequest& req)
    -> tl::expected<WriteRouteResponse, ErrorCode> {
    // Step 1: if max_replicas_per_key is reached, only allow routing to
    //         existing logical replicas (client_id + group_id).
    bool restrict_to_existing_groups = false;
    std::vector<std::pair<UUID, std::string>> existing_groups;
    if (!req.key.empty() && max_replicas_per_key_ > 0) {
        auto accessor = GetMetadataAccessor(req.key);
        if (accessor->Exists()) {
            const auto& metadata = AsP2PObjectMetadata(accessor->Get());
            if (metadata.group_replicas_.size() >= max_replicas_per_key_) {
                restrict_to_existing_groups = true;
                existing_groups.reserve(metadata.group_replicas_.size());
                for (const auto& group_meta : metadata.group_replicas_) {
                    existing_groups.emplace_back(group_meta.client_id,
                                                 group_meta.group_id);
                }
            }
        }
    }

    // Step 2: collect raw physical candidates from all eligible clients.
    std::vector<WriteCandidate> candidates;
    client_manager_->ForEachClient(
        req.config.strategy,
        [&](const std::shared_ptr<ClientMeta>& client)
            -> tl::expected<bool, ErrorCode> {
            auto p2p_client = std::static_pointer_cast<P2PClientMeta>(client);
            if (!p2p_client) {
                LOG(ERROR) << "unexpected client meta type";
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            return p2p_client->CollectWriteRouteCandidates(req, candidates);
        });

    struct GroupedCandidate {
        UUID client_id;
        std::string group_id;
        WriteCandidate candidate;
    };

    // Step 3: collapse physical candidates by logical replica key
    //         (client_id + group_id), and keep the best segment in each group.
    std::vector<GroupedCandidate> grouped_candidates;
    grouped_candidates.reserve(candidates.size());
    for (auto& candidate : candidates) {
        auto client_res = QueryP2PClient(candidate.replica.client_id);
        if (!client_res.has_value()) {
            continue;
        }

        std::string resolved_group_id;
        if (candidate.replica.segment_group_id.has_value() &&
            !candidate.replica.segment_group_id->empty()) {
            resolved_group_id = *candidate.replica.segment_group_id;
        } else {
            auto group_id_res = client_res.value()->QuerySegmentGroupId(
                candidate.replica.segment_id);
            if (!group_id_res.has_value()) {
                continue;
            }
            resolved_group_id = group_id_res.value();
            candidate.replica.segment_group_id = resolved_group_id;
        }

        auto grouped_it = std::find_if(
            grouped_candidates.begin(), grouped_candidates.end(),
            [&](const GroupedCandidate& grouped) {
                return grouped.client_id == candidate.replica.client_id &&
                       grouped.group_id == resolved_group_id;
            });
        if (grouped_it == grouped_candidates.end()) {
            grouped_candidates.push_back(
                GroupedCandidate{candidate.replica.client_id, resolved_group_id,
                                 std::move(candidate)});
            continue;
        }

        const auto& best = grouped_it->candidate;
        bool replace = false;
        if (candidate.priority != best.priority) {
            replace = candidate.priority > best.priority;
        } else if (candidate.available_capacity != best.available_capacity) {
            replace = candidate.available_capacity > best.available_capacity;
        } else if (candidate.replica.segment_id != best.replica.segment_id) {
            replace = candidate.replica.segment_id < best.replica.segment_id;
        }

        if (replace) {
            grouped_it->candidate = std::move(candidate);
        }
    }

    // Step 4: when restricted by max_replicas_per_key, drop candidates from
    //         new groups and keep only existing logical replicas.
    if (restrict_to_existing_groups) {
        std::vector<GroupedCandidate> filtered_candidates;
        filtered_candidates.reserve(grouped_candidates.size());
        for (auto& grouped : grouped_candidates) {
            auto it =
                std::find_if(existing_groups.begin(), existing_groups.end(),
                             [&](const auto& group_key) {
                                 return group_key.first == grouped.client_id &&
                                        group_key.second == grouped.group_id;
                             });
            if (it != existing_groups.end()) {
                filtered_candidates.push_back(std::move(grouped));
            }
        }

        if (filtered_candidates.empty() && !grouped_candidates.empty()) {
            LOG(WARNING) << "replica num exceeded"
                         << ", key: " << req.key
                         << ", client_id: " << req.client_id
                         << ", current group replica num: "
                         << existing_groups.size()
                         << ", max replica num: " << max_replicas_per_key_;
            return tl::make_unexpected(ErrorCode::REPLICA_NUM_EXCEEDED);
        }
        grouped_candidates = std::move(filtered_candidates);
    }

    // Step 5: materialize response according to max_candidates.
    WriteRouteResponse response;
    if (grouped_candidates.empty()) {
        LOG(ERROR) << "no candidate found for key: " << req.key
                   << ", client_id: " << req.client_id
                   << ", size: " << req.size;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    } else if (req.config.max_candidates ==
                   WriteRouteRequestConfig::RETURN_ALL_CANDIDATES ||
               grouped_candidates.size() <= req.config.max_candidates) {
        // return all candidates
        response.candidates.reserve(grouped_candidates.size());
        for (auto& grouped : grouped_candidates) {
            response.candidates.push_back(std::move(grouped.candidate));
        }
    } else {
        // return top max_candidates candidates
        std::sort(grouped_candidates.begin(), grouped_candidates.end(),
                  [](const auto& a, const auto& b) {
                      if (a.candidate.priority != b.candidate.priority) {
                          return a.candidate.priority > b.candidate.priority;
                      }
                      if (a.candidate.available_capacity !=
                          b.candidate.available_capacity) {
                          return a.candidate.available_capacity >
                                 b.candidate.available_capacity;
                      }
                      return a.candidate.replica.segment_id <
                             b.candidate.replica.segment_id;
                  });
        response.candidates.reserve(req.config.max_candidates);
        for (size_t i = 0; i < req.config.max_candidates; ++i) {
            response.candidates.push_back(
                std::move(grouped_candidates[i].candidate));
        }
    }
    return response;
}

auto P2PMasterService::AddReplica(const AddReplicaRequest& req)
    -> tl::expected<void, ErrorCode> {
    auto accessor = GetMetadataAccessor(req.key);

    // Step 1: resolve request target (client/segment/group).
    auto client_res = QueryP2PClient(req.replica.client_id);
    if (!client_res.has_value()) {
        return tl::make_unexpected(client_res.error());
    }
    auto client = client_res.value();
    auto segment_res = client->QuerySegment(req.replica.segment_id);
    if (!segment_res.has_value()) {
        LOG(ERROR) << "fail to query segment"
                   << ", client_id: " << req.replica.client_id
                   << ", segment_id: " << req.replica.segment_id;
        return tl::make_unexpected(segment_res.error());
    }
    auto group_id_res = client->QuerySegmentGroupId(req.replica.segment_id);
    if (!group_id_res.has_value()) {
        LOG(ERROR) << "fail to query segment group"
                   << ", client_id: " << req.replica.client_id
                   << ", segment_id: " << req.replica.segment_id;
        return tl::make_unexpected(group_id_res.error());
    }
    const std::string& resolved_group_id = group_id_res.value();
    if (req.replica.segment_group_id.has_value() &&
        !req.replica.segment_group_id->empty() &&
        req.replica.segment_group_id.value() != resolved_group_id) {
        LOG(ERROR) << "segment group mismatch"
                   << ", client_id: " << req.replica.client_id
                   << ", segment_id: " << req.replica.segment_id
                   << ", request_group_id: "
                   << req.replica.segment_group_id.value()
                   << ", resolved_group_id: " << resolved_group_id;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::string group_id = resolved_group_id;

    // Step 2: build physical replica object with resolved pointers.
    Replica new_replica(
        P2PProxyReplicaData(client, segment_res.value(), req.size),
        ReplicaStatus::COMPLETE);

    // Step 3: upsert metadata.
    // Existing key path:
    //   - reject duplicate physical replica.
    //   - enforce max_replicas_per_key on logical replicas (groups).
    //   - add physical replica and update group resident set.
    // New key path:
    //   - initialize both physical replica list and logical group metadata.
    if (accessor->Exists()) {
        auto& metadata = AsP2PObjectMetadata(accessor->Get());
        for (const auto& replica : metadata.replicas_) {
            if (!replica.is_p2p_proxy_replica()) {
                LOG(ERROR) << "unexpected replica type" << ", key: " << req.key
                           << ", request client_id: " << req.replica.client_id
                           << ", request segment_id: " << req.replica.segment_id
                           << ", replica:" << replica;
                return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
            }
            auto seg_id = replica.get_segment_id();
            auto cli_id = replica.get_p2p_client_id();
            if (cli_id && seg_id && cli_id == req.replica.client_id &&
                *seg_id == req.replica.segment_id) {
                LOG(WARNING) << "replica has existed" << ", key: " << req.key
                             << ", client_id: " << req.replica.client_id
                             << ", segment_id: " << req.replica.segment_id;
                return tl::make_unexpected(ErrorCode::REPLICA_ALREADY_EXISTS);
            }
        }

        auto group_idx =
            FindGroupReplicaIndex(metadata, req.replica.client_id, group_id);
        if (!group_idx.has_value() && max_replicas_per_key_ > 0 &&
            metadata.group_replicas_.size() >= max_replicas_per_key_) {
            LOG(WARNING) << "replica num exceeded" << ", key: " << req.key
                         << ", client_id: " << req.replica.client_id
                         << ", group_id: " << group_id
                         << ", current group replica num: "
                         << metadata.group_replicas_.size()
                         << ", max replica num: " << max_replicas_per_key_;
            return tl::make_unexpected(ErrorCode::REPLICA_NUM_EXCEEDED);
        }

        metadata.replicas_.push_back(std::move(new_replica));
        AddReplicaToSegmentIndex(accessor->GetShard(), accessor->GetKey(),
                                 metadata.replicas_.back());
        OnReplicaAdded(metadata.replicas_.back());
        if (group_idx.has_value()) {
            metadata.group_replicas_[*group_idx].resident_segments.insert(
                req.replica.segment_id);
        } else {
            P2PGroupReplicaMeta group_meta{
                .client_id = req.replica.client_id,
                .group_id = group_id,
                .resident_segments = {req.replica.segment_id},
            };
            metadata.group_replicas_.push_back(std::move(group_meta));
        }
    } else {
        std::vector<Replica> replicas;
        replicas.push_back(std::move(new_replica));
        std::vector<P2PGroupReplicaMeta> group_replicas;
        group_replicas.push_back(P2PGroupReplicaMeta{
            .client_id = req.replica.client_id,
            .group_id = group_id,
            .resident_segments = {req.replica.segment_id},
        });

        auto new_meta = std::make_unique<P2PObjectMetadata>(
            req.size, std::move(replicas), std::move(group_replicas));

        auto& shard = accessor->GetShard();
        auto it = shard.metadata.emplace(req.key, std::move(new_meta)).first;
        AddReplicaToSegmentIndex(shard, it->first, it->second->replicas_[0]);
        OnReplicaAdded(it->second->replicas_[0]);
    }

    return {};
}

auto P2PMasterService::RemoveReplica(const RemoveReplicaRequest& req)
    -> tl::expected<void, ErrorCode> {
    auto accessor = GetMetadataAccessor(req.key);
    if (!accessor->Exists()) {
        LOG(WARNING) << "object not found" << ", key: " << req.key
                     << ", client_id: " << req.client_id
                     << ", segment_id: " << req.segment_id;
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor->Get();
    auto& p2p_metadata = AsP2PObjectMetadata(metadata);

    // Step 1: remove the target physical replica and corresponding
    //         reverse-index entry.
    bool removed = false;
    for (auto it = p2p_metadata.replicas_.begin();
         it != p2p_metadata.replicas_.end(); ++it) {
        if (!it->is_p2p_proxy_replica()) {
            LOG(ERROR) << "unexpected replica type" << ", key: " << req.key
                       << ", client_id: " << req.client_id
                       << ", segment_id: " << req.segment_id
                       << ", replica: " << *it;
            return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
        } else {
            auto seg_id = it->get_segment_id();
            auto cli_id = it->get_p2p_client_id();
            if (cli_id && seg_id && cli_id == req.client_id &&
                *seg_id == req.segment_id) {
                RemoveReplicaFromSegmentIndex(accessor->GetShard(),
                                              accessor->GetKey(), *it);
                OnReplicaRemoved(*it);
                p2p_metadata.replicas_.erase(it);
                removed = true;
                break;
            }
        }
    }

    if (!removed) {
        LOG(WARNING) << "replica not found" << ", key: " << req.key
                     << ", client_id: " << req.client_id
                     << ", segment_id: " << req.segment_id;
        return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
    }

    // Step 2: update logical group resident set after physical replica removal.
    bool updated_group = false;
    for (auto it = p2p_metadata.group_replicas_.begin();
         it != p2p_metadata.group_replicas_.end(); ++it) {
        if (it->client_id != req.client_id) {
            continue;
        }
        if (it->resident_segments.erase(req.segment_id) > 0) {
            updated_group = true;
            if (it->resident_segments.empty()) {
                p2p_metadata.group_replicas_.erase(it);
            }
            break;
        }
    }
    if (!updated_group) {
        LOG(WARNING) << "group replica not found when removing physical replica"
                     << ", key: " << req.key << ", client_id: " << req.client_id
                     << ", segment_id: " << req.segment_id;
    }

    // Step 3: remove object metadata when all physical replicas are gone.
    if (p2p_metadata.replicas_.empty()) {
        OnObjectRemoved(metadata);
        accessor->Erase();
    }

    return {};
}

auto P2PMasterService::BatchRemoveReplica(const BatchRemoveReplicaRequest& req)
    -> std::vector<tl::expected<void, ErrorCode>> {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(req.segment_ids.size());

    RemoveReplicaRequest single_req;
    single_req.key = req.key;
    single_req.client_id = req.client_id;
    for (const auto& segment_id : req.segment_ids) {
        single_req.segment_id = segment_id;
        auto result = RemoveReplica(single_req);
        if (!result.has_value()) {
            if (result.error() == ErrorCode::OBJECT_NOT_FOUND) {
                // This may happen if the object is removed by another thread
                LOG(INFO) << "object not found when batch remove replica"
                          << ", key: " << req.key
                          << ", client_id: " << req.client_id
                          << ", segment_id: " << segment_id;
                results.push_back({});
            } else if (result.error() == ErrorCode::REPLICA_NOT_FOUND) {
                // This may happen if the replica is removed by another thread
                LOG(INFO) << "replica not found when batch remove replica"
                          << ", key: " << req.key
                          << ", client_id: " << req.client_id
                          << ", segment_id: " << segment_id;
                results.push_back({});
            } else {
                LOG(ERROR) << "failed to remove replica" << ", key: " << req.key
                           << ", client_id: " << req.client_id
                           << ", segment_id: " << segment_id
                           << ", error: " << toString(result.error());
                results.push_back(tl::make_unexpected(result.error()));
            }
        } else {
            results.push_back({});
        }
    }
    return results;
}

void P2PMasterService::OnObjectAccessed(ObjectMetadata& metadata) {
    // do nothing
}

// TODO: wanyue-wy
// For P2P structure, if a object has multiple replicas,
// we don't know which replica is hit.
// The detailed hit statistic of replica should be synced from
// client to master.
void P2PMasterService::OnObjectHit(const ObjectMetadata& metadata) {
    MasterMetricManager::instance().inc_valid_get_nums();
}

void P2PMasterService::OnReplicaRemoved(const Replica& replica) {
    if (replica.is_p2p_proxy_replica()) {
        auto type = replica.get_p2p_memory_type();
        if (!type) {
            LOG(ERROR) << "invalid memory type"
                       << ", replica: " << replica;
        } else if (*type == MemoryType::DRAM) {
            MasterMetricManager::instance().dec_mem_cache_nums();
        } else if (*type == MemoryType::NVME) {
            MasterMetricManager::instance().dec_file_cache_nums();
        }
    }
}

void P2PMasterService::OnReplicaAdded(const Replica& replica) {
    if (replica.is_p2p_proxy_replica()) {
        auto type = replica.get_p2p_memory_type();
        if (!type) {
            LOG(ERROR) << "invalid memory type"
                       << ", replica: " << replica;
        } else if (*type == MemoryType::DRAM) {
            MasterMetricManager::instance().inc_mem_cache_nums();
        } else if (*type == MemoryType::NVME) {
            MasterMetricManager::instance().inc_file_cache_nums();
        }
    }
}

void P2PMasterService::OnSegmentRemoved(const UUID& segment_id) {
    // Step 1: shard-by-shard cleanup based on reverse index
    //         (segment_id -> affected keys).
    for (size_t i = 0; i < GetShardCount(); ++i) {
        auto& shard = GetShard(i);
        MutexLocker lock(&shard.mutex);

        auto idx_it = shard.segment_key_index.find(segment_id);
        if (idx_it == shard.segment_key_index.end()) {
            continue;
        }

        std::vector<std::string> affected_keys;
        affected_keys.reserve(idx_it->second.size());
        for (const auto& item : idx_it->second) {
            affected_keys.emplace_back(item.first);
        }

        shard.segment_key_index.erase(idx_it);

        for (const auto& key : affected_keys) {
            auto meta_it = shard.metadata.find(key);
            if (meta_it == shard.metadata.end()) {
                continue;
            }

            auto& metadata = AsP2PObjectMetadata(*meta_it->second);
            auto& replicas = metadata.replicas_;

            // Step 2: remove all physical replicas bound to the removed
            // segment.
            for (int k = static_cast<int>(replicas.size()) - 1; k >= 0; --k) {
                auto id = replicas[k].get_segment_id();
                if (id.has_value() && id.value() == segment_id) {
                    OnReplicaRemoved(replicas[k]);
                    replicas.erase(replicas.begin() + k);
                }
            }

            // Step 3: update logical group resident set and erase empty groups.
            for (auto it = metadata.group_replicas_.begin();
                 it != metadata.group_replicas_.end();) {
                if (it->resident_segments.erase(segment_id) > 0 &&
                    it->resident_segments.empty()) {
                    it = metadata.group_replicas_.erase(it);
                } else {
                    ++it;
                }
            }

            // Step 4: erase object when no physical replica remains.
            if (replicas.empty()) {
                OnObjectRemoved(metadata);
                shard.metadata.erase(meta_it);
            }
        }
    }
}

}  // namespace mooncake
