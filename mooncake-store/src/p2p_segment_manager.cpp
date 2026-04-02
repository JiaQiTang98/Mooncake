#include "p2p_segment_manager.h"

#include <glog/logging.h>
#include <algorithm>

namespace mooncake {

tl::expected<std::pair<size_t, size_t>, ErrorCode>
P2PSegmentManager::QuerySegments(const std::string& segment) {
    SharedMutexLocker lock_(&segment_mutex_, shared_lock);
    bool found = false;
    size_t capacity = 0;
    size_t used = 0;
    for (const auto& entry : mounted_segments_) {
        if (entry.second->name == segment) {
            capacity += entry.second->size;
            if (entry.second->IsP2PSegment()) {
                used += entry.second->GetP2PExtra().usage;
            }
            found = true;
            break;
        }
    }

    if (!found) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }

    return std::make_pair(used, capacity);
}

tl::expected<void, ErrorCode> P2PSegmentManager::InnerMountSegment(
    const Segment& segment) {
    if (!segment.IsP2PSegment()) {
        LOG(ERROR) << "P2PSegmentManager only supports P2PSegmentExtraData";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Step 1: register physical segment in mounted map.
    auto new_segment = std::make_shared<Segment>(segment);
    mounted_segments_[new_segment->id] = new_segment;

    // Step 2: update group registry and keep segments sorted by priority
    // (high -> low). This ordering is used by group-aware route selection.
    const auto& group_id = new_segment->GetP2PExtra().group_id;
    segment_to_group_[new_segment->id] = group_id;
    auto& group = groups_[group_id];
    if (group.group_id.empty()) {
        group.group_id = group_id;
    }
    group.ordered_segments.push_back(new_segment->id);
    std::sort(
        group.ordered_segments.begin(), group.ordered_segments.end(),
        [this](const UUID& lhs, const UUID& rhs) {
            const auto lhs_it = mounted_segments_.find(lhs);
            const auto rhs_it = mounted_segments_.find(rhs);
            if (lhs_it == mounted_segments_.end() ||
                rhs_it == mounted_segments_.end()) {
                return lhs < rhs;
            }

            const auto lhs_priority = lhs_it->second->GetP2PExtra().priority;
            const auto rhs_priority = rhs_it->second->GetP2PExtra().priority;
            if (lhs_priority != rhs_priority) {
                return lhs_priority > rhs_priority;
            }
            return lhs < rhs;
        });

    // Step 3: notify capacity/accounting callbacks.
    if (on_segment_added_) {
        on_segment_added_(*new_segment);
    }

    return {};
}

tl::expected<void, ErrorCode> P2PSegmentManager::OnUnmountSegment(
    const std::shared_ptr<Segment>& segment) {
    // Step 1: remove segment from group registry.
    auto group_it = segment_to_group_.find(segment->id);
    if (group_it != segment_to_group_.end()) {
        auto registry_it = groups_.find(group_it->second);
        if (registry_it != groups_.end()) {
            auto& ordered_segments = registry_it->second.ordered_segments;
            ordered_segments.erase(
                std::remove(ordered_segments.begin(), ordered_segments.end(),
                            segment->id),
                ordered_segments.end());
            if (ordered_segments.empty()) {
                groups_.erase(registry_it);
            }
        }
        segment_to_group_.erase(group_it);
    }

    // Step 2: notify capacity/accounting callbacks.
    if (on_segment_removed_) {
        on_segment_removed_(*segment);
    }
    return {};
}

auto P2PSegmentManager::QuerySegmentGroupId(const UUID& segment_id) const
    -> tl::expected<std::string, ErrorCode> {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    auto it = segment_to_group_.find(segment_id);
    if (it == segment_to_group_.end()) {
        LOG(WARNING) << "group not found for segment"
                     << ", segment_id=" << segment_id;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    return it->second;
}

auto P2PSegmentManager::QuerySegmentGroup(const std::string& group_id) const
    -> tl::expected<P2PSegmentGroup, ErrorCode> {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    auto it = groups_.find(group_id);
    if (it == groups_.end()) {
        LOG(WARNING) << "group not found"
                     << ", group_id=" << group_id;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    return it->second;
}

tl::expected<size_t, ErrorCode> P2PSegmentManager::UpdateSegmentUsage(
    const UUID& segment_id, size_t usage) {
    SharedMutexLocker lock(&segment_mutex_);
    auto it = mounted_segments_.find(segment_id);
    if (it == mounted_segments_.end()) {
        LOG(WARNING) << "fail to update segment usage, segment doesn't exist"
                     << ", segment_id: " << segment_id;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    } else if (!it->second->IsP2PSegment()) {
        LOG(ERROR) << "unexpected segment type"
                   << ", segment_id=" << segment_id;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    } else if (usage > it->second->size) {
        LOG(ERROR) << "usage is larger than segment size"
                   << ", segment_id=" << segment_id << ", usage=" << usage
                   << ", segment_size=" << it->second->size;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    size_t old_usage = it->second->GetP2PExtra().usage;
    it->second->GetP2PExtra().usage = usage;
    return old_usage;
}

size_t P2PSegmentManager::GetSegmentUsage(const UUID& segment_id) const {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    auto it = mounted_segments_.find(segment_id);
    if (it == mounted_segments_.end()) {
        LOG(WARNING) << "segment does not exist"
                     << ", segment_id=" << segment_id;
    } else if (!it->second->IsP2PSegment()) {
        LOG(ERROR) << "unexpected segment type"
                   << ", segment_id=" << segment_id;
    } else {
        return it->second->GetP2PExtra().usage;
    }
    return 0;
}

void P2PSegmentManager::ForEachSegment(const SegmentVisitor& visitor) const {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    for (const auto& [id, segment] : mounted_segments_) {
        if (!segment->IsP2PSegment()) {
            LOG(ERROR) << "P2PSegmentManager only supports P2PSegmentExtraData"
                       << ", segment_id: " << segment->id;
            continue;
        }
        if (visitor(*segment)) {
            break;
        }
    }
}

}  // namespace mooncake
