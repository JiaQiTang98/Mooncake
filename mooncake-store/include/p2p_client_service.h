#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>

#include "client_service.h"
#include "data_manager.h"
#include "client_rpc_service.h"
#include "peer_client.h"
#include "p2p_master_client.h"
#include "route_cache.h"

namespace mooncake {

class P2PClientService final : public ClientService {
   public:
    /**
     * @brief Constructor for P2PClientService.
     * @param local_ip IP address of the local node.
     * @param te_port TE port of the local node.
     * @param metadata_connstring Connection string for metadata server.
     * @param labels Optional labels for client metrics.
     */
    P2PClientService(const std::string& local_ip, uint16_t te_port,
                     const std::string& metadata_connstring,
                     const std::map<std::string, std::string>& labels = {});

    virtual ~P2PClientService();

    ErrorCode Init(const P2PClientConfig& config);

    /**
     * @brief
     * 1. Stops heartbeat, RPC server, and all background threads of submodules.
     * 2. Rejects all incoming requests.
     */
    void Stop() override;

    /**
     * @brief Release internal resources.
     */
    void Destroy() override;

    /**
     * @brief Single put data for a key.
     * @param key The object key.
     * @param slices Data slices.
     * @param config Replicate configuration.
     * @return An ErrorCode indicating the status.
     */
    tl::expected<void, ErrorCode> Put(const ObjectKey& key,
                                      std::vector<Slice>& slices,
                                      const WriteConfig& config) override;

    /**
     * @brief Batch put data for multiple keys.
     * currently.
     * @param keys The list of object keys.
     * @param batched_slices The list of data slices for each key.
     * @param config Replicate configuration.
     * @return A vector of ErrorCode results for each key.
     */
    std::vector<tl::expected<void, ErrorCode>> BatchPut(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const WriteConfig& config) override;

    /**
     * @brief Gets object metadata without transferring data
     * @param object_key Key to query
     * @return QueryResult containing replicas, or ErrorCode
     * indicating failure
     */
    tl::expected<std::unique_ptr<QueryResult>, ErrorCode> Query(
        const std::string& object_key,
        const ReadRouteConfig& config = {}) override;

    /**
     * @brief Batch query object metadata without transferring data
     * @param object_keys Keys to query
     * @return Vector of QueryResult objects containing replicas
     */
    std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>>
    BatchQuery(const std::vector<std::string>& object_keys,
               const ReadRouteConfig& config = {}) override;

    tl::expected<bool, ErrorCode> IsExist(const std::string& key) override;

    std::vector<tl::expected<bool, ErrorCode>> BatchIsExist(
        const std::vector<std::string>& keys) override;

    DeploymentMode deployment_mode() const override {
        return DeploymentMode::P2P;
    }

    tl::expected<std::shared_ptr<BufferHandle>, ErrorCode> Get(
        const std::string& key,
        std::shared_ptr<ClientBufferAllocator> allocator,
        const ReadRouteConfig& config = {}) override;

    std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>>
    BatchGet(const std::vector<std::string>& keys,
             std::shared_ptr<ClientBufferAllocator> allocator,
             const ReadRouteConfig& config = {}) override;

    tl::expected<int64_t, ErrorCode> Get(
        const std::string& key, const std::vector<void*>& buffers,
        const std::vector<size_t>& sizes,
        const ReadRouteConfig& config = {}) override;

    std::vector<tl::expected<int64_t, ErrorCode>> BatchGet(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& all_buffers,
        const std::vector<std::vector<size_t>>& all_sizes,
        const ReadRouteConfig& config = {},
        bool aggregate_same_segment_task = false) override;

    /**
     * @brief Mount a memory segment in P2P mode.
     * @param buffer Start address of the buffer.
     * @param size Size of the buffer in bytes.
     * @return An ErrorCode indicating success or failure.
     */
    tl::expected<void, ErrorCode> MountSegment(const void* buffer,
                                               size_t size) override;

    /**
     * @brief Unmount a memory segment in P2P mode.
     * @param buffer Start address of the buffer.
     * @param size Size of the buffer in bytes.
     * @return An ErrorCode indicating success or failure.
     */
    tl::expected<void, ErrorCode> UnmountSegment(const void* buffer,
                                                 size_t size) override;

    /**
     * @brief Removes an object and all its replicas
     * @param key Key to remove
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> Remove(const ObjectKey& key) override;

    /**
     * @brief Removes objects from the store whose keys match a regex pattern.
     * @param str The regular expression string to match against object keys.
     * @return An expected object containing the number of removed objects on
     * success, or an ErrorCode on failure.
     */
    tl::expected<long, ErrorCode> RemoveByRegex(const ObjectKey& str) override;

    /**
     * @brief Removes all objects and all its replicas
     * @return tl::expected<long, ErrorCode> number of removed objects or error
     */
    tl::expected<long, ErrorCode> RemoveAll() override;

    MasterClient& GetMasterClient() override { return master_client_; }

   private:
    /**
     * @brief init TieredBackend and DataManager
     *        1. build metadata and segment sync callback
     *        2. build tiered config
     *        3. init tiered backend and data manager
     */
    ErrorCode InitStorage(const P2PClientConfig& config);
    /**
     * @brief build add replica callback.
     *        when tier add replica, call master to update metadata
     */
    AddReplicaCallback BuildAddReplicaCallback();

    /**
     * @brief build remove replica callback.
     *        when tier remove replica, call master to update metadata
     */
    RemoveReplicaCallback BuildRemoveReplicaCallback();

    /**
     * @brief build segment sync callback.
     *        when tier add/remove segment, call master to mount/unmount segment
     */
    SegmentSyncCallback BuildSegmentSyncCallback();

    /**
     * @brief handle COMMIT type callback: notify master to add new replica
     */
    tl::expected<void, ErrorCode> SyncAddReplica(const std::string& key,
                                                 const UUID& tier_id,
                                                 size_t size);

    /**
     * @brief handle DELETE type callback: notify master to remove replica
     */
    tl::expected<void, ErrorCode> SyncRemoveReplica(const std::string& key,
                                                    const UUID& tier_id);

    /**
     * @brief handle batch DELETE: notify master to remove replicas from
     *        multiple segments in one RPC call
     * @param key Key to remove
     * @param segment_ids Vector of segment IDs to remove (it will be moved)
     * @return Vector of ErrorCode results for each segment
     */
    std::vector<tl::expected<void, ErrorCode>> SyncBatchRemoveReplica(
        const std::string& key, std::vector<UUID> segment_ids);

    /**
     * @brief Collect tier info from DataManager and build P2P Segments.
     */
    std::vector<Segment> CollectTierSegments() const;

    /**
     * @brief Register the P2P client with the master server.
     * Collects segments from mounted_segments_ and registers them.
     * @return An ErrorCode indicating success or failure.
     */
    tl::expected<RegisterClientResponse, ErrorCode> RegisterClient() override;

    HeartbeatRequest build_heartbeat_request() override;

   private:
    // --- Internal helpers for P2P read/write modes ---

    /**
     * @brief Put data to local TieredBackend via DataManager.
     */
    tl::expected<void, ErrorCode> PutLocal(const std::string& key,
                                           std::vector<Slice>& slices);

    /**
     * @brief Put data to a remote node via Master's write route.
     * Gets write route from Master, then uses PeerClient to write.
     */
    tl::expected<void, ErrorCode> PutViaRoute(
        const std::string& key, std::vector<Slice>& slices,
        const WriteRouteRequestConfig& config);

    /**
     * @brief Get data from local TieredBackend via DataManager.
     */
    tl::expected<size_t, ErrorCode> GetLocal(const std::string& key,
                                             std::vector<Slice>& slices);

    struct LocalCopyPlan {
        // Keep source handle alive until memcpy finishes.
        AllocationHandle source_handle;
        // Source memory view from local TieredBackend object.
        const char* source_ptr = nullptr;
        size_t source_size = 0;
        // Fast path for one contiguous destination slice (the common case in
        // stress workload and allocator-based Get).
        bool use_single_dest = false;
        void* single_dest_ptr = nullptr;
        size_t single_dest_size = 0;
        // Caller-provided destination slices.
        std::vector<Slice> dest_slices;
    };

    class AsyncMemcpyExecutor {
       public:
        template <typename ResultType>
        struct BatchState {
            std::vector<ResultType> results;
            std::atomic<size_t> remaining{0};
            std::mutex done_mutex;
            std::condition_variable done_cv;
            bool done = false;
        };

        template <typename ResultType>
        struct BatchHandle {
            std::shared_ptr<BatchState<ResultType>> state;

            std::vector<ResultType> Wait() const {
                if (!state) {
                    return {};
                }
                if (state->remaining.load(std::memory_order_acquire) > 0) {
                    std::unique_lock<std::mutex> lock(state->done_mutex);
                    auto state_ptr = state;
                    state->done_cv.wait(lock,
                                        [state_ptr] { return state_ptr->done; });
                }
                return state->results;
            }
        };

        AsyncMemcpyExecutor(size_t worker_num, size_t max_queue_size);
        ~AsyncMemcpyExecutor();

        // Generic batched submit/wait path used by both local memcpy and
        // remote BatchGet/BatchPut fan-out.
        template <typename ResultType, typename TaskFn, typename ErrorFn>
        BatchHandle<ResultType> SubmitBatchTasks(
            const std::vector<size_t>& indices, TaskFn&& task_fn,
            ErrorFn&& on_error);

        void Shutdown();

       private:
        struct QueueTask {
            std::function<void()> run;
            std::function<void()> cancel;
        };

        // Long-running worker loop consuming submitted async tasks.
        void WorkerMain();
        template <typename ResultType>
        static void FinishBatchTask(
            const std::shared_ptr<BatchState<ResultType>>& state,
            size_t batch_index, ResultType result) {
            if (!state || batch_index >= state->results.size()) {
                return;
            }
            state->results[batch_index] = std::move(result);
            if (state->remaining.fetch_sub(1, std::memory_order_acq_rel) ==
                1) {
                std::lock_guard<std::mutex> lock(state->done_mutex);
                state->done = true;
                state->done_cv.notify_one();
            }
        }

        size_t max_queue_size_ = 0;
        bool shutting_down_ = false;
        std::mutex mutex_;
        std::condition_variable queue_not_empty_cv_;
        std::condition_variable queue_not_full_cv_;
        std::queue<QueueTask> tasks_;
        std::vector<std::thread> workers_;
    };
    friend class AsyncMemcpyExecutor;

    tl::expected<LocalCopyPlan, ErrorCode> BuildLocalCopyPlan(
        const std::string& key, const AllocationHandle& handle,
        const std::vector<Slice>& slices) const;
    // Execute one local copy plan synchronously in the current thread.
    static ErrorCode ExecuteLocalCopyPlan(const LocalCopyPlan& plan);
    // Single-key synchronous memcpy fast path (no LocalCopyPlan allocation).
    static tl::expected<size_t, ErrorCode> CopyLocalBufferSync(
        const std::string& key, const AllocationHandle& handle,
        const std::vector<Slice>& slices);
    bool ShouldUseAsyncLocalCopy(size_t batch_key_count) const;
    // Remote async fan-out in BatchGet/BatchPut is controlled by dedicated
    // remote-batch knobs and defaults to disabled for compatibility.
    bool ShouldUseAsyncBatchRpc(size_t batch_key_count) const;
    bool UseLocalTeTransfer() const;

    /**
     * @brief Get data from a remote node via a list of proxy descriptors.
     * Iterates through the list; stops, returns the slice of proxies from the
     * successful one to the end.
     */
    tl::expected<void, ErrorCode> GetRemoteViaRoute(
        const std::string& key, std::vector<Slice>& slices,
        const std::vector<P2PProxyDescriptor>& proxies, bool is_cached_proxies);

    /**
     * @brief Query Master for replica list and calculate total object size.
     * @return Pair of (replicas, total_size) on success.
     */
    tl::expected<std::pair<std::vector<Replica::Descriptor>, uint64_t>,
                 ErrorCode>
    QueryReplicaSize(const std::string& key, const ReadRouteConfig& config);

    /**
     * @brief Get or create a PeerClient for the given endpoint.
     * Thread-safe via peer_clients_mutex_.
     */
    PeerClient& GetOrCreatePeerClient(const std::string& endpoint);

   private:
    P2PMasterClient master_client_;
    uint16_t client_rpc_port_ = 12345;

    std::unique_ptr<coro_rpc::coro_rpc_server> client_rpc_server_;
    std::thread client_rpc_server_thread_;
    std::optional<DataManager> data_manager_;
    std::optional<ClientRpcService> client_rpc_service_;

    // Each PeerClient instance maintains its own fixed-size connection pool.
    std::mutex peer_clients_mutex_;
    std::map<std::string, std::unique_ptr<PeerClient>> peer_clients_;

    // Route cache for reducing Master query pressure
    std::optional<RouteCache> route_cache_;

    // Async local copy configuration
    size_t local_copy_async_key_threshold_ = 2;
    size_t local_copy_async_worker_num_ = 1;
    size_t local_copy_async_queue_depth_ = 1024;
    std::unique_ptr<AsyncMemcpyExecutor> async_local_copy_executor_;
    size_t remote_batch_async_key_threshold_ = 2;
    size_t remote_batch_async_worker_num_ = 0;
    std::unique_ptr<AsyncMemcpyExecutor> async_remote_batch_executor_;
    P2PClientConfig::LocalTransferMode local_transfer_mode_ =
        P2PClientConfig::LocalTransferMode::TE;
};

}  // namespace mooncake
