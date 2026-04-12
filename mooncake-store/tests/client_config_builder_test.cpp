#include <gtest/gtest.h>
#include <stdexcept>

#include "client_config_builder.h"

namespace mooncake {
namespace {

const char* kTieredConfigJson = R"({
  "tiers": [
    {
      "type": "DRAM",
      "capacity": 1048576,
      "priority": 10
    }
  ]
})";

TEST(ClientConfigBuilderTest, BuildP2PClientConfigUsesDefaults) {
    auto config = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson);

    EXPECT_EQ(config.local_copy_async_key_threshold, 2u);
    EXPECT_EQ(config.local_copy_async_worker_num, 1u);
    EXPECT_EQ(config.local_copy_async_queue_depth, 1024u);
    EXPECT_EQ(config.remote_batch_async_key_threshold, 2u);
    EXPECT_EQ(config.remote_batch_async_worker_num, 0u);
    EXPECT_EQ(config.local_transfer_mode,
              P2PClientConfig::LocalTransferMode::TE);
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigAcceptsCustomAsyncCopyConfig) {
    auto config = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "", 12345,
        8, 2048, 512 * 1024 * 1024, 120000, 5, 3, 256, "te", 7, 6);

    EXPECT_EQ(config.local_copy_async_key_threshold, 5u);
    EXPECT_EQ(config.local_copy_async_worker_num, 3u);
    EXPECT_EQ(config.local_copy_async_queue_depth, 256u);
    EXPECT_EQ(config.remote_batch_async_key_threshold, 7u);
    EXPECT_EQ(config.remote_batch_async_worker_num, 6u);
    EXPECT_EQ(config.local_transfer_mode,
              P2PClientConfig::LocalTransferMode::TE);
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigRejectsInvalidTransferMode) {
    EXPECT_THROW(
        ClientConfigBuilder::build_p2p_real_client(
            "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
            std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr,
            "", 12345, 8, 2048, 512 * 1024 * 1024, 120000, 5, 3, 256,
            "invalid_mode"),
        std::runtime_error);
}

}  // namespace
}  // namespace mooncake
