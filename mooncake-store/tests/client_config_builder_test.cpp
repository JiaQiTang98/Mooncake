#include <gtest/gtest.h>

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
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigAcceptsCustomAsyncCopyConfig) {
    auto config = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "", 12345,
        8, 2048, 512 * 1024 * 1024, 120000, 5, 3, 256);

    EXPECT_EQ(config.local_copy_async_key_threshold, 5u);
    EXPECT_EQ(config.local_copy_async_worker_num, 3u);
    EXPECT_EQ(config.local_copy_async_queue_depth, 256u);
}

}  // namespace
}  // namespace mooncake

