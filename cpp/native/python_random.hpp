#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <vector>

namespace ome_zarr_c::native_code {

class PythonRandom {
  public:
    PythonRandom();

    void seed_words(const std::vector<std::uint32_t>& words);

    std::uint32_t getrandbits(int k);

    std::uint32_t randbelow(std::uint32_t n);

  private:
    static constexpr int kStateSize = 624;
    static constexpr int kOffset = 397;
    static constexpr std::uint32_t kMatrixA = 0x9908b0dfU;
    static constexpr std::uint32_t kUpperMask = 0x80000000U;
    static constexpr std::uint32_t kLowerMask = 0x7fffffffU;

    void twist();
    void init_genrand(std::uint32_t seed);

    std::array<std::uint32_t, kStateSize> state_{};
    int index_ = kStateSize;
};

std::vector<std::uint32_t> python_random_seed_words_from_u64(std::uint64_t seed);

PythonRandom python_random_from_seed(std::optional<std::uint64_t> seed);

}  // namespace ome_zarr_c::native_code
