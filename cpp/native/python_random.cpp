#include "python_random.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <limits>
#include <random>
#include <stdexcept>
#include <vector>

namespace ome_zarr_c::native_code {

PythonRandom::PythonRandom() {
    init_genrand(5489U);
}

void PythonRandom::init_genrand(const std::uint32_t seed) {
    state_[0] = seed;
    for (int index = 1; index < kStateSize; ++index) {
        state_[index] = (1812433253U *
                         (state_[index - 1] ^ (state_[index - 1] >> 30)) +
                         static_cast<std::uint32_t>(index));
    }
    index_ = kStateSize;
}

void PythonRandom::seed_words(const std::vector<std::uint32_t>& words) {
    const std::vector<std::uint32_t> init_key =
        words.empty() ? std::vector<std::uint32_t>{0U} : words;

    init_genrand(19650218U);
    std::size_t i = 1;
    std::size_t j = 0;
    std::size_t k = std::max<std::size_t>(kStateSize, init_key.size());
    for (; k > 0; --k) {
        state_[i] = (state_[i] ^
                     ((state_[i - 1] ^ (state_[i - 1] >> 30)) * 1664525U)) +
            init_key[j] + static_cast<std::uint32_t>(j);
        i += 1;
        j += 1;
        if (i >= static_cast<std::size_t>(kStateSize)) {
            state_[0] = state_[kStateSize - 1];
            i = 1;
        }
        if (j >= init_key.size()) {
            j = 0;
        }
    }
    for (k = kStateSize - 1; k > 0; --k) {
        state_[i] = (state_[i] ^
                     ((state_[i - 1] ^ (state_[i - 1] >> 30)) * 1566083941U)) -
            static_cast<std::uint32_t>(i);
        i += 1;
        if (i >= static_cast<std::size_t>(kStateSize)) {
            state_[0] = state_[kStateSize - 1];
            i = 1;
        }
    }
    state_[0] = 0x80000000U;
    index_ = kStateSize;
}

void PythonRandom::twist() {
    static const std::uint32_t mag01[2] = {0x0U, kMatrixA};
    for (int kk = 0; kk < kStateSize - kOffset; ++kk) {
        const std::uint32_t y =
            (state_[kk] & kUpperMask) | (state_[kk + 1] & kLowerMask);
        state_[kk] = state_[kk + kOffset] ^ (y >> 1) ^ mag01[y & 0x1U];
    }
    for (int kk = kStateSize - kOffset; kk < kStateSize - 1; ++kk) {
        const std::uint32_t y =
            (state_[kk] & kUpperMask) | (state_[kk + 1] & kLowerMask);
        state_[kk] =
            state_[kk + (kOffset - kStateSize)] ^ (y >> 1) ^ mag01[y & 0x1U];
    }
    const std::uint32_t y =
        (state_[kStateSize - 1] & kUpperMask) | (state_[0] & kLowerMask);
    state_[kStateSize - 1] =
        state_[kOffset - 1] ^ (y >> 1) ^ mag01[y & 0x1U];
    index_ = 0;
}

std::uint32_t PythonRandom::getrandbits(const int k) {
    if (k < 0 || k > 32) {
        throw std::invalid_argument("getrandbits only supports 0..32 bits");
    }
    if (k == 0) {
        return 0U;
    }
    if (index_ >= kStateSize) {
        twist();
    }
    std::uint32_t y = state_[index_++];
    y ^= (y >> 11);
    y ^= (y << 7) & 0x9d2c5680U;
    y ^= (y << 15) & 0xefc60000U;
    y ^= (y >> 18);
    return y >> (32 - k);
}

std::uint32_t PythonRandom::randbelow(const std::uint32_t n) {
    if (n == 0U) {
        throw std::invalid_argument("randbelow requires n > 0");
    }
    int bits = 0;
    std::uint32_t tmp = n;
    while (tmp > 0U) {
        bits += 1;
        tmp >>= 1;
    }
    std::uint32_t value = getrandbits(bits);
    while (value >= n) {
        value = getrandbits(bits);
    }
    return value;
}

std::vector<std::uint32_t> python_random_seed_words_from_u64(
    const std::uint64_t seed) {
    if (seed == 0U) {
        return {0U};
    }

    std::vector<std::uint32_t> words;
    std::uint64_t value = seed;
    while (value != 0U) {
        words.push_back(static_cast<std::uint32_t>(
            value & std::numeric_limits<std::uint32_t>::max()));
        value >>= 32U;
    }
    if (words.empty()) {
        words.push_back(0U);
    }
    return words;
}

PythonRandom python_random_from_seed(const std::optional<std::uint64_t> seed) {
    PythonRandom generator{};
    if (seed.has_value()) {
        generator.seed_words(python_random_seed_words_from_u64(seed.value()));
        return generator;
    }

    std::random_device device;
    std::vector<std::uint32_t> words;
    words.reserve(8);
    for (int index = 0; index < 8; ++index) {
        words.push_back(device());
    }
    if (std::all_of(words.begin(), words.end(), [](const std::uint32_t value) {
            return value == 0U;
        })) {
        const auto now = std::chrono::high_resolution_clock::now()
                             .time_since_epoch()
                             .count();
        words[0] = static_cast<std::uint32_t>(now);
        words[1] = static_cast<std::uint32_t>(now >> 32U);
    }
    generator.seed_words(words);
    return generator;
}

}  // namespace ome_zarr_c::native_code
