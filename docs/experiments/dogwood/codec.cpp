// Reference kernels for comparing decode schedules, not a production codec.
#include <openssl/sha.h>
#include <sys/resource.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

using Row = std::vector<uint16_t>;
using Matrix = std::vector<Row>;
using Clock = std::chrono::steady_clock;
using Digest = std::array<unsigned char, SHA256_DIGEST_LENGTH>;

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

double elapsed(Clock::time_point start) {
    return std::chrono::duration<double, std::milli>(Clock::now() - start).count();
}

struct Field {
    std::array<uint16_t, 131070> exp{};
    std::array<unsigned, 65536> log{};
    Field() {
        unsigned x = 1;
        for (unsigned i = 0; i < 65535; ++i) {
            exp[i] = static_cast<uint16_t>(x);
            log[x] = i;
            x <<= 1;
            if (x & 65536) x ^= 0x1100b;
        }
        require(x == 1, "field cycle");
        for (unsigned i = 65535; i < exp.size(); ++i) exp[i] = exp[i - 65535];
    }
    uint16_t mul(uint16_t a, uint16_t b) const {
        return a && b ? exp[log[a] + log[b]] : 0;
    }
    uint16_t inv(uint16_t a) const {
        require(a != 0, "inverse of zero");
        return exp[65535 - log[a]];
    }
};
const Field gf;

void axpy(Row& dst, const Row& src, uint16_t factor) {
    if (!factor) return;
    if (factor == 1) {
        for (size_t i = 0; i < dst.size(); ++i) dst[i] ^= src[i];
    } else {
        for (size_t i = 0; i < dst.size(); ++i) dst[i] ^= gf.mul(src[i], factor);
    }
}

Matrix inverse(Matrix a) {
    const size_t k = a.size();
    Matrix b(k, Row(k));
    for (size_t i = 0; i < k; ++i) b[i][i] = 1;
    for (size_t c = 0; c < k; ++c) {
        size_t r = c;
        while (r < k && !a[r][c]) ++r;
        require(r < k, "singular matrix");
        std::swap(a[r], a[c]);
        std::swap(b[r], b[c]);
        auto scale = gf.inv(a[c][c]);
        for (auto& x : a[c]) x = gf.mul(x, scale);
        for (auto& x : b[c]) x = gf.mul(x, scale);
        for (size_t j = 0; j < k; ++j) {
            if (j == c) continue;
            auto f = a[j][c];
            axpy(a[j], a[c], f);
            axpy(b[j], b[c], f);
        }
    }
    return b;
}

// Store the spec's G transposed: one coefficient row per encoded part.
Matrix generator(size_t k, size_t n, bool random, uint64_t seed) {
    Matrix g(n, Row(k));
    for (size_t i = 0; i < k; ++i) g[i][i] = 1;
    if (random) {
        std::mt19937_64 rng(seed);
        for (size_t i = k; i < n; ++i)
            for (auto& x : g[i]) x = static_cast<uint16_t>(rng());
        return g;
    }
    Matrix v(k, Row(k));
    for (size_t r = 0; r < k; ++r)
        for (size_t c = 0; c < k; ++c) v[r][c] = gf.exp[(r * c) % 65535];
    auto vi = inverse(v);
    for (size_t i = k; i < n; ++i)
        for (size_t r = 0; r < k; ++r)
            for (size_t j = 0; j < k; ++j)
                g[i][r] ^= gf.mul(vi[r][j], gf.exp[(j * i) % 65535]);
    return g;
}

Matrix encode(const Matrix& data, const Matrix& g) {
    Matrix parts = data;
    for (size_t i = data.size(); i < g.size(); ++i) {
        Row part(data[0].size());
        for (size_t j = 0; j < data.size(); ++j) axpy(part, data[j], g[i][j]);
        parts.push_back(std::move(part));
    }
    return parts;
}

struct Decoder {
    Matrix rows, payload;
    size_t rank = 0;
    bool eager;
    explicit Decoder(size_t k, bool eager = false) : rows(k), payload(k), eager(eager) {}
    bool add(Row row, Row value) {
        if (eager) for (size_t c = 0; c < rows.size(); ++c) {
            if (rows[c].empty()) continue;
            auto f = row[c];
            axpy(row, rows[c], f);
            axpy(value, payload[c], f);
        }
        for (size_t c = 0; c < rows.size(); ++c) {
            if (!row[c]) continue;
            if (!rows[c].empty()) {
                auto f = row[c];
                axpy(row, rows[c], f);
                axpy(value, payload[c], f);
            } else {
                auto scale = gf.inv(row[c]);
                for (auto& x : row) x = gf.mul(x, scale);
                if (scale != 1) for (auto& x : value) x = gf.mul(x, scale);
                rows[c] = std::move(row);
                payload[c] = std::move(value);
                ++rank;
                if (eager) for (size_t j = 0; j < rows.size(); ++j) {
                    if (j == c || rows[j].empty()) continue;
                    auto f = rows[j][c];
                    axpy(rows[j], rows[c], f);
                    axpy(payload[j], payload[c], f);
                }
                return true;
            }
        }
        return false;
    }
    Matrix finish() {
        require(rank == rows.size(), "insufficient rank");
        if (!eager) for (size_t c = rows.size(); c-- > 0;)
            for (size_t j = c + 1; j < rows.size(); ++j)
                axpy(payload[c], payload[j], rows[c][j]);
        return std::move(payload);
    }
};

// Benchmark-only tree: domain byte, little-endian index, payload; duplicate last leaf.
// The wire profile has not fixed its hash or tree construction.
Digest leaf(const Row& row, size_t index) {
    std::vector<unsigned char> bytes(5 + 2 * row.size());
    for (size_t j = 0; j < 4; ++j) bytes[1 + j] = (index >> (8 * j)) & 255;
    for (size_t j = 0; j < row.size(); ++j) {
        bytes[5 + 2 * j] = row[j] & 255;
        bytes[6 + 2 * j] = row[j] >> 8;
    }
    Digest out;
    SHA256(bytes.data(), bytes.size(), out.data());
    return out;
}

Digest parent(const Digest& left, const Digest& right) {
    std::array<unsigned char, 65> bytes{};
    bytes[0] = 1;
    std::copy(left.begin(), left.end(), bytes.begin() + 1);
    std::copy(right.begin(), right.end(), bytes.begin() + 33);
    Digest out;
    SHA256(bytes.data(), bytes.size(), out.data());
    return out;
}

using Tree = std::vector<std::vector<Digest>>;
Tree tree(const Matrix& parts) {
    Tree levels(1);
    for (size_t i = 0; i < parts.size(); ++i) levels[0].push_back(leaf(parts[i], i));
    while (levels.back().size() > 1) {
        const auto& prev = levels.back();
        std::vector<Digest> next;
        for (size_t i = 0; i < prev.size(); i += 2)
            next.push_back(parent(prev[i], prev[std::min(i + 1, prev.size() - 1)]));
        levels.push_back(std::move(next));
    }
    return levels;
}

bool verify(const Row& part, size_t i, const Tree& levels) {
    auto hash = leaf(part, i);
    for (size_t level = 0; level + 1 < levels.size(); ++level) {
        auto sibling = levels[level][std::min(i ^ 1, levels[level].size() - 1)];
        hash = i & 1 ? parent(sibling, hash) : parent(hash, sibling);
        i /= 2;
    }
    return hash == levels.back()[0];
}

uint16_t slow_mul(unsigned a, unsigned b) {
    unsigned out = 0;
    while (b) {
        if (b & 1) out ^= a;
        b >>= 1;
        a <<= 1;
        if (a & 65536) a ^= 0x1100b;
    }
    return static_cast<uint16_t>(out);
}

void tests() {
    std::mt19937_64 rng(88);
    for (size_t i = 0; i < 100000; ++i) {
        auto a = static_cast<uint16_t>(rng()), b = static_cast<uint16_t>(rng());
        require(gf.mul(a, b) == slow_mul(a, b), "independent field multiplication");
        if (a) require(gf.mul(a, gf.inv(a)) == 1, "inverse");
    }
    require(encode({{1}, {2}}, generator(2, 3, false, 0))[2][0] == 4, "spec vector");
    size_t subsets = 0;
    for (size_t k = 2; k <= 5; ++k) {
        const size_t n = k + 3;
        Matrix data(k, Row(19));
        for (auto& row : data) for (auto& x : row) x = static_cast<uint16_t>(rng());
        data.back().back() = 0;
        auto g = generator(k, n, false, 0), parts = encode(data, g);
        auto commitment = tree(parts);
        for (size_t i = 0; i < n; ++i) require(verify(parts[i], i, commitment), "proof");
        auto corrupted = parts[0];
        corrupted[0] ^= 1;
        require(!verify(corrupted, 0, commitment), "bad membership");
        for (unsigned mask = 0; mask < (1u << n); ++mask) {
            if (__builtin_popcount(mask) != static_cast<int>(k)) continue;
            for (bool eager : {false, true}) {
                Decoder d(k, eager);
                for (size_t i = n; i-- > 0;) if (mask & (1u << i)) d.add(g[i], parts[i]);
                require(d.finish() == data, "every k-part subset");
            }
            ++subsets;
        }
        Decoder incomplete(k);
        for (size_t i = 0; i < k - 1; ++i) incomplete.add(g[i], parts[i]);
        require(!incomplete.add(g[0], parts[0]), "duplicate rank");
        require(incomplete.rank == k - 1, "withheld rank");
        parts.back()[0] ^= 1;
        auto bad_root = tree(parts);
        require(verify(parts.back(), n - 1, bad_root), "committed invalid parity membership");
        Decoder bad(k);
        for (size_t i = 0; i < k - 1; ++i) bad.add(g[i], parts[i]);
        bad.add(g.back(), parts.back());
        require(tree(encode(bad.finish(), g)).back()[0] != bad_root.back()[0], "invalid codeword");
    }
    auto g = generator(8, 10, true, 4);
    g[9] = g[8];
    Decoder deficient(8);
    for (size_t i : {0u, 1u, 2u, 3u, 4u, 5u, 8u, 9u}) deficient.add(g[i], Row{0});
    require(deficient.rank == 7, "RLNC dependent rows");
    std::cout << "codec tests passed; RS subsets=" << subsets << "\n";
}

void benchmark(size_t k, double ratio, bool random, const std::string& trace,
               size_t assemblies, uint64_t seed) {
    const size_t words = 32768, n = k + static_cast<size_t>(std::ceil(k * ratio));
    std::mt19937_64 rng(seed);
    Matrix data(k, Row(words));
    for (auto& row : data) for (auto& x : row) x = static_cast<uint16_t>(rng());
    auto start = Clock::now();
    auto g = generator(k, n, random, seed);
    double matrix_ms = elapsed(start);
    start = Clock::now();
    auto parts = encode(data, g);
    double encode_ms = elapsed(start);
    start = Clock::now();
    auto commitment = tree(parts);
    double root_ms = elapsed(start);
    std::vector<size_t> indices(n);
    std::iota(indices.begin(), indices.end(), 0);
    if (trace == "random") std::shuffle(indices.begin(), indices.end(), rng);
    if (trace == "parity_first") std::rotate(indices.begin(), indices.begin() + k, indices.end());
    if (trace == "withheld") indices.erase(indices.begin(), indices.begin() + (n - k));
    if (trace == "insufficient") indices.resize(k - 1);
    if (trace == "invalid_codeword") {
        parts.back()[0] ^= 1;
        commitment = tree(parts);
        std::rotate(indices.begin(), indices.end() - 1, indices.end());
    }
    // Determine required arrivals by rank, not by assuming k RLNC rows suffice.
    Decoder rank(k);
    size_t used = 0;
    for (auto i : indices) {
        rank.add(g[i], Row{0});
        ++used;
        if (rank.rank == k) break;
    }
    indices.resize(used);
    for (bool eager : {false, true}) {
        std::vector<Decoder> decoders;
        for (size_t a = 0; a < assemblies; ++a) decoders.emplace_back(k, eager);
        std::vector<double> proof_cost, add_cost, finish_cost(assemblies), check_cost(assemblies);
        double proof_ms = 0, elimination_ms = 0, finish_ms = 0, check_ms = 0;
        for (auto i : indices) for (auto& decoder : decoders) {
            start = Clock::now();
            require(verify(parts[i], i, commitment), "benchmark membership");
            auto cost = elapsed(start);
            proof_cost.push_back(cost);
            proof_ms += cost;
            start = Clock::now();
            decoder.add(g[i], parts[i]);
            cost = elapsed(start);
            add_cost.push_back(cost);
            elimination_ms += cost;
        }
        bool valid = rank.rank == k;
        if (valid) for (size_t a = 0; a < assemblies; ++a) {
            start = Clock::now();
            auto decoded = decoders[a].finish();
            finish_cost[a] = elapsed(start);
            finish_ms += finish_cost[a];
            start = Clock::now();
            bool matches = tree(encode(decoded, g)).back()[0] == commitment.back()[0];
            check_cost[a] = elapsed(start);
            check_ms += check_cost[a];
            if (trace == "invalid_codeword") require(!matches, "invalid codeword accepted");
            else require(matches && decoded == data, "benchmark decode");
            valid = valid && matches;
        }
        // Replay measured task durations on one serial worker. No sleep or ideal overlap.
        // Both schedules verify at arrival; batch holds elimination until the last input.
        for (bool online : {false, true}) for (double gap_ms : {0.0, 0.1, 1.0}) {
            if (eager && !online) continue;
            double worker = 0, before = 0;
            double last = (used - 1) * gap_ms;
            for (size_t j = 0; j < proof_cost.size(); ++j) {
                double arrival = (j / assemblies) * gap_ms;
                worker = std::max(worker, arrival);
                worker += proof_cost[j];
                if (online) {
                    before += std::max(0.0, std::min(add_cost[j], last - worker));
                    worker += add_cost[j];
                }
            }
            if (!online) worker += elimination_ms;
            worker += finish_ms + check_ms;
            rusage usage{};
            getrusage(RUSAGE_SELF, &usage);
            // Explicit array-storage bound excludes allocator overhead and crypto internals.
            size_t storage = (k + 2 * n) * words * 2 + n * k * 2 +
                assemblies * (2 * k * words * 2 + k * k * 2);
            std::cout << (random ? "rlnc16" : "rs16") << ',' << k << ',' << n << ','
                << trace << ',' << assemblies << ',' << seed << ','
                << (eager ? "online_eager" : online ? "online" : "batch")
                << ',' << gap_ms << ',' << used << ',' << rank.rank << ',' << valid << ','
                << matrix_ms << ',' << encode_ms << ',' << root_ms << ',' << proof_ms << ','
                << elimination_ms << ',' << finish_ms << ',' << check_ms << ',' << before << ','
                << (worker - last) << ',' << worker << ',' << storage << ',' << usage.ru_maxrss << '\n';
        }
    }
}

int main(int argc, char** argv) {
    try {
        if (argc == 2 && std::string(argv[1]) == "--test") { tests(); return 0; }
        require(argc == 8, "usage: codec k parity_ratio rs|rlnc trace assemblies seed repeats");
        size_t k = std::stoul(argv[1]), assemblies = std::stoul(argv[5]);
        double ratio = std::stod(argv[2]);
        require(k >= 2 && k <= 256 && ratio > 0 && ratio <= 1 && assemblies >= 1 && assemblies <= 8,
                "bounded benchmark arguments");
        require(std::string(argv[3]) == "rs" || std::string(argv[3]) == "rlnc", "codec name");
        std::string trace = argv[4];
        require(trace == "systematic" || trace == "random" || trace == "parity_first" ||
                trace == "withheld" || trace == "insufficient" || trace == "invalid_codeword", "trace name");
        std::cout << "codec,k,n,trace,assemblies,seed,schedule,gap_ms,arrivals,rank,valid,matrix_ms,"
            "encode_ms,root_ms,proof_ms,elimination_ms,backsub_ms,reencode_root_ms,"
            "elimination_before_last_ms,tail_ms,completion_ms,array_storage_bound_bytes,process_peak_rss_kib\n";
        for (size_t r = 0; r < std::stoul(argv[7]); ++r)
            benchmark(k, ratio, std::string(argv[3]) == "rlnc", trace, assemblies, std::stoull(argv[6]) + r);
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
