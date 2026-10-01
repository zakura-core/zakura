#include <cstdint>
#include "script/script.h"

extern "C" uint32_t oracle_sigops(const unsigned char* bytes, size_t size, bool accurate) {
    CScript script(bytes, bytes + size);
    return script.GetSigOpCount(accurate);
}

extern "C" uint32_t oracle_p2sh_sigops(const unsigned char* key, size_t key_size,
                                     const unsigned char* sig, size_t sig_size) {
    CScript script_pub_key(key, key + key_size);
    CScript script_sig(sig, sig + sig_size);
    return script_pub_key.GetSigOpCount(script_sig);
}
