#ifndef NDD_CAPI_H
#define NDD_CAPI_H

/**
 * C ABI for the ndd data plane — one generic entry point over ndd::api so the
 * database can be embedded from any language (Python/Java/Rust/Go) via FFI.
 *
 * No C++ types cross this boundary: requests/responses are byte buffers (JSON
 * for the control plane, msgpack for the vector-heavy ops), collection ids are
 * C strings, and errors are an int code + a message buffer. Tokens/auth/tiers
 * are NOT here — they are server-only; a library caller is trusted in-process
 * and scopes data by the collection_id namespace string.
 *
 * Memory: the library malloc()s every output buffer (*out, *msg, *err_out); the
 * caller MUST release each with ndd_free(). Inputs stay caller-owned.
 *
 * Threads: one handle per process (settings are process-global). A single handle
 * is safe to call concurrently — CollectionManager is internally locked.
 */

#include <stddef.h>
#include <stdint.h>

/* Export only the ndd_* symbols from the shared library (built with
 * -fvisibility=hidden), so it doesn't leak ~thousands of MDBX/OpenSSL symbols
 * that could collide with a host process's own libraries. */
#ifndef NDD_CAPI_EXPORT
#  if defined(_WIN32)
#    define NDD_CAPI_EXPORT __declspec(dllexport)
#  else
#    define NDD_CAPI_EXPORT __attribute__((visibility("default")))
#  endif
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ndd_handle ndd_handle;

/** Options for ndd_open. Zero-initialize, then set data_dir (required). */
typedef struct ndd_open_options {
    const char* data_dir;             /* required: on-disk data directory */
    int skip_sanity;                  /* 1 = skip startup sanity checks (dev/edge) */
    int save_on_shutdown;             /* 1 = flush dirty collections in ndd_close */
    size_t vector_cache_max_bytes;    /* 0 = auto (percent of available RAM) */
    int parallel_insert_threads;      /* <=0 = keep the default */
} ndd_open_options;

/**
 * Open (or create) a database rooted at opts->data_dir. Runs the one-time
 * process-global init (settings, cache ceiling, sanity checks unless skipped),
 * then constructs the engine. Returns NULL on failure; if err_out != NULL it is
 * set to a malloc'd, NUL-terminated message the caller must ndd_free().
 * One handle per process: a second open with a different data_dir fails.
 */
NDD_CAPI_EXPORT ndd_handle* ndd_open(const ndd_open_options* opts, char** err_out);

/** Close the handle (flushes if save_on_shutdown). NULL-safe. */
NDD_CAPI_EXPORT void ndd_close(ndd_handle* handle);

/**
 * Generic dispatch. `op` names the operation; `collection_id` is the
 * "<namespace>/<collection>" string (or a db-name where an op takes one; NULL
 * where unused). `req`/`req_len` is the request body (JSON for most ops, msgpack
 * for add_objects_msgpack). On SUCCESS the response bytes are returned via
 * out/out_len (JSON, or msgpack for search/get_objects); on error the message
 * is returned via msg/msg_len. Both out buffers are malloc'd — release with
 * ndd_free(). Any of out, out_len, msg, msg_len may be NULL to discard.
 *
 * Returns the OperationResult code: 0 ok; 1 not-found; 2 validation; 3 tier;
 * 100 internal / caught exception; 101 unknown op; 102 null handle/arg.
 *
 * Ops: create_collection, describe_collection, list_collections,
 *      delete_collection, shrink_collection, add_objects_json,
 *      add_objects_msgpack, get_objects, search, delete_object, delete_by_filter,
 *      update_filters, create_backup, restore_backup, list_backups,
 *      delete_backup, backup_info, create_rebuild, rebuild_status.
 */
NDD_CAPI_EXPORT int ndd_call(ndd_handle* handle, const char* op, const char* collection_id,
                             const uint8_t* req, size_t req_len,
                             uint8_t** out, size_t* out_len,
                             char** msg, size_t* msg_len);

/** Release a buffer returned by ndd_call / ndd_open. NULL-safe. */
NDD_CAPI_EXPORT void ndd_free(void* p);

/**
 * Library version + the SIMD ISA this .so was built for, as semver build
 * metadata: "<version>+<isa>", e.g. "2.1.0+avx2" (avx2/avx512/neon/sve2).
 * Lets a caller confirm which ISA build it loaded. Static storage; do NOT ndd_free.
 */
NDD_CAPI_EXPORT const char* ndd_version(void);

#ifdef __cplusplus
}
#endif

#endif /* NDD_CAPI_H */
