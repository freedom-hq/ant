#ifndef ANTSTREAM_BRIDGING_HEADER_H
#define ANTSTREAM_BRIDGING_HEADER_H

/*
 * Bridging header for the AntStream demo app. Re-exports the
 * hand-written C surface in `crates/ant-ffi/include/ant.h` so Swift can
 * call the node lifecycle, gateway, identity, storage, and account entry
 * points directly.
 *
 * The Xcode project adds `crates/ant-ffi/include` to HEADER_SEARCH_PATHS,
 * so this lookup is by basename.
 */
#import "ant.h"

#endif /* ANTSTREAM_BRIDGING_HEADER_H */
