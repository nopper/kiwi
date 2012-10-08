//
//  node.h
//  indexer
//
//  Created by nopper on 9/15/12.
//  Copyright (c) 2012 nopper. All rights reserved.
//

#ifndef indexer_node_h
#define indexer_node_h

typedef struct _node {
    uint64 id;      // Node identifier
    uint64 rel_id;  // First relationship identifier
    uint64 prop_id; // First property identifier

} node;

#endif
