import pickle
import networkx as nx
import pandas as pd
import os
import time
from datetime import datetime
import pytz

# Define GMT+7 timezone
GMT7 = pytz.timezone('Asia/Bangkok')  # Bangkok is in GMT+7

def fix_graph_timestamps():
    """
    Fix the knowledge graph by setting node timestamps based on the earliest incoming edge timestamps.
    All timestamps will be made timezone-aware and set to GMT+7.
    """
    input_file = "knowledge_graph_p3_0201-0305.pkl"
    output_file = "knowledge_graph_p3_fixed_0201-0305.pkl"
    
    print(f"Loading graph from {input_file}...")
    with open(input_file, "rb") as f:
        G = pickle.load(f)
    
    print(f"Loaded graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
    
    # Dictionary to track which nodes have been processed
    processed_nodes = set()
    
    # Process nodes by looking at incoming edges
    total_nodes = G.number_of_nodes()
    print(f"Processing {total_nodes} nodes...")
    
    # Process nodes with incoming edges
    for i, node in enumerate(G.nodes()):
        if i % 10 == 0:
            print(f"Processed {i}/{total_nodes} nodes")
        
        # Get all incoming edges for this node
        incoming_edges = list(G.in_edges(node, data=True))
        
        # Skip if the node already has a timestamp
        if 'timestamp' in G.nodes[node] and G.nodes[node]['timestamp'] is not None:
            # Make sure existing timestamp is timezone aware
            existing_ts = G.nodes[node]['timestamp']
            if hasattr(existing_ts, 'tzinfo') and existing_ts.tzinfo is None:
                # Convert naive timestamp to GMT+7
                try:
                    G.nodes[node]['timestamp'] = pd.Timestamp(existing_ts).tz_localize('UTC').tz_convert(GMT7)
                except:
                    pass
            processed_nodes.add(node)
            continue
        
        # Find the earliest timestamp from incoming edges
        earliest_timestamp = None
        for _, _, data in incoming_edges:
            if 'timestamp' in data and data['timestamp'] is not None:
                edge_timestamp = data['timestamp']
                # Convert to timestamp if it's not already
                try:
                    if hasattr(edge_timestamp, 'timestamp'):
                        # It's a datetime or Timestamp object
                        edge_timestamp = edge_timestamp.timestamp()
                    elif isinstance(edge_timestamp, str):
                        # Try to parse string timestamp
                        try:
                            edge_timestamp = pd.to_datetime(edge_timestamp).timestamp()
                        except:
                            continue
                    
                    if earliest_timestamp is None or edge_timestamp < earliest_timestamp:
                        earliest_timestamp = edge_timestamp
                except Exception as e:
                    print(f"Error processing timestamp: {e}")
                    continue
        
        # Set the node timestamp if found
        if earliest_timestamp is not None:
            # Convert back to pd.Timestamp for consistency, making it timezone aware
            try:
                # Create a UTC timestamp and convert to GMT+7
                timestamp_utc = pd.Timestamp.fromtimestamp(earliest_timestamp, tz='UTC')
                G.nodes[node]['timestamp'] = timestamp_utc.tz_convert(GMT7)
                processed_nodes.add(node)
            except Exception as e:
                print(f"Error setting timestamp for node {node}: {e}")
    
    # Handle nodes with no incoming edges or no timestamp on edges
    # For these, we'll use timestamps from outgoing edges
    for node in G.nodes():
        if node not in processed_nodes:
            outgoing_edges = list(G.out_edges(node, data=True))
            earliest_timestamp = None
            
            for _, _, data in outgoing_edges:
                if 'timestamp' in data and data['timestamp'] is not None:
                    edge_timestamp = data['timestamp']
                    # Convert to timestamp if it's not already
                    try:
                        if hasattr(edge_timestamp, 'timestamp'):
                            edge_timestamp = edge_timestamp.timestamp()
                        elif isinstance(edge_timestamp, str):
                            try:
                                edge_timestamp = pd.to_datetime(edge_timestamp).timestamp()
                            except:
                                continue
                        
                        if earliest_timestamp is None or edge_timestamp < earliest_timestamp:
                            earliest_timestamp = edge_timestamp
                    except Exception as e:
                        print(f"Error processing timestamp: {e}")
                        continue
            
            if earliest_timestamp is not None:
                try:
                    # Create a UTC timestamp and convert to GMT+7
                    timestamp_utc = pd.Timestamp.fromtimestamp(earliest_timestamp, tz='UTC')
                    G.nodes[node]['timestamp'] = timestamp_utc.tz_convert(GMT7)
                    processed_nodes.add(node)
                except Exception as e:
                    print(f"Error setting timestamp for node {node}: {e}")
    
    # Count nodes that still don't have timestamps
    nodes_without_timestamp = total_nodes - len(processed_nodes)
    if nodes_without_timestamp > 0:
        print(f"Warning: {nodes_without_timestamp} nodes still don't have timestamps")
        
        # Set a default timestamp for nodes without one (current time in GMT+7)
        current_time = datetime.now(GMT7)
        current_time_pd = pd.Timestamp(current_time)
        
        for node in G.nodes():
            if node not in processed_nodes:
                print(f"Setting default timestamp for node: {node}")
                G.nodes[node]['timestamp'] = current_time_pd
    
    # Verify all timestamps are timezone aware
    tz_issues = 0
    for node in G.nodes():
        if 'timestamp' in G.nodes[node]:
            ts = G.nodes[node]['timestamp']
            if hasattr(ts, 'tzinfo') and ts.tzinfo is None:
                # Try to fix any remaining timezone-naive timestamps
                try:
                    G.nodes[node]['timestamp'] = pd.Timestamp(ts).tz_localize('UTC').tz_convert(GMT7)
                    tz_issues += 1
                except:
                    pass
    
    if tz_issues > 0:
        print(f"Fixed timezone issues for {tz_issues} nodes")
    
    # Save the fixed graph
    print(f"Saving fixed graph to {output_file}...")
    with open(output_file, "wb") as f:
        pickle.dump(G, f)
    
    print("Graph fixing complete!")
    print(f"Total nodes processed: {len(processed_nodes)}")
    
    # Create a backup
    backup_file = f"knowledge_graph_p3_fixed_backup_{datetime.now(GMT7).strftime('%Y%m%d_%H%M%S')}.pkl"
    with open(backup_file, "wb") as f:
        pickle.dump(G, f)
    print(f"Created backup at {backup_file}")
    
    return G

if __name__ == "__main__":
    start_time = time.time()
    G = fix_graph_timestamps()
    elapsed_time = time.time() - start_time
    print(f"Processing completed in {elapsed_time:.2f} seconds")
