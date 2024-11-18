import xmlrpc.client
import logging
import time
# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define IPs and ports for each node in the cluster
# NODES = {
#     "node1": "http://localhost:8000/",
#     "node2": "http://localhost:8001/",
#     "node3": "http://localhost:8002/"
# }


# Correct format for NODES dictionary in client.py
NODES = {
    "node1": "http://10.128.0.4:17000/",
    "node2": "http://10.128.0.6:17001/",
    "node3": "http://10.128.0.5:17002/"
}


def find_leader(current_leader=None):
    """Attempts to find the leader by querying each node, and reprints the leader if it changes."""
    for node_url in NODES.values():
        try:
            with xmlrpc.client.ServerProxy(node_url) as client:
                if client.is_leader():
                    # If the leader has changed, notify the user
                    if node_url != current_leader:
                        logging.info(f"Leader changed: New leader found at {node_url}")
                    else:
                        logging.info(f"Leader found at {node_url}")
                    return node_url
        except Exception as e:
            logging.warning(f"Failed to connect to {node_url}: {e}")
    
    logging.error("Leader not found after checking all nodes.")
    return None


def delete_log_file(node_url):
    """Sends a request to the specified node to delete its log file."""
    if node_url:
        try:
            with xmlrpc.client.ServerProxy(node_url) as client:
                result = client.delete_log_file()
                if result:
                    logging.info(f"Log file successfully deleted at {node_url}.")
                else:
                    logging.error(f"Failed to delete log file at {node_url}.")
        except Exception as e:
            logging.error(f"Could not connect to {node_url} to delete log file: {e}")
    else:
        logging.warning("No valid node URL provided for log file deletion.")




def set_heartbeat_interval(leader_url):
    """Set the heartbeat interval of the leader temporarily to a new value."""
    new_interval = 40.0  # Desired temporary heartbeat interval in seconds
    if leader_url:
        try:
            with xmlrpc.client.ServerProxy(leader_url) as client:
                original_interval = client.get_heartbeat_interval()
                response = client.set_heartbeat_interval(new_interval)
                # logging.info(f"Heartbeat interval temporarily set to {new_interval} seconds on {leader_url}. Response: {response}")
                

                time.sleep(new_interval)

                client.set_heartbeat_interval(original_interval)
                # logging.info(f"Heartbeat interval reset to original value of {original_interval} seconds on {leader_url}.")


                

        except Exception as e:
            logging.error(f"Failed to set or reset heartbeat interval on {leader_url}: {e}")
    else:
        logging.warning("No leader found to set heartbeat interval.")
    return leader_url


def write_value_to_leader(leader_url,simulate_failure=False):
    """Submit a value to the current leader."""
    value = input("Enter the value to write: ")
    logging.info(f"Attempting to write value: {value}")

    if leader_url:
        try:
            with xmlrpc.client.ServerProxy(leader_url) as client:
                client.set_replication_simulation(simulate_failure)
                response = client.submit_value(value)
                logging.info(f"Response from leader: {response}")
                
                if "Error" in response:
                    logging.warning("Error submitting value, attempting to find new leader.")
                    return find_leader(leader_url)  # Retry finding the leader
        except Exception as e:
            logging.error(f"Failed to submit value to leader at {leader_url}: {e}")
            return find_leader(leader_url)  # Retry finding the leader
    else:
        logging.warning("No leader found to write to.")
    return leader_url

def submit_values_with_leader_detection():
    """Main loop for user interactions with the Raft cluster."""
    leader_url = find_leader()  # Initial leader detection

    while True:
        command = input(
            'To set heartbeat interval enter "1"\n'
            'To write values to all nodes through leader enter "2"\n'
            'To write values to only leader and stimulate a failure enter "3"\n'
            'To delete log file of a follower enter "4"\n'
            '(or "exit" to quit): '
        )

        if command == "1":
            leader_url = set_heartbeat_interval(leader_url)
        elif command == "2":
            leader_url = write_value_to_leader(leader_url, simulate_failure=False)
        elif command == "3":
            leader_url = write_value_to_leader(leader_url, simulate_failure=True)
        elif command == "4":
            node = input("Enter the node name to delete its log file (node1, node2, node3): ")
            if node in NODES:
                delete_log_file(NODES[node])
            else:
                logging.warning("Invalid node name. Please enter one of the specified node names.")
        elif command.lower() == "exit":
            logging.info("Exiting.")
            break
        else:
            logging.warning("Invalid command.")

        # Re-check leader after operations
        leader_url = find_leader(leader_url)
    
        # Check if the leader has changed after each command
        new_leader_url = find_leader(leader_url)
        if new_leader_url != leader_url:
            leader_url = new_leader_url  # Update the leader URL if it has changed

if __name__ == "__main__":
    submit_values_with_leader_detection()



