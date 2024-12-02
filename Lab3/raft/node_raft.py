

import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import os
import threading
import time
import random
import argparse
import logging
import json



# Function to load the configuration from the config file
def load_config(config_file):
    """Load the configuration from a JSON file."""
    try:
        with open(config_file, 'r') as f:
            config_data = json.load(f)
            return config_data
    except FileNotFoundError:
        print(f"Error: Configuration file {config_file} not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from {config_file}.")
        return None


config_data = load_config('./config_file.json')

# server = SimpleXMLRPCServer(("0.0.0.0", NODES['node1'][1]), allow_none=True)  # Adjust for each node's port

class QuietXMLRPCServer(SimpleXMLRPCServer):
    def __init__(self, *args, **kwargs):
        # Use the QuietXMLRPCRequestHandler to suppress logging
        kwargs['requestHandler'] = QuietXMLRPCRequestHandler
        super().__init__(*args, **kwargs)



class QuietXMLRPCRequestHandler(SimpleXMLRPCRequestHandler):
    def log_message(self, format, *args):
        # Override log_message to suppress all HTTP log messages
        pass

class LogEntry:
    def __init__(self, term, command):
        self.term = term
        self.command = command

    def to_string(self):
        """Convert log entry to a string format."""
        return f"{self.term},{self.command}"

    @staticmethod
    def from_string(entry_str):
        """Create a LogEntry object from a string format."""
        term, command = entry_str.split(",")
        return LogEntry(int(term), command)


class Node:
    def __init__(self, name,cluster_name):
        self.name = name
        self.cluster_name=cluster_name

        


        if config_data and cluster_name in config_data:
            # Create a dictionary of nodes, with node_name as the key and (ip, port) as the value
            cluster = config_data[cluster_name]
            if name in cluster:
                self.ip, self.port = cluster[name]
            else:
                raise ValueError(f"Node {name} not found in the provided cluster configuration.")
            
            # Create peers dictionary (excluding the current node)
            self.peers = {n: addr for n, addr in cluster.items() if n != self.name}
        else:
            raise ValueError(f"Invalid configuration data. '{cluster_name}' not found.")
        
    
        self.lock = threading.Lock()
        self.running = True
        self.is_leader_flag = False
        self.votes_received = 0
        self.current_term = 0
        self.last_heartbeat_time = time.time()
        self.voted_for = None 

        # Initialize log, next_index, and match_index for log replication
        # self.log = []  # List of log entries
        self.next_index = {peer: 1 for peer in self.peers}  # Next log index to send to each peer
        self.match_index = {peer: 0 for peer in self.peers}  # Highest log entry known to be replicated on each peer
        self.commit_index = 0  # Index of the highest log entry known to be committed

        # Set a unique log file for each node
        self.LOG_FILE = f"./logs/{self.name}.log"
        self.log = self.load_log_from_file()  # Load existing log entries from file
        self.simulate_replication_failure = False  # Flag for simulating replication failure

        self.election_timeout = random.uniform(2.0, 20.0)
       

        self.default_heartbeat_interval = 0.1
        self.heartbeat_interval = self.default_heartbeat_interval
        self.role = "follower"  # Each node starts as a follower

        # Cooldown mechanism
        self.cooldown_period = self.election_timeout  # Cooldown period after heartbeat timeout
        self.last_election_time = 0  # Last time the node participated in an election
        self.in_cooldown = False  # Flag to indicate cooldown state



    def request_vote(self):
        """Request votes from peers to become a candidate."""
        self.votes_received = 1
        self.current_term += 1
        self.role = "candidate"
        print(f"{self.name} is requesting votes for term {self.current_term}")

        # Get the term and index of this node's last log entry
        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index].term if self.log else 0
        self.election_timeout = random.uniform(2.0, 15.0)
        # prin()
        

        for peer, (ip, port) in self.peers.items():
            try:
                with xmlrpc.client.ServerProxy(f"http://{ip}:{port}/") as client:
                    # Pass candidate's term, last log term, and last log index
                    print(f"time out set for {self.name} to {self.election_timeout} ")
                    response = client.vote(self.name, self.current_term, last_log_term, last_log_index)
                    if response:
                        self.votes_received += 1
            except ConnectionRefusedError:
                print(f"Connection to {peer} failed.")

        # Check if received majority votes
        if self.votes_received > len(self.peers) // 2:
            self.start_leader()

    
    def vote(self, candidate, term, last_log_term, last_log_index):
        """Vote for a candidate if the candidate's term is greater than the current term
        and the candidate's log is at least as up-to-date as this node's log."""
        with self.lock:
            self.refresh_log_from_file()
            print(f"Received vote request from {candidate} for term {term} with last_log_term {last_log_term}, last_log_index {last_log_index}")

            if term < self.current_term:
                print(f"Vote denied for {candidate}: candidate's term {term} is less than current term {self.current_term}")
                return False

            my_last_log_index = len(self.log) - 1
            my_last_log_term = self.log[my_last_log_index].term if self.log else 0

            if (last_log_term < my_last_log_term) or (last_log_term == my_last_log_term and last_log_index < my_last_log_index):
                print(f"Vote denied for {candidate}: candidate's log is not up-to-date")
                return False

            if (term > self.current_term) or \
            (term == self.current_term and not self.voted_for) or \
            (self.voted_for == candidate):
                self.current_term = term
                self.voted_for = candidate
                self.last_heartbeat_time = time.time()  # Reset the election timeout
                print(f"Vote granted to {candidate} for term {term}")
                return True

            print(f"Vote denied for {candidate}: already voted for {self.voted_for} in term {self.current_term}")
            return False



    def start_leader(self):
        """Become the leader and start sending heartbeats."""
        self.is_leader_flag = True
        self.votes_received = 0
        self.role = "leader"
        print(f"{self.name} is now the leader.")
        threading.Thread(target=self.heartbeat).start()

    def set_heartbeat_interval(self, interval):
        """Set a new heartbeat interval, usually called by the client."""
        self.heartbeat_interval = interval
        # print(f"{self.name} heartbeat interval set to {self.heartbeat_interval} seconds.")


    def heartbeat(self):
        """Send periodic heartbeats to followers."""
        while self.is_leader_flag:
            # print(f"{self.name} sending heartbeat (term {self.current_term} )...")
            for peer, (ip, port) in self.peers.items():
                try:
                    with xmlrpc.client.ServerProxy(f"http://{ip}:{port}/") as client:
                        client.receive_heartbeat(self.current_term)
                        # print(f"Heartbeat sent to {peer}")
                except ConnectionRefusedError:
                    print(f"Connection to {peer} failed.")
            time.sleep(self.heartbeat_interval)  # Sleep based on the heartbeat interval
            ##print(self.heartbeat_interval)

    def receive_heartbeat(self, leader_term):
        """Process a heartbeat received from the leader."""
        with self.lock:
            if leader_term >= self.current_term:
                self.current_term = leader_term
                ##print(f"heart beat recieve at {self.last_heartbeat_time} from leader"  )
                self.last_heartbeat_time = time.time()  # Reset the election timeout
                ##print(f"heart reset at {self.last_heartbeat_time} for follower"  )

                
                if self.role != "follower":
                    print(f"{self.name} switching to follower due to received heartbeat.")
                    self.role = "follower"
                    self.is_leader_flag = False
                    self.in_cooldown = True  # Enter cooldown after receiving a heartbeat
                    # threading.Timer(self.cooldown_period, self.end_cooldown).start()
         
            else:
                print(f"{self.name} ignored heartbeat with lower term {leader_term}.")

    def periodic_receive_status_print(self):
        """Prints follower's status at the set interval."""
        while not self.is_leader_flag:
            print(f"Status: {self.name} is follower, last heartbeat received at {self.last_heartbeat_time}")
            time.sleep(self.status_print_interval)


    def end_cooldown(self):
        with self.lock:
            self.in_cooldown = False
            print(f"{self.name} is out of cooldown and ready to participate in elections.")



    def is_leader(self):
        """Check if the node is the leader."""
        return self.is_leader_flag


    def get_heartbeat_interval(self):
        """Get the current heartbeat interval."""
        return self.heartbeat_interval

    def run_election(self):
        """Monitor election timeouts and initiate elections when necessary."""
        while self.running:
            with self.lock:
                
                # if not self.in_cooldown and (time.time() - self.last_heartbeat_time > self.election_timeout):
                if (time.time() - self.last_heartbeat_time > self.election_timeout):

                    if self.role == "follower":
                        print(f"{self.name} timeout, starting election.")
                        self.last_heartbeat_time = time.time()
                        self.request_vote()
                        self.election_timeout = random.uniform(2.0, 15.0)  # Adjust this range as needed
                        print(f"election timeout {self.election_timeout}")

    def detect_leader_failure(self):
        """Actively check for leader failure across the cluster."""
        while self.running:
            for peer, (ip, port) in self.peers.items():
                try:
                    with xmlrpc.client.ServerProxy(f"http://{ip}:{port}/") as client:
                        # Check if any node believes the leader is down
                        if not client.is_leader():
                            # Trigger election if leader is confirmed down
                            self.request_vote()
                            break
                except ConnectionRefusedError:
                    print(f"Connection to {peer} failed.")
            time.sleep(1)  # Check periodically
        
    def load_log_from_file(self):
        """Load the log from a file at startup or initialize to an empty log if the file is missing."""
        log = []
        try:
            with open(self.LOG_FILE, "r") as f:
                for line in f:
                    term, command = line.strip().split(',')
                    log.append(LogEntry(int(term), command))
            print(f"{self.name} loaded log from file with {len(log)} entries.")
        except FileNotFoundError:
            # Reinitialize log as empty if file is missing
            log = []
            print(f"{self.name} log file not found. Starting with an empty log.")

        return log

    def refresh_log_from_file(self):
        """Refresh self.log by reloading from the file if it has been updated."""
        try:
            # Get the current modification time of the log file
            current_mtime = os.path.getmtime(self.LOG_FILE)

            # Check if the log file has been modified since the last load
            if getattr(self, 'last_log_mtime', None) != current_mtime:
                # Load the log entries from the file
                log = []
                with open(self.LOG_FILE, "r") as f:
                    for line in f:
                        term, command = line.strip().split(',')
                        log.append(LogEntry(int(term), command))

                # Update the in-memory log and modification time
                self.log = log
                self.last_log_mtime = current_mtime  # Update last modification time
                print(f"{self.name}: Log refreshed from file with {len(self.log)} entries.")

        except FileNotFoundError:
            # If the file is missing, reset self.log to an empty list
            self.log = []
            self.last_log_mtime = None
            print(f"{self.name}: Log file not found. Resetting to empty log.")

    def set_replication_simulation(self, simulate_failure):
        """Toggle replication simulation mode based on client request."""
        self.simulate_replication_failure = simulate_failure
        status = "enabled" if simulate_failure else "disabled"
        print(f"Replication failure simulation {status} on {self.name}.")

    def append_entries(self, term, entries):
        """Leader appends entries and attempts replication to followers."""
        if not self.is_leader_flag:
            return False

        # Convert each entry to a LogEntry if they are not already objects
        entries = [LogEntry(term, command) if not isinstance(command, LogEntry) else command for command in entries]

        # Append new entries to leader's log and save to file
        for entry in entries:
            self.log.append(entry)
            with open(self.LOG_FILE, "a") as f:
                f.write(entry.to_string() + "\n")
            print(f"{self.name} appended log entry: {entry.to_string()}")

        # Attempt replication to all followers with simulation check
        for peer, (ip, port) in self.peers.items():
            if self.simulate_replication_failure and peer != "leader":
                print(f"Replication to {peer} skipped due to simulation.")
                continue  # Skip replication to simulate failure

            while True:
                try:
                    # Simulate regular replication attempts
                    prev_log_index = self.next_index[peer] - 1
                    prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 else 0
                    entries_to_send = self.log[self.next_index[peer]:]

                    if not entries_to_send:
                        break  # No new entries to send

                    with xmlrpc.client.ServerProxy(f"http://{ip}:{port}/") as client:
                        success = client.receive_append_entries(
                            self.current_term,
                            prev_log_index,
                            prev_log_term,
                            [entry.to_string() for entry in entries_to_send],
                            self.commit_index
                        )

                        if success:
                            self.match_index[peer] = len(self.log) - 1
                            self.next_index[peer] = len(self.log)
                            print(f"Successfully updated {peer} with {len(entries_to_send)} entries.")
                            break
                        else:
                            self.next_index[peer] = max(0, self.next_index[peer] - 1)
                            print(f"Backtracking nextIndex for {peer} to {self.next_index[peer]} due to mismatch.")
                            time.sleep(0.1)
                except ConnectionRefusedError:
                    print(f"Connection to {peer} failed.")
                    break
        self.check_commit_index()
        return True
     
 

    def receive_append_entries(self, term, prev_log_index, prev_log_term, entries, leader_commit):
        """Follower receives and appends multiple log entries from the leader, ensuring consistency."""
        
        # Refresh in-memory log from file before processing entries
        self.refresh_log_from_file()

        with self.lock:
            if term < self.current_term:
                return False  # Reject entries from an outdated leader

            # Update term and reset role if in a new term
            if term > self.current_term:
                self.current_term = term
                self.role = "follower"
                self.is_leader_flag = False

            # Reset election timer on heartbeat
            self.last_heartbeat_time = time.time()

            # Log consistency check at `prev_log_index`
            if prev_log_index >= len(self.log):
                print(f"{self.name}: Missing entry at prev_log_index {prev_log_index}. Leader will backtrack.")
                return False  # Leader will retry with a lower `nextIndex`

            # Ensure log matches at `prev_log_index`
            if prev_log_index >= 0 and (len(self.log) <= prev_log_index or self.log[prev_log_index].term != prev_log_term):
                print(f"{self.name}: Log mismatch at index {prev_log_index}. Truncating to resolve conflict.")
                self.log = self.log[:prev_log_index]  # Truncate to remove conflicting entries
                with open(self.LOG_FILE, "w") as f:  # Rewrite log file
                    for entry in self.log:
                        f.write(entry.to_string() + "\n")
                return False  # Leader should retry

            # Process and append entries from the leader
            new_index = prev_log_index + 1
            for entry_str in entries:
                entry = LogEntry.from_string(entry_str)

                # Truncate if there's a conflicting entry
                if new_index < len(self.log) and self.log[new_index].term != entry.term:
                    self.log = self.log[:new_index]
                    print(f"{self.name}: Truncated conflicting entries from index {new_index}.")
                    with open(self.LOG_FILE, "w") as f:  # Rewrite log file
                        for log_entry in self.log:
                            f.write(log_entry.to_string() + "\n")

                # Append new entries if beyond current log length
                if new_index >= len(self.log):
                    self.log.append(entry)
                    with open(self.LOG_FILE, "a") as f:
                        f.write(entry.to_string() + "\n")
                    print(f"{self.name}: Appended entry at index {new_index}: {entry.to_string()}")

                new_index += 1

            # Update commit index and apply new entries if needed
            if leader_commit > self.commit_index:
                prev_commit_index = self.commit_index
                self.commit_index = min(leader_commit, len(self.log) - 1)
                if self.commit_index > prev_commit_index:
                    print(f"{self.name}: Updated commit index from {prev_commit_index} to {self.commit_index}")
                    self.apply_entries_to_state_machine()

            return True

        
    def get_log_length(self):
        """Return the length of this node's log."""
        with self.lock:
            return len(self.log)

   
    def check_commit_index(self):
        """Check if a new entry can be committed based on follower match indexes."""
        majority_index = len(self.peers) // 2 + 1
        for i in range(self.commit_index + 1, len(self.log) + 1):
            if sum(1 for match in self.match_index.values() if match >= i) >= majority_index:
                self.commit_index = i
                print(f"Leader {self.name} committed entry at index {self.commit_index}")
                self.apply_entries_to_state_machine()
            else:
                break

    def apply_entries_to_state_machine(self):
        """Apply committed entries to the state machine up to the commit index."""
        for i in range(self.commit_index):
            entry = self.log[i]
            print(f"{self.name} applying entry {i} (term {entry.term}): {entry.command}")





    def submit_value(self, value):
        """Submit a value to the leader; if this node is not the leader, it forwards the request."""
        if self.is_leader_flag:
            # Convert the submitted string value to a LogEntry object for storage
            entry = LogEntry(self.current_term, value)
            self.append_entries(self.current_term, [entry])
            return "Success: Value logged and distributed."
        else:
            for peer, (ip, port) in self.peers.items():
                try:
                    with xmlrpc.client.ServerProxy(f"http://{ip}:{port}/") as client:
                        if client.is_leader():
                            return client.submit_value(value)
                except ConnectionRefusedError:
                    print(f"Connection to {peer} failed.")
            return "Error: No leader available to handle the request."
        


    def run_server(self):
        """Run the XML-RPC server to handle incoming requests.""" 

       
        # with QuietXMLRPCServer((self.ip, self.port), allow_none=True) as server:
        with QuietXMLRPCServer(("0.0.0.0", self.port), allow_none=True) as server:


            server.register_instance(self)
            # server.register_function(self.delete_log_file)
            # print(f"{self.name} is listening on {self.ip}:{self.port}")
            logging.info(f"{self.name} is listening on {self.ip}:{self.port}")
            try:
                while self.running:
                    server.handle_request()
            except KeyboardInterrupt:
                print(f"{self.name} server is shutting down.")    
                self.running = False
            finally:
                print(f"{self.name} has shut down cleanly.")  


    def start_leader(self):
        """Become the leader and start sending heartbeats."""
        self.is_leader_flag = True
        self.votes_received = 0
        self.role = "leader"
        print(f"{self.name} is now the leader.")
        
        # Initialize `next_index` for each follower to the current log length
        # This ensures the leader will start replicating from the latest entry
        self.next_index = {peer: len(self.log) for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}  # Reset matchIndex
        
        # Start the heartbeat mechanism
        threading.Thread(target=self.heartbeat).start()

    def append_entries(self, term, entries):
        """Leader appends entries and attempts replication to followers."""
        if not self.is_leader_flag:
            return False

        # Convert each entry to a LogEntry if they are not already objects
        entries = [LogEntry(term, command) if not isinstance(command, LogEntry) else command for command in entries]

        # Append new entries to leader's log and save to file
        for entry in entries:
            self.log.append(entry)
            with open(self.LOG_FILE, "a") as f:
                f.write(entry.to_string() + "\n")
            print(f"{self.name} appended log entry: {entry.to_string()}")

        # Attempt replication to all followers with simulation check
        for peer, (ip, port) in self.peers.items():
            if self.simulate_replication_failure and peer != "leader":
                print(f"Replication to {peer} skipped due to simulation.")
                continue  # Skip replication to simulate failure

            success = False
            while not success:
                try:
                    # Use `next_index` for determining where to start replication
                    prev_log_index = self.next_index[peer] - 1
                    prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 else 0
                    entries_to_send = self.log[self.next_index[peer]:]

                    if not entries_to_send:
                        # No new entries to replicate
                        break

                    with xmlrpc.client.ServerProxy(f"http://{ip}:{port}/") as client:
                        success = client.receive_append_entries(
                            self.current_term,
                            prev_log_index,
                            prev_log_term,
                            [entry.to_string() for entry in entries_to_send],
                            self.commit_index
                        )

                        if success:
                            # Update matchIndex and nextIndex on success
                            self.match_index[peer] = len(self.log) - 1
                            self.next_index[peer] = len(self.log)
                            print(f"Successfully updated {peer} with {len(entries_to_send)} entries.")
                        else:
                            # Backtrack nextIndex on failure and retry
                            self.next_index[peer] = max(0, self.next_index[peer] - 1)
                            print(f"Backtracking nextIndex for {peer} to {self.next_index[peer]} due to mismatch.")
                            time.sleep(0.1)  # Short delay to prevent tight looping

                except ConnectionRefusedError:
                    print(f"Connection to {peer} failed.")
                    break  # Stop retrying on connection failure

        # Check if entries can be committed after successful replication
        self.check_commit_index()
        return True
    

    def delete_log_file(self):
        """Deletes the log file for this node."""
        try:
            os.remove(self.LOG_FILE)
            logging.info(f"Log file {self.LOG_FILE} deleted successfully.")
            return True
        except FileNotFoundError:
            logging.error("Log file not found.")
            return False
        except Exception as e:
            logging.error(f"Error while deleting log file: {e}")
            return False


# def run_node(clustername, node_name):
#     print(f"Cluster: {clustername}, Node: {node_name}")
#     node = Node(node_name, clustername)
    
#     # Running the server and election logic in separate threads for each node
#     server_thread = threading.Thread(target=node.run_server)
#     election_thread = threading.Thread(target=node.run_election)
    
#     server_thread.start()
#     election_thread.start()
    
#     # Ensure both threads complete before shutting down
#     server_thread.join()
#     election_thread.join()
#     print(f"{node_name} has shut down cleanly.")

# if __name__ == "__main__":
#     clusters = ['clusterA', 'clusterB']
    
#     parser = argparse.ArgumentParser(description="Run Raft Nodes for multiple clusters.")
#     parser.add_argument("clustername", choices=clusters, help="The cluster to use.")
#     args = parser.parse_args()

#     # Ensure that the selected cluster exists in config_data
#     if args.clustername not in config_data:
#         parser.error(f"Invalid cluster: {args.clustername}")

#     # Get the available nodes for the selected cluster
#     available_nodes = list(config_data[args.clustername].keys())

#     # Loop through all the available nodes for the selected cluster and start them
#     threads = []
#     for node_name in available_nodes:
#         # Create a new thread for each node in the selected cluster
#         thread = threading.Thread(target=run_node, args=(args.clustername, node_name))
#         thread.start()
#         threads.append(thread)
    
#     # Wait for all threads to finish
#     for thread in threads:
#         thread.join()

#     print("All nodes have shut down cleanly.")
    
if __name__ == "__main__":
    clusters = ['clusterA', 'clusterB']
    parser = argparse.ArgumentParser(description="Run a Raft Node.")
    parser.add_argument("clustername", choices=clusters, help="The cluster to use.")
    parser.add_argument("node_name", help="The name of the node to run.")
    args = parser.parse_args()

    # Validate node_name is in the selected cluster
    available_nodes = list(config_data[args.clustername].keys())
    if args.node_name not in available_nodes:
        parser.error(f"Invalid node. Available nodes for {args.clustername}: {available_nodes}")

    print(f"Cluster: {args.clustername}, Node: {args.node_name}")
    node = Node(args.node_name, args.clustername)
    
    server_thread = threading.Thread(target=node.run_server)
    server_thread.start()

    election_thread = threading.Thread(target=node.run_election)
    election_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"{args.node_name} shutting down.")
        node.running = False
        server_thread.join()
        election_thread.join()
        print(f"{args.node_name} has shut down cleanly.")